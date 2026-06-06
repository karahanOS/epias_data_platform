"""
epias_backfill_dag.py — One-Time Historical Backfill DAG
=========================================================
Purpose  : Ingest EPIAS data from BACKFILL_START_DATE to today in weekly chunks.
           Run ONCE manually via Airflow UI (do not schedule).

Design   :
  - Processes data WEEK_CHUNK_DAYS days at a time to stay within API limits.
  - max_active_tasks=2 prevents parallel API hammering (EPIAS rate limit: 80 req/min).
  - Each chunk saves directly to GCS Bronze as parquet.
  - After all chunks complete, a single dbt run builds the full Gold layer.
  - Model training runs ONCE at the end on the full dataset.

Usage    :
  1. Set BACKFILL_START_DATE below (or as Airflow Variable).
  2. Trigger manually: Airflow UI → epias_historical_backfill → Trigger DAG
  3. After completion, disable this DAG — it is not meant to run again.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from epias_sources import EPIAS_SOURCES, DBT_EXCLUDE_PENDING_BACKFILL, SPARK_CONN_ID, make_silver_task

sys.path.insert(0, "/opt/airflow/src")
try:
    from epias_client import EPIASClient
except ImportError as exc:
    logging.error(f"Modül yükleme hatası: {exc}")

logger = logging.getLogger(__name__)

# ── BACKFILL PARAMETERS ───────────────────────────────────────────────────────
# Adjust via Airflow Variable "backfill_start_date" or hardcode below.
BACKFILL_START_DATE = Variable.get("backfill_start_date", default_var="2025-01-01")
BACKFILL_END_DATE   = datetime.utcnow().strftime("%Y-%m-%d")
WEEK_CHUNK_DAYS     = 7      # Process 1 week at a time per task
BUCKET_NAME         = "epias-data-lake"

# ── SOURCES TO BACKFILL ───────────────────────────────────────────────────────
# Only include sources where historical data is meaningful.
# Excluded: get_market_participants (static, no date), get_uevcb_list (slow bulk)
# v[3] = backfill_eligible; v[:3] = (method_name, gcs_path, allow_empty)
BACKFILL_SOURCES = {k: v[:3] for k, v in EPIAS_SOURCES.items() if v[3]}

# ── CHUNK GENERATOR ───────────────────────────────────────────────────────────

def _generate_chunks(start: str, end: str, chunk_days: int) -> list[tuple[str, str]]:
    """Split a date range into (start, end) tuples of chunk_days each."""
    chunks = []
    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt  = datetime.strptime(end,   "%Y-%m-%d")
    while current <= end_dt:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_dt)
        chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = chunk_end + timedelta(days=1)
    return chunks


# ── CALLABLE: BACKFILL ONE CHUNK FOR ONE SOURCE ───────────────────────────────

def backfill_chunk(method_name: str, bucket_path: str,
                   chunk_start: str, chunk_end: str,
                   allow_empty: bool = False, **context) -> None:
    """
    Fetch one week of data for one source and write to GCS.
    File path: gs://bucket/bronze/<source>/<chunk_start>_<chunk_end>.parquet
    This avoids overwriting the daily files already written by the main pipeline.
    """
    import time
    client = EPIASClient()
    method = getattr(client, method_name)

    logger.info(f"Backfilling {method_name} | {chunk_start} → {chunk_end}")

    try:
        data = method(chunk_start, chunk_end)
    except Exception as exc:
        if allow_empty:
            logger.warning(f"Skipping {method_name} ({chunk_start}→{chunk_end}): {exc}")
            return
        raise

    if not data:
        if allow_empty:
            logger.warning(f"Empty response for {method_name} ({chunk_start}→{chunk_end}) — skipping.")
            return
        raise ValueError(f"No data returned for {method_name} ({chunk_start}→{chunk_end})")

    df = pd.DataFrame(data)

    # Coerce integer columns to float64 so all weekly chunks share DOUBLE physical type
    # in Parquet. Without this, pandas infers int64 when API values happen to be round
    # numbers, creating INT64 columns that Spark cannot coerce to DOUBLE at read time.
    int_cols = df.select_dtypes(include=["int32", "int64"]).columns.tolist()
    if int_cols:
        df[int_cols] = df[int_cols].astype("float64")

    # Write one parquet per chunk — never overwrites daily operational files
    gcs_path = f"gs://{BUCKET_NAME}/{bucket_path}/backfill_{chunk_start}_{chunk_end}.parquet"
    df.to_parquet(gcs_path, index=False)
    logger.info(f"✅ Written {len(df):,} rows → {gcs_path}")

    # Polite delay between chunks to respect EPIAS rate limit (80 req/min)
    time.sleep(1)


def backfill_weather_chunk(chunk_start: str, chunk_end: str, **context) -> None:
    """
    Fetch historical weather data for all 4 cities and write to GCS.
    Uses open-meteo archive API — no rate limit, no auth required.
    Saved per-city so stg_weather (unique_key=[date, hour, city_name]) can ingest correctly.
    """
    try:
        from weather_client import WeatherClient, CITIES
    except ImportError as exc:
        logger.error(f"WeatherClient import failed: {exc}")
        return

    client = WeatherClient()
    all_rows = []

    for city_name in CITIES:
        try:
            df = client.get_weather_for_city(city_name, chunk_start, chunk_end)
            # Rename to match silver schema
            df = df.rename(columns={
                "datetime":           "date",
                "city":               "city",
                "temperature_2m":     "temperature_2m",
                "wind_speed_10m":     "wind_speed_10m",
                "shortwave_radiation":"shortwave_radiation",
                "relative_humidity_2m":"relative_humidity_2m",
            })
            all_rows.append(df)
            logger.info(f"✅ Weather {city_name} | {chunk_start}→{chunk_end} | {len(df)} rows")
        except Exception as exc:
            logger.warning(f"Weather {city_name} ({chunk_start}→{chunk_end}) skipped: {exc}")

    if not all_rows:
        logger.warning(f"No weather data for {chunk_start}→{chunk_end}")
        return

    import pandas as pd
    combined = pd.concat(all_rows, ignore_index=True)
    gcs_path = f"gs://{BUCKET_NAME}/bronze/weather/backfill_{chunk_start}_{chunk_end}.parquet"
    combined.to_parquet(gcs_path, index=False)
    logger.info(f"✅ Written {len(combined):,} weather rows → {gcs_path}")


# ── DAG DEFINITION ────────────────────────────────────────────────────────────

default_args = {
    "owner":       "epias_team",
    "retries":     3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="epias_historical_backfill",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # MANUAL TRIGGER ONLY — never scheduled
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,        # 2 Bronze API calls + 2 Spark Silver jobs in parallel
    tags=["epias", "backfill", "one-time"],
) as dag:

    chunks = _generate_chunks(BACKFILL_START_DATE, BACKFILL_END_DATE, WEEK_CHUNK_DAYS)
    logger.info(
        f"Backfill plan: {len(chunks)} weekly chunks × {len(BACKFILL_SOURCES)} sources "
        f"= {len(chunks) * len(BACKFILL_SOURCES)} Bronze tasks"
    )

    # =========================================================================
    # PHASE 1 — BRONZE: Fetch all weekly chunks from EPIAS API → GCS
    # Per-source chunks run sequentially; sources run in parallel (throttled)
    # =========================================================================
    bronze_last_tasks: dict[str, PythonOperator] = {}

    for source_key, (method_name, bucket_path, allow_empty) in BACKFILL_SOURCES.items():
        prev_task = None

        for chunk_start, chunk_end in chunks:
            task = PythonOperator(
                task_id=f"bronze_{source_key}_{chunk_start}",
                python_callable=backfill_chunk,
                op_kwargs={
                    "method_name":  method_name,
                    "bucket_path":  bucket_path,
                    "chunk_start":  chunk_start,
                    "chunk_end":    chunk_end,
                    "allow_empty":  allow_empty,
                },
            )
            if prev_task is not None:
                prev_task >> task
            prev_task = task

        bronze_last_tasks[source_key] = prev_task   # last chunk task per source

    # =========================================================================
    # PHASE 2 — SILVER: Run Spark job per source once ALL its Bronze chunks land
    # --backfill flag → BaseEpiasSparkJob reads backfill_*.parquet + appends Silver
    # =========================================================================
    silver_tasks: dict[str, SparkSubmitOperator] = {}

    for source_key in BACKFILL_SOURCES:
        silver_t = make_silver_task(dag, source_key, is_backfill=True)
        # Bronze last chunk → Silver Spark job (per source, independent of other sources)
        bronze_last_tasks[source_key] >> silver_t
        silver_tasks[source_key] = silver_t

    # =========================================================================
    # PHASE 2b — WEATHER BACKFILL: open-meteo archive (no EPIAS rate limit)
    # Runs in parallel with EPIAS silver jobs.  Each chunk is independent.
    # =========================================================================
    weather_last_task = None
    for chunk_start, chunk_end in chunks:
        wt = PythonOperator(
            task_id=f"bronze_weather_{chunk_start}",
            python_callable=backfill_weather_chunk,
            op_kwargs={"chunk_start": chunk_start, "chunk_end": chunk_end},
        )
        if weather_last_task is not None:
            weather_last_task >> wt
        weather_last_task = wt

    silver_weather_backfill = SparkSubmitOperator(
        task_id="silver_weather_backfill",
        application="/opt/airflow/spark/bronze_to_silver_weather.py",
        py_files="/opt/airflow/spark/spark_utils.py",
        jars="/opt/spark/jars/gcs-connector.jar",
        conn_id=SPARK_CONN_ID,
        application_args=["1970-01-01", "--backfill"],
        deploy_mode="client",
        name="epias_silver_weather_backfill",
    )
    weather_last_task >> silver_weather_backfill

    # =========================================================================
    # PHASE 3 — BQ BRIDGE: Register / refresh Silver external tables in BigQuery
    # Runs once after ALL Silver jobs (EPIAS + weather) complete
    # =========================================================================
    register_bq_tables = BashOperator(
        task_id="register_silver_external_tables",
        bash_command="python /opt/airflow/src/load_to_bigquery.py",
    )

    for silver_t in silver_tasks.values():
        silver_t >> register_bq_tables
    silver_weather_backfill >> register_bq_tables

    # =========================================================================
    # PHASE 4 — GOLD: dbt full-refresh rebuilds all mart tables from scratch
    # =========================================================================
    run_dbt_backfill = BashOperator(
        task_id="run_dbt_full_refresh",
        bash_command=(
            "cd /opt/airflow/epias_dbt && dbt run --profiles-dir . --full-refresh "
            "--exclude " + " ".join(DBT_EXCLUDE_PENDING_BACKFILL)
        ),
    )

    # =========================================================================
    # PHASE 5 — ML: Train model ONCE on the now-complete historical dataset
    # =========================================================================
    train_initial_model = BashOperator(
        task_id="train_initial_model",
        bash_command="python /opt/airflow/src/ptf_trainer.py",
    )

    register_bq_tables >> run_dbt_backfill >> train_initial_model
