"""
EPIAS Medallion Pipeline v3
============================
Mimari  : Bronze (raw GCS) → Silver (Spark/parquet) → BigQuery (External) → Gold (dbt) → ML (XGBoost)
Zamanlama: Her gün 05:00 UTC (08:00 TR)

Temizlenmiş, optimize edilmiş ve Predictive Analytics için güncellenmiş üretim hattı.
"""
from __future__ import annotations
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, Tuple

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# Silver layer runs on Dataproc Serverless (ADR-0002), consolidated into
# DATAPROC_POOL_SIZE batches instead of one per source (ADR-0003) — see
# epias_sources.group_sources / make_silver_batch_task.
from epias_sources import (
    EPIAS_SOURCES,
    DBT_EXCLUDE_PENDING_BACKFILL,
    group_sources,
    make_silver_batch_task,
)

# ── MODÜL YOLU ────────────────────────────────────────────────────────────────
sys.path.insert(0, "/opt/airflow/src")
try:
    from epias_client import EPIASClient
    from weather_client import WeatherClient
    from fx_client import FXClient
except ImportError as exc:
    logging.error(f"Modül yükleme hatası: {exc}")

logger = logging.getLogger(__name__)

# ── AYARLAR ───────────────────────────────────────────────────────────────────
BUCKET_NAME   = "epias-data-lake"

# ── VERİ GECİKME TABLOSU ─────────────────────────────────────────────────────
DATA_DELAYS: Dict[str, int] = {
    "get_ptf_smf_sdf":                  0,
    # smf/idm_transaction_history/outages: EPIAS API rejects endDate >= today
    # for these three ("endDate must be in the past"). Under the daily
    # schedule this never surfaced (ds was effectively yesterday by the time
    # the DAG ran). Under hourly scheduling ds is always "today" intraday, so
    # these three failed on every single hourly run once the hourly schedule
    # went live (2026-07-24) — cascading into silver_batch failures and
    # blocking load_silver_to_bigquery/run_dbt_gold_models for the whole
    # pipeline. Fix: delay=1, same pattern as get_injection_quantity.
    "get_smf":                          1,
    "get_supply_demand":                0,
    "get_dam_clearing_quantity":        0,
    "get_price_independent_bid":        0,
    "get_idm_transaction_history":      1,
    "get_order_summary_up":             0,
    "get_order_summary_down":           0,
    "get_system_direction":             0,
    "get_dpp":                          0,
    "get_injection_quantity":           1,
    "get_aic":                          0,
    "get_imbalance_quantity":           0,
    "get_res_generation_and_forecast":  0,
    "get_licensed_realtime_generation": 0,
    "get_load_estimation_plan":         0,
    "get_unlicensed_generation":        35,
    "get_uevcb_list":                   0,
    "get_outages":                      1,
    "get_dams":                         0,
    # get_interim_mcp: negative delay = LEAD. K.PTF (itiraz-öncesi PTF)
    # teslim gününden ~1 gün önce, GÖP açık artırması kapanır kapanmaz
    # mevcut oluyor — final PTF'nin aksine, teslim gününün kendi 14:00'ini
    # beklemiyor. -1 ile her saatlik run "ds+1" (yarın) için K.PTF ister;
    # açık artırma henüz kapanmadıysa allow_empty=True zaten boş dönüşü
    # sorunsuz karşılıyor (pricing'in "veri saat 14 öncesinde mevcut değil"
    # davranışıyla aynı mekanizma).
    "get_interim_mcp":                  -1,
}

NO_DATE_METHODS: frozenset = frozenset({
    "get_market_participants",
    
})

# ── HATA YÖNETİMİ ─────────────────────────────────────────────────────────────
def notify_failure(context: dict) -> None:
    ti = context["task_instance"]
    logger.error(
        "🚨 TASK HATASI | dag=%s | task=%s | tarih=%s | hata=%s",
        ti.dag_id, ti.task_id, context.get("ds"), context.get("exception"),
    )

# ── CALLABLE'LAR ──────────────────────────────────────────────────────────────
def get_epias_data_callable(method_name: str, **context) -> list:
    client = EPIASClient()
    ds     = context["ds"]
    delay  = DATA_DELAYS.get(method_name, 0)

    # delay=0 -> target=ds (unchanged). delay>0 -> look back N days (unchanged).
    # delay<0 -> look AHEAD |delay| days (lead) — e.g. get_interim_mcp's -1
    # fetches ds+1 ("tomorrow"), since K.PTF for tomorrow is already
    # available today once the day-ahead auction clears (~14:00 TRT).
    target = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=delay)).strftime("%Y-%m-%d")

    method = getattr(client, method_name)
    result = method() if method_name in NO_DATE_METHODS else method(target, target)
    return result

def get_weather_data_callable(**context) -> list:
    client = WeatherClient()
    ds     = context["ds"]
    rows: list = []

    for city in ("istanbul", "izmir", "ankara", "konya"):
        try:
            city_df = client.get_weather_for_city(city, ds, ds)
            if hasattr(city_df, "to_dict"):
                for col in city_df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]", "datetimetz"]).columns:
                    city_df[col] = city_df[col].astype(str)
                records = city_df.to_dict(orient="records")
            elif isinstance(city_df, list):
                records = city_df
            else:
                records = [city_df]
            rows.extend(records)
        except Exception as exc:
            logger.error("weather/%s hatası: %s", city, exc)
    return rows

def get_fx_data_callable(**context) -> list:
    client = FXClient()
    ds = context["ds"]
    # T-1: TCMB ~15:30 TR'de yayınlar, DAG 08:00 TR'de çalışır — bir önceki günün kuru alınır
    target = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    return client.get_usdtry(target)

def save_to_gcs_callable(task_id: str, bucket_path: str, allow_empty: bool = False, **context) -> None:
    data = context["ti"].xcom_pull(task_ids=task_id)

    if not data:
        if allow_empty:
            return
        raise ValueError(f"🚨 {task_id} task'ından veri gelmedi, GCS'e yazılamıyor!")

    if isinstance(data, list) and data and isinstance(data[0], list):
        flat: list = []
        for sub in data:
            flat.extend(sub)
    else:
        flat = data

    ds       = context["ds"]
    gcs_path = f"gs://{BUCKET_NAME}/{bucket_path}/{ds}.parquet"

    df = pd.DataFrame(flat)

    # Coerce integer columns to float64 so every daily Bronze file uses DOUBLE
    # physical type in Parquet.  Without this, pandas infers int64 when all API
    # values for a column happen to be round numbers (e.g. downRegulationOneCoded=0
    # or 1 all day).  Spark / BigQuery external tables registered as DOUBLE then
    # refuse to read those partitions — same fix already present in backfill_chunk.
    int_cols = df.select_dtypes(include=["int32", "int64"]).columns.tolist()
    if int_cols:
        df[int_cols] = df[int_cols].astype("float64")

    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].astype(str)

    df.to_parquet(gcs_path, index=False)

# ── DAG ───────────────────────────────────────────────────────────────────────
# ADR-0004 (2026-07-27): on_failure_callback only ever logged an error line --
# nobody was actually notified unless they opened the Airflow UI. email_on_failure
# uses Airflow's built-in SMTP support (see docker-compose.yml's AIRFLOW__SMTP__*
# env vars) to send a real email on any task failure, at zero added cost.
default_args = {
    "owner":               "epias_team",
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "email_on_failure":    True,
    "email":               ["mehmetkarahanc@gmail.com"],
}

with DAG(
    dag_id="epias_medallion_pipeline_v3",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    # Hourly (ADR-0002 action item 9 + ADR-0003): EPIAS publishes most sources
    # hour-by-hour, so re-fetching "today" (ds stays constant intra-day under
    # Airflow's hourly data-interval semantics) each hour naturally picks up
    # newly-published hours as the day progresses — Silver's overwrite mode
    # reflects the growing day. Made viable by ADR-0003's batch consolidation
    # (~4 min Silver layer instead of ~40 min) fitting well inside the hour.
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    tags=["epias", "medallion", "dbt", "ml"],
) as dag:

    # =========================================================================
    # BRONZE LAYER (Sadece Yeni Mimarideki Aktif Kaynaklar)
    # =========================================================================
    # v[4] = daily_eligible; v[:3] = (method_name, gcs_path, allow_empty)
    ALL_SOURCES = {k: v[:3] for k, v in EPIAS_SOURCES.items() if v[4]}

    # Hava Durumu Özel Akış
    get_weather = PythonOperator(task_id="get_weather", python_callable=get_weather_data_callable)
    save_weather = PythonOperator(
        task_id="save_weather_to_gcs",
        python_callable=save_to_gcs_callable,
        op_kwargs={"task_id": "get_weather", "bucket_path": "bronze/weather", "allow_empty": False},
    )
    get_weather >> save_weather

    # Döviz Kuru Özel Akışı (TCMB EVDS)
    get_fx = PythonOperator(task_id="get_fx_rates", python_callable=get_fx_data_callable)
    save_fx = PythonOperator(
        task_id="save_fx_to_gcs",
        python_callable=save_to_gcs_callable,
        op_kwargs={"task_id": "get_fx_rates", "bucket_path": "bronze/fx_rates", "allow_empty": False},
    )
    get_fx >> save_fx

    bronze_save_tasks: Dict[str, PythonOperator] = {}

    for key, (method, path, allow_empty) in ALL_SOURCES.items():
        get_t = PythonOperator(
            task_id=f"get_{key}",
            python_callable=get_epias_data_callable,
            op_kwargs={"method_name": method},
        )
        save_t = PythonOperator(
            task_id=f"save_{key}_to_gcs",
            python_callable=save_to_gcs_callable,
            op_kwargs={"task_id": f"get_{key}", "bucket_path": path, "allow_empty": allow_empty},
        )
        get_t >> save_t
        bronze_save_tasks[key] = save_t

    # =========================================================================
    # SILVER LAYER (Dataproc Serverless, consolidated — ADR-0002 + ADR-0003)
    # =========================================================================
    # All daily sources (incl. weather/fx, which have their own bronze flow but
    # the same bronze_to_silver_<key>.py job pattern) are grouped into
    # DATAPROC_POOL_SIZE batches — one shared Spark session per group instead
    # of one Dataproc batch per source — to amortize cold start (~3-4 min)
    # across many sources rather than paying it ~24 times. See ADR-0003.
    all_silver_keys = list(ALL_SOURCES.keys()) + ["weather", "fx_rates"]
    bronze_dep_tasks = {**bronze_save_tasks, "weather": save_weather, "fx_rates": save_fx}

    silver_batch_tasks = []
    for i, group in enumerate(group_sources(all_silver_keys)):
        batch_t = make_silver_batch_task(f"silver_batch_{i}", group, ["{{ ds }}"])
        for key in group:
            bronze_dep_tasks[key] >> batch_t
        silver_batch_tasks.append(batch_t)

    # =========================================================================
    # BIGQUERY BRIDGE & DBT (GOLD) & ML
    # =========================================================================
    
    load_to_bq = BashOperator(
        task_id='load_silver_to_bigquery',
        bash_command='python /opt/airflow/src/load_to_bigquery.py',
    )

    # ADR-0004 (2026-07-27): dbt run never ran dbt test in production -- the
    # 2026-07-25/26 Silver dedup investigation found ~12 tables silently
    # duplicated for months because schema.yml's tests were never actually
    # executed. A separate run_dbt_tests task closed the *detection* gap
    # (failures become visible within the hour) but not the *prevention* gap
    # -- dbt run had already written the (possibly bad) data before dbt test
    # got a chance to catch it. `dbt build` interleaves each model with its
    # own tests in dependency order; `--fail-fast` aborts the rest of the run
    # on the first real failure, so a bad stg_pricing row (say) now stops
    # mart_ml_features/mart_ptf_lag_features from building on top of it that
    # hour, instead of the bad data quietly propagating downstream. The 2
    # known pre-existing, non-duplication test failures (assert_no_hourly_gaps,
    # mart_gop_volume_analysis's ptf_try not_null -- both reflect the old
    # Faz-0 laptop-uptime gap) are set to severity=warn in schema.yml /
    # assert_no_hourly_gaps.sql specifically so they don't trip --fail-fast.
    run_dbt = BashOperator(
        task_id='run_dbt_gold_models',
        bash_command=(
            'cd /opt/airflow/epias_dbt && dbt build --fail-fast --profiles-dir . --target prod '
            '--exclude ' + ' '.join(DBT_EXCLUDE_PENDING_BACKFILL)
        ),
    )

    # Training (haftalık) ve inference (saatlik) artık bu DAG'da değil — kendi
    # cadence'lerine sahip ayrı DAG'lar: epias_ptf_training_weekly.py ve
    # epias_ptf_inference_hourly.py. Bu DAG artık saatlik çalıştığı için
    # (yukarıdaki schedule_interval="0 * * * *") ağır XGBoost training'i
    # (10-30 dk) burada tutmak onu günde 24 kez çalıştırırdı — yorumun kendi
    # belirttiği "haftada bir" niyetiyle çelişirdi. Inference zaten bağımsız
    # olarak "son 180 satır + önceden eğitilmiş model" ile çalıştığı için bu
    # DAG'ın dbt run'ını beklemesine gerek yok, gold tablo her ikisi için de
    # ortak veri kaynağı.

    # Zinciri Bağlama
    for batch_t in silver_batch_tasks:
        batch_t >> load_to_bq
    load_to_bq >> run_dbt