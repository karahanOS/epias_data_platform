"""
ptf_inference.py — XGBoost PTF Hourly Inference Job
====================================================
Cadence : Hourly via Airflow — task_id: run_ptf_inference
Purpose : Load pre-trained model from GCS, pull only the last 168 rows needed
          for lag features, predict next hour's PTF, write result to BigQuery.
Runtime : 3–8 seconds. Scales to any cadence without retraining.
"""

import logging
import joblib
import tempfile
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage, bigquery
from config import GCP_PROJECT_ID as PROJECT_ID, BQ_GOLD_DATASET as DATASET_ID, GCS_BUCKET, get_bq_client
from ptf_features import build_ptf_features

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PTFInference")
MODEL_GCS_PATH  = "models/ptf_xgb_model.joblib"
PREDICTIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_ptf_predictions"

# Minimum lookback for lag-168 + rolling-168 features
LOOKBACK_HOURS = 180

# Turkey is permanently UTC+3 (DST abolished in 2016).
# All staging models (stg_pricing, stg_load_estimation, etc.) use Turkish local
# date/hour as their (date, hour) key.  We apply this offset when converting the
# UTC datetime index from mart_forecasted_residual_load to a Turkish (date, hour)
# key before writing to gold_ptf_predictions.
_TR_UTC_OFFSET = pd.Timedelta(hours=3)


# ── MODEL LOADER ──────────────────────────────────────────────────────────────

def load_model_from_gcs() -> dict:
    """Download model artifact from GCS and deserialize. Fast: ~1 sec."""
    logger.info(f"Loading model from gs://{GCS_BUCKET}/{MODEL_GCS_PATH}...")
    bucket = storage.Client().bucket(GCS_BUCKET)
    with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as tmp:
        bucket.blob(MODEL_GCS_PATH).download_to_filename(tmp.name)
        artifact = joblib.load(tmp.name)
    logger.info("Model loaded.")
    return artifact  # {"model": XGBRegressor, "features": [...]}


# ── FEATURE EXTRACTION ────────────────────────────────────────────────────────

def extract_recent_data() -> pd.DataFrame:
    """Pull only the last LOOKBACK_HOURS rows — not the full history."""
    logger.info(f"Pulling last {LOOKBACK_HOURS} hours from BigQuery...")
    client = get_bq_client()

    # Use `datetime` (hourly TIMESTAMP), not `date` (daily DATE).
    # mart_forecasted_residual_load exposes both; `datetime` is
    # TIMESTAMP_ADD(CAST(date AS TIMESTAMP), INTERVAL hour HOUR) — the
    # same index that ptf_trainer.py sets, so lag/rolling features align.
    # Ordering by `f.datetime` instead of `f.date` is also unambiguous:
    # `ORDER BY f.date` is non-deterministic within the same day (24 rows
    # share the same date value), leading to random row ordering.
    query = f"""
        SELECT
            f.datetime,
            f.ptf_try,
            f.forecasted_residual_load_mwh,
            f.price_independent_bid_mwh,
            s.total_available_capacity_mwh,
            s.total_outage_mwh,
            s.supply_shock_index
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_forecasted_residual_load` f
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.mart_supply_shock_index` s
            ON f.date = s.date
        ORDER BY f.datetime DESC
        LIMIT {LOOKBACK_HOURS}
    """
    df = client.query(query).to_dataframe()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").set_index("datetime")
    logger.info(f"Fetched {len(df)} rows — latest: {df.index.max()}")
    return df


# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

def build_inference_features(df: pd.DataFrame, required_features: list) -> pd.DataFrame:
    """Build the same feature set as training, return only the latest row."""
    df = build_ptf_features(df)

    # Mirror ptf_trainer.py engineer_features(): drop ONLY warm-up NaN rows
    # (first ~168 rows where lag/rolling features are undefined).
    # Do NOT use bare dropna() — optional joined columns such as
    # `forecasted_residual_load_mwh` may be NULL when upstream marts are still
    # being backfilled (e.g. stg_res_forecast pending backfill).  A bare dropna()
    # would wipe the entire DataFrame, causing the downstream iloc[[-1]] crash.
    # XGBoost handles NaN feature values natively, so sparse columns are safe.
    core_cols = [c for c in ["ptf_lag_24h", "ptf_lag_168h", "supply_shock_trend_7d"]
                 if c in df.columns]
    df.dropna(subset=core_cols, inplace=True)

    if df.empty:
        raise RuntimeError(
            f"DataFrame is empty after dropna on {core_cols}. "
            f"Ensure mart_forecasted_residual_load has at least {LOOKBACK_HOURS} hourly rows "
            f"with non-null ptf_try. Check upstream data pipeline status."
        )

    # Return only the most recent row as the inference input
    missing = [f for f in required_features if f not in df.columns]
    if missing:
        logger.warning(f"Required features missing from DataFrame (will be NaN): {missing}")

    latest = df.reindex(columns=required_features).iloc[[-1]]
    logger.info(f"Inference input built for timestamp: {latest.index[0]}")
    return latest


# ── PREDICTION WRITER ─────────────────────────────────────────────────────────

# Schema for gold_ptf_predictions — created on first inference run if absent.
# Columns mirror what the Streamlit dashboard and downstream dbt models expect.
_PREDICTIONS_SCHEMA = [
    bigquery.SchemaField("predicted_date", "DATE",      mode="REQUIRED",
                         description="Calendar date the prediction applies to (TR local date)"),
    bigquery.SchemaField("hour",           "INTEGER",   mode="REQUIRED",
                         description="Hour-of-day [0–23] the prediction applies to"),
    bigquery.SchemaField("predicted_ptf",  "FLOAT64",   mode="REQUIRED",
                         description="XGBoost point forecast for PTF (TRY/MWh)"),
    bigquery.SchemaField("predicted_at",   "TIMESTAMP", mode="REQUIRED",
                         description="UTC wall-clock time the inference job wrote this row"),
]


def _ensure_predictions_table(client: bigquery.Client) -> None:
    """Create gold_ptf_predictions if it does not yet exist.

    Using exists_ok=True means this is idempotent — safe to call every run.
    The table is partitioned by predicted_date so the dashboard can query a
    rolling window cheaply without scanning the full history.
    """
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_ref   = dataset_ref.table("gold_ptf_predictions")
    table       = bigquery.Table(table_ref, schema=_PREDICTIONS_SCHEMA)

    # Partition by predicted_date; expire nothing (keep full history).
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="predicted_date",
    )
    table.clustering_fields = ["hour"]

    created = client.create_table(table, exists_ok=True)
    if created.created is not None:
        # Newly created — log so the operator knows the table was provisioned
        logger.info(f"✅ Created BigQuery table: {PREDICTIONS_TABLE}")
    else:
        logger.debug(f"Table already exists: {PREDICTIONS_TABLE}")


def write_prediction_to_bq(predicted_date: pd.Timestamp, predicted_ptf: float) -> None:
    """Idempotent upsert of a single prediction row to BigQuery.

    Uses a DML MERGE statement so that Airflow task retries cannot create
    duplicate rows.  The natural key is (predicted_date, hour): if a row
    already exists for this period, its predicted_ptf and predicted_at are
    updated in place rather than a second row being appended.

    DML jobs run on the BigQuery query engine (not the streaming API), so they
    are not subject to the streaming-insert propagation delay that previously
    caused 404 errors immediately after table creation.

    Key convention — Turkish local time (UTC+3):
        mart_forecasted_residual_load.datetime is a *UTC* TIMESTAMP.  All staging
        models use Turkish local date/hour as their natural key (stg_pricing,
        stg_load_estimation, etc.).  We therefore add _TR_UTC_OFFSET to convert the
        UTC index to Turkish local before computing predicted_date / hour, so that
        gold_ptf_predictions keys can be directly joined with stg_pricing in the
        backtesting section of the dashboard.
    """
    client = get_bq_client()

    # Auto-create the table on first inference run — no manual DDL required.
    _ensure_predictions_table(client)

    # Convert UTC timestamp to Turkish local for storage
    ts_utc  = predicted_date.tz_localize(None) if predicted_date.tzinfo is None else \
              predicted_date.tz_convert("UTC").tz_localize(None)
    ts_tr   = ts_utc + _TR_UTC_OFFSET          # naive datetime in Turkish local time
    date_str = ts_tr.strftime("%Y-%m-%d")
    hour     = int(ts_tr.hour)

    ptf_rounded  = round(float(predicted_ptf), 4)
    predicted_at = datetime.now(timezone.utc).isoformat()

    # MERGE upsert: INSERT if (predicted_date, hour) is new; UPDATE if it
    # already exists (idempotent — safe on any number of Airflow retries).
    merge_sql = f"""
        MERGE `{PREDICTIONS_TABLE}` T
        USING (
            SELECT
                DATE '{date_str}'                 AS predicted_date,
                {int(hour)}                        AS hour,
                {ptf_rounded}                      AS predicted_ptf,
                TIMESTAMP '{predicted_at}'         AS predicted_at
        ) S
        ON T.predicted_date = S.predicted_date AND T.hour = S.hour
        WHEN MATCHED THEN
            UPDATE SET
                predicted_ptf = S.predicted_ptf,
                predicted_at  = S.predicted_at
        WHEN NOT MATCHED THEN
            INSERT (predicted_date, hour, predicted_ptf, predicted_at)
            VALUES (S.predicted_date, S.hour, S.predicted_ptf, S.predicted_at)
    """

    job = client.query(merge_sql)
    job.result()   # blocks until the DML job completes

    logger.info(f"✅ Prediction upserted — {date_str} hour={hour} PTF={predicted_ptf:.2f} TRY")


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

def run():
    artifact          = load_model_from_gcs()
    model             = artifact["model"]
    required_features = artifact["features"]

    df_recent         = extract_recent_data()
    X_latest          = build_inference_features(df_recent, required_features)

    predicted_ptf     = model.predict(X_latest)[0]
    predicted_ts      = X_latest.index[0]   # UTC TIMESTAMP from mart_forecasted_residual_load

    # write_prediction_to_bq converts predicted_ts (UTC) → Turkish local internally
    # before computing predicted_date / hour stored in gold_ptf_predictions.
    write_prediction_to_bq(
        predicted_date=predicted_ts,
        predicted_ptf=predicted_ptf,
    )
    logger.info("🏁 Inference job complete.")


if __name__ == "__main__":
    run()
