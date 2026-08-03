"""
ptf_inference.py — XGBoost PTF Hourly Inference Job
====================================================
Cadence : Hourly via Airflow — task_id: run_ptf_inference
Purpose : Load pre-trained model from GCS, pull only the last LOOKBACK_HOURS
          rows needed for lag features, predict every hour that has become
          newly available since the last write, write results to BigQuery.
Runtime : A few seconds per predicted hour. Scales to any cadence without retraining.

Note on cadence: mart_ptf_lag_features only gains new *settled* rows once a
day (EPİAŞ publishes GÖP/PTF for the whole next day in a single batch around
14:00 TRT — see epias_sources.py's allow_empty comments) even though this job
runs hourly. Predicting only the single latest row (the old behavior) meant
23 of every 24 runs re-predicted the same hour the moment a new day's batch
landed, and the backtesting chart only ever accumulated one point per day.
This version instead predicts every new row since the last write — so when a
day's worth of hours lands at once, all of them get written in one run.
"""

import logging
import joblib
import tempfile
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
from config import GCP_PROJECT_ID as PROJECT_ID, BQ_GOLD_DATASET as DATASET_ID, GCS_BUCKET, get_bq_client
from ptf_features import build_ptf_features, FEATURE_COLS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PTFInference")
MODEL_GCS_PATH  = "models/ptf_xgb_model.joblib"
PREDICTIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_ptf_predictions"

# Separate table for genuinely forward-looking predictions (hours with no
# settled PTF yet at all — see run_forward_forecast() / mart_ptf_forward_features).
# Kept apart from PREDICTIONS_TABLE so the backtest-oriented dashboard page
# never accidentally mixes "predicted a price we already knew" rows (used to
# measure model accuracy) with genuine before-the-fact forecasts (used for
# decision support, e.g. the Vardiya Optimizasyonu page).
FORWARD_PREDICTIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_ptf_forward_predictions"

# Lookback for lag-168 + rolling-168 features, plus a full day's worth of
# margin (up to 24 rows can be newly-predicted in one run — see module
# docstring) so even the oldest row in a full-day batch still has 168 true
# preceding rows for its rolling-168 features.
LOOKBACK_HOURS = 204

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
    """Pull only the last LOOKBACK_HOURS rows from mart_ptf_lag_features.

    Uses the same Gold table as the trainer so feature columns match exactly.
    LIMIT + DESC ordering fetches only the tail of history — fast for inference.
    """
    logger.info(f"Pulling last {LOOKBACK_HOURS} hours from mart_ptf_lag_features...")
    client = get_bq_client()

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_ptf_lag_features`
        WHERE date IS NOT NULL AND ptf_try IS NOT NULL
        ORDER BY date DESC, hour DESC
        LIMIT {LOOKBACK_HOURS}
    """
    df = client.query(query).to_dataframe()

    # Build datetime index (Turkish local, UTC+3)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_localize(None)
    else:
        df["datetime"] = (
            pd.to_datetime(df["date"].astype(str))
            + pd.to_timedelta(df["hour"], unit="h")
        )

    df = df.sort_values("datetime").set_index("datetime")
    logger.info(f"Fetched {len(df)} rows — latest: {df.index.max()}")
    return df


def extract_forward_features() -> pd.DataFrame:
    """Pull genuinely future (not-yet-settled) rows from
    mart_ptf_forward_features — hours where stg_pricing has no matching
    (date, hour) yet, i.e. EPİAŞ hasn't published/cleared GÖP/PTF for them.

    Unlike extract_recent_data() (which requires ptf_try IS NOT NULL, and so
    can only ever "predict" hours whose true price is already known — see
    2026-08-03 investigation), this is the actual before-the-fact forecast
    path: features here come only from day-ahead-available sources (LEP
    load forecast, lagged settled prices, etc. — see
    mart_ptf_forward_features.sql's own docstring for the full discipline).
    """
    logger.info("Pulling not-yet-settled rows from mart_ptf_forward_features...")
    client = get_bq_client()

    # date >= CURRENT_DATE('Asia/Istanbul') matters: an anti-join against
    # stg_pricing alone can't distinguish "genuinely hasn't happened yet"
    # from "a past date stg_pricing happens to be missing" (a data gap, not
    # a forecast target) — confirmed 2026-08-03 via a dry run that pulled in
    # stray historical gap rows (e.g. 2024-12-31, 2026-07-25/26) alongside
    # the real future day. Restricting to today-or-later excludes those.
    query = f"""
        SELECT f.*
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_ptf_forward_features` f
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.stg_pricing` p
          ON p.date = f.date AND p.hour = f.hour
        WHERE p.ptf_try IS NULL
          AND f.date >= CURRENT_DATE('Asia/Istanbul')
        ORDER BY f.date, f.hour
    """
    df = client.query(query).to_dataframe()

    if df.empty:
        logger.info("No genuinely future rows available yet.")
        return df

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_localize(None)
    else:
        df["datetime"] = (
            pd.to_datetime(df["date"].astype(str))
            + pd.to_timedelta(df["hour"], unit="h")
        )
    df = df.sort_values("datetime").set_index("datetime")
    logger.info(f"Fetched {len(df)} genuinely future row(s): "
                f"{df.index.min()} → {df.index.max()}")
    return df


# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

def build_inference_features(
    df: pd.DataFrame, required_features: list, since: pd.Timestamp = None
) -> pd.DataFrame:
    """Build the same feature set as training.

    Returns every row strictly newer than `since` (all rows if `since` is
    None) — one row per hour that hasn't been predicted yet, not just the
    single latest one.  See module docstring for why this matters.
    """
    df = build_ptf_features(df)

    # Mirror ptf_trainer.py engineer_features(): drop ONLY warm-up NaN rows
    # (first ~168 rows where lag/rolling features are undefined).
    # Do NOT use bare dropna() — optional joined columns such as
    # `forecasted_residual_load_mwh` may be NULL when upstream marts are still
    # being backfilled (e.g. stg_res_forecast pending backfill).  A bare dropna()
    # would wipe the entire DataFrame, causing the downstream iloc[[-1]] crash.
    core_cols = [c for c in ["ptf_lag_24h", "ptf_lag_168h"]
                 if c in df.columns]
    df.dropna(subset=core_cols, inplace=True)

    if df.empty:
        raise RuntimeError(
            f"DataFrame is empty after dropna on {core_cols}. "
            f"Ensure mart_forecasted_residual_load has at least {LOOKBACK_HOURS} hourly rows "
            f"with non-null ptf_try. Check upstream data pipeline status."
        )

    if since is not None:
        df = df[df.index > since]

    batch = _cast_to_model_input(df, required_features)
    if batch.empty:
        logger.info("No new timestamps since the last prediction — nothing to do.")
    else:
        logger.info(
            f"Inference input built for {len(batch)} timestamp(s): "
            f"{batch.index.min()} → {batch.index.max()}"
        )
    return batch


def _cast_to_model_input(df: pd.DataFrame, required_features: list) -> pd.DataFrame:
    """Cast + fill exactly like ptf_trainer.py's _to_float() does for X_train/
    X_test. This is NOT optional: the model was trained on data where every
    missing value was pd.to_numeric()'d then filled with 0 (not left as
    genuine NaN). XGBoost happily accepts raw NaN as "missing" and picks a
    learned default split direction for it — but that split direction was
    fit assuming missing == 0, not fit on genuine missingness. Passing raw
    NaN at inference silently uses the wrong branch and produces
    substantially different (and visibly worse) predictions than passing
    the same 0-filled values the model was actually trained on. Confirmed
    2026-08-03 via BigQuery time-travel: skipping this step reproduced
    gold_ptf_predictions' actual (badly under-predicting) stored values
    almost exactly; restoring it moved predictions ~1,000-1,800 TL/MWh
    closer to the realized price. Shared by both the backtest-oriented
    (build_inference_features) and forward-looking
    (build_forward_inference_features) paths — same model, same requirement.
    """
    missing = [f for f in required_features if f not in df.columns]
    if missing:
        logger.warning(f"Required features missing from DataFrame (will be NaN): {missing}")
    return (df.reindex(columns=required_features)
              .apply(pd.to_numeric, errors="coerce")
              .fillna(0)
              .astype("float64"))


def build_forward_inference_features(df: pd.DataFrame, required_features: list) -> pd.DataFrame:
    """Build model input for genuinely future (not-yet-settled) hours from
    mart_ptf_forward_features. No dropna/since filtering here — unlike the
    backtest path, every row extract_forward_features() returns is already
    guaranteed genuinely future (anti-joined against stg_pricing), and we
    always want to (re-)predict all of them since day-ahead-available inputs
    like LEP can get revised between runs.
    """
    df = build_ptf_features(df)
    batch = _cast_to_model_input(df, required_features)
    if batch.empty:
        logger.info("No genuinely future rows to predict.")
    else:
        logger.info(
            f"Forward inference input built for {len(batch)} timestamp(s): "
            f"{batch.index.min()} → {batch.index.max()}"
        )
    return batch


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


def _ensure_predictions_table(client: bigquery.Client, table_id: str) -> None:
    """Create the given predictions table if it does not yet exist.

    Using exists_ok=True means this is idempotent — safe to call every run.
    The table is partitioned by predicted_date so the dashboard can query a
    rolling window cheaply without scanning the full history. Shared by both
    PREDICTIONS_TABLE and FORWARD_PREDICTIONS_TABLE — same schema, different
    table names.
    """
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_name  = table_id.rsplit(".", 1)[-1]
    table_ref   = dataset_ref.table(table_name)
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
        logger.info(f"✅ Created BigQuery table: {table_id}")
    else:
        logger.debug(f"Table already exists: {table_id}")


def write_prediction_to_bq(predicted_date: pd.Timestamp, predicted_ptf: float,
                           table: str = PREDICTIONS_TABLE) -> None:
    """Idempotent upsert of a single prediction row to BigQuery.

    Uses a DML MERGE statement so that Airflow task retries cannot create
    duplicate rows.  The natural key is (predicted_date, hour): if a row
    already exists for this period, its predicted_ptf and predicted_at are
    updated in place rather than a second row being appended — for
    FORWARD_PREDICTIONS_TABLE specifically, this means a forecast naturally
    refreshes in place as day-ahead-available inputs (e.g. LEP) get revised
    across multiple runs, rather than accumulating stale duplicates.

    DML jobs run on the BigQuery query engine (not the streaming API), so they
    are not subject to the streaming-insert propagation delay that previously
    caused 404 errors immediately after table creation.

    Key convention — Turkish local time (UTC+3):
        The UTC datetime index this is called with gets converted to Turkish
        local date/hour before writing, so predicted_date/hour align with
        stg_pricing and every other Turkish-local-keyed staging model.
    """
    client = get_bq_client()

    # Auto-create the table on first inference run — no manual DDL required.
    _ensure_predictions_table(client, table)

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
        MERGE `{table}` T
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

    logger.info(f"✅ Prediction upserted [{table.rsplit('.', 1)[-1]}] — {date_str} hour={hour} PTF={predicted_ptf:.2f} TRY")


def get_last_predicted_ts() -> pd.Timestamp:
    """Return the most recent (predicted_date, hour) already written to
    gold_ptf_predictions, converted back to the naive-UTC representation used
    by mart_ptf_lag_features.datetime — the exact inverse of
    write_prediction_to_bq's UTC→Turkish-local conversion above.

    Returns None if the table doesn't exist yet or has no rows (first-ever
    run), in which case the caller should predict every available row.
    """
    client = get_bq_client()
    query = f"""
        SELECT predicted_date, hour
        FROM `{PREDICTIONS_TABLE}`
        ORDER BY predicted_date DESC, hour DESC
        LIMIT 1
    """
    try:
        rows = list(client.query(query).result())
    except NotFound:
        return None
    if not rows:
        return None
    ts_tr  = pd.Timestamp(rows[0].predicted_date) + pd.Timedelta(hours=int(rows[0].hour))
    return ts_tr - _TR_UTC_OFFSET


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

def run_backtest_inference(model, required_features: list) -> int:
    """Predict every newly-settled hour since the last write (backtest-
    oriented path — see module docstring). Returns the number of rows written."""
    df_recent = extract_recent_data()
    last_ts   = get_last_predicted_ts()
    X_batch   = build_inference_features(df_recent, required_features, since=last_ts)

    if X_batch.empty:
        logger.info("Backtest inference: no new settled hours to predict.")
        return 0

    predictions = model.predict(X_batch)
    for ts, ptf in zip(X_batch.index, predictions):
        write_prediction_to_bq(predicted_date=ts, predicted_ptf=float(ptf), table=PREDICTIONS_TABLE)

    logger.info(f"Backtest inference: wrote {len(X_batch)} prediction(s).")
    return len(X_batch)


def run_forward_forecast(model, required_features: list) -> int:
    """Predict every genuinely future (not-yet-settled) hour — the actual
    before-the-fact forecast used for decision support (e.g. Vardiya
    Optimizasyonu). Always (re-)writes all currently-future rows rather than
    tracking a "since" cursor, since day-ahead-available inputs can be
    revised between runs and we want the freshest forecast, not the first
    one ever computed for that hour. Returns the number of rows written."""
    df_forward = extract_forward_features()
    if df_forward.empty:
        logger.info("Forward forecast: no genuinely future rows available yet.")
        return 0

    X_batch = build_forward_inference_features(df_forward, required_features)
    if X_batch.empty:
        return 0

    predictions = model.predict(X_batch)
    for ts, ptf in zip(X_batch.index, predictions):
        write_prediction_to_bq(predicted_date=ts, predicted_ptf=float(ptf), table=FORWARD_PREDICTIONS_TABLE)

    logger.info(f"Forward forecast: wrote {len(X_batch)} prediction(s).")
    return len(X_batch)


def run():
    artifact          = load_model_from_gcs()
    model             = artifact["model"]
    required_features = artifact["features"]

    n_backtest = run_backtest_inference(model, required_features)
    n_forward  = run_forward_forecast(model, required_features)

    logger.info(f"🏁 Inference job complete — {n_backtest} backtest, {n_forward} forward prediction(s) written.")


if __name__ == "__main__":
    run()
