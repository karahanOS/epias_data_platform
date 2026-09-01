"""
smf_inference.py — 2-Stage XGBoost SMF Hourly Inference Job
==============================================================
Cadence : Hourly via Airflow — task_id: run_smf_inference
Purpose : Load pre-trained direction+price models from GCS, predict every hour
          that has become newly available since the last write (backtest path)
          plus every genuinely-future hour (forward path), write results to
          BigQuery. Mirrors ptf_inference.py's structure and rationale closely
          — see that file's module docstring for the "why predict every new
          row, not just the latest" and backtest-vs-forward split reasoning,
          both identical here.
"""

import logging
import os
import joblib
import tempfile
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from config import GCP_PROJECT_ID as PROJECT_ID, BQ_GOLD_DATASET as DATASET_ID, GCS_BUCKET, get_bq_client, get_gcs_client
from smf_features import build_smf_features, DIRECTION_FEATURE_COLS, PRICE_FEATURE_COLS, DIRECTION_CLASSES

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SMFInference")
MODEL_GCS_PATH  = "models/smf_xgb_model.joblib"
PREDICTIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_smf_predictions"

# Forward (genuinely not-yet-settled) predictions — separate table, same
# rationale as ptf_inference.py's FORWARD_PREDICTIONS_TABLE.
FORWARD_PREDICTIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_smf_forward_predictions"

# Fixed-horizon snapshot: the prediction each hour first had at
# <=FORWARD_SNAPSHOT_LEAD_HOURS wall-clock lead time from the current hour,
# written once and never overwritten. Distinct from FORWARD_PREDICTIONS_TABLE,
# which is a live view continuously refreshed as fresher features arrive and
# is EXPECTED to change hour to hour for the same target — that's correct
# behavior for "what does the model think right now," not a bug. This table
# exists so forecast accuracy at a genuine, fixed lead time can be measured
# at all. Added 2026-08-18 after confirming the prior gold_smf_forward_accuracy
# table (now retired) only ever captured 0-2h lead-time snapshots, because it
# archived whatever the live table's last value happened to be right before
# the hour elapsed — never a real forward-looking horizon.
#
# 2026-08-21, two corrections same day:
# 1. Widened 5h -> 7h, then anchored to S (last real-published SMF hour,
#    mart_smf_realized) instead of wall-clock now, per a report that hour 13
#    should be the last frozen hour at 12:23 with S=hour 6 (S+7=13, matched).
# 2. That "confirmation" turned out to be a coincidence, not a real fix —
#    S+7 happens to sit close to wall-clock now most of the time (SMF's own
#    ~5-6h publish lag means now-S is usually ~5-7h, so S+7 wanders around
#    "now +/- a couple hours" rather than reliably being ANY fixed distance
#    ahead of it). Confirmed live: at 15:57 TRT, S was 09:00 TRT, so S+7 =
#    16:00 — 3 minutes ahead of "now", not a meaningful forward freeze at
#    all. Root issue: real trading doesn't wait for SMF to publish (S) —
#    it happens continuously in wall-clock time, so anchoring to S was wrong
#    in principle, not just in width. Reverted to wall-clock `now`, and
#    narrowed the window to 1h so the guarantee is exactly what was asked
#    for: the NEXT hour's prediction (lead_hours in (0,1]) freezes, prior
#    (already-elapsed) predictions stay frozen (unchanged, insert-once as
#    always), hours 2+ out remain live. Table renamed (again) to a
#    threshold-agnostic name so a future width tweak doesn't need a third
#    rename: gold_smf_forward_snapshot_7h -> gold_smf_forward_snapshot.
FORWARD_SNAPSHOT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_smf_forward_snapshot"
FORWARD_SNAPSHOT_LEAD_HOURS = 1

# Same lookback as ptf_inference.py — 168 (rolling window) + a day's margin.
LOOKBACK_HOURS = 204

_TR_UTC_OFFSET = pd.Timedelta(hours=3)
_PROBA_COLS = [f"pred_proba_{c.lower()}" for c in DIRECTION_CLASSES]


# ── MODEL LOADER ──────────────────────────────────────────────────────────────

def load_model_from_gcs() -> dict:
    """Download the combined direction+price model artifact from GCS."""
    logger.info(f"Loading model from gs://{GCS_BUCKET}/{MODEL_GCS_PATH}...")
    # get_gcs_client() (not bare storage.Client()) — see smf_trainer.py's
    # upload_to_gcs() comment for why.
    bucket = get_gcs_client().bucket(GCS_BUCKET)
    with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as tmp:
        bucket.blob(MODEL_GCS_PATH).download_to_filename(tmp.name)
        artifact = joblib.load(tmp.name)
    logger.info("Model loaded.")
    return artifact  # {"direction_model", "price_model", "direction_classes", "direction_features", "price_features"}


# ── FEATURE EXTRACTION ────────────────────────────────────────────────────────

def extract_recent_data() -> pd.DataFrame:
    """Pull only the last LOOKBACK_HOURS rows from mart_smf_lag_features."""
    logger.info(f"Pulling last {LOOKBACK_HOURS} hours from mart_smf_lag_features...")
    client = get_bq_client()

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_smf_lag_features`
        WHERE date IS NOT NULL AND smf_try IS NOT NULL
        ORDER BY date DESC, hour DESC
        LIMIT {LOOKBACK_HOURS}
    """
    df = client.query(query).to_dataframe()

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
    """Pull genuinely future (not-yet-settled AND not-yet-happened) rows from
    mart_smf_forward_features. Mirrors ptf_inference.py's
    extract_forward_features() anti-join, against mart_smf_realized instead
    of mart_ptf_realized — but with one deliberate difference: PTF settles a
    whole day atomically (~14:00 TRT the day before), so "date >= today" is
    enough to mean "genuinely future" there. SMF settles hour-by-hour with a
    genuine ~5-6h publication lag (S+5, confirmed 2026-08-15), so an hour
    that already happened 1-6h ago still has smf_try IS NULL purely because
    reporting hasn't caught up — not because it's actually still future.
    Confirmed 2026-08-18: without the f.datetime > CURRENT_TIMESTAMP() guard,
    the pipeline kept re-"predicting" already-elapsed hours right up until
    real data landed, producing archived gold_smf_forward_accuracy rows with
    NEGATIVE lead_time_hours (predicted_at recorded hours AFTER the target
    hour already occurred) — not genuine forecasts at all.
    """
    logger.info("Pulling rows with no real SMF yet from mart_smf_forward_features...")
    client = get_bq_client()

    query = f"""
        SELECT f.*
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_smf_forward_features` f
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.mart_smf_realized` r
          ON r.date = f.date AND r.hour = f.hour
        WHERE r.smf_try IS NULL
          AND f.datetime > CURRENT_TIMESTAMP()
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
                f"{df.index.min()} -> {df.index.max()}")
    return df


# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

def _cast_to_model_input(df: pd.DataFrame, required_features: list) -> pd.DataFrame:
    """Identical rationale/technique to ptf_inference.py's _cast_to_model_input()
    — missing==0, not raw NaN, to match how the model was actually trained."""
    missing = [f for f in required_features if f not in df.columns]
    if missing:
        logger.warning(f"Required features missing from DataFrame (will be NaN): {missing}")
    return (df.reindex(columns=required_features)
              .apply(pd.to_numeric, errors="coerce")
              .fillna(0)
              .astype("float64"))


def build_inference_features(
    df: pd.DataFrame, direction_features: list, since: pd.Timestamp = None
) -> pd.DataFrame:
    """Build the direction-stage feature set for every row newer than `since`
    (all rows if None). Mirrors ptf_inference.py's build_inference_features()."""
    df = build_smf_features(df)

    core_cols = [c for c in ["smf_try_lag_24h", "smf_try_lag_168h"] if c in df.columns]
    df.dropna(subset=core_cols, inplace=True)

    if df.empty:
        raise RuntimeError(
            f"DataFrame is empty after dropna on {core_cols}. "
            f"Ensure mart_smf_lag_features has at least {LOOKBACK_HOURS} hourly rows "
            f"with non-null smf_try. Check upstream data pipeline status."
        )

    if since is not None:
        df = df[df.index > since]

    batch = _cast_to_model_input(df, direction_features)
    if batch.empty:
        logger.info("No new timestamps since the last prediction — nothing to do.")
    else:
        logger.info(
            f"Inference input built for {len(batch)} timestamp(s): "
            f"{batch.index.min()} -> {batch.index.max()}"
        )
    return batch


def build_forward_inference_features(df: pd.DataFrame, direction_features: list) -> pd.DataFrame:
    """Build the direction-stage feature set for genuinely future rows. No
    dropna/since filtering — every row extract_forward_features() returns is
    already guaranteed genuinely future."""
    df = build_smf_features(df)
    batch = _cast_to_model_input(df, direction_features)
    if batch.empty:
        logger.info("No genuinely future rows to predict.")
    else:
        logger.info(
            f"Forward inference input built for {len(batch)} timestamp(s): "
            f"{batch.index.min()} -> {batch.index.max()}"
        )
    return batch


def _predict_both_stages(artifact: dict, X_direction: pd.DataFrame) -> pd.DataFrame:
    """Runs Stage 1 (direction) then Stage 2 (price, fed Stage 1's
    probabilities) — the same feature construction used at training time.
    Returns a DataFrame indexed like X_direction with predicted_direction,
    the 3 probability columns, and predicted_smf.
    """
    direction_model = artifact["direction_model"]
    price_model     = artifact["price_model"]
    classes         = artifact["direction_classes"]
    price_features  = artifact["price_features"]

    proba = direction_model.predict_proba(X_direction)
    result = pd.DataFrame(proba, index=X_direction.index, columns=_PROBA_COLS)
    result["predicted_direction"] = [classes[i] for i in proba.argmax(axis=1)]

    X_price = pd.concat([X_direction, result[_PROBA_COLS]], axis=1)
    X_price = X_price.reindex(columns=price_features).astype("float64")
    result["predicted_smf"] = price_model.predict(X_price)

    return result


# ── PREDICTION WRITER ─────────────────────────────────────────────────────────

_PREDICTIONS_SCHEMA = [
    bigquery.SchemaField("predicted_date",  "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("hour",            "INTEGER",   mode="REQUIRED"),
    bigquery.SchemaField("predicted_smf",   "FLOAT64",   mode="REQUIRED"),
    bigquery.SchemaField("predicted_direction", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("proba_energy_deficit",  "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("proba_energy_surplus",  "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("proba_in_balance",      "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("predicted_at",    "TIMESTAMP", mode="REQUIRED"),
]


def _ensure_predictions_table(client: bigquery.Client, table_id: str) -> None:
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_name  = table_id.rsplit(".", 1)[-1]
    table_ref   = dataset_ref.table(table_name)
    table       = bigquery.Table(table_ref, schema=_PREDICTIONS_SCHEMA)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="predicted_date",
    )
    table.clustering_fields = ["hour"]

    created = client.create_table(table, exists_ok=True)
    if created.created is not None:
        logger.info(f"Created BigQuery table: {table_id}")
    else:
        logger.debug(f"Table already exists: {table_id}")


def write_prediction_to_bq(predicted_date: pd.Timestamp, row: pd.Series,
                           table: str = PREDICTIONS_TABLE) -> None:
    """Idempotent MERGE upsert — same convention as ptf_inference.py's
    write_prediction_to_bq()."""
    client = get_bq_client()
    _ensure_predictions_table(client, table)

    ts_utc  = predicted_date.tz_localize(None) if predicted_date.tzinfo is None else \
              predicted_date.tz_convert("UTC").tz_localize(None)
    ts_tr   = ts_utc + _TR_UTC_OFFSET
    date_str = ts_tr.strftime("%Y-%m-%d")
    hour     = int(ts_tr.hour)

    smf_rounded  = round(float(row["predicted_smf"]), 4)
    direction    = str(row["predicted_direction"])
    p_deficit    = round(float(row["pred_proba_energy_deficit"]), 6)
    p_surplus    = round(float(row["pred_proba_energy_surplus"]), 6)
    p_balance    = round(float(row["pred_proba_in_balance"]), 6)
    predicted_at = datetime.now(timezone.utc).isoformat()

    merge_sql = f"""
        MERGE `{table}` T
        USING (
            SELECT
                DATE '{date_str}'          AS predicted_date,
                {int(hour)}                 AS hour,
                {smf_rounded}                AS predicted_smf,
                '{direction}'                AS predicted_direction,
                {p_deficit}                  AS proba_energy_deficit,
                {p_surplus}                  AS proba_energy_surplus,
                {p_balance}                  AS proba_in_balance,
                TIMESTAMP '{predicted_at}'  AS predicted_at
        ) S
        ON T.predicted_date = S.predicted_date AND T.hour = S.hour
        WHEN MATCHED THEN
            UPDATE SET
                predicted_smf        = S.predicted_smf,
                predicted_direction  = S.predicted_direction,
                proba_energy_deficit = S.proba_energy_deficit,
                proba_energy_surplus = S.proba_energy_surplus,
                proba_in_balance     = S.proba_in_balance,
                predicted_at         = S.predicted_at
        WHEN NOT MATCHED THEN
            INSERT (predicted_date, hour, predicted_smf, predicted_direction,
                    proba_energy_deficit, proba_energy_surplus, proba_in_balance, predicted_at)
            VALUES (S.predicted_date, S.hour, S.predicted_smf, S.predicted_direction,
                    S.proba_energy_deficit, S.proba_energy_surplus, S.proba_in_balance, S.predicted_at)
    """

    job = client.query(merge_sql)
    job.result()

    logger.info(f"Prediction upserted [{table.rsplit('.', 1)[-1]}] — {date_str} hour={hour} "
                f"SMF={row['predicted_smf']:.2f} TRY direction={direction}")


_FORWARD_SNAPSHOT_SCHEMA = [
    bigquery.SchemaField("predicted_date",       "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("hour",                 "INTEGER",   mode="REQUIRED"),
    bigquery.SchemaField("predicted_smf",        "FLOAT64",   mode="REQUIRED"),
    bigquery.SchemaField("predicted_direction",  "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("proba_energy_deficit", "FLOAT64",   mode="REQUIRED"),
    bigquery.SchemaField("proba_energy_surplus", "FLOAT64",   mode="REQUIRED"),
    bigquery.SchemaField("proba_in_balance",     "FLOAT64",   mode="REQUIRED"),
    bigquery.SchemaField("lead_hours_at_snapshot", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("snapshotted_at",       "TIMESTAMP", mode="REQUIRED"),
]


def _ensure_forward_snapshot_table(client: bigquery.Client) -> None:
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_ref   = dataset_ref.table("gold_smf_forward_snapshot")
    table       = bigquery.Table(table_ref, schema=_FORWARD_SNAPSHOT_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="predicted_date",
    )
    created = client.create_table(table, exists_ok=True)
    if created.created is not None:
        logger.info(f"Created BigQuery table: {FORWARD_SNAPSHOT_TABLE}")


def write_fixed_horizon_snapshot(predicted_date: pd.Timestamp, row: pd.Series,
                                  lead_hours: float) -> None:
    """Insert-once (never update) snapshot of the prediction the first time a
    target hour is seen at <=FORWARD_SNAPSHOT_LEAD_HOURS wall-clock lead time.
    WHEN NOT MATCHED THEN INSERT with no WHEN MATCHED clause makes this
    idempotent/safe to call every hourly run — later runs for the same
    (date, hour) are no-ops once a snapshot exists, so an outage that skips
    the exact FORWARD_SNAPSHOT_LEAD_HOURS mark just captures the closest
    lead time still <=FORWARD_SNAPSHOT_LEAD_HOURS once the pipeline
    recovers, rather than losing the snapshot entirely."""
    client = get_bq_client()
    _ensure_forward_snapshot_table(client)

    ts_utc  = predicted_date.tz_localize(None) if predicted_date.tzinfo is None else \
              predicted_date.tz_convert("UTC").tz_localize(None)
    ts_tr   = ts_utc + _TR_UTC_OFFSET
    date_str = ts_tr.strftime("%Y-%m-%d")
    hour     = int(ts_tr.hour)

    smf_rounded  = round(float(row["predicted_smf"]), 4)
    direction    = str(row["predicted_direction"])
    p_deficit    = round(float(row["pred_proba_energy_deficit"]), 6)
    p_surplus    = round(float(row["pred_proba_energy_surplus"]), 6)
    p_balance    = round(float(row["pred_proba_in_balance"]), 6)
    snapshotted_at = datetime.now(timezone.utc).isoformat()

    merge_sql = f"""
        MERGE `{FORWARD_SNAPSHOT_TABLE}` T
        USING (
            SELECT
                DATE '{date_str}'          AS predicted_date,
                {int(hour)}                 AS hour,
                {smf_rounded}                AS predicted_smf,
                '{direction}'                AS predicted_direction,
                {p_deficit}                  AS proba_energy_deficit,
                {p_surplus}                  AS proba_energy_surplus,
                {p_balance}                  AS proba_in_balance,
                {round(float(lead_hours), 4)} AS lead_hours_at_snapshot,
                TIMESTAMP '{snapshotted_at}' AS snapshotted_at
        ) S
        ON T.predicted_date = S.predicted_date AND T.hour = S.hour
        WHEN NOT MATCHED THEN
            INSERT (predicted_date, hour, predicted_smf, predicted_direction,
                    proba_energy_deficit, proba_energy_surplus, proba_in_balance,
                    lead_hours_at_snapshot, snapshotted_at)
            VALUES (S.predicted_date, S.hour, S.predicted_smf, S.predicted_direction,
                    S.proba_energy_deficit, S.proba_energy_surplus, S.proba_in_balance,
                    S.lead_hours_at_snapshot, S.snapshotted_at)
    """
    job = client.query(merge_sql)
    job.result()

    if job.num_dml_affected_rows:
        logger.info(f"Fixed-horizon snapshot captured — {date_str} hour={hour} "
                    f"lead={lead_hours:.1f}h SMF={row['predicted_smf']:.2f} TRY")


def get_last_predicted_ts() -> pd.Timestamp:
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

def run_backtest_inference(artifact: dict) -> int:
    """Predict every newly-settled hour since the last write."""
    direction_features = artifact["direction_features"]

    df_recent = extract_recent_data()
    last_ts   = get_last_predicted_ts()
    X_batch   = build_inference_features(df_recent, direction_features, since=last_ts)

    if X_batch.empty:
        logger.info("Backtest inference: no new settled hours to predict.")
        return 0

    predictions = _predict_both_stages(artifact, X_batch)
    for ts, row in predictions.iterrows():
        write_prediction_to_bq(predicted_date=ts, row=row, table=PREDICTIONS_TABLE)

    logger.info(f"Backtest inference: wrote {len(X_batch)} prediction(s).")
    return len(X_batch)


def _cleanup_stale_forward_predictions() -> int:
    """Delete FORWARD_PREDICTIONS_TABLE rows whose (date, hour) has since
    acquired a real SMF in mart_smf_realized — the live table only ever holds
    genuinely-future rows (see extract_forward_features()), so once an hour
    settles its row is stale and gets removed. No longer archives to
    gold_smf_forward_accuracy (retired 2026-08-18): that table only ever
    captured whatever the live prediction happened to be right before the
    hour elapsed (0-2h lead time in practice, confirmed from production
    data), not a genuine forward-looking horizon. See
    write_fixed_horizon_snapshot() / FORWARD_SNAPSHOT_TABLE for the
    replacement, which captures a real fixed-lead-time prediction instead."""
    client = get_bq_client()
    delete_sql = f"""
        DELETE FROM `{FORWARD_PREDICTIONS_TABLE}` p
        WHERE EXISTS (
            SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.mart_smf_realized` r
            WHERE r.date = p.predicted_date AND r.hour = p.hour
        )
    """
    try:
        job = client.query(delete_sql)
        job.result()
    except NotFound:
        return 0
    n = job.num_dml_affected_rows or 0
    if n:
        logger.info(f"Cleaned up {n} stale forward prediction(s) now covered by mart_smf_realized.")
    return n


def run_forward_forecast(artifact: dict) -> int:
    """Predict every genuinely future (not-yet-settled) hour. Also captures a
    fixed-horizon snapshot (see write_fixed_horizon_snapshot()) the first
    time each target hour is seen at <=FORWARD_SNAPSHOT_LEAD_HOURS wall-clock
    lead time, so forecast accuracy can be measured at a real, stable lead
    time instead of only against whatever the live table's last value was."""
    direction_features = artifact["direction_features"]

    _cleanup_stale_forward_predictions()
    df_forward = extract_forward_features()
    if df_forward.empty:
        logger.info("Forward forecast: no genuinely future rows available yet.")
        return 0

    X_batch = build_forward_inference_features(df_forward, direction_features)
    if X_batch.empty:
        return 0

    predictions = _predict_both_stages(artifact, X_batch)
    # Anchored to wall-clock now, NOT S (last real-published SMF hour) — see
    # FORWARD_SNAPSHOT_LEAD_HOURS's 2026-08-21 comment above for why the S
    # anchor (tried earlier the same day) was wrong: real trading happens
    # continuously regardless of when SMF itself gets published, so the
    # freeze boundary needs to track wall-clock time, not the data pipeline's
    # own publish lag.
    anchor_ts = pd.Timestamp.now(tz="UTC").tz_localize(None)
    for ts, row in predictions.iterrows():
        write_prediction_to_bq(predicted_date=ts, row=row, table=FORWARD_PREDICTIONS_TABLE)

        lead_hours = (ts - anchor_ts) / pd.Timedelta(hours=1)
        if lead_hours <= FORWARD_SNAPSHOT_LEAD_HOURS:
            write_fixed_horizon_snapshot(predicted_date=ts, row=row, lead_hours=lead_hours)

    logger.info(f"Forward forecast: wrote {len(X_batch)} prediction(s).")
    return len(X_batch)


def run():
    artifact = load_model_from_gcs()

    n_backtest = run_backtest_inference(artifact)
    n_forward  = run_forward_forecast(artifact)

    logger.info(f"Inference job complete — {n_backtest} backtest, {n_forward} forward prediction(s) written.")


if __name__ == "__main__":
    run()
    # Force-exit rather than let the interpreter shut down naturally. Observed
    # 2026-08-21/2026-09-01 (see dags/smf_inference_dag.py's execution_timeout
    # comment): this class of script has hung with the OS process still alive
    # but ~0 CPU accumulated after run() had already returned/logged — a
    # lingering non-daemon thread (most likely the BigQuery client's own
    # connection-pool/gRPC threads) keeping the process from exiting on its
    # own. os._exit(0) skips normal interpreter teardown (atexit handlers,
    # thread joins) and terminates immediately — safe here since run() has
    # already completed all its writes by this point, there's nothing left to
    # flush.
    os._exit(0)
