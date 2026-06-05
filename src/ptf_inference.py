"""
ptf_inference.py — XGBoost PTF Hourly Inference Job
====================================================
Cadence : Hourly via Airflow — task_id: run_ptf_inference
Purpose : Load pre-trained model from GCS, pull only the last 168 rows needed
          for lag features, predict next hour's PTF, write result to BigQuery.
Runtime : 3–8 seconds. Scales to any cadence without retraining.
"""

import os
import logging
import joblib
import tempfile
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PTFInference")

PROJECT_ID      = os.getenv("GCP_PROJECT_ID",  "epias-data-platform")
DATASET_ID      = os.getenv("BQ_GOLD_DATASET", "epias_gold")
GCS_BUCKET      = os.getenv("GCS_BUCKET",      "epias-data-lake")
MODEL_GCS_PATH  = "models/ptf_xgb_model.joblib"
PREDICTIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_ptf_predictions"

# Minimum lookback for lag-168 + rolling-168 features
LOOKBACK_HOURS = 180


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
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT
            f.date,
            f.ptf_try,
            f.forecasted_residual_load_mwh,
            f.price_independent_bid_mwh,
            s.total_available_capacity_mwh,
            s.total_outage_mwh,
            s.supply_shock_index
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_forecasted_residual_load` f
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.mart_supply_shock_index` s
            ON f.date = s.date
        ORDER BY f.date DESC
        LIMIT {LOOKBACK_HOURS}
    """
    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    logger.info(f"Fetched {len(df)} rows — latest: {df.index.max()}")
    return df


# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

def build_inference_features(df: pd.DataFrame, required_features: list) -> pd.DataFrame:
    """Build the same feature set as training, return only the latest row."""
    df["hour"]        = df.index.hour
    df["day_of_week"] = df.index.dayofweek
    df["month"]       = df.index.month
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)

    df["ptf_lag_24h"]  = df["ptf_try"].shift(24)
    df["ptf_lag_168h"] = df["ptf_try"].shift(168)

    df["supply_shock_trend_7d"] = df["supply_shock_index"].rolling(window=168).mean()

    df.dropna(inplace=True)

    # Return only the most recent row as the inference input
    latest = df[required_features].iloc[[-1]]
    logger.info(f"Inference input built for timestamp: {latest.index[0]}")
    return latest


# ── PREDICTION WRITER ─────────────────────────────────────────────────────────

def write_prediction_to_bq(predicted_date: pd.Timestamp, hour: int, predicted_ptf: float) -> None:
    """Append a single prediction row to BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    rows = [{
        "predicted_date": predicted_date.strftime("%Y-%m-%d"),
        "hour":           str(hour),
        "predicted_ptf":  round(float(predicted_ptf), 4),
        "predicted_at":   datetime.now(timezone.utc).isoformat(),
    }]
    errors = client.insert_rows_json(PREDICTIONS_TABLE, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    logger.info(f"✅ Prediction written — {predicted_date.date()} hour={hour} PTF={predicted_ptf:.2f} TRY")


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

def run():
    artifact          = load_model_from_gcs()
    model             = artifact["model"]
    required_features = artifact["features"]

    df_recent         = extract_recent_data()
    X_latest          = build_inference_features(df_recent, required_features)

    predicted_ptf     = model.predict(X_latest)[0]
    predicted_ts      = X_latest.index[0]

    write_prediction_to_bq(
        predicted_date=predicted_ts,
        hour=predicted_ts.hour,
        predicted_ptf=predicted_ptf,
    )
    logger.info("🏁 Inference job complete.")


if __name__ == "__main__":
    run()
