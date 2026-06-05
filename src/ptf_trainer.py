"""
ptf_trainer.py — XGBoost PTF Model Training Job
================================================
Cadence : Weekly (or daily) via Airflow — task_id: train_ptf_model
Purpose : Full retraining on all historical data. Saves model artifact to GCS.
Runtime : 5–20 minutes. Acceptable for weekly/daily batch.
"""

import os
import logging
import joblib
import pandas as pd
import xgboost as xgb
from google.cloud import bigquery, storage
from sklearn.metrics import mean_absolute_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PTFTrainer")

PROJECT_ID   = os.getenv("GCP_PROJECT_ID",   "epias-data-platform")
DATASET_ID   = os.getenv("BQ_GOLD_DATASET",  "epias_gold")
GCS_BUCKET   = os.getenv("GCS_BUCKET",       "epias-data-lake")
MODEL_GCS_PATH = "models/ptf_xgb_model.joblib"
IMPORTANCE_GCS_PATH = "models/ptf_shap_importance.csv"
LOCAL_TMP    = "/tmp"


# ── DATA EXTRACTION ───────────────────────────────────────────────────────────

def extract_training_data() -> pd.DataFrame:
    """Pull full history from Gold layer for training."""
    logger.info("Pulling full training data from BigQuery...")
    client = bigquery.Client(project=PROJECT_ID)

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
        ORDER BY f.datetime ASC
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Pulled {len(df):,} rows ({df.memory_usage(deep=True).sum() / 1e6:.1f} MB in memory)")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.set_index("datetime", inplace=True)
    logger.info(f"Date range: {df.index.min()} → {df.index.max()}")
    return df


# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build full feature set for training. Requires full history for lag correctness."""
    logger.info("Engineering features...")

    df["hour"]         = df.index.hour
    df["day_of_week"]  = df.index.dayofweek
    df["month"]        = df.index.month
    df["is_weekend"]   = df["day_of_week"].isin([5, 6]).astype(int)

    df["ptf_lag_24h"]  = df["ptf_try"].shift(24)
    df["ptf_lag_168h"] = df["ptf_try"].shift(168)

    # Supply shock columns come from a LEFT JOIN on mart_supply_shock_index, which is
    # keyed by outage *start* date — not by the date each outage was *active*.  Outages
    # that started before the training window have no matching row, so the join produces
    # NULL for most dates.  Forward-fill propagates the last known value across the gap
    # (reasonable: an outage present yesterday is likely still present today); any
    # remaining NaN (e.g., no outage history at all) is filled with 0 (no supply shock).
    for col in ["supply_shock_index", "total_outage_mwh", "total_available_capacity_mwh"]:
        if col in df.columns:
            df[col] = df[col].ffill().fillna(0.0)

    df["supply_shock_trend_7d"] = df["supply_shock_index"].rolling(window=168).mean()

    # Diagnose NaN distribution before dropping so failures are debuggable.
    nan_counts = df.isna().sum()
    nan_cols = nan_counts[nan_counts > 0]
    if not nan_cols.empty:
        logger.info(f"NaN counts before dropna:\n{nan_cols.to_string()}")

    # Drop rows where the target or any lag/rolling feature is NaN.
    # "ptf_try" must be included: XGBoost rejects NaN labels outright.
    # Lag columns are NaN only during the first ~168-hour warm-up period.
    # Using subset= avoids wiping the dataset when an optional joined column is sparse.
    required_cols = [c for c in ["ptf_try", "ptf_lag_24h", "ptf_lag_168h", "supply_shock_trend_7d"]
                     if c in df.columns]
    before = len(df)
    df.dropna(subset=required_cols, inplace=True)
    logger.info(f"Dropped {before - len(df)} NaN rows (label/lag warm-up). Training rows: {len(df):,}")
    return df


# ── TRAINING ──────────────────────────────────────────────────────────────────

def train(df: pd.DataFrame) -> xgb.XGBRegressor:
    """Train XGBoost with time-series split. Returns fitted model."""
    target   = "ptf_try"
    features = [c for c in df.columns if c != target]

    split_date = df.index.max() - pd.Timedelta(days=30)
    X_train = df.loc[df.index < split_date, features]
    y_train = df.loc[df.index < split_date, target]
    X_test  = df.loc[df.index >= split_date, features]
    y_test  = df.loc[df.index >= split_date, target]

    model = xgb.XGBRegressor(
        n_estimators=800,
        learning_rate=0.03,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        objective="reg:squarederror",
        early_stopping_rounds=50,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=100,
    )

    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)

    # Standard MAPE divides by y_true, blowing up when PTF = 0 (zero-price hours
    # occur in Turkish markets during high-renewable periods).  Use a symmetric MAPE
    # (sMAPE) instead: denominator is (|y_true| + |y_pred|) / 2, which is always ≥ 0
    # and degrades gracefully near zero rather than producing astronomical values.
    denom = (y_test.abs() + pd.Series(preds, index=y_test.index).abs()) / 2
    smape = (y_test - pd.Series(preds, index=y_test.index)).abs().div(denom.replace(0, float("nan"))).mean()
    logger.info(f"✅ Training complete — MAE: {mae:.2f} TRY | sMAPE: {smape*100:.2f}%")

    # Save feature importance (columns aligned with dashboard expectation)
    imp_df = pd.DataFrame({
        "col_name": features,
        "feature_importance_vals": model.feature_importances_,
    }).sort_values("feature_importance_vals", ascending=False)
    logger.info(f"Top 3 features:\n{imp_df.head(3).to_string(index=False)}")

    return model, features, imp_df


# ── GCS UPLOAD ────────────────────────────────────────────────────────────────

def upload_to_gcs(local_path: str, gcs_path: str) -> None:
    bucket = storage.Client().bucket(GCS_BUCKET)
    bucket.blob(gcs_path).upload_from_filename(local_path)
    logger.info(f"Uploaded {local_path} → gs://{GCS_BUCKET}/{gcs_path}")


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

def run():
    df_raw = extract_training_data()

    # Minimum data guard: need at least 168h lag warmup + 30 days test + buffer
    MIN_ROWS_REQUIRED = 168 + (30 * 24) + 48   # ~888 hourly rows (~37 days)
    if len(df_raw) < MIN_ROWS_REQUIRED:
        logger.warning(
            f"⚠️  Insufficient data for training: {len(df_raw):,} rows available, "
            f"{MIN_ROWS_REQUIRED:,} required. "
            f"Backfill more historical data before running the trainer. Skipping."
        )
        return

    df_engineered = engineer_features(df_raw)
    model, features, imp_df = train(df_engineered)

    # Save model locally then push to GCS
    local_model = f"{LOCAL_TMP}/ptf_xgb_model.joblib"
    local_imp   = f"{LOCAL_TMP}/ptf_shap_importance.csv"

    joblib.dump({"model": model, "features": features}, local_model)
    imp_df.to_csv(local_imp, index=False)

    upload_to_gcs(local_model, MODEL_GCS_PATH)
    upload_to_gcs(local_imp,   IMPORTANCE_GCS_PATH)
    logger.info("🏁 Training job complete.")


if __name__ == "__main__":
    run()
