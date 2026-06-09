"""
ptf_trainer.py — XGBoost PTF Model Training Job
================================================
Cadence : Weekly (or daily) via Airflow — task_id: train_ptf_model
Purpose : Full retraining on all historical data. Saves model artifact to GCS.
Runtime : 10–30 minutes (Optuna adds ~10 min on top of base training).
"""

import logging
import tempfile
import warnings
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from google.cloud import storage
from sklearn.metrics import mean_absolute_error

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _OPTUNA_AVAILABLE = True
except ImportError:
    _OPTUNA_AVAILABLE = False

from config import GCP_PROJECT_ID as PROJECT_ID, BQ_GOLD_DATASET as DATASET_ID, GCS_BUCKET, get_bq_client
from ptf_features import build_ptf_features, FEATURE_COLS

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PTFTrainer")

MODEL_GCS_PATH      = "models/ptf_xgb_model.joblib"
IMPORTANCE_GCS_PATH = "models/ptf_shap_importance.csv"
LOCAL_TMP           = tempfile.gettempdir()   # /tmp on Linux/Docker, %TEMP% on Windows

# Number of Optuna trials — 20 gives good coverage; reduce to 5 for CI/smoke
OPTUNA_N_TRIALS = 20


# ── DATA EXTRACTION ───────────────────────────────────────────────────────────

def extract_training_data() -> pd.DataFrame:
    """Pull full history from mart_ptf_lag_features (Gold layer) for training.

    mart_ptf_lag_features → mart_ml_features which already contains:
      PTF/SMF prices, load forecasts, RES forecasts, GÖP volumes, capacity
      utilisation, actual generation mix, weather, actual consumption,
      GİP-GÖP spreads, DGP signals, and all PTF lag/rolling features.
    """
    logger.info("Pulling full training data from mart_ptf_lag_features...")
    client = get_bq_client()

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_ptf_lag_features`
        WHERE date IS NOT NULL
          AND ptf_try IS NOT NULL
        ORDER BY date, hour
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Pulled {len(df):,} rows ({df.memory_usage(deep=True).sum() / 1e6:.1f} MB)")

    # Build a proper DatetimeIndex (Turkish local time, UTC+3, no DST).
    # mart_ptf_lag_features now includes a pre-computed `datetime` column;
    # fall back to constructing it from date+hour if the column is absent
    # (e.g. running against an older Gold table before a full-refresh).
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_localize(None)
    else:
        df["datetime"] = pd.to_datetime(df["date"].astype(str)) + pd.to_timedelta(df["hour"], unit="h")

    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    logger.info(f"Date range: {df.index.min()} → {df.index.max()}")
    return df


# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build full feature set. Requires full history for lag correctness."""
    logger.info("Engineering features...")
    df = build_ptf_features(df)

    nan_counts = df.isna().sum()
    nan_cols = nan_counts[nan_counts > 0]
    if not nan_cols.empty:
        logger.info(f"NaN counts before dropna:\n{nan_cols.to_string()}")

    # Drop rows where target or core lag features are NaN (warm-up window).
    required = [c for c in ["ptf_try", "ptf_lag_24h", "ptf_lag_168h"]
                if c in df.columns]
    before = len(df)
    df.dropna(subset=required, inplace=True)
    logger.info(f"Dropped {before - len(df):,} NaN rows (lag warm-up). "
                f"Training rows: {len(df):,}")
    return df


# ── HYPERPARAMETER OPTIMISATION ───────────────────────────────────────────────

def _optimise_hyperparams(X_train: pd.DataFrame, y_train: pd.Series,
                          n_trials: int = OPTUNA_N_TRIALS) -> dict:
    """Optuna time-series CV to find best XGBoost hyperparameters.

    Uses a 80/20 inner split (no shuffle) rather than k-fold cross-validation
    so temporal order is always respected.
    """
    from sklearn.model_selection import TimeSeriesSplit

    tscv = TimeSeriesSplit(n_splits=3)

    # Ensure float64 dtype — BigQuery returns some numeric cols as object
    X_f = X_train.apply(pd.to_numeric, errors="coerce").fillna(0).astype("float64")

    def objective(trial):
        params = {
            "n_estimators":      trial.suggest_int("n_estimators", 200, 1200),
            "max_depth":         trial.suggest_int("max_depth", 3, 10),
            "learning_rate":     trial.suggest_float("learning_rate", 0.005, 0.2, log=True),
            "subsample":         trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.4, 1.0),
            "min_child_weight":  trial.suggest_int("min_child_weight", 1, 10),
            "gamma":             trial.suggest_float("gamma", 0.0, 1.0),
            "reg_alpha":         trial.suggest_float("reg_alpha", 0.0, 1.0),
            "reg_lambda":        trial.suggest_float("reg_lambda", 0.5, 5.0),
            "random_state":      42,
            "objective":         "reg:squarederror",
        }
        scores = []
        for tr_idx, val_idx in tscv.split(X_f):
            m = xgb.XGBRegressor(**params)
            m.fit(X_f.iloc[tr_idx], y_train.iloc[tr_idx], verbose=False)
            preds = m.predict(X_f.iloc[val_idx])
            scores.append(mean_absolute_error(y_train.iloc[val_idx], preds))
        return float(np.mean(scores))

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    logger.info(f"Optuna best MAE (CV): {study.best_value:.2f} TL | "
                f"params: {study.best_params}")
    return study.best_params


# Default hyperparameters used when Optuna is not installed or skipped.
_DEFAULT_PARAMS = {
    "n_estimators":     800,
    "learning_rate":    0.03,
    "max_depth":        6,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "random_state":     42,
    "objective":        "reg:squarederror",
    "early_stopping_rounds": 50,
}


# ── TRAINING ──────────────────────────────────────────────────────────────────

def train(df: pd.DataFrame) -> tuple:
    """Train XGBoost with time-series split. Returns (model, feature_list, importance_df)."""
    target = "ptf_try"

    # Keep only features that are both in FEATURE_COLS and actually in the df.
    # This makes the trainer robust to columns that are present in the schema
    # but happened to be all-NULL for the current pull (e.g. weather data gaps).
    available = set(df.columns) - {target, "date", "hour", "year", "month_col"}
    features  = [c for c in FEATURE_COLS if c in available]

    # Drop rows where ALL selected features are NaN (degenerate rows)
    df = df.dropna(subset=[target])

    def _to_float(df_slice: pd.DataFrame) -> pd.DataFrame:
        """Cast every column to float64, coercing non-numeric values to NaN, then fill."""
        return df_slice.apply(pd.to_numeric, errors="coerce").fillna(0).astype("float64")

    split_date = df.index.max() - pd.Timedelta(days=30)
    X_train = _to_float(df.loc[df.index <  split_date, features])
    y_train = df.loc[df.index <  split_date, target]
    X_test  = _to_float(df.loc[df.index >= split_date, features])
    y_test  = df.loc[df.index >= split_date, target]

    logger.info(f"Train: {len(X_train):,} rows | Test (last 30d): {len(X_test):,} rows | "
                f"Features: {len(features)}")

    # Hyperparameter optimisation
    if _OPTUNA_AVAILABLE:
        logger.info(f"Running Optuna with {OPTUNA_N_TRIALS} trials...")
        best_params = _optimise_hyperparams(X_train, y_train)
        # These are passed explicitly to XGBRegressor() below — pop them from
        # best_params regardless of what Optuna returns so there is never a
        # "multiple values for keyword argument" crash.
        best_params.pop("early_stopping_rounds", None)
        best_params.pop("random_state", None)
    else:
        logger.warning("Optuna not installed — using default hyperparameters.")
        best_params = {k: v for k, v in _DEFAULT_PARAMS.items()
                       if k not in ("early_stopping_rounds", "random_state")}

    model = xgb.XGBRegressor(**best_params, early_stopping_rounds=50,
                              random_state=42)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=100,
    )

    preds = model.predict(X_test)
    mae   = mean_absolute_error(y_test, preds)

    # sMAPE — handles zero-PTF hours (solar peak collapse) without blowing up
    denom = (y_test.abs() + pd.Series(preds, index=y_test.index).abs()) / 2
    smape = (
        (y_test - pd.Series(preds, index=y_test.index))
        .abs()
        .div(denom.replace(0, float("nan")))
        .mean()
    )
    logger.info(f"✅ Training complete — MAE: {mae:.2f} TL/MWh | sMAPE: {smape*100:.2f}%")

    # Feature importance
    imp_df = pd.DataFrame({
        "col_name":               features,
        "feature_importance_vals": model.feature_importances_,
    }).sort_values("feature_importance_vals", ascending=False)
    logger.info(f"Top 5 features:\n{imp_df.head(5).to_string(index=False)}")

    return model, features, imp_df


# ── GCS UPLOAD ────────────────────────────────────────────────────────────────

def upload_to_gcs(local_path: str, gcs_path: str) -> None:
    bucket = storage.Client().bucket(GCS_BUCKET)
    bucket.blob(gcs_path).upload_from_filename(local_path)
    logger.info(f"Uploaded {local_path} → gs://{GCS_BUCKET}/{gcs_path}")


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

def run():
    df_raw = extract_training_data()

    # Minimum data guard: 168h lag warm-up + 30 days test + buffer
    MIN_ROWS = 168 + (30 * 24) + 48   # ~888 hourly rows (~37 days)
    if len(df_raw) < MIN_ROWS:
        logger.warning(
            f"⚠️  Insufficient data: {len(df_raw):,} rows, need {MIN_ROWS:,}. "
            f"Backfill more history first. Skipping."
        )
        return

    df_engineered = engineer_features(df_raw)
    model, features, imp_df = train(df_engineered)

    local_model = f"{LOCAL_TMP}/ptf_xgb_model.joblib"
    local_imp   = f"{LOCAL_TMP}/ptf_shap_importance.csv"

    joblib.dump({"model": model, "features": features}, local_model)
    imp_df.to_csv(local_imp, index=False)

    upload_to_gcs(local_model, MODEL_GCS_PATH)
    upload_to_gcs(local_imp,   IMPORTANCE_GCS_PATH)
    logger.info("🏁 Training job complete.")


if __name__ == "__main__":
    run()
