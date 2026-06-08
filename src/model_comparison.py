"""
model_comparison.py — PTF Forecasting Model Comparison
=======================================================
Trains and evaluates 5 models on the same train/test split to benchmark
XGBoost against simpler baselines.

Models:
    1. Linear Regression   — simplest possible baseline
    2. Ridge Regression    — regularized linear (handles multicollinear lags)
    3. Random Forest       — non-boosted ensemble
    4. LightGBM            — gradient boosting competitor to XGBoost
    5. XGBoost             — production model (default params, no Optuna)

Run:
    python src/model_comparison.py
Output:
    models/model_comparison_results.csv
"""

import logging
import warnings
import sys
import os

import numpy as np
import pandas as pd
import joblib
import json

from sklearn.linear_model import LinearRegression, Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb

try:
    import lightgbm as lgb
    _LGB_AVAILABLE = True
except ImportError:
    _LGB_AVAILABLE = False
    logging.warning("LightGBM not installed. Run: pip install lightgbm")

# Allow running from repo root or from src/
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from config import GCP_PROJECT_ID as PROJECT_ID, BQ_GOLD_DATASET as DATASET_ID, get_bq_client
from ptf_features import build_ptf_features, FEATURE_COLS

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ModelComparison")

TARGET = "ptf_try"
RESULTS_PATH = "models/model_comparison_results.csv"


# ── DATA ──────────────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    logger.info("Loading data from BigQuery...")
    client = get_bq_client()
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_ptf_lag_features`
        WHERE date IS NOT NULL AND ptf_try IS NOT NULL
        ORDER BY date, hour
    """
    df = client.query(query).to_dataframe()

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_localize(None)
    else:
        df["datetime"] = (
            pd.to_datetime(df["date"].astype(str))
            + pd.to_timedelta(df["hour"], unit="h")
        )
    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    logger.info(f"Loaded {len(df):,} rows | {df.index.min()} → {df.index.max()}")
    return df


def prepare(df: pd.DataFrame):
    """Feature engineering + train/test split identical to ptf_trainer.py."""
    df = build_ptf_features(df)

    required = [c for c in ["ptf_try", "ptf_lag_24h", "ptf_lag_168h"] if c in df.columns]
    df.dropna(subset=required, inplace=True)

    available = set(df.columns) - {TARGET, "date", "hour", "year", "month_col"}
    features = [c for c in FEATURE_COLS if c in available]

    df.dropna(subset=[TARGET], inplace=True)

    def _to_float(d):
        return d.apply(pd.to_numeric, errors="coerce").fillna(0).astype("float64")

    split_date = df.index.max() - pd.Timedelta(days=30)
    X_train = _to_float(df.loc[df.index < split_date,  features])
    y_train = df.loc[df.index < split_date,  TARGET]
    X_test  = _to_float(df.loc[df.index >= split_date, features])
    y_test  = df.loc[df.index >= split_date, TARGET]

    logger.info(
        f"Train: {len(X_train):,} rows | Test: {len(X_test):,} rows | "
        f"Features: {len(features)}"
    )
    return X_train, X_test, y_train, y_test, features


# ── METRICS ───────────────────────────────────────────────────────────────────

def compute_metrics(y_true: pd.Series, y_pred: np.ndarray, name: str) -> dict:
    mae   = mean_absolute_error(y_true, y_pred)
    rmse  = np.sqrt(mean_squared_error(y_true, y_pred))
    r2    = r2_score(y_true, y_pred)

    # sMAPE — symmetric, handles near-zero PTF values without exploding
    denom = (y_true.abs().values + np.abs(y_pred)) / 2
    with np.errstate(divide="ignore", invalid="ignore"):
        smape = np.nanmean(np.where(denom == 0, 0, np.abs(y_true.values - y_pred) / denom)) * 100

    logger.info(f"[{name}] MAE={mae:.1f}  RMSE={rmse:.1f}  sMAPE={smape:.2f}%  R²={r2:.4f}")
    return {"model": name, "mae": round(mae, 2), "rmse": round(rmse, 2),
            "smape": round(smape, 2), "r2": round(r2, 4)}


# ── MODELS ────────────────────────────────────────────────────────────────────

MODELS = {
    "Linear Regression": LinearRegression(),

    "Ridge Regression": Ridge(alpha=10.0),

    "Random Forest": RandomForestRegressor(
        n_estimators=200,
        max_depth=12,
        min_samples_leaf=5,
        n_jobs=-1,
        random_state=42,
    ),

    "XGBoost": xgb.XGBRegressor(
        n_estimators=800,
        learning_rate=0.03,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        objective="reg:squarederror",
        random_state=42,
        verbosity=0,
    ),
}

if _LGB_AVAILABLE:
    MODELS["LightGBM"] = lgb.LGBMRegressor(
        n_estimators=800,
        learning_rate=0.03,
        max_depth=6,
        num_leaves=63,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbose=-1,
    )


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run():
    df = load_data()
    X_train, X_test, y_train, y_test, features = prepare(df)

    results = []
    for name, model in MODELS.items():
        logger.info(f"Training {name}...")
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        results.append(compute_metrics(y_test, preds, name))

    results_df = pd.DataFrame(results).sort_values("mae")
    os.makedirs("models", exist_ok=True)
    results_df.to_csv(RESULTS_PATH, index=False)

    print("\n" + "=" * 60)
    print("MODEL COMPARISON RESULTS (Test: last 30 days)")
    print("=" * 60)
    print(results_df.to_string(index=False))
    print(f"\nResults saved to {RESULTS_PATH}")


if __name__ == "__main__":
    run()
