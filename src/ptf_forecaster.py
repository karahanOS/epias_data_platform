"""
PTF Day-Ahead Forecasting - Saf TL Versiyonu
Yarınki saatlik elektrik fiyatlarını (PTF) tahmin eder.
Model: XGBoost
Feature kaynağı: dbt mart_ptf_lag_features (BigQuery)
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import json
from datetime import datetime, timedelta
import holidays
import warnings
import optuna

warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.WARNING)

# ── CONFIG ────────────────────────────────────────────────────────────────────

PROJECT      = "epias-data-platform"
DATASET      = "epias_gold" # dbt modellerinin yazdığı dataset
TABLE        = "mart_ptf_lag_features"
MODEL_PATH   = "models/ptf_xgb_model.joblib"
METRICS_PATH = "models/ptf_model_metrics.json"

# Kolon isimlerini "ptf_usd" yerine "ptf" olarak güncelledik
FEATURE_COLS = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "month_of_year",
    "temperature",
    "wind_speed",
    "solar_radiation",
    "humidity",
    "wind_generation",
    "solar_generation",
    "hydro_generation",
    "gas_generation",
    "total_generation",
    "actual_consumption",
    "forecast_consumption",  # Artık dbt modelinde mevcut, aktif ettik
    "ptf_lag_1h",
    "ptf_lag_24h",
    "ptf_lag_168h",
    "ptf_rolling_avg_24h",
    "ptf_rolling_avg_168h",
    "ptf_rolling_max_24h",
    "ptf_rolling_min_24h",
]

TARGET_COL = "ptf" # Saf TL hedefi

# ── BIGQUERY'DEN VERİ ÇEK ────────────────────────────────────────────────────

def get_bq_client():
    creds_path = "credentials/gcp-key.json"
    if os.path.exists(creds_path):
        return bigquery.Client.from_service_account_json(creds_path)
    return bigquery.Client(project=PROJECT)

def load_features() -> pd.DataFrame:
    print(f"BigQuery'den {TABLE} tablosu yükleniyor...")
    client = get_bq_client()

    # ptf_usd referanslarını ptf olarak düzelttik
    query = f"""
        SELECT
            date,
            {', '.join(FEATURE_COLS)},
            {TARGET_COL}
        FROM `{PROJECT}.{DATASET}.{TABLE}`
        WHERE date IS NOT NULL
        ORDER BY date
    """

    df = client.query(query).to_dataframe()
    # BigQuery'deki float64'leri Python uyumlu float'a çekiyoruz
    for col in FEATURE_COLS + [TARGET_COL]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    print(f"Toplam kayıt: {len(df)}")
    return df

# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Ek analitik özellikler ekler."""
    tr_holidays = holidays.Turkey(years=range(2024, 2027))
    df["is_holiday"] = df["date"].dt.date.apply(lambda d: 1 if d in tr_holidays else 0)
    
    # Cyclical Time Encoding
    df["hour_sin"] = np.sin(2 * np.pi * df["hour_of_day"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour_of_day"] / 24)
    
    # Yenilenebilir Oranı (Merit Order Etkisi için kritik)
    df["renewable_ratio"] = (
        (df["wind_generation"] + df["solar_generation"] + df["hydro_generation"]) / 
        df["total_generation"].replace(0, np.nan)
    )
    
    # Tüketim Sapması (Yük tahmin hatasının fiyata etkisi)
    df["consumption_error"] = df["actual_consumption"] - df["forecast_consumption"]
    
    return df

# ── MODEL OPTİMİZASYONU & EĞİTİM ──────────────────────────────────────────────

def optimize_hyperparams(X_train, y_train):
    """Optuna ile en iyi parametreleri bulur."""
    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        }
        model = xgb.XGBRegressor(**params, random_state=42)
        tscv = TimeSeriesSplit(n_splits=3)
        scores = []
        for train_idx, val_idx in tscv.split(X_train):
            model.fit(X_train.iloc[train_idx], y_train.iloc[train_idx])
            preds = model.predict(X_train.iloc[val_idx])
            scores.append(mean_absolute_error(y_train.iloc[val_idx], preds))
        return np.mean(scores)

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=15)
    return study.best_params

def train_model(df: pd.DataFrame):
    df = engineer_features(df)
    
    extra_features = ["hour_sin", "hour_cos", "renewable_ratio", "consumption_error", "is_holiday"]
    all_features = FEATURE_COLS + extra_features
    
    df = df.dropna(subset=all_features + [TARGET_COL])
    X = df[all_features]
    y = df[TARGET_COL]

    # Time Series Split (Shuffle=False kuralı)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print("Hiperparametre optimizasyonu yapılıyor...")
    best_params = optimize_hyperparams(X_train, y_train)
    
    final_model = xgb.XGBRegressor(**best_params, random_state=42)
    final_model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    # Metriklerin Hesaplanması (Backtesting için gerekli)
    y_pred = final_model.predict(X_test)
    metrics = {
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape": np.mean(np.abs((y_test - y_pred) / y_test.replace(0, np.nan))) * 100,
        "trained_at": datetime.now().isoformat()
    }
    
    print(f"Model Eğitildi: MAE={metrics['mae']:.2f} TL/MWh")
    
    os.makedirs("models", exist_ok=True)
    joblib.dump(final_model, MODEL_PATH)
    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f)
        
    return final_model, metrics

# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df = load_features()
    model, metrics = train_model(df)
    print("İşlem başarıyla tamamlandı. Model ve metrikler kaydedildi.")