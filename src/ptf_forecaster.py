"""
PTF Day-Ahead Forecasting
Yarınki saatlik elektrik fiyatlarını tahmin eder.
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
import warnings
warnings.filterwarnings("ignore")

# ── CONFIG ────────────────────────────────────────────────────────────────────

PROJECT    = "epias-data-platform"
DATASET    = "epias_dbt_marts"
TABLE      = "mart_ptf_lag_features"
MODEL_PATH = "models/ptf_xgb_model.joblib"
METRICS_PATH = "models/ptf_model_metrics.json"

FEATURE_COLS = [
    # Zaman özellikleri
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "month_of_year",
    # Hava durumu
    "temperature",
    "wind_speed",
    "solar_radiation",
    "humidity",
    # Üretim
    "wind_generation",
    "solar_generation",
    "hydro_generation",
    "gas_generation",
    "total_generation",
    # Tüketim
    "actual_consumption",
    #"forecast_consumption",
    # Lag features (dbt'den geliyor)
    "ptf_lag_1h",
    "ptf_lag_24h",
    "ptf_lag_168h",
    "ptf_rolling_avg_24h",
    "ptf_rolling_avg_168h",
    "ptf_rolling_max_24h",
    "ptf_rolling_min_24h",
]

TARGET_COL = "ptf"

# ── BIGQUERY'DEN VERİ ÇEK ────────────────────────────────────────────────────

def get_bq_client():
    creds_path = "credentials/gcp-key.json"
    if os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=PROJECT, credentials=credentials)
    return bigquery.Client(project=PROJECT)


def load_features() -> pd.DataFrame:
    print("BigQuery'den feature tablosu yükleniyor...")
    client = get_bq_client()

    query = f"""
        SELECT
            date,
            {', '.join(FEATURE_COLS)},
            {TARGET_COL}
        FROM `{PROJECT}.{DATASET}.{TABLE}`
        ORDER BY date
    """

    df = client.query(query).to_dataframe()
    print(f"Toplam kayıt: {len(df)}")
    print(f"Tarih aralığı: {df['date'].min()} → {df['date'].max()}")
    return df


# ── FEATURE ENGİNEERİNG ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Ek feature'lar ekle."""

    # Mevsim encoding (one-hot yerine cyclical)
    df["month_sin"] = np.sin(2 * np.pi * df["month_of_year"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month_of_year"] / 12)
    df["hour_sin"]  = np.sin(2 * np.pi * df["hour_of_day"] / 24)
    df["hour_cos"]  = np.cos(2 * np.pi * df["hour_of_day"] / 24)
    df["dow_sin"]   = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"]   = np.cos(2 * np.pi * df["day_of_week"] / 7)

    # Yenilenebilir oran
    df["renewable_ratio"] = (
        df["wind_generation"] + df["solar_generation"] + df["hydro_generation"]
    ) / df["total_generation"].replace(0, np.nan)

    # PTF volatilite
    df["ptf_range_24h"] = df["ptf_rolling_max_24h"] - df["ptf_rolling_min_24h"]

    return df

# ── MODEL EĞİTİMİ ─────────────────────────────────────────────────────────────

def train_model(df: pd.DataFrame):
    """XGBoost modeli eğit — Time Series Cross Validation ile."""

    df = engineer_features(df)

    # Ek feature kolonları
    extra_features = [
        "month_sin", "month_cos", "hour_sin", "hour_cos",
        "dow_sin", "dow_cos", "renewable_ratio",
        "ptf_range_24h"
    ]
    all_features = FEATURE_COLS + extra_features

    # Null temizliği
    df = df.dropna(subset=all_features + [TARGET_COL])

    X = df[all_features]
    y = df[TARGET_COL]

    print(f"\nEğitim veri seti: {len(X)} kayıt, {len(all_features)} feature")

    # Time Series Cross Validation — shuffle ETME, zaman sırası önemli
    tscv = TimeSeriesSplit(n_splits=5)

    cv_scores = []
    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model = xgb.XGBRegressor(
            n_estimators=500,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            early_stopping_rounds=50,
            random_state=42,
            verbosity=0,
        )

        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )

        y_pred = model.predict(X_val)
        mae = mean_absolute_error(y_val, y_pred)
        cv_scores.append(mae)
        print(f"  Fold {fold+1}: MAE = {mae:.2f} TL/MWh")

    print(f"\nOrtalama CV MAE: {np.mean(cv_scores):.2f} ± {np.std(cv_scores):.2f} TL/MWh")

    # Final model — tüm veri ile eğit
    print("\nFinal model eğitiliyor...")

    # Train/test split — son %20 test
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    final_model = xgb.XGBRegressor(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        early_stopping_rounds=50,
        random_state=42,
        verbosity=0,
    )

    final_model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # Test metrikleri
    y_pred_test = final_model.predict(X_test)
    mae  = mean_absolute_error(y_test, y_pred_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
    r2   = r2_score(y_test, y_pred_test)
    mape = np.mean(np.abs((y_test - y_pred_test) / y_test.replace(0, np.nan))) * 100

    print(f"\n── Test Metrikleri ──────────────────")
    print(f"MAE  : {mae:.2f} TL/MWh")
    print(f"RMSE : {rmse:.2f} TL/MWh")
    print(f"R²   : {r2:.4f}")
    print(f"MAPE : {mape:.2f}%")

    # Feature importance
    importance = pd.DataFrame({
        "feature": all_features,
        "importance": final_model.feature_importances_
    }).sort_values("importance", ascending=False)

    print(f"\n── Top 10 Feature ───────────────────")
    print(importance.head(10).to_string(index=False))

    # Modeli kaydet
    os.makedirs("models", exist_ok=True)
    joblib.dump(final_model, MODEL_PATH)
    print(f"\nModel kaydedildi: {MODEL_PATH}")

    # Metrikleri kaydet
    metrics = {
        "trained_at": datetime.now().isoformat(),
        "n_samples": len(X),
        "n_features": len(all_features),
        "cv_mae_mean": round(float(np.mean(cv_scores)), 2),
        "cv_mae_std": round(float(np.std(cv_scores)), 2),
        "test_mae": round(float(mae), 2),
        "test_rmse": round(float(rmse), 2),
        "test_r2": round(float(r2), 4),
        "test_mape": round(float(mape), 2),
        "top_features": importance.head(10)["feature"].tolist(),
    }

    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"Metrikler kaydedildi: {METRICS_PATH}")

    return final_model, metrics


# ── TAHMİN ────────────────────────────────────────────────────────────────────

def predict_next_day(model, df: pd.DataFrame) -> pd.DataFrame:
    """Son mevcut veriden yarınki 24 saati tahmin eder."""

    df = engineer_features(df)
    extra_features = [
        "month_sin", "month_cos", "hour_sin", "hour_cos",
        "dow_sin", "dow_cos", "renewable_ratio",
        "ptf_range_24h"
    ]
    all_features = FEATURE_COLS + extra_features

    # Son 24 saatin verisini al — yarın için proxy
    last_day = df.dropna(subset=all_features).tail(24)

    if len(last_day) < 24:
        print("Uyarı: Yeterli veri yok, mevcut veri kullanılıyor.")

    X_pred = last_day[all_features].copy()

    # Saat bilgisini bir gün ilerlet
    next_day = last_day["date"].max() + timedelta(days=1)
    X_pred["hour_of_day"] = list(range(24))[-len(X_pred):]

    predictions = model.predict(X_pred)

    result = pd.DataFrame({
        "predicted_date": pd.date_range(
            start=next_day.replace(hour=0),
            periods=len(predictions),
            freq="h"
        ),
        "hour": [f"{h:02d}:00" for h in range(len(predictions))],
        "predicted_ptf": predictions.round(2),
    })

    return result


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Veriyi yükle
    df = load_features()

    # Null kontrol
    print("\nNull sayıları:")
    print(df[FEATURE_COLS].isnull().sum())
    print(f"\nTüm feature'lar dolu olan satır: {df[FEATURE_COLS].dropna().shape[0]}")

    # Modeli eğit
    model, metrics = train_model(df)

    # Yarını tahmin et
    print("\n── Yarın İçin PTF Tahmini ───────────")
    predictions = predict_next_day(model, df)
    print(predictions.to_string(index=False))

    # Tahminleri CSV'ye kaydet
    predictions.to_csv("models/next_day_predictions.csv", index=False)
    print("\nTahminler kaydedildi: models/next_day_predictions.csv")