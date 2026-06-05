"""
ptf_forecaster.py — XGBoost ile Gelişmiş PTF Fiyat Tahmin Modeli (v3.0)
================================================================================
Bu model, BigQuery'deki dbt Gold tablolarından (Özellikle 'Forecasted Residual Load' 
ve 'Supply Shock Index') beslenerek ertesi günün PTF fiyatlarını tahminler.
"""

import os
import logging
import pandas as pd
import numpy as np
import xgboost as xgb
from google.cloud import bigquery
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
import joblib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PTFForecaster")

class PredictivePTFForecaster:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID", "epias-data-platform")
        self.dataset_id = os.getenv("BQ_GOLD_DATASET", "epias_gold")
        self.client = bigquery.Client(project=self.project_id)
        self.model_path = "models/ptf_advanced_xgb_model.joblib"
        os.makedirs("models", exist_ok=True)

    def extract_gold_data(self) -> pd.DataFrame:
        """Yeni dbt Gold tablolarından Gelecek Kalan Yük ve Arz Şoku verilerini çeker."""
        logger.info("BigQuery'den Gold (Predictive) veriler çekiliyor...")
        
        query = f"""
        SELECT
            f.date,
            f.ptf_try,
            f.forecasted_residual_load_mwh,
            f.price_independent_bid_mwh,
            s.total_available_capacity_mwh,
            s.total_outage_mwh,
            s.supply_shock_index
        FROM `{self.project_id}.{self.dataset_id}.mart_forecasted_residual_load` f
        LEFT JOIN `{self.project_id}.{self.dataset_id}.mart_supply_shock_index` s
            ON f.date = s.date
        ORDER BY f.date ASC
        """
        df = self.client.query(query).to_dataframe()
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df

    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Zaman serisi özelliklerini (Lags & Rolling) üretir."""
        logger.info("Özellik Mühendisliği (Feature Engineering) uygulanıyor...")
        
        # Takvim Özellikleri
        df['hour'] = df.index.hour
        df['day_of_week'] = df.index.dayofweek
        df['month'] = df.index.month
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        # Geçmiş Fiyat Gecikmeleri (T-24, T-168)
        df['ptf_lag_24h'] = df['ptf_try'].shift(24)
        df['ptf_lag_168h'] = df['ptf_try'].shift(168)

        # Arz şoku 7 günlük hareketli ortalama trendi
        df['supply_shock_trend_7d'] = df['supply_shock_index'].rolling(window=168).mean()

        # NaN değerleri temizle (İlk 168 saat laglardan dolayı boştur)
        df.dropna(inplace=True)
        return df

    def train_and_evaluate(self, df: pd.DataFrame):
        """XGBoost modelini eğitir ve Feature Importance (Özellik Önemi) çıkarır."""
        logger.info("Model eğitimi başlıyor...")
        
        target = 'ptf_try'
        features = [col for col in df.columns if col != target]
        
        # Son 30 Günü Test Seti Olarak Ayır (Time Series Split)
        split_date = df.index.max() - pd.Timedelta(days=30)
        
        X_train, y_train = df.loc[df.index < split_date, features], df.loc[df.index < split_date, target]
        X_test, y_test = df.loc[df.index >= split_date, features], df.loc[df.index >= split_date, target]
        
        self.model = xgb.XGBRegressor(
            n_estimators=800,
            learning_rate=0.03,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            objective='reg:squarederror'
        )
        
        self.model.fit(X_train, y_train, eval_set=[(X_train, y_train), (X_test, y_test)], verbose=100)
        
        # Değerlendirme
        preds = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, preds)
        mape = mean_absolute_percentage_error(y_test, preds)
        
        logger.info(f"✅ Eğitim Bitti! Test MAE: {mae:.2f} TRY | MAPE: %{mape*100:.2f}")
        
        # Feature Importance
        importance_df = pd.DataFrame({'col_name': features, 'feature_importance_vals': self.model.feature_importances_})
        importance_df = importance_df.sort_values(by='feature_importance_vals', ascending=False)
        importance_df.to_csv("models/ptf_shap_importance.csv", index=False)
        
        logger.info(f"🥇 En Önemli 3 Özellik:\n{importance_df.head(3).to_string(index=False)}")
        joblib.dump(self.model, self.model_path)

    def run(self):
        df_raw = self.extract_gold_data()
        df_engineered = self.engineer_features(df_raw)
        self.train_and_evaluate(df_engineered)

if __name__ == "__main__":
    PredictivePTFForecaster().run()