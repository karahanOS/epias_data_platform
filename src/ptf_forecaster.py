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
        """mart_ml_features'dan genişletilmiş feature store'u çeker.

        Kaynak: mart_ml_features (Tschora et al. 2022 + Karatekin & Başaran
        önerileriyle genişletildi) — GÖP hacimleri, AIC, gerçekleşen üretim
        karışımı, imbalance ve GİP-GÖP spread dahil.
        """
        logger.info("BigQuery'den mart_ml_features çekiliyor...")

        query = f"""
        SELECT
            TIMESTAMP(DATETIME(date, TIME(hour, 0, 0)))  AS ts,
            ptf_try,
            smf_try,
            ptf_smf_spread,
            forecasted_load_mwh,
            forecasted_res_mwh,
            forecasted_residual_load_mwh,
            gop_matched_bids_mwh,
            gop_matched_offers_mwh,
            gop_volume_imbalance_mwh,
            total_outage_mwh,
            total_aic_mwh,
            capacity_utilization_ratio,
            actual_renewable_mwh,
            actual_renewable_ratio,
            net_imbalance_mwh,
            avg_gip_price_try,
            total_gip_volume_mwh,
            gip_gop_price_spread,
            hour,
            day_of_week,
            month
        FROM `{self.project_id}.{self.dataset_id}.mart_ml_features`
        ORDER BY date, hour
        """
        df = self.client.query(query).to_dataframe()
        df['ts'] = pd.to_datetime(df['ts'])
        df = df.set_index('ts').sort_index()
        return df

    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Zaman serisi özelliklerini (Lags, Rolling, Cyclic encoding) üretir.

        Tschora et al. (Applied Energy 2022): lag-24h ve lag-168h en güçlü
        single features; GÖP imbalance consistently top-3.
        Karatekin & Başaran: sin/cos encoding sezonal örüntüleri yakalar.
        Maciejowska et al.: GİP-GÖP spread ertesi gün PTF yönünü sinyaller.
        """
        logger.info("Özellik Mühendisliği (Feature Engineering) uygulanıyor...")

        # ── Geçmiş Fiyat Gecikmeleri ──────────────────────────────────────
        df['ptf_lag_24h']  = df['ptf_try'].shift(24)
        df['ptf_lag_48h']  = df['ptf_try'].shift(48)
        df['ptf_lag_168h'] = df['ptf_try'].shift(168)

        # ── Hareketli Ortalamalar & Volatilite ───────────────────────────
        df['ptf_rolling_avg_24h']  = df['ptf_try'].shift(1).rolling(24).mean()
        df['ptf_rolling_avg_168h'] = df['ptf_try'].shift(1).rolling(168).mean()
        df['ptf_rolling_std_24h']  = df['ptf_try'].shift(1).rolling(24).std()

        # ── GÖP İmbalance Lag (T-24) ─────────────────────────────────────
        # Tschora: bir önceki günün volume imbalance'ı ertesi gün PTF'i etkiler
        df['gop_imbalance_lag_24h'] = df['gop_volume_imbalance_mwh'].shift(24)

        # ── Kapasite Kullanım Trendi ──────────────────────────────────────
        df['cap_util_rolling_7d'] = (
            df['capacity_utilization_ratio'].shift(1).rolling(168).mean()
        )

        # ── Siklik (Cyclic) Zaman Encoding ────────────────────────────────
        # Karatekin & Başaran: sin/cos categorical'dan daha iyi genelleştirir
        df['hour_sin']  = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos']  = np.cos(2 * np.pi * df['hour'] / 24)
        df['dow_sin']   = np.sin(2 * np.pi * df['day_of_week'] / 7)
        df['dow_cos']   = np.cos(2 * np.pi * df['day_of_week'] / 7)
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

        # ── İkili Göstergeler ─────────────────────────────────────────────
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)

        # ── Saat × Gün Etkileşimi ──────────────────────────────────────────
        # Türkiye'de sabah zirvesi (8-10) ve akşam zirvesi (18-20) hafta içi
        # önemli ölçüde farklı → interaksiyon terimi
        df['hour_x_dow'] = df['hour'] * df['day_of_week']

        # ── NaN temizle (ilk 168 saat laglardan boş) ────────────────────
        df.dropna(inplace=True)
        return df

    def train_and_evaluate(self, df: pd.DataFrame):
        """XGBoost modelini Walk-Forward CV ile eğitir, feature importance kaydeder.

        Walk-Forward (expanding window) split: Tschora et al. ve Karatekin &
        Başaran'ın önerdiği time-series-aware değerlendirme yaklaşımı.
        Sabit son-30-gün test yerine 3-fold WF → daha güvenilir MAE/MAPE.
        """
        logger.info("Model eğitimi başlıyor (Walk-Forward CV)...")

        # Ham zaman sütunlarını modele verme
        drop_cols = {'ptf_try', 'hour', 'day_of_week', 'month',
                     'smf_try',        # hedef ile neredeyse aynı zamanda bilinen değil
                     'avg_gip_price_try', 'gip_gop_price_spread',  # günlük → timestamp leak riski
                     }
        target   = 'ptf_try'
        features = [c for c in df.columns if c not in drop_cols]

        # ── Walk-Forward: 3 fold, her fold son 30 gün ────────────────────
        fold_metrics = []
        for fold in range(3, 0, -1):
            test_end   = df.index.max() - pd.Timedelta(days=30 * (fold - 1))
            test_start = test_end - pd.Timedelta(days=30)

            X_tr = df.loc[df.index < test_start, features]
            y_tr = df.loc[df.index < test_start, target]
            X_te = df.loc[(df.index >= test_start) & (df.index < test_end), features]
            y_te = df.loc[(df.index >= test_start) & (df.index < test_end), target]

            if len(X_tr) < 500 or len(X_te) < 24:
                continue

            m = xgb.XGBRegressor(
                n_estimators=800,
                learning_rate=0.03,
                max_depth=6,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                objective='reg:squarederror',
            )
            m.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)

            preds = m.predict(X_te)
            mae   = mean_absolute_error(y_te, preds)
            rmse  = mean_squared_error(y_te, preds) ** 0.5
            mape  = mean_absolute_percentage_error(y_te, preds)
            fold_metrics.append({'fold': fold, 'mae': mae, 'rmse': rmse, 'mape': mape})
            logger.info(f"  Fold {fold}: MAE={mae:.2f} TL | RMSE={rmse:.2f} | MAPE={mape*100:.2f}%")

        if fold_metrics:
            avg_mae  = np.mean([f['mae']  for f in fold_metrics])
            avg_mape = np.mean([f['mape'] for f in fold_metrics])
            logger.info(f"✅ WF-CV Ortalama → MAE: {avg_mae:.2f} TL | MAPE: %{avg_mape*100:.2f}")

        # ── Final model: tüm veri ile eğit ───────────────────────────────
        logger.info("Final model (tüm veri) eğitiliyor...")
        self.model = xgb.XGBRegressor(
            n_estimators=800,
            learning_rate=0.03,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            objective='reg:squarederror',
        )
        self.model.fit(df[features], df[target], verbose=False)

        # ── Feature Importance ────────────────────────────────────────────
        importance_df = pd.DataFrame({
            'col_name': features,
            'feature_importance_vals': self.model.feature_importances_,
        }).sort_values('feature_importance_vals', ascending=False)
        importance_df.to_csv("models/ptf_shap_importance.csv", index=False)
        logger.info(f"🥇 En Önemli 5 Feature:\n{importance_df.head(5).to_string(index=False)}")

        joblib.dump(self.model, self.model_path)
        logger.info(f"💾 Model kaydedildi: {self.model_path}")

    def run(self):
        df_raw = self.extract_gold_data()
        df_engineered = self.engineer_features(df_raw)
        self.train_and_evaluate(df_engineered)

if __name__ == "__main__":
    PredictivePTFForecaster().run()