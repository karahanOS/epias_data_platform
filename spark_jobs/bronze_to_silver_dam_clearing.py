# /opt/airflow/src/spark/bronze_to_silver_dam_clearing.py
# spark_jobs/bronze_to_silver_dam_clearing.py

import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class DamClearingSilverJob(BaseEpiasSparkJob):
    """
    Gün Öncesi Piyasası (GÖP) Eşleşme (Clearing) verilerini işler.
    Bu veri dbt katmanında GÖP hacim ve piyasa katılım analizlerinde kullanılacaktır.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_DamClearing",
            source_name="dam_clearing",
            primary_keys=["date"]
        )

    def run(self, ds: str):
        # 1. Okuma (Extract)
        try:
            df = self.read_bronze(ds)
        except Exception as e:
            self.logger.error(f"Veri okuma hatası: {e}")
            self.spark.stop()
            return

        if df.rdd.isEmpty():
            self.logger.warning(f"Bronze veri boş: {ds}. İşlem atlanıyor.")
            self.spark.stop()
            return

        # 2. Dönüşüm ve Şema Dayatması (Transform)
        self.logger.info("Dam Clearing verisi için tipler dönüştürülüyor...")
        
        # Tarih formatını standartlaştır
        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        
        # Miktar ve hacim kolonlarını güvenli bir şekilde Double'a cast et
        numeric_cols = [
            "matchedBidsQuantity", 
            "matchedOffersQuantity", 
            "blockBidQuantity", 
            "blockOfferQuantity"
        ]
        
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        # 3. Ortak İşlemler (Partitioning & Deduplication)
        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)

        # 4. Yazma (Load - Dynamic Overwrite)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = DamClearingSilverJob()
    job.run(target_ds)