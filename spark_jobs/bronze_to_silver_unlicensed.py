# spark_jobs/bronze_to_silver_unlicensed.py

import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class UnlicensedSilverJob(BaseEpiasSparkJob):
    """
    Lisanssız Üretim (Güneş, Rüzgar, Biyokütle vb.) verilerini işler.
    Bu veri, yenilenebilir enerjinin şebekeye olan maliyet/hacim etkisini ölçmek için kullanılır.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_Unlicensed",
            source_name="unlicensed",
            primary_keys=["date"]
        )

    def run(self, ds: str):
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

        self.logger.info("Lisanssız üretim tipleri dönüştürülüyor...")
        
        # Tarih standardizasyonu
        df = df.withColumn("date", self.parse_epias_timestamp())
        
        # Üretim tiplerini topluca Double yapıyoruz
        generation_types = ["solar", "wind", "biogas", "biomass", "canalType", "river", "total"]
        
        for gen_col in generation_types:
            if gen_col in df.columns:
                df = df.withColumn(gen_col, F.col(gen_col).cast(DoubleType()))
                
        # (Opsiyonel Veri Kalitesi Kuralı): Negatif üretim olamaz, null veya negatifleri 0 yap
        # df = df.fillna(0.0, subset=generation_types)

        # Base işlemler
        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)

        # Dynamic overwrite ile Silver'a gönder
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = UnlicensedSilverJob()
    job.run(target_ds)