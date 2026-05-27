# spark_jobs/bronze_to_silver_dpp.py

import sys
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class DppSilverJob(BaseEpiasSparkJob):
    """
    Kesinleşmiş Günlük Üretim Planı (KGÜP / DPP) verilerini işler.
    "Şirketler ne kadar üretim planladı?" sorusunun temelini oluşturur.
    """
    def __init__(self):
        # Eski: primary_keys=["date", "organizationId", "uevcbId"]
        super().__init__(app_name="BronzeToSilver_DPP", source_name="dpp", primary_keys=["date", "time"])

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

        self.logger.info("DPP (KGÜP) verisi için tipler dönüştürülüyor...")
        
        # Tip Dönüşümleri
        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        
        if "total" in df.columns:
            df = df.withColumn("total", F.col("total").cast(DoubleType()))
            
        for col_name in ["organizationId", "uevcbId"]:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(LongType()))

        # Base işlemler ve GCS'e yazma
        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = DppSilverJob()
    job.run(target_ds)