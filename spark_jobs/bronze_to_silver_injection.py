# spark_jobs/bronze_to_silver_injection.py

import sys
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class InjectionSilverJob(BaseEpiasSparkJob):
    """
    Uzlaştırmaya Esas Veriş Miktarı (UEVM / Injection) verilerini işler.
    Gerçekleşen üretim ve tüketim sapmalarını bulmak için hayati önem taşır.
    """
    def __init__(self):
        # Eski: primary_keys=["date", "organizationId", "uevcbId"]
        super().__init__(app_name="BronzeToSilver_Injection", source_name="injection", primary_keys=["date", "hour"])

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

        self.logger.info("Gerçekleşen Üretim (Injection) tipleri dönüştürülüyor...")
        
        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        
        if "total" in df.columns:
            df = df.withColumn("total", F.col("total").cast(DoubleType()))
            
        for col_name in ["organizationId", "uevcbId"]:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(LongType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = InjectionSilverJob()
    job.run(target_ds)