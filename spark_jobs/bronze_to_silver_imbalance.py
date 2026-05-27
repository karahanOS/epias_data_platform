# spark_jobs/bronze_to_silver_imbalance.py

import sys
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class ImbalanceSilverJob(BaseEpiasSparkJob):
    """
    Sistem Dengesizlik miktarını ve genel üretim/tüketim farklarını işler.
    "DGP'de sistem yönü neydi?" sorusunu Gold katmanında çözmek için kullanılır.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_Imbalance",
            source_name="imbalance",
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

        self.logger.info("Dengesizlik (Imbalance) verileri dönüştürülüyor...")
        
        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        
        numeric_cols = ["generationTotal", "consumption", "imbalanceQuantity"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
                
        if "systemDirection" in df.columns:
            df = df.withColumn("systemDirection", F.col("systemDirection").cast(StringType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = ImbalanceSilverJob()
    job.run(target_ds)