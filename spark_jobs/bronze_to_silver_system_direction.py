import sys
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class SystemDirectionSilverJob(BaseEpiasSparkJob):
    """
    Sistem Yönü verilerini işler. (Dengeleme Güç Piyasası)
    Sistemin enerji açığı mı yoksa fazlası mı verdiğini gösterir.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_SystemDirection",
            source_name="system_direction",
            primary_keys=["date", "hour"] # Saatlik bazda tekilleştirme
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): 
            self.logger.warning(f"{ds} tarihi için bronze katmanında veri bulunamadı.")
            return

        # Zaman formatını düzeltme
        if "date" in df.columns:
            df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))

        # Akıllı tekilleştirme ve yazma
        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    SystemDirectionSilverJob().run(target_ds)