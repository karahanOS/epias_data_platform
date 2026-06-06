import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class ConsumptionSilverJob(BaseEpiasSparkJob):
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_Consumption", 
            source_name="consumption", 
            primary_keys=["date", "hour"]
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        # Şema düzeltme (Swagger'daki isimlendirmelere göre)
        if "actualConsumption" in df.columns:
            df = df.withColumnRenamed("actualConsumption", "consumption")

        # Zaman dönüşümü
        df = df.withColumn("date", self.parse_epias_timestamp())
        
        # Sayısal alanları cast et
        df = df.withColumn("consumption", F.col("consumption").cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    ConsumptionSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")