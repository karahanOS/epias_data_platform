import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class AicSilverJob(BaseEpiasSparkJob):
    def __init__(self):
        super().__init__(app_name="BronzeToSilver_AIC", source_name="aic", primary_keys=["date"])

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return
        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        for col in df.columns:
            if col != "date": df = df.withColumn(col, F.col(col).cast(DoubleType()))
        self.write_silver(self.deduplicate(self.add_partition_columns(df, ds)))
        self.spark.stop()

if __name__ == "__main__":
    AicSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")