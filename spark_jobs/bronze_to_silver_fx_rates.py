import sys
from pyspark.sql.types import DoubleType, BooleanType, StringType, DateType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob


class FXRatesSilverJob(BaseEpiasSparkJob):
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_FXRates",
            source_name="fx_rates",
            primary_keys=["date"],
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty():
            self.logger.warning("FX bronze boş (%s) — silver yazılmıyor.", ds)
            return

        df = (
            df.withColumn("date",       F.to_date(F.col("date"), "yyyy-MM-dd"))
              .withColumn("usdtry",     F.col("usdtry").cast(DoubleType()))
              .withColumn("is_ffilled", F.col("is_ffilled").cast(BooleanType()))
              .withColumn("source",     F.col("source").cast(StringType()))
        )

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()


if __name__ == "__main__":
    FXRatesSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")
