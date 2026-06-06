import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class ResForecastSilverJob(BaseEpiasSparkJob):
    def __init__(self):
        super().__init__(app_name="BronzeToSilver_ResForecast", source_name="res_forecast", primary_keys=["date"])

    def run(self, ds: str):
        # Bronze Parquet may contain TIMESTAMP(NANOS,true) if the date column was
        # written from a timezone-aware pandas DatetimeIndex.  nanosAsLong converts
        # it to bigint (nanos since epoch) instead of throwing AnalysisException.
        self.spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        # Handle both: ISO-string date and bigint nanoseconds-since-epoch date.
        date_type = dict(df.dtypes).get("date", "")
        if date_type in ("bigint", "long"):
            df = df.withColumn("date", (F.col("date").cast("double") / 1_000_000_000).cast("timestamp"))
        else:
            df = df.withColumn("date", self.parse_epias_timestamp())

        for col in df.columns:
            if col != "date":
                df = df.withColumn(col, F.col(col).cast(DoubleType()))

        self.write_silver(self.deduplicate(self.add_partition_columns(df, ds)))
        self.spark.stop()

if __name__ == "__main__":
    ResForecastSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")