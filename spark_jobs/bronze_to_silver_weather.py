import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class WeatherSilverJob(BaseEpiasSparkJob):
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_Weather", 
            source_name="weather", 
            primary_keys=["date", "city"]
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        # Kolon ismi düzeltme
        if "datetime" in df.columns:
            df = df.withColumnRenamed("datetime", "date")

        # Güvenli Zaman Dönüşümü
        df = df.withColumn(
            "date", 
            F.coalesce(
                F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                F.to_timestamp(F.col("date"), "yyyy-MM-dd HH:mm:ss"),
                F.col("date").cast("timestamp")
            )
        )

        # Sayısal alanları cast et
        numeric_cols = ["temperature_2m", "wind_speed_10m", "shortwave_radiation", "relative_humidity_2m"]
        for col in numeric_cols:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    WeatherSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")