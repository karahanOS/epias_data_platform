from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

def transform_generation():
    spark = SparkSession.builder.appName("BronzeToSilver_Generation").getOrCreate()

    # İki farklı bronze veriyi birleştirme stratejisi (Gerçek Zamanlı + Lisanssız)
    df_realtime = spark.read.parquet("gs://epias-data-lake/bronze/gen_realtime/*.parquet")
    df_unlicensed = spark.read.parquet("gs://epias-data-lake/bronze/unlicensed/*.parquet")

    # Lisanssız veriyi temizle
    df_unl_silver = df_unlicensed.select(
        F.to_date(F.col("date")).alias("date"),
        F.col("hour").cast(IntegerType()).alias("hour"),
        F.col("totalMwh").cast(DoubleType()).alias("unlicensed_total_mwh"),
        F.col("sunMwh").cast(DoubleType()).alias("unlicensed_solar_mwh"),
        F.col("windMwh").cast(DoubleType()).alias("unlicensed_wind_mwh")
    ).dropDuplicates(["date", "hour"])

    # Silver katmanına kaydet
    df_unl_silver.write.mode("overwrite").partitionBy("date").parquet("gs://epias-data-lake/silver/generation_unlicensed/")

    if __name__ == "__main__":
    transform_generation()