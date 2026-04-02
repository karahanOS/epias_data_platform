import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from spark_utils import get_spark_session

if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! Lütfen YYYY-MM-DD formatında bir tarih gönderin.")

target_date = sys.argv[1]
spark = get_spark_session("epias_bronze_to_silver_consumption")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"
BRONZE_PATH = f"gs://{BUCKET}/bronze/consumption/{target_date}.parquet"
SILVER_PATH = f"gs://{BUCKET}/silver/consumption/"

print(f"{target_date} tarihi için Bronze okunuyor: {BRONZE_PATH}")
df = spark.read.parquet(BRONZE_PATH)

print("Dönüşümler uygulanıyor...")
df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["date", "consumption"]) \
    .withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("hour", F.lpad(F.col("time"), 5, "0")) \
    .drop("time") \
    .withColumn("consumption", F.round(F.col("consumption").cast(DoubleType()), 2)) \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day", F.dayofmonth(F.col("date")))

print(f"\nSilver'a yazılıyor: {SILVER_PATH}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_PATH)

print(f"{target_date} için Bronze → Silver Consumption dönüşümü tamamlandı!")
spark.stop()