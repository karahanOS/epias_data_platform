import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from spark_utils import get_spark_session

if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! Lütfen YYYY-MM-DD formatında bir tarih gönderin.")

target_date = sys.argv[1]  

spark = get_spark_session("epias_bronze_to_silver_ptf")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"

# --- DOĞRU KLASÖR YÖNLENDİRMESİ ---
if target_date == "ALL":
    BRONZE_PATH = f"gs://{BUCKET}/bronze/ptf/"
else:
    BRONZE_PATH = f"gs://{BUCKET}/bronze/ptf/{target_date}.parquet"

SILVER_PATH = f"gs://{BUCKET}/silver/ptf/"

print(f"{target_date} tarihi için Bronze okunuyor: {BRONZE_PATH}")
df = spark.read.parquet(BRONZE_PATH)

print(f"Bronze kayıt sayısı: {df.count()}")

df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["date", "price"]) \
    .withColumn(
        "date", F.to_utc_timestamp(F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "Europe/Istanbul")
    ) \
    .withColumn("hour", F.lpad(F.col("hour"), 5, "0")) \
    .withColumn("price", F.round(F.col("price").cast(DoubleType()), 2)) \
    .withColumn("price_usd", F.round(F.col("priceUsd").cast(DoubleType()), 4)) \
    .drop("priceUsd", "priceEur") \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day", F.dayofmonth(F.col("date")))

print(f"\nSilver'a yazılıyor: {SILVER_PATH}")

df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_PATH)

print(f"{target_date} için Bronze → Silver dönüşümü tamamlandı!")
spark.stop()