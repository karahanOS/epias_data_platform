import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from spark_utils import get_spark_session

if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! Lütfen YYYY-MM-DD formatında bir tarih gönderin.")

# ... [Önceki kısımlar aynı]
target_date = sys.argv[1]
spark = get_spark_session("epias_bronze_to_silver_smf")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"

# --- YENİ EKLENEN MANTIK ---
if target_date == "ALL":
    BRONZE_PATH = f"gs://{BUCKET}/bronze/smf/"
else:
    BRONZE_PATH = f"gs://{BUCKET}/bronze/smf/{target_date}.parquet"

SILVER_PATH = f"gs://{BUCKET}/silver/smf/"

# ... [Şema ve geri kalan dönüşümler tamamen aynı kalacak]

# --- 1. ŞEMAYA smpUsd EKLENDİ ---
schema = StructType([
    StructField("date", StringType(), True),
    StructField("hour", StringType(), True),
    StructField("systemMarginalPrice", DoubleType(), True),
    StructField("smpUsd", DoubleType(), True), # YENİ
])

print(f"{target_date} tarihi için Bronze okunuyor: {BRONZE_PATH}")
df = spark.read.schema(schema).parquet(BRONZE_PATH)

print("Dönüşümler uygulanıyor...")
df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["date", "systemMarginalPrice"]) \
    .withColumn("date", F.to_utc_timestamp(F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "Europe/Istanbul")) \
    .withColumn("hour", F.date_format(F.to_timestamp(F.col("hour"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "HH:mm")) \
    .withColumn("system_marginal_price", F.round(F.col("systemMarginalPrice").cast(DoubleType()), 2)) \
    .withColumn("smp_usd", F.round(F.col("smpUsd").cast(DoubleType()), 4)) \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day", F.dayofmonth(F.col("date")))

print(f"\nSilver'a yazılıyor: {SILVER_PATH}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_PATH)

print(f"{target_date} için Bronze → Silver SMF dönüşümü tamamlandı!")
spark.stop()