from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os

# ── SPARK SESSION ─────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("epias_bronze_to_silver_ptf") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/credentials/gcp-key.json") \
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── CONFIG ────────────────────────────────────────────────────────────────────

BUCKET = "epias-data-lake"
BRONZE_PATH = f"gs://{BUCKET}/bronze/ptf/"
SILVER_PATH = f"gs://{BUCKET}/silver/ptf/"

# ── 1. BRONZE'U OKU ───────────────────────────────────────────────────────────

print("Bronze okunuyor...")
df = spark.read.parquet(BRONZE_PATH)

print(f"Bronze kayıt sayısı: {df.count()}")
print("Bronze schema:")
df.printSchema()
df.show(5)

# ── 2. DÖNÜŞÜMLER ─────────────────────────────────────────────────────────────

print("Dönüşümler uygulanıyor...")

df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["date", "price"]) \
    .withColumn(
        # date: string → timestamp
        # EPIAS formatı: "2024-01-01T00:00:00+03:00"
        "date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")
    ) \
    .withColumn(
        # hour: "00:00" formatını standart tut, 24 saatlik sisteme çevir
        "hour", F.lpad(F.col("hour"), 5, "0")
    ) \
    .withColumn(
        # price: 2 ondalık basamak
        "price", F.round(F.col("price").cast(DoubleType()), 2)
    ) \
    .withColumn(
        # priceUsd → price_usd (snake_case standardizasyonu)
        "price_usd", F.round(F.col("priceUsd").cast(DoubleType()), 4)
    ) \
    .withColumn(
        # priceEur → price_eur
        "price_eur", F.round(F.col("priceEur").cast(DoubleType()), 4)
    ) \
    .drop("priceUsd", "priceEur") \
    .withColumn(
        # partition için yıl/ay/gün kolonları ekle
        "year", F.year(F.col("date"))
    ) \
    .withColumn(
        "month", F.month(F.col("date"))
    ) \
    .withColumn(
        "day", F.dayofmonth(F.col("date"))
    )

# ── 3. NULL KONTROL RAPORU ────────────────────────────────────────────────────

print("\nNull kontrol raporu:")
df_silver.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_silver.columns
]).show()

# ── 4. SILVER'A YAZ ───────────────────────────────────────────────────────────

print(f"\nSilver'a yazılıyor: {SILVER_PATH}")
print(f"Silver kayıt sayısı: {df_silver.count()}")

df_silver.show(5)

df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(SILVER_PATH)

print("Bronze → Silver dönüşümü tamamlandı!")

spark.stop()