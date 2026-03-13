from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── SPARK SESSION ─────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("epias_bronze_to_silver_smf") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/credentials/gcp-key.json") \
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── CONFIG ────────────────────────────────────────────────────────────────────

BUCKET = "epias-data-lake"
BRONZE_PATH = f"gs://{BUCKET}/bronze/smf/"
SILVER_PATH = f"gs://{BUCKET}/silver/smf/"

# ── 1. BRONZE'U OKU ───────────────────────────────────────────────────────────

print("Bronze okunuyor...")
df = spark.read.parquet(BRONZE_PATH)

print(f"Bronze kayıt sayısı: {df.count()}")
df.printSchema()
df.show(5)

# ── 2. DÖNÜŞÜMLER ─────────────────────────────────────────────────────────────

print("Dönüşümler uygulanıyor...")

df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["date", "systemMarginalPrice"]) \
    .withColumn(
        "date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")
    ) \
    .withColumn(
        "hour", F.lpad(F.col("hour"), 5, "0")
    ) \
    .withColumn(
        # snake_case standardizasyonu
        "system_marginal_price", F.round(F.col("systemMarginalPrice").cast(DoubleType()), 2)
    ) \
    .drop("systemMarginalPrice") \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day", F.dayofmonth(F.col("date")))

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

print("Bronze → Silver SMF dönüşümü tamamlandı!")

spark.stop()