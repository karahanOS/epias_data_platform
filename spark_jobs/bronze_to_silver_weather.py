from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# ── SPARK SESSION ─────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("epias_bronze_to_silver_weather") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/credentials/gcp-key.json") \
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── CONFIG ────────────────────────────────────────────────────────────────────

BUCKET = "epias-data-lake"
BRONZE_PATH = f"gs://{BUCKET}/bronze/weather/"
SILVER_PATH = f"gs://{BUCKET}/silver/weather/"

schema = StructType([
    StructField("datetime",                     StringType(), True),
    StructField("weighted_temperature_2m",      DoubleType(), True),
    StructField("weighted_wind_speed_10m",      DoubleType(), True),
    StructField("weighted_shortwave_radiation", DoubleType(), True),
    StructField("weighted_relative_humidity_2m",DoubleType(), True),
    StructField("istanbul_temp",                DoubleType(), True),
    StructField("istanbul_wind",                DoubleType(), True),
    StructField("istanbul_radiation",           DoubleType(), True),
    StructField("izmir_temp",                   DoubleType(), True),
    StructField("izmir_wind",                   DoubleType(), True),
    StructField("izmir_radiation",              DoubleType(), True),
    StructField("konya_temp",                   DoubleType(), True),
    StructField("konya_wind",                   DoubleType(), True),
    StructField("konya_radiation",              DoubleType(), True),
    StructField("ankara_temp",                  DoubleType(), True),
    StructField("ankara_wind",                  DoubleType(), True),
    StructField("ankara_radiation",             DoubleType(), True),
])

# ── 1. BRONZE'U OKU ───────────────────────────────────────────────────────────

print("Bronze okunuyor...")
df = spark.read.schema(schema).parquet(BRONZE_PATH)

print(f"Bronze kayıt sayısı: {df.count()}")
df.printSchema()
df.show(3)

# ── 2. DÖNÜŞÜMLER ─────────────────────────────────────────────────────────────

print("Dönüşümler uygulanıyor...")

df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["datetime", "weighted_temperature_2m"]) \
    .withColumn(
        "date",
        F.to_timestamp(F.col("datetime"), "yyyy-MM-dd'T'HH:mm:ssXXX")
    ) \
    .withColumn("hour", F.date_format(F.col("date"), "HH:mm")) \
    .drop("datetime") \
    .withColumn("year",  F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day",   F.dayofmonth(F.col("date")))

# Sayısal kolonları round
numeric_cols = [
    "weighted_temperature_2m", "weighted_wind_speed_10m",
    "weighted_shortwave_radiation", "weighted_relative_humidity_2m",
    "istanbul_temp", "istanbul_wind", "istanbul_radiation",
    "izmir_temp", "izmir_wind", "izmir_radiation",
    "konya_temp", "konya_wind", "konya_radiation",
    "ankara_temp", "ankara_wind", "ankara_radiation",
]

for col in numeric_cols:
    df_silver = df_silver.withColumn(col, F.round(F.col(col), 2))

# ── 3. NULL KONTROL ───────────────────────────────────────────────────────────

print("\nNull kontrol raporu:")
df_silver.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in ["date", "hour", "weighted_temperature_2m",
              "weighted_wind_speed_10m", "weighted_shortwave_radiation"]
]).show()

# ── 4. SILVER'A YAZ ───────────────────────────────────────────────────────────

print(f"\nSilver'a yazılıyor: {SILVER_PATH}")
print(f"Silver kayıt sayısı: {df_silver.count()}")
df_silver.show(5)

df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(SILVER_PATH)

print("Bronze → Silver Weather dönüşümü tamamlandı!")

spark.stop()