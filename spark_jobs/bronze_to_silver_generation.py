from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── SPARK SESSION ─────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("epias_bronze_to_silver_generation") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/credentials/gcp-key.json") \
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── CONFIG ────────────────────────────────────────────────────────────────────

BUCKET = "epias-data-lake"
BRONZE_PATH = f"gs://{BUCKET}/bronze/generation/"
SILVER_PATH = f"gs://{BUCKET}/silver/generation/"

# camelCase → snake_case kolon eşleştirmesi
COLUMN_MAPPING = {
    "naturalGas"     : "natural_gas",
    "dammedHydro"    : "dammed_hydro",
    "importCoal"     : "import_coal",
    "asphaltiteCoal" : "asphaltite_coal",
    "blackCoal"      : "black_coal",
    "importExport"   : "import_export",
    "wasteheat"      : "waste_heat",
    "fueloil"        : "fuel_oil",
}

# ── 1. BRONZE'U OKU ───────────────────────────────────────────────────────────

print("Bronze okunuyor...")
df = spark.read.parquet(BRONZE_PATH)

print(f"Bronze kayıt sayısı: {df.count()}")
df.printSchema()
df.show(5)

# ── 2. DÖNÜŞÜMLER ─────────────────────────────────────────────────────────────

print("Dönüşümler uygulanıyor...")

# Tüm sayısal kolonları double'a çevir ve 2 ondalık basamağa yuvarla
numeric_cols = [
    "total", "naturalGas", "dammedHydro", "lignite", "river",
    "importCoal", "wind", "sun", "fueloil", "geothermal",
    "asphaltiteCoal", "blackCoal", "biomass", "naphta", "lng",
    "importExport", "wasteheat"
]

df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["date", "total"])

# Sayısal kolonları dönüştür
for col in numeric_cols:
    df_silver = df_silver.withColumn(
        col, F.round(F.col(col).cast(DoubleType()), 2)
    )

# date ve hour dönüşümleri
df_silver = df_silver \
    .withColumn(
        "date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")
    ) \
    .withColumn(
        "hour", F.lpad(F.col("hour"), 5, "0")
    )

# camelCase → snake_case
for old_name, new_name in COLUMN_MAPPING.items():
    df_silver = df_silver.withColumnRenamed(old_name, new_name)

# Partition kolonları
df_silver = df_silver \
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

print("Bronze → Silver Generation dönüşümü tamamlandı!")

spark.stop()