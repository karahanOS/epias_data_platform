import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from spark_utils import get_spark_session

# ── 1. DIŞARIDAN GELEN TARİH PARAMETRESİNİ YAKALA ─────────────────────────────
if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! Lütfen YYYY-MM-DD formatında bir tarih gönderin.")

target_date = sys.argv[1]  # Airflow'dan gelecek olan tarih (Örn: "2024-01-03")

spark = get_spark_session("epias_bronze_to_silver_ptf")
spark.sparkContext.setLogLevel("WARN")

# ── CONFIG ────────────────────────────────────────────────────────────────────
BUCKET = "epias-data-lake"

# DİKKAT: Artık tüm klasörü değil, SADECE o günün dosyasını okuyoruz!
# Senin save_to_gcs fonksiyonun dosyaları "2024-01-03.parquet" formatında kaydediyordu.
BRONZE_PATH = f"gs://{BUCKET}/bronze/ptf/{target_date}.parquet"
SILVER_PATH = f"gs://{BUCKET}/silver/ptf/"

# ── 2. BRONZE'U OKU ───────────────────────────────────────────────────────────
print(f"{target_date} tarihi için Bronze okunuyor: {BRONZE_PATH}")
df = spark.read.parquet(BRONZE_PATH)

print(f"Bronze kayıt sayısı: {df.count()}")

# ── 3. DÖNÜŞÜMLER (Aynı kalıyor) ──────────────────────────────────────────────
df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["date", "price"]) \
    .withColumn(
        "date", F.to_utc_timestamp(F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "Europe/Istanbul")
    ) \
    .withColumn("hour", F.lpad(F.col("hour"), 5, "0")) \
    .withColumn("price", F.round(F.col("price").cast(DoubleType()), 2)) \
    .withColumn("price_usd", F.round(F.col("mcpUsd").cast(DoubleType()), 4)) \
    .withColumn("price_eur", F.round(F.col("mcpEur").cast(DoubleType()), 4)) \
    .drop("priceUsd", "priceEur") \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day", F.dayofmonth(F.col("date"))) # Gün kırılımı önemli!

# ── 4. SILVER'A YAZ (Dinamik Ezme) ────────────────────────────────────────────
print(f"\nSilver'a yazılıyor: {SILVER_PATH}")

# Dinamik partition overwrite sayesinde, sadece işlediğimiz ayın/günün verisi ezilir.
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_PATH)

print(f"{target_date} için Bronze → Silver dönüşümü tamamlandı!")
spark.stop()