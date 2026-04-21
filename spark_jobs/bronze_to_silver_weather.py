import sys
from pyspark.sql import functions as F
from spark_utils import get_spark_session

if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! YYYY-MM-DD formatında gönderin.")

execution_date = sys.argv[1]
# HATA DÜZELTİLDİ: get_session() → get_spark_session() + spark_utils ile GCS config
spark = get_spark_session("BronzeToSilver_Weather")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"
input_path  = f"gs://{BUCKET}/bronze/weather/{execution_date}.parquet"
output_path = f"gs://{BUCKET}/silver/weather/"

print(f"Bronze okunuyor: {input_path}")
df = spark.read.parquet(input_path)

# HATA DÜZELTİLDİ: Kolon isimleri WeatherClient'ın gerçek çıktısına göre düzeltildi
# Gerçek kolonlar: datetime, city, temperature_2m, wind_speed_10m,
#                  shortwave_radiation, relative_humidity_2m
df_silver = df.select(
    F.to_timestamp(F.col("datetime")).alias("datetime"),
    F.col("city"),
    F.col("temperature_2m").cast("double").alias("temp_c"),
    F.col("relative_humidity_2m").cast("double").alias("humidity_pct"),
    F.col("wind_speed_10m").cast("double").alias("wind_speed_kmh"),
    F.col("shortwave_radiation").cast("double").alias("solar_radiation_wm2")
).dropDuplicates(["datetime", "city"]) \
 .withColumn("date",  F.to_date(F.col("datetime"))) \
 .withColumn("hour",  F.hour(F.col("datetime"))) \
 .withColumn("year",  F.year(F.col("datetime"))) \
 .withColumn("month", F.month(F.col("datetime"))) \
 .withColumn("day",   F.dayofmonth(F.col("datetime")))

print(f"Silver'a yazılıyor: {output_path}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)

print(f"✅ {execution_date} weather Bronze → Silver tamamlandı!")
spark.stop()