import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from spark_utils import get_spark_session

if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! Lütfen YYYY-MM-DD formatında bir tarih gönderin.")

target_date = sys.argv[1]
spark = get_spark_session("epias_bronze_to_silver_weather")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"
BRONZE_PATH = f"gs://{BUCKET}/bronze/weather/{target_date}.parquet"
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

print(f"{target_date} tarihi için Bronze okunuyor: {BRONZE_PATH}")
df = spark.read.schema(schema).parquet(BRONZE_PATH)

print("Dönüşümler uygulanıyor...")
df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["datetime", "weighted_temperature_2m"]) \
    .withColumn("date", F.to_timestamp(F.col("datetime"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("hour", F.date_format(F.col("date"), "HH:mm")) \
    .drop("datetime") \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day", F.dayofmonth(F.col("date")))

numeric_cols = [
    "weighted_temperature_2m", "weighted_wind_speed_10m", "weighted_shortwave_radiation", 
    "weighted_relative_humidity_2m", "istanbul_temp", "istanbul_wind", "istanbul_radiation",
    "izmir_temp", "izmir_wind", "izmir_radiation", "konya_temp", "konya_wind", "konya_radiation",
    "ankara_temp", "ankara_wind", "ankara_radiation",
]

for col in numeric_cols:
    df_silver = df_silver.withColumn(col, F.round(F.col(col), 2))

print(f"\nSilver'a yazılıyor: {SILVER_PATH}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_PATH)

print(f"{target_date} için Bronze → Silver Weather dönüşümü tamamlandı!")
spark.stop()