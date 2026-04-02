import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from spark_utils import get_spark_session

if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! Lütfen YYYY-MM-DD formatında bir tarih gönderin.")

target_date = sys.argv[1]
spark = get_spark_session("epias_bronze_to_silver_load_estimation")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"
BRONZE_PATH = f"gs://{BUCKET}/bronze/load_estimation/{target_date}.parquet"
SILVER_PATH = f"gs://{BUCKET}/silver/load_estimation/"

schema = StructType([
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("lep",  DoubleType(), True),
])

print(f"{target_date} tarihi için Bronze okunuyor: {BRONZE_PATH}")
df = spark.read.schema(schema).parquet(BRONZE_PATH)

print("Dönüşümler uygulanıyor...")
df_silver = df \
    .dropDuplicates() \
    .dropna(subset=["date", "lep"]) \
    .withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("hour", F.date_format(F.to_timestamp(F.col("time"), "HH:mm"), "HH:mm")) \
    .drop("time") \
    .withColumn("lep", F.round(F.col("lep"), 2)) \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day", F.dayofmonth(F.col("date")))

print(f"\nSilver'a yazılıyor: {SILVER_PATH}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_PATH)

print(f"{target_date} için Bronze → Silver Load Estimation dönüşümü tamamlandı!")
spark.stop()