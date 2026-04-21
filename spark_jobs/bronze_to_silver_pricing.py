import sys
from pyspark.sql import functions as F
from spark_utils import get_spark_session
 
if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! YYYY-MM-DD formatında gönderin.")
 
execution_date = sys.argv[1]
# HATA DÜZELTİLDİ: get_OrCreate() → getOrCreate()
spark = get_spark_session("BronzeToSilver_Pricing")
spark.sparkContext.setLogLevel("WARN")
 
BUCKET = "epias-data-lake"
input_path  = f"gs://{BUCKET}/bronze/pricing/{execution_date}.parquet"
output_path = f"gs://{BUCKET}/silver/pricing/"
 
print(f"Bronze okunuyor: {input_path}")
df = spark.read.parquet(input_path)
 
df_silver = df.select(
    F.to_date(F.col("date")).alias("date"),
    F.expr("substring(hour, 1, 2)").cast("int").alias("hour"),
    F.col("ptf").cast("double").alias("ptf_tl"),
    F.col("smf").cast("double").alias("smf_tl"),
    F.col("sdf").cast("double").alias("sdf_tl"),
    F.current_timestamp().alias("processed_at")
).dropDuplicates(["date", "hour"]) \
 .withColumn("year",  F.year("date")) \
 .withColumn("month", F.month("date")) \
 .withColumn("day",   F.dayofmonth("date"))
 
print(f"Silver'a yazılıyor: {output_path}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)
 
print(f"✅ {execution_date} pricing Bronze → Silver tamamlandı!")
spark.stop()