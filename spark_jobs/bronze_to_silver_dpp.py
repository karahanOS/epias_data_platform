import sys
from pyspark.sql import functions as F
from spark_utils import get_spark_session
 
if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! YYYY-MM-DD formatında gönderin.")
 
execution_date = sys.argv[1]
# HATA DÜZELTİLDİ: get_OrCreate() → getOrCreate()
spark = get_spark_session("BronzeToSilver_DPP")
spark.sparkContext.setLogLevel("WARN")
 
BUCKET = "epias-data-lake"
input_path  = f"gs://{BUCKET}/bronze/dpp/{execution_date}.parquet"
output_path = f"gs://{BUCKET}/silver/dpp/"
 
print(f"Bronze okunuyor: {input_path}")
df = spark.read.parquet(input_path)
 
df_silver = df.select(
    F.to_date(F.col("date")).alias("date"),
    F.expr("substring(hour, 1, 2)").cast("int").alias("hour"),
    F.col("organizationId").alias("org_id"),
    F.col("total").cast("double").alias("planned_mwh"),
    F.col("naturalGas").cast("double").alias("gas_planned_mwh"),
    F.col("wind").cast("double").alias("wind_planned_mwh")
).dropDuplicates(["date", "hour", "org_id"]) \
 .withColumn("year",  F.year("date")) \
 .withColumn("month", F.month("date")) \
 .withColumn("day",   F.dayofmonth("date"))
 
print(f"Silver'a yazılıyor: {output_path}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)
 
print(f"✅ {execution_date} dpp Bronze → Silver tamamlandı!")
spark.stop()
  