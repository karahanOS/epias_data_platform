import sys
from pyspark.sql import functions as F
from spark_utils import get_spark_session
 
if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! YYYY-MM-DD formatında gönderin.")
 
execution_date = sys.argv[1]
# HATA DÜZELTİLDİ: get_OrCreate() → getOrCreate()
spark = get_spark_session("BronzeToSilver_Imbalance")
spark.sparkContext.setLogLevel("WARN")
 
BUCKET = "epias-data-lake"
input_path  = f"gs://{BUCKET}/bronze/imbalance/{execution_date}.parquet"
output_path = f"gs://{BUCKET}/silver/imbalance/"
 
print(f"Bronze okunuyor: {input_path}")
df = spark.read.parquet(input_path)
 
df_silver = df.select(
    F.to_date(F.col("date")).alias("date"),
    F.expr("substring(hour, 1, 2)").cast("int").alias("hour"),
    F.col("positiveImbalance").cast("double").alias("pos_imbalance_mwh"),
    F.col("negativeImbalance").cast("double").alias("neg_imbalance_mwh")
).withColumn("net_imbalance",
    F.col("pos_imbalance_mwh") - F.col("neg_imbalance_mwh")
).dropDuplicates(["date", "hour"]) \
 .withColumn("year",  F.year("date")) \
 .withColumn("month", F.month("date")) \
 .withColumn("day",   F.dayofmonth("date"))
 
print(f"Silver'a yazılıyor: {output_path}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)
 
print(f"✅ {execution_date} imbalance Bronze → Silver tamamlandı!")
spark.stop()