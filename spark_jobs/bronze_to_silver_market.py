import sys
from pyspark.sql import functions as F
from spark_utils import get_spark_session

if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! YYYY-MM-DD formatında gönderin.")

execution_date = sys.argv[1]
# HATA DÜZELTİLDİ: spark tanımsızdı, spark_utils eklendi
spark = get_spark_session("BronzeToSilver_Participants")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"
input_path  = f"gs://{BUCKET}/bronze/participants/{execution_date}.parquet"
output_path = f"gs://{BUCKET}/silver/market_participants/"

print(f"Bronze okunuyor: {input_path}")
df = spark.read.parquet(input_path)

df_silver = df.select(
    F.col("organizationName").alias("org_name"),
    F.col("organizationId").alias("org_id"),
    F.col("eic").alias("eic_code"),
    F.col("damParticipation").cast("boolean").alias("is_dam_active"),
    F.col("idmParticipation").cast("boolean").alias("is_idm_active"),
    F.current_date().alias("as_of_date")
).dropDuplicates(["org_id"]) \
 .withColumn("year",  F.year(F.col("as_of_date"))) \
 .withColumn("month", F.month(F.col("as_of_date"))) \
 .withColumn("day",   F.dayofmonth(F.col("as_of_date")))

print(f"Silver'a yazılıyor: {output_path}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)

print(f"✅ {execution_date} participants Bronze → Silver tamamlandı!")
spark.stop()