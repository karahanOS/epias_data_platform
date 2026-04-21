from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def transform_bronze_to_silver(source_name, spark):
    # 1. READ: Bronze veriyi oku (Parquet formatında)
    df_raw = spark.read.parquet(f"gs://epias-data-lake/bronze/{source_name}/*.parquet")

    # 2. CLEAN: Sütun isimlerini normalize et ve tipleri düzelt
    df_clean = df_raw \
        .withColumn("date", F.to_date(F.col("date"))) \
        .withColumn("hour", F.substring(F.col("hour"), 1, 2).cast("int")) \
        .withColumn("processed_at", F.current_timestamp())

    # 3. DEDUPLICATE: Mükerrerleri sil (Unique Key stratejisi)
    unique_keys = ["date", "hour"]
    if "organizationId" in df_raw.columns:
        unique_keys.append("organizationId")
    
    df_final = df_clean.dropDuplicates(unique_keys)

    # 4. WRITE: Silver katmanına Partition yaparak kaydet
    # Neden Partition? Sorgu maliyetini (BigQuery) ve hızı optimize etmek için.
    df_final.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"gs://epias-data-lake/silver/{source_name}/")

    return df_final