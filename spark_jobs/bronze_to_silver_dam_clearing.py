# /opt/airflow/src/spark/bronze_to_silver_dam_clearing.py
import os
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType
from spark_utils import get_spark_session, get_execution_date, MarketSchemas, logger

def main():
    # 1. Standart Spark Session Başlatma ve Tarih Argümanını Güvenli Yakalama
    spark = get_spark_session("BronzeToSilver_DamClearing")
    target_date = get_execution_date()
    
    logger.info(f"🚀 GÖP Eşleşme Miktarı (DAM Clearing) Silver katmanı dönüşümü başlatıldı. Hedef Tarih: {target_date}")
    
    # Projenizin GCS bucket ismini environment variable'dan veya default değerden alıyoruz
    base_bucket = os.getenv("SPARK_GCS_BUCKET", "gs://epias-data-lake") 
    
    # Airflow pipeline yapınıza göre Bronze okuma ve Silver yazma yollarını tanımlıyoruz
    # (Eğer klasör yapınız tarih partisyonlu ise path'i ona göre güncelleyebilirsiniz)
    bronze_path = f"{base_bucket}/bronze/dam_clearing/{target_date}.parquet"
    silver_path = f"{base_bucket}/silver/dam_clearing"
    
    logger.info(f"📋 Veri kaynağı okunuyor: {bronze_path}")
    
    # 2. Katı Şema Güvencesi (Schema Enforcement)
    # Otomatik şema çıkarımı (inferSchema) yerine spark_utils'teki şemayı dikte ediyoruz.
    try:
        df_raw = spark.read.schema(MarketSchemas.DAM_CLEARING).parquet(bronze_path)
    except Exception as e:
        # Eğer boru hattınızın Bronze katmanı Parquet yerine ham JSON/CSV basıyorsa fallback mekanizması:
        logger.warning(f"⚠️ Parquet formatında okuma yapılamadı, JSON formatı deneniyor. Detay: {str(e)}")
        bronze_path_json = bronze_path.replace(".parquet", ".json")
        df_raw = spark.read.schema(MarketSchemas.DAM_CLEARING).json(bronze_path_json)

    # Boş veri kontrolü
    if df_raw.rdd.isEmpty():
        logger.warning(f"🛑 {target_date} tarihi için işlenecek ham veri bulunamadı. Akış durduruluyor.")
        return

    # 3. Veri Temizliği, Standardizasyon ve Tip Dönüşümleri (Deduplication & Transformation)
    # EPİAŞ'tan gelen tarih formatı: "2025-01-01T00:00:00+03:00"
    df_silver = df_raw \
        .withColumn("parsed_time", F.to_timestamp("date", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
        .withColumn("partition_date", F.to_date("parsed_time")) \
        .withColumn("hour", F.hour("parsed_time")) \
        .withColumn("quantity", F.col("quantity").cast(DoubleType())) \
        .withColumn("organizationId", F.col("organizationId").cast(LongType())) \
        .withColumn("direction", F.upper(F.trim(F.col("direction")))) \
        .filter(F.col("partition_date") == target_date) \
        .dropDuplicates(["partition_date", "hour", "organizationId", "direction"]) \
        .select(
            "partition_date",
            "hour",
            "organizationId",
            "quantity",
            "direction",
            F.current_timestamp().alias("processed_at")
        )

    # 4. Küçük Dosya Probleminin Önlenmesi ve İdempotent Yazım (Dynamic Overwrite)
    # coalesce(1) kullanarak her gün/partisyon için GCS üzerinde tek bir optimize Parquet dosyası üretiyoruz.
    # spark_utils içindeki dynamic overwrite modu sayesinde sadece hedef tarihin klasörünü güvenle ezer.
    logger.info(f"💾 Temizlenmiş veri Silver katmanına yazılıyor: {silver_path}")
    
    df_silver.coalesce(1).write \
        .mode("overwrite") \
        .partitionBy("partition_date") \
        .parquet(silver_path)
        
    logger.info(f"✅ GÖP Eşleşme Miktarı Silver katmanına başarıyla yazıldı. Partisyon: partition_date={target_date}")

if __name__ == "__main__":
    main()