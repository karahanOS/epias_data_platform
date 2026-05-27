"""
spark_utils.py — EPIAS Data Platform Base Spark Sınıfı (v2.0)
===============================================================
OOP (Nesne Yönelimli) ve Idempotent (Tekrarlanabilir) mimari.
Tüm bronze_to_silver_*.py job'ları BaseEpiasSparkJob sınıfından türetilir.
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class BaseEpiasSparkJob:
    def __init__(self, app_name: str, source_name: str, primary_keys: list):
        self.app_name = app_name
        self.source_name = source_name
        self.primary_keys = primary_keys
        
        # Spark Session Başlatma
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
            
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(self.app_name)

    def read_bronze(self, ds: str):
        path = f"gs://epias-data-lake/bronze/{self.source_name}/{ds}.parquet"
        self.logger.info(f"💾 Bronze katmanından Parquet okunuyor: {path}")
        df = self.spark.read.parquet(path)
        
        # SWAGGER FIX: Eğer API veriyi 'body' sütunu altında nested sarmışsa otomatik dışa çıkar (Flatten)
        if "body" in df.columns:
            self.logger.info("📦 Nested 'body' yapısı tespit edildi, şema düzleştiriliyor...")
            df = df.select("body.*")
        if "items" in df.columns:
            df = df.select(F.explode("items").alias("data")).select("data.*")
            
        return df

    def add_partition_columns(self, df, ds: str):
        # Tarih parçalama işlemleri
        return df.withColumn("year", F.lit(ds.split("-")[0])) \
                 .withColumn("month", F.lit(ds.split("-")[1])) \
                 .withColumn("day", F.lit(ds.split("-")[2]))

    def deduplicate(self, df):
        """Swagger uyumlu esnek tekilleştirme mekanizması"""
        if not self.primary_keys:
            return df.dropDuplicates()

        # DataFrame'de gerçekten var olan anahtarları bul
        valid_keys = [col for col in self.primary_keys if col in df.columns]

        if not valid_keys:
            # Akıllı yedek plan anahtarları
            alternatives = ["date", "datetime", "hour", "time", "id", "mcp"]
            valid_keys = [col for col in alternatives if col in df.columns]

        if not valid_keys:
            self.logger.warning("⚠️ Belirtilen hiçbir anahtar şemada bulunamadı. Full-row dropDuplicates uygulanıyor.")
            df_dedup = df.dropDuplicates()
        else:
            self.logger.info(f"✅ Tekilleştirme başarıyla uygulandı. Anahtarlar: {valid_keys}")
            df_dedup = df.dropDuplicates(valid_keys)

        # dbt Gold katmanı veri bütünlüğü takibi için benzersiz satır hash'i (Parmak İzi)
        return df_dedup.withColumn(
            "_record_hash", 
            F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_dedup.columns]))
        )

    def write_silver(self, df):
        output_path = f"gs://epias-data-lake/silver/{self.source_name}/"
        self.logger.info(f"🚀 Silver katmanına yazılıyor (Partitioned): {output_path}")
        df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)