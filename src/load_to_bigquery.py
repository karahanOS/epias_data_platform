"""
load_to_bigquery.py — GCS Silver Katmanını BigQuery'ye Bağlama (External Tables)
=================================================================================
Bu script, GCS üzerindeki Hive-partitioned Parquet dosyalarını (Silver Katmanı),
BigQuery'de "External Table" olarak yaratır/günceller. 
Veri kopyalanmaz, dbt doğrudan bu tabloları okuyarak Gold katmanını BigQuery içinde inşa eder.
"""

import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from config import GCP_PROJECT_ID, GCS_BUCKET, get_bq_client

# Loglama Ayarları
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BQLoader")

class BQExternalTableManager:
    def __init__(self):
        self.project_id = GCP_PROJECT_ID
        self.dataset_id = "silver"
        self.bucket_name = GCS_BUCKET
        self.client = get_bq_client()
        
        self._ensure_dataset_exists()

    def _ensure_dataset_exists(self):
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"✅ Dataset '{dataset_ref}' zaten mevcut.")
        except NotFound:
            logger.info(f"⚠️ Dataset '{dataset_ref}' bulunamadı, oluşturuluyor...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "EU"  # veya senin seçtiğin bir lokasyon (US vb.)
            self.client.create_dataset(dataset, timeout=30)
            logger.info(f"✅ Dataset '{dataset_ref}' başarıyla oluşturuldu.")

    def create_or_update_external_table(self, table_name: str):
        """
        Belirtilen tablo adı için GCS'teki Parquet dosyalarını gösteren 
        bir BigQuery External (Dış) Tablosu oluşturur.
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        gcs_uri = f"gs://{self.bucket_name}/silver/{table_name}/*"
        source_uri_prefix = f"gs://{self.bucket_name}/silver/{table_name}"

        logger.info(f"External Tablo ayarlanıyor: {table_id} -> {gcs_uri}")

        # External Table Yapılandırması
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [gcs_uri]
        
        # Hive Partitioning Ayarları (year=.../month=.../day=...)
        hive_options = bigquery.HivePartitioningOptions()
        hive_options.mode = "AUTO" # Şemayı ve partition tiplerini otomatik algılar
        hive_options.source_uri_prefix = source_uri_prefix
        external_config.hive_partitioning = hive_options

        # Tablo Tanımı
        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        # Tabloyu yarat veya varsa güncelle
        try:
            self.client.delete_table(table_id, not_found_ok=True) # Şema değişmişse diye temizle
            table = self.client.create_table(table)
            logger.info(f"✅ Başarılı: {table_id} dış tablosu oluşturuldu.")
        except Exception as e:
            logger.error(f"❌ Tablo oluşturulurken hata: {table_name} - {e}")

    def run_all_tables(self):
        """Tüm Silver tablolarını BigQuery'ye bağlar."""
        # Spark ile oluşturduğumuz tüm silver tabloların listesi
        tables = [
            "pricing", 
            "dam_clearing", 
            "idm_transactions", 
            "smf", 
            "system_direction",
            "generation",
            "unlicensed",
            "res_forecast",
            "aic",
            "dpp",
            "injection",
            "supply_demand",
            "load_estimation",
            "price_ind_bid",
            "consumption",  # <--- Eksik olan Silver Consumption eklendi
            "imbalance", 
            "order_up", 
            "order_down", 
            "participants",
            "uevcb_list",
            "dams",
            "outages",
            "weather"
        ]
        
        logger.info(f"Toplam {len(tables)} tablo BigQuery'ye tanımlanıyor...")
        for table in tables:
            self.create_or_update_external_table(table)
            
        logger.info("🎉 Tüm tablolar başarıyla BigQuery'ye bağlandı!")

if __name__ == "__main__":
    manager = BQExternalTableManager()
    
    # Bu script Airflow'dan çağrıldığında dışarıdan tablo adı alabilir,
    # Argüman yoksa tüm tabloları günceller.
    import sys
    if len(sys.argv) > 1:
        target_table = sys.argv[1]
        manager.create_or_update_external_table(target_table)
    else:
        manager.run_all_tables()