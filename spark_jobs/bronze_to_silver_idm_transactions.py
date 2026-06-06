# spark_jobs/bronze_to_silver_idm_transactions.py

import sys
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class IdmTransactionsSilverJob(BaseEpiasSparkJob):
    """
    Gün İçi Piyasası (GİP / IDM) ikili işlem (transaction) verilerini işler.
    "Hangi şirket GİP'te aktif rol oynuyor?" analizinin kalbidir.
    """
    def __init__(self):
        # Eski: primary_keys=["date", "buyerOrganizationId", "sellerOrganizationId", "contractName"]
        super().__init__(app_name="BronzeToSilver_IdmTransactions", source_name="idm_transactions", primary_keys=["date", "contractName", "id"])

    def run(self, ds: str):
        try:
            df = self.read_bronze(ds)
        except Exception as e:
            self.logger.error(f"Veri okuma hatası: {e}")
            self.spark.stop()
            return

        if df.rdd.isEmpty():
            self.logger.warning(f"Bronze veri boş: {ds}. İşlem atlanıyor.")
            self.spark.stop()
            return

        self.logger.info("GİP İşlemleri (IDM) için tipler dönüştürülüyor...")
        
        df = df.withColumn("date", self.parse_epias_timestamp())
        
        for col_name in ["price", "quantity"]:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
                
        for col_name in ["buyerOrganizationId", "sellerOrganizationId"]:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(LongType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = IdmTransactionsSilverJob()
    job.run(target_ds)