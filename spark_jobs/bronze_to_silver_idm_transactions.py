# spark_jobs/bronze_to_silver_idm_transactions.py

import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class IdmTransactionsSilverJob(BaseEpiasSparkJob):
    """
    Gün İçi Piyasası (GİP / IDM) ikili işlem (transaction) verilerini işler.
    NOT: bu endpoint'in gerçek response şeması (TransactionHistoryGipDataDto)
    organizasyon atfı içermiyor (contractName/date/hour/id/price/quantity) —
    doğrulandı 2026-08-09, bkz. plans/07-company-level-market-activity-kgup.md.
    Şirket bazlı piyasa aktivitesi için mart_company_gop_activity.sql'e bakın.
    """
    def __init__(self, spark=None):
        super().__init__(app_name="BronzeToSilver_IdmTransactions", source_name="idm_transactions", primary_keys=["date", "contractName", "id"], spark=spark)

    def run(self, ds: str):
        try:
            df = self.read_bronze(ds)
        except Exception as e:
            self.logger.error(f"Veri okuma hatası: {e}")
            self.finish()
            return

        if df.rdd.isEmpty():
            self.logger.warning(f"Bronze veri boş: {ds}. İşlem atlanıyor.")
            self.finish()
            return

        self.logger.info("GİP İşlemleri (IDM) için tipler dönüştürülüyor...")

        df = df.withColumn("date", self.parse_epias_timestamp())

        for col_name in ["price", "quantity"]:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.finish()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = IdmTransactionsSilverJob()
    job.run(target_ds)