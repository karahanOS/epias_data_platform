# spark_jobs/bronze_to_silver_dam_clearing_by_org.py

import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class DamClearingByOrgSilverJob(BaseEpiasSparkJob):
    """
    GÖP şirket bazlı saatlik eşleşme miktarı (matchedBids/matchedOffers).
    ADR-0007 Faz 1 (plans/07-company-level-market-activity-kgup.md) — GİP'te
    organizasyon atfı yok, GÖP'ün clearing-quantity + organizationId filtresi
    üzerinden üretiliyor. Bronze'da her satır zaten (date, hour, organizationId,
    organizationName, matchedBids, matchedOffers) şeklinde düz (flat) — client
    tarafında roster ile birleştirilip organizationId enjekte edilmiş halde geliyor.
    """
    def __init__(self, spark=None):
        super().__init__(
            app_name="BronzeToSilver_DamClearingByOrg",
            source_name="dam_clearing_by_org",
            primary_keys=["date", "hour", "organizationId"],
            spark=spark,
        )

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

        self.logger.info("GÖP şirket bazlı eşleşme miktarı için tipler dönüştürülüyor...")

        df = df.withColumn("date", self.parse_epias_timestamp())

        # date/hour/organizationId/organizationName sabit kalır (id + saat metni);
        # sadece gerçek sayısal alanlar (matchedBids/matchedOffers) Double'a çevrilir
        # — aynı FLOAT64 kuralı (INT64/DOUBLE schema drift'ini önlemek için).
        _id_cols = {"date", "hour", "organizationId", "organizationName"}
        for col_name in df.columns:
            if col_name not in _id_cols:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.finish()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = DamClearingByOrgSilverJob()
    job.run(target_ds)
