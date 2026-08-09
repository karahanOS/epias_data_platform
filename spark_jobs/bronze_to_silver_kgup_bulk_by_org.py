# spark_jobs/bronze_to_silver_kgup_bulk_by_org.py

import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class KgupBulkByOrgSilverJob(BaseEpiasSparkJob):
    """
    UEVÇB + şirket bazlı KGÜP (Kesinleşmiş Günlük Üretim Planı) — saatlik
    yakıt-mix kırılımı. ADR-0007 Faz 2 (plans/07-company-level-market-activity-kgup.md).
    Bronze'da her satır zaten (date, time, orgId, uevcbId, uevcbName, + yakıt
    kolonları) şeklinde düz — client tarafında organization-list/uevcb-list-bulk
    ile birleştirilip orgId/uevcbId zaten API'nin kendi response'unda geliyor
    (Faz 1'in clearing-quantity'sinin aksine enjeksiyon gerekmedi).
    """
    def __init__(self, spark=None):
        super().__init__(
            app_name="BronzeToSilver_KgupBulkByOrg",
            source_name="kgup_bulk_by_org",
            primary_keys=["date", "time", "orgId", "uevcbId"],
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

        self.logger.info("UEVÇB bazlı KGÜP için tipler dönüştürülüyor...")

        df = df.withColumn("date", self.parse_epias_timestamp())

        # date/time/orgId/uevcbId/uevcbName sabit kalır (id + saat metni);
        # sadece gerçek sayısal yakıt-mix alanları Double'a çevrilir.
        _id_cols = {"date", "time", "orgId", "uevcbId", "uevcbName"}
        for col_name in df.columns:
            if col_name not in _id_cols:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.finish()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = KgupBulkByOrgSilverJob()
    job.run(target_ds)
