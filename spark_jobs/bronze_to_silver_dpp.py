# spark_jobs/bronze_to_silver_dpp.py

import sys
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class DppSilverJob(BaseEpiasSparkJob):
    """
    Beyan Edilen Günlük Üretim Planı (BGÜP / DPP) verilerini işler.
    Kaynak bazında saatlik planlanan üretimi gösterir.
    NOT: Kesinleşmiş plan (KGÜP) için bronze_to_silver_sbfgp.py kullanılmalıdır.
    """
    def __init__(self):
        # Eski: primary_keys=["date", "organizationId", "uevcbId"]
        super().__init__(app_name="BronzeToSilver_DPP", source_name="dpp", primary_keys=["date", "time"])

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

        self.logger.info("DPP (BGÜP) verisi için tipler dönüştürülüyor...")

        df = df.withColumn("date", self.parse_epias_timestamp())

        # Cast all numeric columns to Double to prevent INT64/DOUBLE schema drift
        # across weekly backfill files (pandas infers int64 for round-number values).
        _id_cols = {"date", "time", "organizationId", "uevcbId"}
        for col_name in df.columns:
            if col_name not in _id_cols:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        for id_col in ["organizationId", "uevcbId"]:
            if id_col in df.columns:
                df = df.withColumn(id_col, F.col(id_col).cast(LongType()))

        # Base işlemler ve GCS'e yazma
        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = DppSilverJob()
    job.run(target_ds)