# spark_jobs/bronze_to_silver_sbfgp.py

import sys
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class SbfgpSilverJob(BaseEpiasSparkJob):
    """
    Kesinleşmiş Günlük Üretim Planı (KGÜP / SBFGP) verilerini işler.
    GİP kapanışından sonra DUY 69. madde kapsamında güncellenen nihai plandır.
    BGÜP (stg_dpp) ile karşılaştırılarak intraday revizyon miktarı hesaplanabilir.
    """
    def __init__(self):
        super().__init__(app_name="BronzeToSilver_SBFGP", source_name="sbfgp", primary_keys=["date", "time"])

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

        self.logger.info("SBFGP (KGÜP) verisi için tipler dönüştürülüyor...")

        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))

        # Cast all numeric columns to Double to prevent INT64/DOUBLE schema drift
        _id_cols = {"date", "time"}
        for col_name in df.columns:
            if col_name not in _id_cols:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = SbfgpSilverJob()
    job.run(target_ds)
