# spark_jobs/bronze_to_silver_order_up.py

import sys
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class OrderUpSilverJob(BaseEpiasSparkJob):
    """
    DGP Yük Alma (YAL / Order Up) talimatlarını işler.
    "Hangi şirket SMF'yi belirliyor ve DGP'den kazanıyor?" sorusunun yanıtıdır.
    """
    def __init__(self):
        # Eski: primary_keys=["date", "organizationId"]
        super().__init__(app_name="BronzeToSilver_OrderUp", source_name="order_up", primary_keys=["date", "hour"])

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

        self.logger.info("Yük Alma (YAL) verileri dönüştürülüyor...")
        
        df = df.withColumn("date", self.parse_epias_timestamp())
        
        dgp_metrics = [
            "upRegulationZeroCodedOfferPrice",
            "upRegulationDeliveredAmount",
            "upRegulationActivatedAmount",
            "upRegulationOneCoded",
        ]

        for metric in dgp_metrics:
            if metric in df.columns:
                df = df.withColumn(metric, F.col(metric).cast(DoubleType()))

        if "organizationId" in df.columns:
            df = df.withColumn("organizationId", F.col("organizationId").cast(LongType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = OrderUpSilverJob()
    job.run(target_ds)