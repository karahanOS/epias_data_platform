# spark_jobs/bronze_to_silver_order_down.py

import sys
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class OrderDownSilverJob(BaseEpiasSparkJob):
    """
    DGP Yük Atma (YAT / Order Down) talimatlarını işler.
    Sistemde enerji fazlası olduğunda devreye giren santralleri takip eder.
    """
    def __init__(self):
        # Eski: primary_keys=["date", "organizationId"]
        super().__init__(app_name="BronzeToSilver_OrderDown", source_name="order_down", primary_keys=["date", "hour"])

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

        self.logger.info("Yük Atma (YAT) verileri dönüştürülüyor...")
        
        df = df.withColumn("date", self.parse_epias_timestamp())
        
        dgp_metrics = [
            "downRegulationZeroCodedBidPrice",
            "downRegulationDeliveredAmount",
            "downRegulationActivatedAmount",
            "downRegulationOneCoded",
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
    job = OrderDownSilverJob()
    job.run(target_ds)