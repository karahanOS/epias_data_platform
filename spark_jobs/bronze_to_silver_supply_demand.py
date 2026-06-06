# spark_jobs/bronze_to_silver_supply_demand.py

import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class SupplyDemandSilverJob(BaseEpiasSparkJob):
    """
    GÖP Arz-Talep Eğrisi verilerini işler.
    "PTF neden taban/tavan yaptı?" analizinin temel verisidir.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_SupplyDemand",
            source_name="supply_demand",
            # Aynı saat içinde birden fazla fiyat seviyesi (price) olabilir
            primary_keys=["date", "price"]
        )

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

        self.logger.info("Arz-Talep Eğrisi verileri dönüştürülüyor...")
        
        df = df.withColumn("date", self.parse_epias_timestamp())
        
        numeric_cols = ["demand", "supply", "price", "matchPrice"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = SupplyDemandSilverJob()
    job.run(target_ds)