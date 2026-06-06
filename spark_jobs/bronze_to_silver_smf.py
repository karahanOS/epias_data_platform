import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class SmfSilverJob(BaseEpiasSparkJob):
    """
    Sistem Marjinal Fiyatı (SMF) verilerini işler.
    Dengeleme Güç Piyasasındaki (DGP) fiyatı ifade eder.
    """
    def __init__(self):
        super().__init__(app_name="BronzeToSilver_SMF", source_name="smf", primary_keys=["date"])

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return
        
        df = df.withColumn("date", self.parse_epias_timestamp())
        
        if "systemMarginalPrice" in df.columns:
            df = df.withColumn("systemMarginalPrice", F.col("systemMarginalPrice").cast(DoubleType()))
            
        self.write_silver(self.deduplicate(self.add_partition_columns(df, ds)))
        self.spark.stop()

if __name__ == "__main__":
    SmfSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")