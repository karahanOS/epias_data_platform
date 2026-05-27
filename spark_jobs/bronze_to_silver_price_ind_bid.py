import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class PriceIndBidSilverJob(BaseEpiasSparkJob):
    """
    Fiyattan Bağımsız Alış Teklifleri (Must-Take Demand).
    Piyasadaki katı/esneksiz talebi ölçmek için kullanılır.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_PriceIndBid",
            source_name="price_ind_bid", 
            primary_keys=["date"]
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        
        if "priceIndependentBidAmount" in df.columns:
            df = df.withColumn("priceIndependentBidAmount", F.col("priceIndependentBidAmount").cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    PriceIndBidSilverJob().run(target_ds)