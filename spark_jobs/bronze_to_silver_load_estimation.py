import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class LoadEstimationSilverJob(BaseEpiasSparkJob):
    """
    Yük Tahmin Planı (LEP) verilerini işler.
    Ertesi günün beklenen enerji talebini (tüketimini) gösterir.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_LoadEstimation",
            source_name="load_estimation",
            primary_keys=["date"]
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        df = df.withColumn("date", self.parse_epias_timestamp())
        
        if "lep" in df.columns:
            df = df.withColumn("lep", F.col("lep").cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    LoadEstimationSilverJob().run(target_ds)