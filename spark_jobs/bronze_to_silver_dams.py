import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class DamsSilverJob(BaseEpiasSparkJob):
    """
    Baraj Aktif Hacim ve Su Seviyeleri verilerini işler.
    Hidroelektrik arz kapasitesini (ve dolayısıyla PTF üzerindeki ucuzluk baskısını) ölçer.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_Dams",
            source_name="dams", # API'deki adına göre 'dams_active_volume' da olabilir
            # Veriler havza/baraj bazında günlük geldiği için PK bu şekildedir
            primary_keys=["date", "basinName"] 
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        # Tarih kolonunu standart formata çeviriyoruz
        df = df.withColumn("date", self.parse_epias_timestamp())

        # Hacim ve seviye metriklerini ondalıklı sayıya (Double) çevir
        numeric_cols = ["activeVolume", "waterLevel", "totalCapacity"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    DamsSilverJob().run(target_ds)