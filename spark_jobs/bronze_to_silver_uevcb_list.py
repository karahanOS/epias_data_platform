import sys
from pyspark.sql.types import LongType, StringType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class UevcbListSilverJob(BaseEpiasSparkJob):
    """
    UEVCB (Santral/Üretim Birimi) Boyut Tablosunu işler.
    dpp ve injection tablolarındaki üretimlerin HANGİ santralden (ve hangi kaynaktan)
    geldiğini çözümlemek (JOIN) için kullanılır.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_UevcbList",
            source_name="uevcb_list",
            # API'den tarih dönmediği için sadece santral ID'si üzerinden tekilleştiriyoruz
            primary_keys=["id"] 
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        # Gerekli ID alanlarını güvenli tipe çekiyoruz
        if "id" in df.columns:
            df = df.withColumn("id", F.col("id").cast(LongType()))
        if "organizationId" in df.columns:
            df = df.withColumn("organizationId", F.col("organizationId").cast(LongType()))

        # API bu tabloda date dönmüyor. Bu yüzden doğrudan "ds" (çalışma tarihi) 
        # üzerinden partition ekliyoruz. Bu, her günün santral envanterinin fotoğrafını çeker.
        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    UevcbListSilverJob().run(target_ds)