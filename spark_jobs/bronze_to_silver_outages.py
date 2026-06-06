import sys
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class OutagesSilverJob(BaseEpiasSparkJob):
    """
    Planlı/Plansız Santral Kesintileri (Outages) verilerini işler.
    Sistemdeki ani arz şoklarını ve kapasite düşüşlerini yakalamak için kullanılır.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_Outages",
            source_name="outages", 
            # Her bir kesinti kaydının (arızanın) EPİAŞ tarafında benzersiz bir ID'si vardır
            primary_keys=["id"] 
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        # Kesinti verilerinde bazen doğrudan 'date' yerine 'startDate' ve 'endDate' bulunur.
        # Partitioning için 'date' kolonu üretiyoruz.
        if "date" in df.columns:
            df = df.withColumn("date", self.parse_epias_timestamp())
        elif "startDate" in df.columns:
            df = df.withColumn("date", self.parse_epias_timestamp("startDate"))
            df = df.withColumn("startDate", self.parse_epias_timestamp("startDate"))
            if "endDate" in df.columns:
                df = df.withColumn("endDate", self.parse_epias_timestamp("endDate"))

        # Kapasite kayıplarını Double yapıyoruz
        numeric_cols = ["installedCapacity", "availableCapacity", "outageCapacity"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        # İlişkisel ID'leri (UEVCB ID) JOIN atabilmek için LongType'a çekiyoruz
        id_cols = ["id", "uevcbId", "organizationId"]
        for col_name in id_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(LongType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    OutagesSilverJob().run(target_ds)