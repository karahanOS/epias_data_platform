# spark_jobs/bronze_to_silver_participants.py

import sys
from pyspark.sql.types import LongType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class ParticipantsSilverJob(BaseEpiasSparkJob):
    """
    Piyasa Katılımcıları (Şirketler) boyut tablosunu işler.
    ID'leri şirket isimlerine ve EIC kodlarına çevirmek için dbt'de JOIN'lenecektir.
    """
    def __init__(self):
        # Eski: primary_keys=["organizationId"]
        super().__init__(app_name="BronzeToSilver_Participants", source_name="participants", primary_keys=["id"])

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

        self.logger.info("Katılımcı (Participant) boyut tablosu işleniyor...")
        
        if "organizationId" in df.columns:
            df = df.withColumn("organizationId", F.col("organizationId").cast(LongType()))

        # API bu tabloda date dönmüyor. Bu yüzden doğrudan "ds" üzerinden partition ekliyoruz.
        # Bu bize katılımcıların günlük snapshot'ını tutma imkanı veriyor (şirket isim değiştirirse yakalarız)
        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = ParticipantsSilverJob()
    job.run(target_ds)