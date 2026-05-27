# spark_jobs/bronze_to_silver_generation.py

import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class GenerationSilverJob(BaseEpiasSparkJob):
    """
    Gerçekleşen Genel Lisanslı Üretim (Generation Mix) verilerini işler.
    Yenilenebilir enerjinin şebeke üzerindeki genel ağırlığını hesaplamak için kullanılır.
    """
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_Generation",
            source_name="licensed_gen", # Veya API endpoint'ine göre "generation"
            primary_keys=["date"]
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

        self.logger.info("Lisanslı Üretim (Generation) verileri dönüştürülüyor...")
        
        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        
        # Olası kaynak tipleri (API'den dönen yapıya göre genişletilebilir)
        gen_types = ["total", "naturalGas", "dammedHydro", "lignite", "river", "importedCoal", "wind", "solar", "geothermal"]
        
        for gen_col in gen_types:
            if gen_col in df.columns:
                df = df.withColumn(gen_col, F.col(gen_col).cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    target_ds = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    job = GenerationSilverJob()
    job.run(target_ds)