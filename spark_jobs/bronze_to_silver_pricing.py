import sys
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class PricingSilverJob(BaseEpiasSparkJob):
    def __init__(self):
        super().__init__(
            app_name="BronzeToSilver_Pricing", 
            source_name="pricing", 
            primary_keys=["date", "hour"]
        )

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        # SWAGGER ALIAS FIX: 'mcp' kolonu varsa dbt uyumluluğu için 'marketTradePrice' yap
        if "mcp" in df.columns:
            df = df.withColumnRenamed("mcp", "marketTradePrice")
        if "mcpEur" in df.columns:
            df = df.withColumnRenamed("mcpEur", "priceEur")
        if "mcpUsd" in df.columns:
            df = df.withColumnRenamed("mcpUsd", "priceUsd")

        # Zaman dönüşümü
        if "date" in df.columns:
            df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))

        # Veri tipi cast işlemleri
        if "marketTradePrice" in df.columns:
            df = df.withColumn("marketTradePrice", F.col("marketTradePrice").cast(DoubleType()))

        df = self.add_partition_columns(df, ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.spark.stop()

if __name__ == "__main__":
    PricingSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")