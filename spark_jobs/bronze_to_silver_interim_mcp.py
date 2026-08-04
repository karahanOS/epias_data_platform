import sys
from datetime import datetime, timedelta
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class InterimMcpSilverJob(BaseEpiasSparkJob):
    def __init__(self, spark=None):
        super().__init__(
            app_name="BronzeToSilver_InterimMcp",
            source_name="interim_mcp",
            primary_keys=["date", "hour"], spark=spark)

    def run(self, ds: str):
        df = self.read_bronze(ds)
        if df.rdd.isEmpty(): return

        if "date" in df.columns:
            df = df.withColumn("date", self.parse_epias_timestamp())

        if "marketTradePrice" in df.columns:
            df = df.withColumn("marketTradePrice", F.col("marketTradePrice").cast(DoubleType()))

        # DATA_DELAYS["get_interim_mcp"] = -1 (lead) in epias_dag.py means the
        # bronze file named `ds` actually contains ds+1's data (tomorrow's
        # K.PTF, fetched today once the day-ahead auction clears). In normal
        # (non-backfill) mode, add_partition_columns() derives year/month/day
        # from the `ds` string passed to it, NOT from the rows' own `date`
        # column — so passing raw `ds` here would physically partition
        # tomorrow's prices under today's date. Pass the content date instead.
        content_ds = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        df = self.add_partition_columns(df, content_ds)
        df = self.deduplicate(df)
        self.write_silver(df)
        self.finish()

if __name__ == "__main__":
    InterimMcpSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")
