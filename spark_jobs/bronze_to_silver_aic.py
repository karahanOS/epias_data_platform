import sys
import functools
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from spark_utils import BaseEpiasSparkJob

class AicSilverJob(BaseEpiasSparkJob):
    def __init__(self):
        super().__init__(app_name="BronzeToSilver_AIC", source_name="aic", primary_keys=["date"])

    def _read_backfill(self):
        """
        Reads weekly AIC bronze files one-by-one to sidestep the nafta/tasKomur
        BIGINT↔DOUBLE schema drift.  Spark's Parquet reader cannot coerce between
        INT64 and DOUBLE physical types at read time (neither vectorized nor
        non-vectorized), so a shared wildcard read always fails for whichever
        physical type the inferred/explicit schema doesn't match.
        Reading each file individually lets Spark infer the correct physical schema
        per file; all numeric columns are then cast to Double at the PySpark level
        before union.
        """
        pattern = f"gs://epias-data-lake/bronze/{self.source_name}/backfill_*.parquet"
        self.logger.info(f"💾 AIC backfill: dosya bazlı okuma — {pattern}")

        # Collect file paths via binaryFile (header-only read, no data transferred)
        paths = sorted(
            row.path
            for row in self.spark.read.format("binaryFile").load(pattern).select("path").collect()
        )
        if not paths:
            self.logger.warning("⚠️  Hiçbir backfill dosyası bulunamadı.")
            return self.spark.createDataFrame([], self.spark.read.parquet(pattern).schema)

        dfs = []
        for path in paths:
            df = self.spark.read.parquet(path)
            for col_name in df.columns:
                if col_name not in ("date", "time"):
                    df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
            dfs.append(df)

        return functools.reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True),
            dfs
        )

    def run(self, ds: str):
        if self.backfill_mode:
            df = self._read_backfill()
        else:
            df = self.read_bronze(ds)

        if df.rdd.isEmpty():
            self.spark.stop()
            return

        df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        for col in df.columns:
            if col != "date":
                df = df.withColumn(col, F.col(col).cast(DoubleType()))
        self.write_silver(self.deduplicate(self.add_partition_columns(df, ds)))
        self.spark.stop()

if __name__ == "__main__":
    AicSilverJob().run(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")
