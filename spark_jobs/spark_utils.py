"""
spark_utils.py — EPIAS Data Platform Base Spark Sınıfı (v2.0)
===============================================================
OOP (Nesne Yönelimli) ve Idempotent (Tekrarlanabilir) mimari.
Tüm bronze_to_silver_*.py job'ları BaseEpiasSparkJob sınıfından türetilir.
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class BaseEpiasSparkJob:
    def __init__(self, app_name: str, source_name: str, primary_keys: list):
        self.app_name = app_name
        self.source_name = source_name
        self.primary_keys = primary_keys

        # --backfill flag: signals wildcard Bronze read + append Silver write
        self.backfill_mode = "--backfill" in sys.argv

        # DYNAMIC partition overwrite: mode=overwrite only replaces the specific
        # year/month/day partition being written, leaving all other partitions
        # untouched. Without DYNAMIC, the default STATIC mode wipes the entire
        # Silver table on every daily run — destroying all backfill history.
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

        if self.backfill_mode:
            # GCS streaming upload: avoids buffering the entire partition in heap.
            # SYNCABLE_COMPOSITE writes incrementally to GCS instead of one giant chunk.
            builder = builder \
                .config("spark.hadoop.fs.gs.outputstream.type", "SYNCABLE_COMPOSITE") \
                .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "8388608")  # 8 MiB chunks
            # Use more shuffle partitions so each output file is smaller
            builder = builder \
                .config("spark.sql.shuffle.partitions", "400")

        self.spark = builder.getOrCreate()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(self.app_name)
        if self.backfill_mode:
            self.logger.info(f"🔄 BACKFILL MODE aktif — {self.source_name}")

    def read_bronze(self, ds: str, schema=None):
        """
        Normal (daily) mode: reads gs://.../bronze/<source>/<ds>.parquet
        Backfill mode     : reads gs://.../bronze/<source>/backfill_*.parquet (all chunks)
        schema: optional explicit StructType — pass when the source has BIGINT/DOUBLE type
                drift across files to bypass schema inference entirely.
        """
        if self.backfill_mode:
            return self._read_bronze_backfill(schema=schema)

        path = f"gs://epias-data-lake/bronze/{self.source_name}/{ds}.parquet"
        self.logger.info(f"💾 Bronze okunuyor (günlük): {path}")
        reader = self.spark.read.schema(schema) if schema else self.spark.read
        return self._normalize(reader.parquet(path))

    def _read_bronze_backfill(self, schema=None):
        """Reads all weekly backfill chunks for this source.

        Per-file reads handle INT64/DOUBLE physical-type drift between weekly files.
        Spark's Parquet reader (vectorized or non-vectorized) cannot coerce between
        INT64 and DOUBLE at read time — a wildcard scan fails when pandas inferred
        int64 instead of float64 for a chunk where the API returned round numbers.
        Each file is read with its own native schema; columns that drifted from the
        first file's schema are cast before union.
        """
        pattern = f"gs://epias-data-lake/bronze/{self.source_name}/backfill_*.parquet"
        self.logger.info(f"💾 Bronze okunuyor (backfill): {pattern}")

        if schema:
            return self._normalize(self.spark.read.schema(schema).parquet(pattern))

        # List files via binaryFile — reads only file metadata, no row data transferred
        paths = sorted(
            row.path
            for row in self.spark.read.format("binaryFile").load(pattern).select("path").collect()
        )

        if not paths:
            self.logger.warning("⚠️ Hiçbir backfill dosyası bulunamadı.")
            return self.spark.createDataFrame([], self.spark.read.parquet(pattern).schema)

        # First (earliest) file defines the reference schema
        ref_df = self.spark.read.parquet(paths[0])
        ref_schema = ref_df.schema

        dfs = [ref_df]
        for path in paths[1:]:
            df = self.spark.read.parquet(path)
            for field in ref_schema.fields:
                if field.name in df.columns and df.schema[field.name].dataType != field.dataType:
                    df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            dfs.append(df)

        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)

        self.logger.info(f"✅ {len(paths)} backfill dosyası birleştirildi (schema-safe)")
        return self._normalize(result)

    def _normalize(self, df):
        """Shared post-read normalization (nested schema flattening)."""
        if "body" in df.columns:
            self.logger.info("📦 Nested 'body' yapısı düzleştiriliyor...")
            df = df.select("body.*")
        if "items" in df.columns:
            df = df.select(F.explode("items").alias("data")).select("data.*")
        return df

    @staticmethod
    def parse_epias_timestamp(col_name: str = "date"):
        """Parse an EPIAS ISO-8601 timestamp column to Spark TimestampType."""
        return F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ssXXX")

    def add_partition_columns(self, df, ds: str):
        """
        Normal mode : partition columns from the run date string (ds).
        Backfill mode: derive year/month/day from the 'date' column in the data itself
                       so every historical row lands in the correct partition.
        """
        if self.backfill_mode:
            date_col = next(
                (c for c in ["date", "datetime", "time"] if c in df.columns), None
            )
            if date_col:
                self.logger.info(f"📅 Backfill partition: tarih sütunundan türetiliyor ({date_col})")
                ts = F.to_timestamp(F.col(date_col))
                return df \
                    .withColumn("year",  F.year(ts).cast("string")) \
                    .withColumn("month", F.lpad(F.month(ts).cast("string"), 2, "0")) \
                    .withColumn("day",   F.lpad(F.dayofmonth(ts).cast("string"), 2, "0"))
            self.logger.warning("⚠️  Tarih sütunu bulunamadı — ds string'i kullanılıyor.")

        return df.withColumn("year",  F.lit(ds.split("-")[0])) \
                 .withColumn("month", F.lit(ds.split("-")[1])) \
                 .withColumn("day",   F.lit(ds.split("-")[2]))

    def deduplicate(self, df):
        """Swagger uyumlu esnek tekilleştirme mekanizması"""
        if not self.primary_keys:
            return df.dropDuplicates()

        valid_keys = [col for col in self.primary_keys if col in df.columns]

        if not valid_keys:
            alternatives = ["date", "datetime", "hour", "time", "id", "mcp"]
            valid_keys = [col for col in alternatives if col in df.columns]

        if not valid_keys:
            self.logger.warning("⚠️ Hiçbir anahtar bulunamadı — full-row dropDuplicates.")
            df_dedup = df.dropDuplicates()
        else:
            self.logger.info(f"✅ Tekilleştirme: {valid_keys}")
            df_dedup = df.dropDuplicates(valid_keys)

        return df_dedup.withColumn(
            "_record_hash",
            F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_dedup.columns]))
        )

    def write_silver(self, df):
        """
        Normal mode  : overwrite the single day partition (idempotent daily runs).
        Backfill mode: append — preserves any daily data already written by the
                       main pipeline while filling historical gaps.

        Backfill repartitioning strategy:
          AQE default coalesces the full dataset into very few large partitions.
          Each partition is buffered entirely in the GCS upload client heap before
          being sent, causing OOM on executors with 1-4 GiB RAM.
          Repartitioning to ~200 output tasks spreads the data into many small
          files (~5-20 MiB each), keeping heap pressure within executor limits.
        """
        output_path = f"gs://epias-data-lake/silver/{self.source_name}/"
        # DYNAMIC partition overwrite — replaces only the specific year/month/day
        # partitions being written, leaving all others untouched.  Safe for both
        # daily runs (single partition) and backfill (many partitions).
        # We no longer use "append" for backfill because it creates duplicate Silver
        # rows when the daily pipeline has already written some of the same dates.
        write_mode  = "overwrite"

        if self.backfill_mode:
            # Repartition to keep each output file small and heap-safe.
            # 200 partitions across a ~1-year backfill → ~2 days of data per file.
            df = df.repartition(200, "year", "month", "day")
            self.logger.info(f"🔀 Backfill repartition(200) uygulandı — küçük dosya boyutu")

        self.logger.info(f"🚀 Silver yazılıyor (mode={write_mode}): {output_path}")
        df.write \
            .mode(write_mode) \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)