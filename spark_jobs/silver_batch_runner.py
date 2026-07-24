"""
silver_batch_runner.py — Consolidated Dataproc Serverless entrypoint (ADR-0003)
================================================================================
Runs several sources' bronze_to_silver_*.py transforms sequentially inside ONE
shared SparkSession, instead of submitting one Dataproc batch per source. Each
source's own transform logic is completely unchanged — this only changes how
many separate Dataproc batches (and therefore how many ~3-4 min cold starts)
a run pays for.

Usage:
    python silver_batch_runner.py <ds> --sources=pricing,smf,dam_clearing
    python silver_batch_runner.py 1970-01-01 --backfill --sources=pricing,smf

--backfill is detected via sys.argv by BaseEpiasSparkJob itself (unchanged
behavior) — this runner just needs to make sure it's present in sys.argv when
required, which the caller (Airflow operator args) already guarantees.
"""
from __future__ import annotations

import sys
import importlib
import inspect
import logging

from pyspark.sql import SparkSession

from spark_utils import BaseEpiasSparkJob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_batch_runner")


def _parse_args(argv: list[str]) -> tuple[str, list[str]]:
    ds = None
    sources: list[str] = []
    for arg in argv[1:]:
        if arg.startswith("--sources="):
            sources = [s.strip() for s in arg.split("=", 1)[1].split(",") if s.strip()]
        elif arg == "--backfill":
            continue  # detected globally via sys.argv by BaseEpiasSparkJob
        elif ds is None:
            ds = arg
    if ds is None or not sources:
        raise ValueError(
            "Usage: silver_batch_runner.py <ds> [--backfill] --sources=a,b,c"
        )
    return ds, sources


def _find_job_class(source: str):
    """Import bronze_to_silver_<source> and return its BaseEpiasSparkJob subclass."""
    module = importlib.import_module(f"bronze_to_silver_{source}")
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, BaseEpiasSparkJob) and obj is not BaseEpiasSparkJob:
            return obj
    raise ImportError(f"No BaseEpiasSparkJob subclass found in bronze_to_silver_{source}.py")


def main():
    ds, sources = _parse_args(sys.argv)
    backfill_mode = "--backfill" in sys.argv

    builder = SparkSession.builder \
        .appName("EpiasSilverBatch") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

    if backfill_mode:
        builder = builder \
            .config("spark.hadoop.fs.gs.outputstream.type", "SYNCABLE_COMPOSITE") \
            .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "8388608") \
            .config("spark.sql.shuffle.partitions", "400")

    spark = builder.getOrCreate()

    logger.info(f"=== Silver batch run: {len(sources)} kaynak, ds={ds}, backfill={backfill_mode} ===")
    failures = []

    for source in sources:
        logger.info(f"--- Başlıyor: {source} ---")
        try:
            job_cls = _find_job_class(source)
            job = job_cls(spark=spark)  # each subclass forwards spark= to BaseEpiasSparkJob
            job.run(ds)
            logger.info(f"--- Bitti (başarılı): {source} ---")
        except Exception:
            logger.exception(f"--- HATA: {source} — bu kaynak atlandı, diğerleri devam ediyor ---")
            failures.append(source)

    spark.stop()

    if failures:
        raise RuntimeError(f"Şu kaynaklar başarısız oldu: {', '.join(failures)}")


if __name__ == "__main__":
    main()
