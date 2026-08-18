"""
silver_lookback_fix.py — corrects SMF/system_direction's Silver partition for
"yesterday" once EPİAŞ's S+5 settlement lag has fully cleared.

Root cause (2026-08-18 investigation): SMF/system_direction hours 22-23
(Istanbul) settle at 03:00/04:00 the NEXT Istanbul day. The last hourly
Airflow run for a given `ds` executes right around that boundary — too early
to catch them (see epias_client.py's _safe_end_iso, which narrows same-day
requests to "now - 1h") — and since DATA_DELAYS["get_smf"]/["get_system_direction"]
= 0 means the DAG's `ds` never revisits a past date, Bronze permanently
freezes at whatever was available in that narrow window. Confirmed via
direct Bronze/API inspection 2026-08-18: every day since same-day SMF fetch
went live (2026-08-15) was missing exactly hours 22-23.

silver.smf / silver.system_direction are BigQuery EXTERNAL TABLES reading
live from GCS Hive-partitioned Parquet (hive_partitioning.mode="AUTO", see
load_to_bigquery.py) — there is no load/copy step, so overwriting the GCS
partition is immediately visible to BigQuery. This module bypasses
Dataproc/Spark entirely for this correction (a full Serverless batch is
disproportionate compute for ~24 rows) by replicating just the relevant
transform logic from bronze_to_silver_smf.py / bronze_to_silver_system_direction.py
in plain pandas/pyarrow. The physical schema (column names/types, including
_record_hash) is matched exactly against the live BigQuery table schema —
confirmed via `bq.get_table()` — since Hive-partitioned external tables
require a consistent schema across every partition file.
"""

import hashlib
import logging
import tempfile
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import get_gcs_client

logger = logging.getLogger("SilverLookbackFix")

_BUCKET = "epias-data-lake"

# Matches the live `bq.get_table()` schema exactly (verified 2026-08-18) —
# BigQuery's Hive-partitioned external tables need every partition file to
# share one physical schema. year/month/day are NOT included here: they are
# derived from the GCS path itself (hive_partitioning.mode="AUTO"), never
# physically written into the partition's own parquet file (matches Spark's
# partitionBy() behavior, which strips partition columns from the file body).
_SMF_SCHEMA = pa.schema([
    ("date",                pa.timestamp("us", tz="UTC")),
    ("hour",                pa.string()),
    ("systemMarginalPrice", pa.float64()),
    ("_record_hash",        pa.string()),
])

_SYSTEM_DIRECTION_SCHEMA = pa.schema([
    ("date",           pa.timestamp("us", tz="UTC")),
    ("hour",           pa.string()),
    ("systemDirection", pa.string()),
    ("smpDirectionId", pa.float64()),
    ("_record_hash",   pa.string()),
])


def _record_hash(row: pd.Series) -> str:
    key = "||".join(str(v) if pd.notna(v) else "" for v in row)
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def _partition_prefix(source_name: str, date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"silver/{source_name}/year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}/"


def _write_partition(source_name: str, date_str: str, df: pd.DataFrame,
                      schema: pa.Schema, gcs_root: str = None) -> str:
    """Overwrites the single (year, month, day) Hive partition for date_str —
    same semantics as Spark's partitionOverwriteMode=DYNAMIC: only this exact
    partition is replaced, every other date is untouched. Returns the GCS
    prefix written to. Pass gcs_root="scratch/silver_lookback_test" to write
    to a throwaway location instead of the real production path."""
    prefix = _partition_prefix(source_name, date_str)
    if gcs_root:
        prefix = f"{gcs_root}/{prefix}"

    client = get_gcs_client()
    bucket = client.bucket(_BUCKET)

    existing = list(bucket.list_blobs(prefix=prefix))
    for blob in existing:
        blob.delete()
    if existing:
        logger.info(f"Deleted {len(existing)} existing file(s) under {prefix}")

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        pq.write_table(table, tmp.name)
        blob = bucket.blob(f"{prefix}part-lookback-fix.parquet")
        blob.upload_from_filename(tmp.name)

    logger.info(f"Wrote corrected partition: gs://{_BUCKET}/{prefix} ({len(df)} rows)")
    return prefix


def build_smf_frame(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["systemMarginalPrice"] = df["systemMarginalPrice"].astype("float64")
    df = df[["date", "hour", "systemMarginalPrice"]].drop_duplicates(subset=["date"])
    df["_record_hash"] = df.apply(_record_hash, axis=1)
    return df.reset_index(drop=True)


def build_system_direction_frame(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["smpDirectionId"] = pd.to_numeric(df["smpDirectionId"], errors="coerce").astype("float64")
    df = df[["date", "hour", "systemDirection", "smpDirectionId"]].drop_duplicates(subset=["date"])
    df["_record_hash"] = df.apply(_record_hash, axis=1)
    return df.reset_index(drop=True)


def fix_smf_partition(client, date_str: str, gcs_root: str = None) -> int:
    rows = client.get_smf(date_str, date_str)
    if not rows:
        logger.warning(f"get_smf returned no rows for {date_str} — nothing to fix.")
        return 0
    df = build_smf_frame(rows)
    _write_partition("smf", date_str, df, _SMF_SCHEMA, gcs_root=gcs_root)
    return len(df)


def fix_system_direction_partition(client, date_str: str, gcs_root: str = None) -> int:
    rows = client.get_system_direction(date_str, date_str)
    if not rows:
        logger.warning(f"get_system_direction returned no rows for {date_str} — nothing to fix.")
        return 0
    df = build_system_direction_frame(rows)
    _write_partition("system_direction", date_str, df, _SYSTEM_DIRECTION_SCHEMA, gcs_root=gcs_root)
    return len(df)
