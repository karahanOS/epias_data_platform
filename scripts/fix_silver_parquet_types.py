"""
Silver Parquet Type Repair
==========================
INT64 vs FLOAT64 tip uyumsuzluğunu düzeltir.
BigQuery external table Parquet okurken tip uyuşmazlığında fail oluyor.

Neden olur: Spark JSON infer schema, tüm değerler tam sayıysa INT64 çıkarır.
Spark'ın cast kodu (if metric in df.columns) bu durumda çalışmaz çünkü
sütun zaten INT64 olarak var. DoubleType cast işlemi tutarlı olmayabiliyor.

Çalıştır:
    python scripts/fix_silver_parquet_types.py --source order_down
    python scripts/fix_silver_parquet_types.py --source order_up
    python scripts/fix_silver_parquet_types.py --source order_down --source order_up
"""
import argparse
import io
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\epias_data_platform\credentials\gcp-key.json"

from google.cloud import storage
import pyarrow.parquet as pq
import pyarrow as pa

BUCKET = "epias-data-lake"
# Partition columns should stay INTEGER (Hive compatible)
KEEP_AS_INT = {"year", "month", "day"}


def fix_file(blob: storage.Blob) -> bool:
    """
    Reads a Parquet blob, casts INT64 non-partition columns to FLOAT64,
    and rewrites in-place. Returns True if file was modified.
    """
    data = blob.download_as_bytes()
    table = pq.read_table(io.BytesIO(data))

    int_cols = [
        f.name for f in table.schema
        if pa.types.is_integer(f.type) and f.name not in KEEP_AS_INT
    ]

    if not int_cols:
        return False

    logger.info(f"  INT64 sütunlar bulundu: {int_cols}")
    new_fields = []
    for field in table.schema:
        if pa.types.is_integer(field.type) and field.name not in KEEP_AS_INT:
            new_fields.append(field.with_type(pa.float64()))
        else:
            new_fields.append(field)

    new_schema = pa.schema(new_fields)
    new_table = table.cast(new_schema)

    buf = io.BytesIO()
    pq.write_table(new_table, buf, compression="snappy")
    blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", action="append", required=True,
                        help="Silver kaynak adı (örn: order_down). Tekrar edilebilir.")
    parser.add_argument("--partition", default=None,
                        help="Sadece bu partition'ı fix et (örn: year=2026/month=05/day=29)")
    args = parser.parse_args()

    client = storage.Client(project="epias-data-platform")
    bucket = client.bucket(BUCKET)

    for source in args.source:
        prefix = f"silver/{source}/"
        if args.partition:
            prefix = f"silver/{source}/{args.partition}/"

        logger.info(f"=== {source} taranıyor ({prefix}) ===")
        blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")]
        logger.info(f"{len(blobs)} Parquet dosyası bulundu")

        fixed = 0
        for blob in blobs:
            logger.info(f"Kontrol ediliyor: {blob.name}")
            try:
                if fix_file(blob):
                    logger.info(f"  FIXED: {blob.name}")
                    fixed += 1
            except Exception as exc:
                logger.error(f"  HATA ({blob.name}): {exc}")

        logger.info(f"=== {source}: {fixed}/{len(blobs)} dosya düzeltildi ===")


if __name__ == "__main__":
    main()
