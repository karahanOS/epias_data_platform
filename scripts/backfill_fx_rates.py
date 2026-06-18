"""
FX Rates Backfill
=================
TCMB XML endpoint kullanarak tarihsel USD/TRY kurlarini BigQuery silver.fx_rates'e yazar.
API key gerektirmez.

Calistir:
    python scripts/backfill_fx_rates.py --start 2025-01-01 --end 2026-06-03
"""
import argparse
import os
import sys
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\epias_data_platform\credentials\gcp-key.json"
sys.path.insert(0, r"C:\epias_data_platform\src")

from dotenv import load_dotenv
load_dotenv(r"C:\epias_data_platform\.env")

from fx_client import FXClient
from google.cloud import bigquery

PROJECT    = "epias-data-platform"
FULL_TABLE = f"{PROJECT}.silver.fx_rates"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end",
                        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"))
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d")
    biz_days = sum(
        1 for i in range((end_dt - start_dt).days + 1)
        if (start_dt + timedelta(days=i)).weekday() < 5
    )
    logger.info(f"TCMB XML'den {biz_days} is-gunu kuru cekilecek ({args.start} -> {args.end})")
    logger.info("Tahmini sure: ~%.0f saniye (hafta sonlari otomatik atlanir)", biz_days * 0.15)

    client_fx = FXClient()
    rows = client_fx.get_usdtry_range(args.start, args.end)

    if not rows:
        logger.error("Hic veri cekilemedi. TCMB XML erisimini kontrol et.")
        return

    logger.info(f"{len(rows)} satir BigQuery'e yaziliyor -> {FULL_TABLE}")

    client_bq  = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("date",       "DATE"),
            bigquery.SchemaField("usdtry",     "FLOAT64"),
            bigquery.SchemaField("is_ffilled", "BOOL"),
            bigquery.SchemaField("source",     "STRING"),
        ],
    )

    job = client_bq.load_table_from_json(rows, FULL_TABLE, job_config=job_config)
    job.result()

    table = client_bq.get_table(FULL_TABLE)
    logger.info(f"Tamamlandi. {table.num_rows} satir {FULL_TABLE} tablosunda.")


if __name__ == "__main__":
    main()
