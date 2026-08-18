"""
epias_gop_company_activity_dag.py — Günlük GÖP Şirket Bazlı Eşleşme Miktarı
================================================================================
ADR-0007 Faz 1 (plans/07-company-level-market-activity-kgup.md): şirket bazlı
GÖP eşleşme miktarı (matchedBids/matchedOffers). GİP'te organizasyon atfı hiç
yok (16 endpoint tek tek doğrulandı) ama GÖP'te clearing-quantity endpoint'i
organizationId filtresi destekliyor — eksik olan tek şey roster'dı, onu da
clearing-quantity-organization-list veriyor.

Neden ayrı bir DAG (epias_medallion_pipeline_v3'e değil): roster canlı testte
(2026-08-09) 1629 organizasyon döndürdü. Bulk endpoint YOK — her organizasyon
için ayrı bir POST demek, ~80 req/dk limitine göre ~20 dakika. epias_sources.py
EPIAS_SOURCES'taki "daily_eligible" flag'i GERÇEKTE "hourly ALL_SOURCES loop'una
dahil" anlamına geliyor (epias_medallion_pipeline_v3'ün schedule_interval'ı
"0 * * * *" — saatlik, günlük değil). Bu kaynağı oraya eklemek 20 dakikalık işi
saatte bir tekrarlamak, günde ~8 saat API çağrısı ve muhtemelen üst üste binen
DAG run'ları demek olurdu.

Zamanlama: 11:30 UTC (~14:30 TRT) — GÖP açık artırması ~14:00 TRT'de kapanıp
yayınlandıktan sonra, aynı gün içinde veri gerçekten mevcut olsun diye (aynı
mantık dam_clearing/pricing/price_ind_bid'in allow_empty=True gerekçesiyle —
bkz. epias_sources.py).
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from epias_sources import EPIAS_SOURCES, make_silver_batch_task

sys.path.insert(0, "/opt/airflow/src")
try:
    from epias_client import EPIASClient
except ImportError as exc:
    logging.error(f"Modül yükleme hatası: {exc}")

logger = logging.getLogger(__name__)

BUCKET_NAME = "epias-data-lake"
SOURCE_KEY  = "dam_clearing_by_org"
METHOD_NAME, GCS_SUBPATH, ALLOW_EMPTY, _, _ = EPIAS_SOURCES[SOURCE_KEY]


def fetch_and_save_callable(**context) -> None:
    """
    epias_dag.py'deki get_epias_data_callable + save_to_gcs_callable'ın tek
    task'a birleştirilmiş hali — bu kaynak kendi DAG'ında yalnız, xcom üzerinden
    iki task arasında taşımaya gerek yok. GCS yazım deseni (int64->float64,
    datetimetz->str, gs://{BUCKET}/{path}/{ds}.parquet) ana DAG'daki
    save_to_gcs_callable ile birebir aynı — Silver'daki BaseEpiasSparkJob.read_bronze()
    her kaynak için aynı path yapısını beklediği için.
    """
    ds = context["ds"]
    client = EPIASClient()
    method = getattr(client, METHOD_NAME)
    data = method(ds, ds)

    if not data:
        if ALLOW_EMPTY:
            logger.warning(
                f"{SOURCE_KEY}: {ds} için veri yok (14:00 TRT öncesi tetiklenmiş "
                f"olabilir ya da o gün hiçbir organizasyon eşleşme yapmamış olabilir)."
            )
            return
        raise ValueError(f"🚨 {SOURCE_KEY}: {ds} için veri gelmedi.")

    df = pd.DataFrame(data)

    # Aynı gerekçe save_to_gcs_callable ile birebir: bir sütunun o günkü tüm
    # değerleri (örn. çoğu organizasyon için matchedBids=0) yuvarlak sayı ise
    # pandas int64 çıkarır; Spark/BigQuery external table DOUBLE bekliyor.
    int_cols = df.select_dtypes(include=["int32", "int64"]).columns.tolist()
    if int_cols:
        df[int_cols] = df[int_cols].astype("float64")
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].astype(str)

    gcs_path = f"gs://{BUCKET_NAME}/{GCS_SUBPATH}/{ds}.parquet"
    df.to_parquet(gcs_path, index=False)
    logger.info(f"✅ {SOURCE_KEY}: {len(df)} satır -> {gcs_path}")


default_args = {
    "owner":       "epias_team",
    "retries":     1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="epias_gop_company_activity_daily",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="30 11 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["epias", "gop", "company-activity"],
) as dag:

    fetch_bronze = PythonOperator(
        task_id="fetch_and_save_dam_clearing_by_org",
        python_callable=fetch_and_save_callable,
    )

    # Tek kaynaklu Dataproc Serverless batch — ADR-0003'teki pool ile aynı
    # dataproc_batches slot'larını paylaşır (bkz. make_silver_batch_task),
    # hourly pipeline'la aynı anda tetiklenirse Airflow pool semantiği ile
    # sıraya girer, çakışma riski yok.
    silver_batch = make_silver_batch_task(
        "silver_dam_clearing_by_org", [SOURCE_KEY], ["{{ ds }}"]
    )

    fetch_bronze >> silver_batch
