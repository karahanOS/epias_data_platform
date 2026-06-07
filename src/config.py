"""
config.py — Merkezi GCP Konfigürasyonu
=======================================
Tüm src/ scriptleri (trainer, inference, loader, quality check) bu modülü import eder.
Değerler önce ortam değişkenlerinden okunur; yoksa güvenli varsayılan değerler kullanılır.

Ortam değişkenleri (Docker/Airflow'da ayarla):
  GCP_PROJECT_ID              = epias-data-platform
  BQ_GOLD_DATASET             = epias_gold
  BQ_SILVER_DATASET           = silver
  GCS_BUCKET                  = epias-data-lake
  GOOGLE_APPLICATION_CREDENTIALS = /path/to/gcp-key.json   (veya Workload Identity)
"""
from __future__ import annotations

import os
from google.cloud import bigquery
from google.cloud import storage as gcs

# ── GCP Proje & Dataset Ayarları ─────────────────────────────────────────────
GCP_PROJECT_ID    : str = os.getenv("GCP_PROJECT_ID",    "epias-data-platform")
BQ_GOLD_DATASET   : str = os.getenv("BQ_GOLD_DATASET",   "epias_gold")
BQ_SILVER_DATASET : str = os.getenv("BQ_SILVER_DATASET", "silver")
GCS_BUCKET        : str = os.getenv("GCS_BUCKET",        "epias-data-lake")


# ── İstemci Fabrikaları ───────────────────────────────────────────────────────

def get_bq_client() -> bigquery.Client:
    """
    Proje kimliğiyle yapılandırılmış bir BigQuery istemcisi döner.
    Kimlik doğrulama sırası:
      1. GOOGLE_APPLICATION_CREDENTIALS ortam değişkeni (servis hesabı JSON)
      2. Workload Identity / GCE metadata sunucusu
      3. gcloud auth application-default credentials (yerel geliştirme)
    """
    return bigquery.Client(project=GCP_PROJECT_ID)


def get_gcs_client() -> gcs.Client:
    """GCS işlemleri (model artefakt okuma/yazma) için Storage istemcisi döner."""
    return gcs.Client(project=GCP_PROJECT_ID)
