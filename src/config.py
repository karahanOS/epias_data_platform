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
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage as gcs
from google.oauth2 import service_account

# ── GCP Proje & Dataset Ayarları ─────────────────────────────────────────────
GCP_PROJECT_ID    : str = os.getenv("GCP_PROJECT_ID",    "epias-data-platform")
BQ_GOLD_DATASET   : str = os.getenv("BQ_GOLD_DATASET",   "epias_gold")
BQ_SILVER_DATASET : str = os.getenv("BQ_SILVER_DATASET", "silver")
GCS_BUCKET        : str = os.getenv("GCS_BUCKET",        "epias-data-lake")

# ── Kimlik Doğrulama ─────────────────────────────────────────────────────────
# Kimlik doğrulama sırası:
#   1. GOOGLE_APPLICATION_CREDENTIALS ortam değişkeni (Docker/Airflow/CI)
#   2. credentials/gcp-key.json (yerel geliştirme — Windows, macOS)
#   3. Workload Identity / GCE metadata sunucusu
#   4. gcloud auth application-default credentials
#
# Öncelik sırasını açık tutmak için credentials nesnesini burada çözüyoruz;
# Client() her çağrıda tekrar çözümleme yapmak yerine aynı nesneyi kullanır.

_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/devstorage.read_write",
]

# Bilinen yerel anahtar dosyası konumları (öncelik sırasına göre)
_LOCAL_KEY_CANDIDATES = [
    Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")),          # env var (any path)
    Path(__file__).parents[1] / "credentials" / "gcp-key.json",    # repo-root/credentials/
    Path("C:/epias_data_platform/credentials/gcp-key.json"),        # Windows absolute path
    Path("/opt/airflow/credentials/gcp-key.json"),                  # Docker/Airflow
]


def _resolve_credentials():
    """Return a google.auth credentials object, preferring the service-account key."""
    for candidate in _LOCAL_KEY_CANDIDATES:
        if candidate and candidate.is_file():
            return service_account.Credentials.from_service_account_file(
                str(candidate), scopes=_SCOPES
            )
    # Fall back to ADC (gcloud auth, Workload Identity, GCE metadata)
    import google.auth
    creds, _ = google.auth.default(scopes=_SCOPES)
    return creds


_CREDENTIALS = _resolve_credentials()


# ── İstemci Fabrikaları ───────────────────────────────────────────────────────

def get_bq_client() -> bigquery.Client:
    """Proje kimliğiyle yapılandırılmış bir BigQuery istemcisi döner."""
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=_CREDENTIALS)


def get_gcs_client() -> gcs.Client:
    """GCS işlemleri (model artefakt okuma/yazma) için Storage istemcisi döner."""
    return gcs.Client(project=GCP_PROJECT_ID, credentials=_CREDENTIALS)
