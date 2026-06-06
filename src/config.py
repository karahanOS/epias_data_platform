import os
from google.cloud import bigquery

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "epias-data-platform")
BQ_GOLD_DATASET = os.getenv("BQ_GOLD_DATASET", "epias_gold")
GCS_BUCKET = os.getenv("GCS_BUCKET", "epias-data-lake")


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT_ID)
