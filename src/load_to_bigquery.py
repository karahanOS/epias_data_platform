import os
from google.cloud import bigquery, storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/gcp-key.json"

bq_client = bigquery.Client()
gcs_client = storage.Client()

BUCKET_NAME = "epias-data-lake"
DATASET = "epias_gold"

tables = [
    "price_spread_analysis",
    "generation_mix_price_impact",
    "supply_demand_summary",
    "monthly_executive_metrics",
    "load_vs_actual",
    "renewable_deep_analysis",
    "ml_features", 
]

def list_parquet_files(prefix):
    """GCS'deki tüm parquet dosyalarını listele"""
    bucket = gcs_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=prefix)
    uris = [
        f"gs://{BUCKET_NAME}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]
    return uris

for table_name in tables:
    print(f"\n{table_name} yükleniyor...")

    prefix = f"gold/{table_name}/"
    uris = list_parquet_files(prefix)

    if not uris:
        print(f"⚠️  {table_name} için dosya bulunamadı, atlanıyor.")
        continue

    print(f"  {len(uris)} parquet dosyası bulundu")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    load_job = bq_client.load_table_from_uri(
        source_uris=uris,
        destination=f"{DATASET}.{table_name}",
        job_config=job_config,
    )

    load_job.result()
    table = bq_client.get_table(f"{DATASET}.{table_name}")
    print(f"✅ {table_name} tamamlandı! ({table.num_rows} satır)")

print("\n🎉 Tüm tablolar BigQuery'e yüklendi!")