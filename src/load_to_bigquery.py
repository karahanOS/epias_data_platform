import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/gcp-key.json"

client = bigquery.Client()

tables = [
    ("price_spread_analysis",       "gs://epias-data-lake/gold/price_spread_analysis/"),
    ("generation_mix_price_impact", "gs://epias-data-lake/gold/generation_mix_price_impact/"),
    ("supply_demand_summary",       "gs://epias-data-lake/gold/supply_demand_summary/"),
    ("monthly_executive_metrics",   "gs://epias-data-lake/gold/monthly_executive_metrics/"),
]

for table_name, gcs_uri in tables:
    print(f"{table_name} yükleniyor...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(
        source_uris=gcs_uri + "**/*.parquet",
        destination=f"epias_gold.{table_name}",
        job_config=job_config,
    )

    load_job.result()  # Bitmesini bekle
    print(f"✅ {table_name} tamamlandı!")

print("\n🎉 Tüm tablolar BigQuery'e yüklendi!")