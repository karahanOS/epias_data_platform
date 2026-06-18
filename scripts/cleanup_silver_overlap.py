"""
Silver Overlap Cleanup
======================
Backfill DAG'ın eski "append" moduyla daily pipeline'ın başladığı tarihten
itibaren oluşturduğu duplicate Silver partition'larını siler.

Çalıştır (lokal):
    python scripts/cleanup_silver_overlap.py

Silme işlemi geri alınamaz. Daily DAG bir sonraki çalışmasında
bu partition'ları yeniden yazar.
"""
import os
from google.cloud import storage

CREDENTIALS_PATH = r"C:\epias_data_platform\credentials\gcp-key.json"
BUCKET_NAME      = "epias-data-lake"

# Daily pipeline'ın başladığı gün (dahil) → bugün (dahil)
OVERLAP_YEAR  = "2026"
OVERLAP_MONTH = "06"
OVERLAP_DAYS  = [str(d).zfill(2) for d in range(4, 17)]  # "04" … "16"

SOURCES = [
    "pricing", "smf", "consumption", "dam_clearing", "price_ind_bid",
    "idm_transactions", "order_up", "order_down", "system_direction",
    "dpp", "aic", "imbalance", "res_forecast", "generation",
    "load_estimation", "outages", "dams",
]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

total_deleted = 0

for source in SOURCES:
    for day in OVERLAP_DAYS:
        prefix = f"silver/{source}/year={OVERLAP_YEAR}/month={OVERLAP_MONTH}/day={day}/"
        blobs  = list(bucket.list_blobs(prefix=prefix))
        if blobs:
            bucket.delete_blobs(blobs)
            total_deleted += len(blobs)
            print(f"✅  {len(blobs):3d} dosya silindi → {prefix}")
        else:
            print(f"   (boş) {prefix}")

print(f"\nToplam {total_deleted} dosya silindi.")
print("Daily DAG bir sonraki çalışmasında bu partition'ları yeniden yazar.")
