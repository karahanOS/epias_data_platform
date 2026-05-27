import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeSchemaInspector") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

TARGET_DATE = sys.argv[1]  # örn: 2025-01-01

TABLES = [
    "weather",
    "pricing",
    "idm_transactions",
    "order_up",
    "order_down",
    "dpp",
    "injection",
    "participants",
]

GCS_BASE = "gs://epias-data-lake/bronze"

print("\n" + "="*60)
print(f"BRONZE SCHEMA INSPECTION | date={TARGET_DATE}")
print("="*60)

for table in TABLES:
    path = f"{GCS_BASE}/{table}/{TARGET_DATE}.parquet"
    print(f"\n{'─'*60}")
    print(f"TABLE : {table}")
    print(f"PATH  : {path}")
    try:
        df = spark.read.parquet(path)
        print(f"ROWS  : {df.count()}")
        print("SCHEMA:")
        df.printSchema()
        print("SAMPLE (1 row):")
        df.show(1, truncate=False, vertical=True)
    except Exception as e:
        print(f"ERROR : {e}")

spark.stop()