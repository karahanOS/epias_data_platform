import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from spark_utils import get_spark_session

if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! Lütfen YYYY-MM-DD formatında bir tarih gönderin.")

target_date = sys.argv[1]
spark = get_spark_session("epias_bronze_to_silver_generation")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"
BRONZE_PATH = f"gs://{BUCKET}/bronze/generation/{target_date}.parquet"
SILVER_PATH = f"gs://{BUCKET}/silver/generation/"

COLUMN_MAPPING = {
    "naturalGas": "natural_gas", "dammedHydro": "dammed_hydro",
    "importCoal": "import_coal", "asphaltiteCoal": "asphaltite_coal",
    "blackCoal": "black_coal", "importExport": "import_export",
    "wasteheat": "waste_heat", "fueloil": "fuel_oil",
}

schema = StructType([
    StructField("date", StringType(), True), StructField("hour", StringType(), True),
    StructField("total", DoubleType(), True), StructField("naturalGas", DoubleType(), True),
    StructField("dammedHydro", DoubleType(), True), StructField("lignite", DoubleType(), True),
    StructField("river", DoubleType(), True), StructField("importCoal", DoubleType(), True),
    StructField("wind", DoubleType(), True), StructField("sun", DoubleType(), True),
    StructField("fueloil", DoubleType(), True), StructField("geothermal", DoubleType(), True),
    StructField("asphaltiteCoal", DoubleType(), True), StructField("blackCoal", DoubleType(), True),
    StructField("biomass", DoubleType(), True), StructField("naphta", DoubleType(), True),
    StructField("lng", DoubleType(), True), StructField("importExport", DoubleType(), True),
    StructField("wasteheat", DoubleType(), True),
])

print(f"{target_date} tarihi için Bronze okunuyor: {BRONZE_PATH}")
df = spark.read.schema(schema).option("mergeSchema", "false").parquet(BRONZE_PATH)

print("Dönüşümler uygulanıyor...")
numeric_cols = [
    "total", "naturalGas", "dammedHydro", "lignite", "river", "importCoal", "wind", "sun", 
    "fueloil", "geothermal", "asphaltiteCoal", "blackCoal", "biomass", "naphta", "lng", 
    "importExport", "wasteheat"
]

df_silver = df.dropDuplicates().dropna(subset=["date", "total"])

for col in numeric_cols:
    df_silver = df_silver.withColumn(col, F.round(F.col(col).cast(DoubleType()), 2))

df_silver = df_silver \
    .withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("hour", F.lpad(F.col("hour"), 5, "0"))

for old_name, new_name in COLUMN_MAPPING.items():
    df_silver = df_silver.withColumnRenamed(old_name, new_name)

df_silver = df_silver \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("day", F.dayofmonth(F.col("date")))

print(f"\nSilver'a yazılıyor: {SILVER_PATH}")
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_PATH)

print(f"{target_date} için Bronze → Silver Generation dönüşümü tamamlandı!")
spark.stop()