from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── SPARK SESSION ─────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("epias_silver_to_gold") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/credentials/gcp-key.json") \
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"

# ── SILVER'DAN OKU ────────────────────────────────────────────────────────────

print("Silver veriler okunuyor...")

ptf = spark.read.parquet(f"gs://{BUCKET}/silver/ptf/")
smf = spark.read.parquet(f"gs://{BUCKET}/silver/smf/")
gen = spark.read.parquet(f"gs://{BUCKET}/silver/generation/")
con = spark.read.parquet(f"gs://{BUCKET}/silver/consumption/")

print(f"PTF: {ptf.count()} kayıt")
print(f"SMF: {smf.count()} kayıt")
print(f"Generation: {gen.count()} kayıt")
print(f"Consumption: {con.count()} kayıt")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 1: gold_price_spread_analysis
# PTF vs SMF — fiyat makası ve sistem dengesizliği analizi
# ═════════════════════════════════════════════════════════════════════════════

print("\n[1/4] gold_price_spread_analysis oluşturuluyor...")

gold_price_spread = ptf.alias("ptf").join(
    smf.alias("smf"),
    on=["date", "hour"],
    how="inner"
) \
.withColumn(
    "price_spread", F.round(F.col("price") - F.col("system_marginal_price"), 2)
) \
.withColumn(
    # Spread pozitifse sistem enerji açığında (SMF > PTF değil, PTF > SMF)
    # Spread negatifse sistem enerji fazlasında
    "system_direction",
    F.when(F.col("price_spread") > 0, "Enerji Açığı")
     .when(F.col("price_spread") < 0, "Enerji Fazlası")
     .otherwise("Dengeli")
) \
.withColumn("season",
    F.when(F.month("date").isin(12, 1, 2), "Kış")
     .when(F.month("date").isin(3, 4, 5), "İlkbahar")
     .when(F.month("date").isin(6, 7, 8), "Yaz")
     .otherwise("Sonbahar")
) \
.select(
    "date", "hour",
    F.col("ptf.price").alias("ptf"),
    F.col("smf.system_marginal_price").alias("smf"),
    "price_spread",
    "system_direction",
    "season",
    F.col("ptf.year").alias("year"),
    F.col("ptf.month").alias("month"),
)

gold_price_spread.show(5)
gold_price_spread.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"gs://{BUCKET}/gold/price_spread_analysis/")

print("gold_price_spread_analysis tamamlandı!")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 2: gold_generation_mix_price_impact
# Yenilenebilir üretim oranı ve PTF ilişkisi (Merit Order Effect)
# ═════════════════════════════════════════════════════════════════════════════

print("\n[2/4] gold_generation_mix_price_impact oluşturuluyor...")

renewable_cols = ["wind", "sun", "river", "dammed_hydro", "geothermal", "biomass"]
fossil_cols    = ["natural_gas", "lignite", "import_coal", "asphaltite_coal",
                  "black_coal", "fuel_oil", "naphta", "lng"]

gen_with_ratios = gen \
    .withColumn(
        "renewable_generation",
        sum(F.col(c) for c in renewable_cols)
    ) \
    .withColumn(
        "fossil_generation",
        sum(F.col(c) for c in fossil_cols)
    ) \
    .withColumn(
        "renewable_ratio",
        F.round(F.col("renewable_generation") / F.col("total") * 100, 2)
    ) \
    .withColumn(
        "fossil_ratio",
        F.round(F.col("fossil_generation") / F.col("total") * 100, 2)
    )

gold_gen_mix = gen_with_ratios.alias("gen").join(
    ptf.alias("ptf"),
    on=["date", "hour"],
    how="inner"
) \
.select(
    "date", "hour",
    F.col("gen.total").alias("total_generation"),
    "renewable_generation",
    "fossil_generation",
    "renewable_ratio",
    "fossil_ratio",
    F.col("ptf.price").alias("ptf"),
    F.col("gen.year").alias("year"),
    F.col("gen.month").alias("month"),
)

gold_gen_mix.show(5)
gold_gen_mix.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"gs://{BUCKET}/gold/generation_mix_price_impact/")

print("gold_generation_mix_price_impact tamamlandı!")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 3: gold_supply_demand_summary
# Arz-talep karşılaması ve peak saat analizi
# ═════════════════════════════════════════════════════════════════════════════

print("\n[3/4] gold_supply_demand_summary oluşturuluyor...")

gold_supply_demand = gen.alias("gen").join(
    con.alias("con"),
    on=["date", "hour"],
    how="inner"
) \
.withColumn(
    "coverage_ratio",
    F.round(F.col("gen.total") / F.col("con.consumption") * 100, 2)
) \
.withColumn(
    "time_of_day",
    F.when(F.col("hour").between("06:00", "11:00"), "Sabah Peak")
     .when(F.col("hour").between("12:00", "16:00"), "Öğle")
     .when(F.col("hour").between("17:00", "22:00"), "Akşam Peak")
     .otherwise("Gece")
) \
.select(
    "date", "hour",
    "time_of_day",
    F.col("gen.total").alias("total_generation"),
    F.col("con.consumption").alias("total_consumption"),
    "coverage_ratio",
    F.col("gen.year").alias("year"),
    F.col("gen.month").alias("month"),
)

gold_supply_demand.show(5)
gold_supply_demand.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"gs://{BUCKET}/gold/supply_demand_summary/")

print("gold_supply_demand_summary tamamlandı!")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 4: gold_monthly_executive_metrics
# Aylık özet — executive summary KPI tablosu
# ═════════════════════════════════════════════════════════════════════════════

print("\n[4/4] gold_monthly_executive_metrics oluşturuluyor...")

monthly_ptf = ptf.groupBy("year", "month").agg(
    F.round(F.avg("price"), 2).alias("avg_ptf"),
    F.round(F.max("price"), 2).alias("max_ptf"),
    F.round(F.min("price"), 2).alias("min_ptf"),
)

monthly_consumption = con.groupBy("year", "month").agg(
    F.round(F.sum("consumption"), 2).alias("total_consumption"),
    F.round(F.avg("consumption"), 2).alias("avg_hourly_consumption"),
)

monthly_spread = gold_price_spread.groupBy("year", "month").agg(
    F.round(F.avg("price_spread"), 2).alias("avg_price_spread"),
    F.count(F.when(F.col("system_direction") == "Enerji Açığı", 1))
     .alias("energy_deficit_hours"),
    F.count(F.when(F.col("system_direction") == "Enerji Fazlası", 1))
     .alias("energy_surplus_hours"),
)

gold_monthly = monthly_ptf \
    .join(monthly_consumption, on=["year", "month"], how="left") \
    .join(monthly_spread, on=["year", "month"], how="left") \
    .withColumn(
        "year_month", F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("month"), 2, "0"))
    ) \
    .orderBy("year", "month")

gold_monthly.show(10)
gold_monthly.write \
    .mode("overwrite") \
    .parquet(f"gs://{BUCKET}/gold/monthly_executive_metrics/")

print("gold_monthly_executive_metrics tamamlandı!")
print("\n✅ Tüm Gold tablolar oluşturuldu!")

spark.stop()