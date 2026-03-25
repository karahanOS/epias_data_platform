from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
lep = spark.read.parquet(f"gs://{BUCKET}/silver/load_estimation/")
weather = spark.read.parquet(f"gs://{BUCKET}/silver/weather/")

print(f"Weather: {weather.count()} kayıt")
print(f"Load Estimation: {lep.count()} kayıt")
print(f"PTF: {ptf.count()} kayıt")
print(f"SMF: {smf.count()} kayıt")
print(f"Generation: {gen.count()} kayıt")
print(f"Consumption: {con.count()} kayıt")

# ── DEBUG: FORMAT KONTROLÜ ────────────────────────────────────────────────────

print("\nPTF örnekleri:")
ptf.select("date", "hour").show(5)

print("SMF örnekleri:")
smf.select("date", "hour").show(5)

print("Generation örnekleri:")
gen.select("date", "hour").show(5)

print("Consumption örnekleri:")
con.select("date", "hour").show(5)

# ── JOIN KEY OLUŞTUR ──────────────────────────────────────────────────────────
# date UTC'de, hour TR saatinde — join_key = "yyyy-MM-dd HH:mm"

def add_join_key(df):
    return df.withColumn(
        "join_key",
        F.concat(
            F.date_format(F.col("date"), "yyyy-MM-dd"),
            F.lit(" "),
            F.col("hour")
        )
    )

ptf_clean = add_join_key(ptf)
smf_clean = add_join_key(smf)
gen_clean  = add_join_key(gen)
con_clean  = add_join_key(con)
lep_clean = add_join_key(lep)
weather_clean = add_join_key(weather)

# ── DEBUG: JOIN KEY KONTROLÜ ──────────────────────────────────────────────────
print("LEP tarih aralığı:")
lep.select(F.min("date"), F.max("date")).show()

print("Consumption tarih aralığı:")
con.select(F.min("date"), F.max("date")).show()

print("LEP join_key örnekleri:")
lep_clean.select("date", "hour", "join_key").show(5)

print("Consumption join_key örnekleri:")
con_clean.select("date", "hour", "join_key").show(5)

print("\nPTF join_key örnekleri:")
ptf_clean.select("date", "hour", "join_key").show(5)

print("SMF join_key örnekleri:")
smf_clean.select("date", "hour", "join_key").show(5)

print("Eşleşen kayıt sayısı (PTF-SMF):")
match_count = ptf_clean.alias("ptf").join(
    smf_clean.alias("smf"),
    on=["join_key"],
    how="inner"
).count()
print(f"→ {match_count} kayıt eşleşti")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 1: gold_price_spread_analysis
# ═════════════════════════════════════════════════════════════════════════════

print("\n[1/4] gold_price_spread_analysis oluşturuluyor...")

gold_price_spread = ptf_clean.alias("ptf").join(
    smf_clean.alias("smf"),
    on=["join_key"],
    how="inner"
) \
.withColumn(
    "price_spread", F.round(F.col("ptf.price") - F.col("smf.system_marginal_price"), 2)
) \
.withColumn(
    "system_direction",
    F.when(F.col("price_spread") > 0, "Enerji Açığı")
     .when(F.col("price_spread") < 0, "Enerji Fazlası")
     .otherwise("Dengeli")
) \
.withColumn("season",
    F.when(F.month("ptf.date").isin(12, 1, 2), "Kış")
     .when(F.month("ptf.date").isin(3, 4, 5), "İlkbahar")
     .when(F.month("ptf.date").isin(6, 7, 8), "Yaz")
     .otherwise("Sonbahar")
) \
.select(
    F.col("ptf.date").alias("date"),
    F.col("ptf.hour").alias("hour"),
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
# ═════════════════════════════════════════════════════════════════════════════

print("\n[2/4] gold_generation_mix_price_impact oluşturuluyor...")

renewable_cols = ["wind", "sun", "river", "dammed_hydro", "geothermal", "biomass"]
fossil_cols    = ["natural_gas", "lignite", "import_coal", "asphaltite_coal",
                  "black_coal", "fuel_oil", "naphta", "lng"]

gen_with_ratios = gen_clean \
    .withColumn("renewable_generation", sum(F.col(c) for c in renewable_cols)) \
    .withColumn("fossil_generation", sum(F.col(c) for c in fossil_cols)) \
    .withColumn("renewable_ratio", F.round(F.col("renewable_generation") / F.col("total") * 100, 2)) \
    .withColumn("fossil_ratio", F.round(F.col("fossil_generation") / F.col("total") * 100, 2))

gold_gen_mix = gen_with_ratios.alias("gen").join(
    ptf_clean.alias("ptf"),
    on=["join_key"],
    how="inner"
) \
.select(
    F.col("gen.date").alias("date"),
    F.col("gen.hour").alias("hour"),
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
# ═════════════════════════════════════════════════════════════════════════════

print("\n[3/4] gold_supply_demand_summary oluşturuluyor...")

gold_supply_demand = gen_clean.alias("gen").join(
    con_clean.alias("con"),
    on=["join_key"],
    how="inner"
) \
.withColumn(
    "coverage_ratio",
    F.round(F.col("gen.total") / F.col("con.consumption") * 100, 2)
) \
.withColumn(
    "time_of_day",
    F.when(F.col("gen.hour").between("06:00", "11:00"), "Sabah Peak")
     .when(F.col("gen.hour").between("12:00", "16:00"), "Öğle")
     .when(F.col("gen.hour").between("17:00", "22:00"), "Akşam Peak")
     .otherwise("Gece")
) \
.select(
    F.col("gen.date").alias("date"),
    F.col("gen.hour").alias("hour"),
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
    F.count(F.when(F.col("system_direction") == "Enerji Açığı", 1)).alias("energy_deficit_hours"),
    F.count(F.when(F.col("system_direction") == "Enerji Fazlası", 1)).alias("energy_surplus_hours"),
)

gold_monthly = monthly_ptf \
    .join(monthly_consumption, on=["year", "month"], how="left") \
    .join(monthly_spread, on=["year", "month"], how="left") \
    .withColumn(
        "year_month",
        F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("month"), 2, "0"))
    ) \
    .orderBy("year", "month")

gold_monthly.show(10)
gold_monthly.write \
    .mode("overwrite") \
    .parquet(f"gs://{BUCKET}/gold/monthly_executive_metrics/")

print("gold_monthly_executive_metrics tamamlandı!")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 5: gold_load_vs_actual
# Tahmin edilen tüketim vs gerçekleşen tüketim — sapma analizi
# ═════════════════════════════════════════════════════════════════════════════

print("\n[5/5] gold_load_vs_actual oluşturuluyor...")

gold_load_vs_actual = lep.alias("lep").join(
    con.alias("con"),
    on=["date"],
    how="inner"
) \
.withColumn(
    "deviation", F.round(F.col("con.consumption") - F.col("lep.lep"), 2)
) \
.withColumn(
    "deviation_pct", F.round(
        (F.col("con.consumption") - F.col("lep.lep")) / F.col("lep.lep") * 100, 2
    )
) \
.withColumn(
    "deviation_direction",
    F.when(F.col("deviation") > 0, "Tüketim Fazla")
     .when(F.col("deviation") < 0, "Tüketim Az")
     .otherwise("Dengeli")
) \
.select(
    F.col("lep.date").alias("date"),
    F.col("lep.hour").alias("hour"),
    F.col("lep.lep").alias("forecast_consumption"),
    F.col("con.consumption").alias("actual_consumption"),
    "deviation",
    "deviation_pct",
    "deviation_direction",
    F.col("lep.year").alias("year"),
    F.col("lep.month").alias("month"),
)

gold_load_vs_actual.show(5)
gold_load_vs_actual.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"gs://{BUCKET}/gold/load_vs_actual/")

print("gold_load_vs_actual tamamlandı!")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 6: gold_renewable_deep_analysis
# Rüzgar, güneş, hidrolik bazında Merit Order derinlemesine analiz
# ═════════════════════════════════════════════════════════════════════════════

print("\n[6/6] gold_renewable_deep_analysis oluşturuluyor...")

gold_renewable_deep = gen_clean.alias("gen").join(
    ptf_clean.alias("ptf"),
    on=["join_key"],
    how="inner"
) \
.withColumn("wind_ratio",  F.round(F.col("wind")  / F.col("total") * 100, 2)) \
.withColumn("sun_ratio",   F.round(F.col("sun")   / F.col("total") * 100, 2)) \
.withColumn("hydro_ratio", F.round((F.col("dammed_hydro") + F.col("river")) / F.col("total") * 100, 2)) \
.withColumn("gas_ratio",   F.round(F.col("natural_gas") / F.col("total") * 100, 2)) \
.withColumn("coal_ratio",  F.round((F.col("lignite") + F.col("import_coal") + F.col("black_coal") + F.col("asphaltite_coal")) / F.col("total") * 100, 2)) \
.withColumn("combined_renewable_ratio", F.round(
    (F.col("wind") + F.col("sun") + F.col("dammed_hydro") + F.col("river") + F.col("geothermal") + F.col("biomass")) / F.col("total") * 100, 2
)) \
.withColumn("time_of_day",
    F.when(F.col("gen.hour").between("06:00", "09:00"), "Sabah")
     .when(F.col("gen.hour").between("10:00", "14:00"), "Öğle")
     .when(F.col("gen.hour").between("15:00", "19:00"), "Akşam")
     .otherwise("Gece")
) \
.withColumn("season",
    F.when(F.month("gen.date").isin(12, 1, 2), "Kış")
     .when(F.month("gen.date").isin(3, 4, 5), "İlkbahar")
     .when(F.month("gen.date").isin(6, 7, 8), "Yaz")
     .otherwise("Sonbahar")
) \
.select(
    F.col("gen.date").alias("date"),
    F.col("gen.hour").alias("hour"),
    "time_of_day",
    "season",
    F.col("gen.total").alias("total_generation"),
    F.col("gen.wind").alias("wind"),
    F.col("gen.sun").alias("sun"),
    F.col("gen.dammed_hydro").alias("dammed_hydro"),
    F.col("gen.river").alias("river"),
    F.col("gen.natural_gas").alias("natural_gas"),
    "wind_ratio",
    "sun_ratio",
    "hydro_ratio",
    "gas_ratio",
    "coal_ratio",
    "combined_renewable_ratio",
    F.col("ptf.price").alias("ptf"),
    F.col("gen.year").alias("year"),
    F.col("gen.month").alias("month"),
)

gold_renewable_deep.show(5)
gold_renewable_deep.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"gs://{BUCKET}/gold/renewable_deep_analysis/")

print("gold_renewable_deep_analysis tamamlandı!")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 7: gold_ml_features
# PTF tahmini için feature tablosu — tüm veriler birleşik
# ═════════════════════════════════════════════════════════════════════════════

print("\n[7/7] gold_ml_features oluşturuluyor...")

gold_ml = ptf_clean.alias("ptf").join(
    weather_clean.alias("w"), on=["join_key"], how="inner"
).join(
    gen_clean.alias("gen"), on=["join_key"], how="left"
).join(
    con_clean.alias("con"), on=["join_key"], how="left"
).join(
    lep_clean.alias("lep"), on=["join_key"], how="left"
) \
.withColumn("hour_of_day", F.hour(F.col("ptf.date"))) \
.withColumn("day_of_week", F.dayofweek(F.col("ptf.date"))) \
.withColumn("is_weekend",
    F.when(F.dayofweek(F.col("ptf.date")).isin(1, 7), 1).otherwise(0)
) \
.withColumn("month_of_year", F.month(F.col("ptf.date"))) \
.withColumn("season",
    F.when(F.month("ptf.date").isin(12, 1, 2), "Kış")
     .when(F.month("ptf.date").isin(3, 4, 5), "İlkbahar")
     .when(F.month("ptf.date").isin(6, 7, 8), "Yaz")
     .otherwise("Sonbahar")
) \
.select(
    F.col("ptf.date").alias("date"),
    F.col("ptf.hour").alias("hour"),
    F.col("ptf.price").alias("ptf"),                          # TARGET
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "month_of_year",
    "season",
    F.col("w.weighted_temperature_2m").alias("temperature"),
    F.col("w.weighted_wind_speed_10m").alias("wind_speed"),
    F.col("w.weighted_shortwave_radiation").alias("solar_radiation"),
    F.col("w.weighted_relative_humidity_2m").alias("humidity"),
    F.col("gen.wind").alias("wind_generation"),
    F.col("gen.sun").alias("solar_generation"),
    F.col("gen.dammed_hydro").alias("hydro_generation"),
    F.col("gen.natural_gas").alias("gas_generation"),
    F.col("gen.total").alias("total_generation"),
    F.col("con.consumption").alias("actual_consumption"),
    F.col("lep.lep").alias("forecast_consumption"),
    F.col("ptf.year").alias("year"),
    F.col("ptf.month").alias("month"),
)

gold_ml.show(5)
gold_ml.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"gs://{BUCKET}/gold/ml_features/")

print("gold_ml_features tamamlandı!")

print("\n✅ Tüm Gold tablolar oluşturuldu!")

spark.stop()