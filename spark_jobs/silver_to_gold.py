import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark_utils import get_spark_session

# ── 1. DIŞARIDAN GELEN TARİH PARAMETRESİNİ YAKALA ─────────────────────────────
if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! Lütfen YYYY-MM-DD formatında bir tarih gönderin.")

target_date = sys.argv[1]
tgt_dt = datetime.strptime(target_date, "%Y-%m-%d")
t_year, t_month, t_day = tgt_dt.year, tgt_dt.month, tgt_dt.day

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = get_spark_session("epias_silver_to_gold")
spark.sparkContext.setLogLevel("WARN")

BUCKET = "epias-data-lake"

# ── 2. SILVER'DAN SADECE İLGİLİ GÜNÜ OKU (PREDICATE PUSHDOWN) ─────────────────
print(f"{target_date} tarihi için Silver veriler okunuyor...")

def read_daily_silver(table_name):
    return spark.read.option("mergeSchema", "true") \
                .parquet(f"gs://{BUCKET}/silver/{table_name}/") \
                .filter((F.col("year") == t_year) & (F.col("month") == t_month) & (F.col("day") == t_day))

ptf = read_daily_silver("ptf")
smf = read_daily_silver("smf")
gen = read_daily_silver("generation")
con = read_daily_silver("consumption")
lep = read_daily_silver("load_estimation")
weather = read_daily_silver("weather")

# ── JOIN KEY OLUŞTUR ──────────────────────────────────────────────────────────
def add_join_key(df):
    return df.withColumn(
        "join_key",
        F.concat(F.date_format(F.col("date"), "yyyy-MM-dd"), F.lit(" "), F.col("hour"))
    )

ptf_clean = add_join_key(ptf)
smf_clean = add_join_key(smf)
gen_clean = add_join_key(gen)
con_clean = add_join_key(con)
lep_clean = add_join_key(lep)
weather_clean = add_join_key(weather)


# ═════════════════════════════════════════════════════════════════════════════
# GOLD 1: gold_price_spread_analysis
# ═════════════════════════════════════════════════════════════════════════════
print("\n[1/7] gold_price_spread_analysis oluşturuluyor...")

gold_price_spread = ptf_clean.alias("ptf").join(
    smf_clean.alias("smf"), on=["join_key"], how="inner"
) \
.withColumn("price_spread", F.round(F.col("ptf.price") - F.col("smf.system_marginal_price"), 2)) \
.withColumn("price_spread_usd", F.round(F.col("smf.smp_usd") - F.col("ptf.price_usd"), 4)) \
.withColumn("system_direction",
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
    F.col("ptf.date").alias("date"), F.col("ptf.hour").alias("hour"),
    F.col("ptf.price").alias("ptf"), F.col("smf.system_marginal_price").alias("smf"),
    F.col("ptf.price_usd").alias("mcpUsd"), F.col("smf.smp_usd").alias("smpUsd"), 
    "price_spread", "price_spread_usd", "system_direction", "season",
    F.col("ptf.year").alias("year"), F.col("ptf.month").alias("month"),
    F.col("ptf.day").alias("day")
)

gold_price_spread.write.mode("overwrite").partitionBy("year", "month", "day").parquet(f"gs://{BUCKET}/gold/price_spread_analysis/")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 2: gold_generation_mix_price_impact
# ═════════════════════════════════════════════════════════════════════════════
print("\n[2/7] gold_generation_mix_price_impact oluşturuluyor...")

renewable_cols = ["wind", "sun", "river", "dammed_hydro", "geothermal", "biomass"]
fossil_cols    = ["natural_gas", "lignite", "import_coal", "asphaltite_coal", "black_coal", "fuel_oil", "naphta", "lng"]

gen_with_ratios = gen_clean \
    .withColumn("renewable_generation", sum(F.col(c) for c in renewable_cols)) \
    .withColumn("fossil_generation", sum(F.col(c) for c in fossil_cols)) \
    .withColumn("renewable_ratio", F.round(F.col("renewable_generation") / F.col("total") * 100, 2)) \
    .withColumn("fossil_ratio", F.round(F.col("fossil_generation") / F.col("total") * 100, 2))

gold_gen_mix = gen_with_ratios.alias("gen").join(
    ptf_clean.alias("ptf"), on=["join_key"], how="inner"
).select(
    F.col("gen.date").alias("date"), F.col("gen.hour").alias("hour"),
    F.col("gen.total").alias("total_generation"),
    "renewable_generation", "fossil_generation", "renewable_ratio", "fossil_ratio",
    F.col("ptf.price_usd").alias("mcpUsd"),
    F.col("gen.year").alias("year"), F.col("gen.month").alias("month"), F.col("gen.day").alias("day")
)

gold_gen_mix.write.mode("overwrite").partitionBy("year", "month", "day").parquet(f"gs://{BUCKET}/gold/generation_mix_price_impact/")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 3: gold_supply_demand_summary
# ═════════════════════════════════════════════════════════════════════════════
print("\n[3/7] gold_supply_demand_summary oluşturuluyor...")

gold_supply_demand = gen_clean.alias("gen").join(
    con_clean.alias("con"), on=["join_key"], how="inner"
) \
.withColumn("coverage_ratio", F.round(F.col("gen.total") / F.col("con.consumption") * 100, 2)) \
.withColumn("time_of_day",
    F.when(F.col("gen.hour").between("06:00", "11:00"), "Sabah Peak")
     .when(F.col("gen.hour").between("12:00", "16:00"), "Öğle")
     .when(F.col("gen.hour").between("17:00", "22:00"), "Akşam Peak")
     .otherwise("Gece")
) \
.select(
    F.col("gen.date").alias("date"), F.col("gen.hour").alias("hour"), "time_of_day",
    F.col("gen.total").alias("total_generation"), F.col("con.consumption").alias("total_consumption"),
    "coverage_ratio",
    F.col("gen.year").alias("year"), F.col("gen.month").alias("month"), F.col("gen.day").alias("day")
)

gold_supply_demand.write.mode("overwrite").partitionBy("year", "month", "day").parquet(f"gs://{BUCKET}/gold/supply_demand_summary/")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 4: gold_load_vs_actual
# ═════════════════════════════════════════════════════════════════════════════
print("\n[4/7] gold_load_vs_actual oluşturuluyor...")

gold_load_vs_actual = lep.alias("lep").join(
    con.alias("con"), on=["date"], how="inner"
) \
.withColumn("deviation", F.round(F.col("con.consumption") - F.col("lep.lep"), 2)) \
.withColumn("deviation_pct", F.round((F.col("con.consumption") - F.col("lep.lep")) / F.col("lep.lep") * 100, 2)) \
.withColumn("deviation_direction",
    F.when(F.col("deviation") > 0, "Tüketim Fazla")
     .when(F.col("deviation") < 0, "Tüketim Az")
     .otherwise("Dengeli")
) \
.select(
    F.col("lep.date").alias("date"), F.col("lep.hour").alias("hour"),
    F.col("lep.lep").alias("forecast_consumption"), F.col("con.consumption").alias("actual_consumption"),
    "deviation", "deviation_pct", "deviation_direction",
    F.col("lep.year").alias("year"), F.col("lep.month").alias("month"), F.col("lep.day").alias("day")
)

gold_load_vs_actual.write.mode("overwrite").partitionBy("year", "month", "day").parquet(f"gs://{BUCKET}/gold/load_vs_actual/")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 5: gold_renewable_deep_analysis
# ═════════════════════════════════════════════════════════════════════════════
print("\n[5/7] gold_renewable_deep_analysis oluşturuluyor...")

gold_renewable_deep = gen_clean.alias("gen").join(
    ptf_clean.alias("ptf"), on=["join_key"], how="inner"
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
    F.col("gen.date").alias("date"), F.col("gen.hour").alias("hour"), "time_of_day", "season",
    F.col("gen.total").alias("total_generation"), F.col("gen.wind").alias("wind"), F.col("gen.sun").alias("sun"),
    F.col("gen.dammed_hydro").alias("dammed_hydro"), F.col("gen.river").alias("river"), F.col("gen.natural_gas").alias("natural_gas"),
    "wind_ratio", "sun_ratio", "hydro_ratio", "gas_ratio", "coal_ratio", "combined_renewable_ratio",
    F.col("ptf.price").alias("ptf"),
    F.col("ptf.price_usd").alias("mcpUsd"),
    F.col("gen.year").alias("year"), F.col("gen.month").alias("month"), F.col("gen.day").alias("day")
)

gold_renewable_deep.write.mode("overwrite").partitionBy("year", "month", "day").parquet(f"gs://{BUCKET}/gold/renewable_deep_analysis/")

# ═════════════════════════════════════════════════════════════════════════════
# GOLD 6: gold_ml_features
# ═════════════════════════════════════════════════════════════════════════════
print("\n[6/7] gold_ml_features oluşturuluyor...")

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
.withColumn("is_weekend", F.when(F.dayofweek(F.col("ptf.date")).isin(1, 7), 1).otherwise(0)) \
.withColumn("month_of_year", F.month(F.col("ptf.date"))) \
.withColumn("season",
    F.when(F.month("ptf.date").isin(12, 1, 2), "Kış")
     .when(F.month("ptf.date").isin(3, 4, 5), "İlkbahar")
     .when(F.month("ptf.date").isin(6, 7, 8), "Yaz")
     .otherwise("Sonbahar")
) \
.select(
    F.col("ptf.date").alias("date"), F.col("ptf.hour").alias("hour"), F.col("ptf.price").alias("ptf"),
    "hour_of_day", "day_of_week", "is_weekend", "month_of_year", "season",
    F.col("w.weighted_temperature_2m").alias("temperature"), F.col("w.weighted_wind_speed_10m").alias("wind_speed"),
    F.col("w.weighted_shortwave_radiation").alias("solar_radiation"), F.col("w.weighted_relative_humidity_2m").alias("humidity"),
    F.col("gen.wind").alias("wind_generation"), F.col("gen.sun").alias("solar_generation"),
    F.col("gen.dammed_hydro").alias("hydro_generation"), F.col("gen.natural_gas").alias("gas_generation"),
    F.col("gen.total").alias("total_generation"), F.col("con.consumption").alias("actual_consumption"),
    F.col("lep.lep").alias("forecast_consumption"),
    F.col("ptf.year").alias("year"), F.col("ptf.month").alias("month"), F.col("ptf.day").alias("day")
)

gold_ml.write.mode("overwrite").partitionBy("year", "month", "day").parquet(f"gs://{BUCKET}/gold/ml_features/")

print("\n✅ Tüm Gold tablolar Incremental olarak oluşturuldu!")
spark.stop()