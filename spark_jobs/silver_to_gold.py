import sys
from datetime import datetime
from pyspark.sql import functions as F
from spark_utils import get_spark_session

# ── 1. TARİH PARAMETRESİ ────────────────────────────────────────────────────
if len(sys.argv) < 2:
    raise ValueError("Tarih parametresi eksik! YYYY-MM-DD formatında gönderin.")

target_date = sys.argv[1]
filter_daily = (target_date != "ALL")
if filter_daily:
    tgt_dt = datetime.strptime(target_date, "%Y-%m-%d")
    t_year, t_month, t_day = tgt_dt.year, tgt_dt.month, tgt_dt.day

spark = get_spark_session("epias_silver_to_gold")
spark.sparkContext.setLogLevel("WARN")
BUCKET = "epias-data-lake"

# ── 2. SILVER OKUMA YARDIMCISI ───────────────────────────────────────────────
def read_silver(table_name):
    """Silver katmanını okur, günlük filtre uygular."""
    df = spark.read.option("mergeSchema", "true").parquet(f"gs://{BUCKET}/silver/{table_name}/")
    if filter_daily:
        # HATA DÜZELTİLDİ: tüm silver tablolarda year/month/day kolonu var
        df = df.filter(
            (F.col("year")  == t_year) &
            (F.col("month") == t_month) &
            (F.col("day")   == t_day)
        )
    return df

# HATA DÜZELTİLDİ: silver tablo isimleri gerçek yazma yollarıyla eşleştirildi
# pricing → silver/pricing/, weather → silver/weather/ vb.
pricing      = read_silver("pricing")          # ptf_tl, smf_tl, sdf_tl
unlicensed   = read_silver("unlicensed")       # total_mwh, solar_mwh, wind_mwh
imbalance    = read_silver("imbalance")        # pos/neg/net_imbalance_mwh
dpp          = read_silver("dpp")              # planned_mwh, org_id
participants = read_silver("market_participants")
weather      = read_silver("weather")          # temp_c, humidity_pct, wind_speed_kmh

# ── 3. JOIN KEY ──────────────────────────────────────────────────────────────
def add_join_key(df, date_col="date", hour_col="hour"):
    return df.withColumn(
        "join_key",
        F.concat(F.date_format(F.col(date_col), "yyyy-MM-dd"), F.lit("-"), F.col(hour_col).cast("string"))
    )

pricing_k    = add_join_key(pricing)
unlicensed_k = add_join_key(unlicensed)
imbalance_k  = add_join_key(imbalance)
weather_k    = add_join_key(weather)

# ═══════════════════════════════════════════════════════════════════════════
# GOLD 1: market_prices — Fiyat Yayılımı Analizi
# ═══════════════════════════════════════════════════════════════════════════
print("\n[1/4] gold_market_prices oluşturuluyor...")

gold_prices = pricing_k \
    .withColumn("price_spread", F.round(F.col("ptf_tl") - F.col("smf_tl"), 2)) \
    .withColumn("system_direction",
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
    .select("date", "hour", "ptf_tl", "smf_tl", "sdf_tl",
            "price_spread", "system_direction", "season",
            "year", "month", "day")

gold_prices.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"gs://{BUCKET}/gold/market_prices/")

# ═══════════════════════════════════════════════════════════════════════════
# GOLD 2: renewable_impact — Yenilenebilir Enerji Etkisi
# ═══════════════════════════════════════════════════════════════════════════
print("\n[2/4] gold_renewable_impact oluşturuluyor...")

gold_renewable = unlicensed_k.alias("unl").join(
    pricing_k.alias("ptf"), on="join_key", how="inner"
).select(
    F.col("unl.date"), F.col("unl.hour"),
    F.col("unl.total_mwh").alias("unlicensed_total_mwh"),
    F.col("unl.solar_mwh"), F.col("unl.wind_mwh"),
    F.col("ptf.ptf_tl"), F.col("ptf.smf_tl"),
    F.col("unl.year"), F.col("unl.month"), F.col("unl.day")
)

gold_renewable.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"gs://{BUCKET}/gold/renewable_impact/")

# ═══════════════════════════════════════════════════════════════════════════
# GOLD 3: imbalance_metrics — Dengesizlik Metrikleri
# ═══════════════════════════════════════════════════════════════════════════
print("\n[3/4] gold_imbalance_metrics oluşturuluyor...")

gold_imbalance = imbalance_k.alias("imb").join(
    pricing_k.alias("ptf"), on="join_key", how="inner"
).select(
    F.col("imb.date"), F.col("imb.hour"),
    F.col("imb.pos_imbalance_mwh"), F.col("imb.neg_imbalance_mwh"),
    F.col("imb.net_imbalance"),
    F.col("ptf.ptf_tl"), F.col("ptf.smf_tl"),
    F.when(F.col("imb.net_imbalance") > 0, "Pozitif").otherwise("Negatif").alias("imbalance_direction"),
    F.col("imb.year"), F.col("imb.month"), F.col("imb.day")
)

gold_imbalance.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"gs://{BUCKET}/gold/imbalance_metrics/")

# ═══════════════════════════════════════════════════════════════════════════
# GOLD 4: player_analysis — Piyasa Katılımcıları
# ═══════════════════════════════════════════════════════════════════════════
print("\n[4/4] gold_player_analysis oluşturuluyor...")

gold_players = dpp.alias("dpp").join(
    participants.alias("par"), 
    F.col("dpp.org_id") == F.col("par.org_id"), 
    how="left"
).select(
    F.col("dpp.date"), F.col("dpp.hour"),
    F.col("dpp.org_id"),
    F.col("par.org_name"),
    F.col("dpp.planned_mwh"),
    F.col("dpp.gas_planned_mwh"),
    F.col("dpp.wind_planned_mwh"),
    F.col("par.is_dam_active"), F.col("par.is_idm_active"),
    F.col("dpp.year"), F.col("dpp.month"), F.col("dpp.day")
)

gold_players.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"gs://{BUCKET}/gold/player_analysis/")

print(f"\n✅ Tüm Gold tablolar {target_date} için oluşturuldu!")
spark.stop()