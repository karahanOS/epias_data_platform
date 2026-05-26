"""
silver_to_gold.py — EPIAS Medallion Gold Katmanı
=================================================
Silver tablolarını okuyup analitik gold tablolarına dönüştürür.

Gold tablolar:
  1. market_prices      — Saatlik PTF/SMF/SDF + arz-talep özeti
  2. dam_supply_demand  — GÖP arz-talep eğrisi ve hacim analizi
  3. idm_activity       — GİP saatlik işlem aktivitesi
  4. dgp_orders         — DGP YAL/YAT talimatları + sistem yönü
  5. renewable_impact   — Yenilenebilir üretim ve fiyat etkisi
  6. player_analysis    — Katılımcı bazlı piyasa aktivitesi
  7. imbalance_metrics  — Dengesizlik ve sistem yönü özeti

Çağrı: spark-submit silver_to_gold.py <YYYY-MM-DD>
"""

import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_utils import get_spark_session, gold_path, silver_path

BUCKET = "epias-data-lake"


# ── YARDIMCI ─────────────────────────────────────────────────────────────────

def read_silver(spark: SparkSession, source: str, ds: str) -> DataFrame:
    """
    Silver tablosunu gün filtresiyle okur.
    Tablo yoksa boş DataFrame döner (pipeline durmasın).
    """
    dt   = datetime.strptime(ds, "%Y-%m-%d")
    path = (
        f"{silver_path(source)}"
        f"/year={dt.year}/month={dt.month}/day={dt.day}"
    )
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        print(f"⚠️  Silver okunamadı ({source}): {exc}")
        return spark.createDataFrame([], schema=None)   # boş df


def write_gold(df: DataFrame, table: str, ds: str) -> None:
    """Gold tablosunu partition'lı yazar."""
    dt   = datetime.strptime(ds, "%Y-%m-%d")
    path = (
        f"{gold_path(table)}"
        f"/year={dt.year}/month={dt.month}/day={dt.day}"
    )
    if df is None or df.rdd.isEmpty():
        print(f"⚠️  Gold boş, yazım atlandı: {table}")
        return
    count = df.count()
    (
        df.write
        .mode("overwrite")
        .parquet(path)
    )
    print(f"✅ Gold yazıldı: {table} | {count} satır → {path}")


# ── GOLD 1: market_prices ─────────────────────────────────────────────────────

def build_market_prices(spark: SparkSession, ds: str) -> DataFrame:
    """
    Saatlik piyasa fiyatları: PTF, SMF, SDF farkları, GÖP eşleşme hacmi.

    Kaynak:  silver/pricing, silver/smf, silver/dam_clearing, silver/imbalance
    Çıktı:
      date, ptf, smf, ptf_smf_spread,
      dam_clearing_mwh, imbalance_mwh, system_direction
    """
    pricing  = read_silver(spark, "pricing",     ds)
    smf      = read_silver(spark, "smf",          ds)
    clearing = read_silver(spark, "dam_clearing", ds)
    imbal    = read_silver(spark, "imbalance",    ds)

    # PTF
    if pricing.rdd.isEmpty():
        return None
    df = (
        pricing
        .select(
            F.col("date"),
            F.col("marketTradePrice").alias("ptf"),
        )
    )

    # SMF join
    if not smf.rdd.isEmpty():
        smf_sel = smf.select(
            F.col("date"),
            F.col("systemMarginalPrice").alias("smf"),
            F.col("positiveImbalancePrice").alias("sdf_positive"),
            F.col("negativeImbalancePrice").alias("sdf_negative"),
        )
        df = df.join(smf_sel, on="date", how="left")
        df = df.withColumn("ptf_smf_spread", F.col("ptf") - F.col("smf"))

    # GÖP eşleşme hacmi
    if not clearing.rdd.isEmpty():
        clr_sel = clearing.select(
            F.col("date"),
            (F.col("matchedBidsQuantity") + F.col("matchedOffersQuantity")).alias("dam_total_mwh"),
        )
        df = df.join(clr_sel, on="date", how="left")

    # Dengesizlik
    if not imbal.rdd.isEmpty():
        imb_sel = imbal.select(
            F.col("date"),
            F.col("imbalanceQuantity").alias("imbalance_mwh"),
            F.col("systemDirection"),
        )
        df = df.join(imb_sel, on="date", how="left")

    return df.orderBy("date")


# ── GOLD 2: dam_supply_demand ─────────────────────────────────────────────────

def build_dam_supply_demand(spark: SparkSession, ds: str) -> DataFrame:
    """
    GÖP arz-talep dengesi ve teklif hacimleri.

    Kaynak: silver/supply_demand, silver/bid_volume, silver/sales_volume,
            silver/price_ind_bid, silver/price_ind_offer, silver/dam_volume
    Çıktı:
      date, ptf, total_demand_mwh, total_supply_mwh,
      bid_0_volume, sales_max_volume, price_ind_bid, price_ind_offer,
      trade_volume_try, surplus_mwh
    """
    sd          = read_silver(spark, "supply_demand",  ds)
    bid_vol     = read_silver(spark, "bid_volume",     ds)
    sales_vol   = read_silver(spark, "sales_volume",   ds)
    pi_bid      = read_silver(spark, "price_ind_bid",  ds)
    pi_offer    = read_silver(spark, "price_ind_offer",ds)
    dam_vol     = read_silver(spark, "dam_volume",     ds)

    if sd.rdd.isEmpty():
        return None

    # PTF (matchPrice) ve saat bazında max talep/arz
    df = (
        sd
        .groupBy("date")
        .agg(
            F.last("matchPrice").alias("ptf"),
            F.sum("demand").alias("total_demand_mwh"),
            F.sum("supply").alias("total_supply_mwh"),
        )
    )
    df = df.withColumn("surplus_mwh", F.col("total_supply_mwh") - F.col("total_demand_mwh"))

    def _join_vol(base, src_df, src_col, alias):
        if src_df.rdd.isEmpty():
            return base
        return base.join(
            src_df.select("date", F.col(src_col).alias(alias)),
            on="date", how="left",
        )

    df = _join_vol(df, bid_vol,   "volume",   "bid_0_volume")
    df = _join_vol(df, sales_vol, "volume",   "sales_max_volume")
    df = _join_vol(df, pi_bid,    "quantity", "price_ind_bid_mwh")
    df = _join_vol(df, pi_offer,  "quantity", "price_ind_offer_mwh")
    df = _join_vol(df, dam_vol,   "volume",   "trade_volume_try")

    return df.orderBy("date")


# ── GOLD 3: idm_activity ──────────────────────────────────────────────────────

def build_idm_activity(spark: SparkSession, ds: str) -> DataFrame:
    """
    GİP saatlik aktivite özeti: işlem hacmi, ağırlıklı ortalama fiyat,
    PTF'den sapma ve toplam alış/satış teklif hacimleri.

    Kaynak: silver/idm_transactions, silver/idm_matching,
            silver/idm_wap, silver/idm_bid_offer, silver/pricing
    Çıktı:
      date, idm_tx_count, idm_total_mwh, idm_wap,
      idm_bid_mwh, idm_offer_mwh, ptf, idm_ptf_spread
    """
    txn     = read_silver(spark, "idm_transactions", ds)
    match   = read_silver(spark, "idm_matching",     ds)
    wap     = read_silver(spark, "idm_wap",          ds)
    bid_off = read_silver(spark, "idm_bid_offer",    ds)
    pricing = read_silver(spark, "pricing",           ds)

    # İşlem sayısı ve toplam hacim
    if not txn.rdd.isEmpty():
        df = (
            txn
            .groupBy("date")
            .agg(
                F.count("*").alias("idm_tx_count"),
                F.sum("quantity").alias("idm_total_mwh"),
                F.avg("price").alias("idm_avg_price"),
            )
        )
    elif not match.rdd.isEmpty():
        df = (
            match
            .groupBy("date")
            .agg(
                F.sum("matchedQuantity").alias("idm_total_mwh"),
                F.avg("matchedPrice").alias("idm_avg_price"),
            )
        )
    else:
        return None

    if not wap.rdd.isEmpty():
        df = df.join(
            wap.select("date", F.col("weightedAveragePrice").alias("idm_wap")),
            on="date", how="left",
        )

    if not bid_off.rdd.isEmpty():
        df = df.join(
            bid_off.select("date", "bidQuantity", "offerQuantity"),
            on="date", how="left",
        )

    # PTF ile spread hesapla
    if not pricing.rdd.isEmpty():
        df = df.join(
            pricing.select("date", F.col("marketTradePrice").alias("ptf")),
            on="date", how="left",
        )
        df = df.withColumn(
            "idm_ptf_spread",
            F.when(F.col("idm_wap").isNotNull(), F.col("idm_wap") - F.col("ptf"))
             .when(F.col("idm_avg_price").isNotNull(), F.col("idm_avg_price") - F.col("ptf"))
        )

    return df.orderBy("date")


# ── GOLD 4: dgp_orders ────────────────────────────────────────────────────────

def build_dgp_orders(spark: SparkSession, ds: str) -> DataFrame:
    """
    DGP talimat özeti: saatlik YAL/YAT hacmi, SMF, sistem yönü.

    Kaynak: silver/order_up, silver/order_down,
            silver/system_direction, silver/smf
    Çıktı:
      date, yal_total_mwh, yat_total_mwh, net_regulation_mwh,
      smf, system_direction, top_yal_org, top_yat_org
    """
    up   = read_silver(spark, "order_up",         ds)
    down = read_silver(spark, "order_down",        ds)
    sdir = read_silver(spark, "system_direction",  ds)
    smf  = read_silver(spark, "smf",               ds)

    if up.rdd.isEmpty() and down.rdd.isEmpty():
        return None

    dfs = []

    if not up.rdd.isEmpty():
        yal = (
            up
            .groupBy("date")
            .agg(
                F.sum("upRegulationDeliveredAmount").alias("yal_total_mwh"),
                F.first("upRegulationZeroCodedOfferPrice").alias("yal_price"),
            )
        )
        dfs.append(("yal", yal))

    if not down.rdd.isEmpty():
        yat = (
            down
            .groupBy("date")
            .agg(
                F.sum("downRegulationDeliveredAmount").alias("yat_total_mwh"),
                F.first("downRegulationZeroCodedBidPrice").alias("yat_price"),
            )
        )
        dfs.append(("yat", yat))

    df = dfs[0][1]
    for _, other in dfs[1:]:
        df = df.join(other, on="date", how="outer")

    df = df.withColumn(
        "net_regulation_mwh",
        F.coalesce(F.col("yal_total_mwh"), F.lit(0.0))
        - F.coalesce(F.col("yat_total_mwh"), F.lit(0.0)),
    )

    if not sdir.rdd.isEmpty():
        df = df.join(sdir.select("date", "systemDirection"), on="date", how="left")

    if not smf.rdd.isEmpty():
        df = df.join(
            smf.select("date", F.col("systemMarginalPrice").alias("smf")),
            on="date", how="left",
        )

    return df.orderBy("date")


# ── GOLD 5: renewable_impact ──────────────────────────────────────────────────

def build_renewable_impact(spark: SparkSession, ds: str) -> DataFrame:
    """
    Yenilenebilir üretimin fiyat üzerindeki etkisi.

    Kaynak: silver/unlicensed, silver/res_forecast, silver/licensed_gen,
            silver/pricing, silver/imbalance, silver/weather
    Çıktı:
      date, total_renewable_mwh, unlicensed_mwh, licensed_mwh,
      res_forecast_mwh, forecast_error_mwh,
      ptf, ptf_vs_renewable_corr (window analizi),
      city (weather join için ayrı satırlar)
    """
    unlic   = read_silver(spark, "unlicensed",   ds)
    res_fc  = read_silver(spark, "res_forecast", ds)
    lic_gen = read_silver(spark, "licensed_gen", ds)
    pricing = read_silver(spark, "pricing",       ds)
    imbal   = read_silver(spark, "imbalance",     ds)
    weather = read_silver(spark, "weather",       ds)

    # Ana tablo: toplam yenilenebilir üretim
    if unlic.rdd.isEmpty() and lic_gen.rdd.isEmpty():
        return None

    dfs = []
    if not unlic.rdd.isEmpty():
        dfs.append(
            unlic.select(
                "date",
                F.col("total").alias("unlicensed_mwh"),
                F.col("solar").alias("unlicensed_solar"),
                F.col("wind").alias("unlicensed_wind"),
            )
        )
    if not lic_gen.rdd.isEmpty():
        dfs.append(
            lic_gen.select(
                "date",
                F.col("total").alias("licensed_mwh"),
                F.col("solar").alias("licensed_solar"),
                F.col("wind").alias("licensed_wind"),
                F.col("geothermal").alias("licensed_geo"),
            )
        )

    df = dfs[0]
    for other in dfs[1:]:
        df = df.join(other, on="date", how="outer")

    df = df.withColumn(
        "total_renewable_mwh",
        F.coalesce(F.col("unlicensed_mwh"), F.lit(0.0))
        + F.coalesce(F.col("licensed_mwh"),  F.lit(0.0)),
    )

    # RES tahmin hatası
    if not res_fc.rdd.isEmpty():
        fc = res_fc.select(
            "date",
            F.col("total").alias("res_actual"),
            F.col("forecast").alias("res_forecast_mwh"),
        )
        fc = fc.withColumn("forecast_error_mwh", F.col("res_actual") - F.col("res_forecast_mwh"))
        df = df.join(fc, on="date", how="left")

    # PTF join
    if not pricing.rdd.isEmpty():
        df = df.join(
            pricing.select("date", F.col("marketTradePrice").alias("ptf")),
            on="date", how="left",
        )

    # Hava durumu: istanbul'un sıcaklık ve rüzgar verisi eklenir
    if not weather.rdd.isEmpty():
        ist = (
            weather
            .filter(F.col("city") == "istanbul")
            .select(
                "date",
                F.col("temperature_2m").alias("istanbul_temp"),
                F.col("windspeed_10m").alias("istanbul_wind"),
                F.col("shortwave_radiation").alias("istanbul_solar_rad"),
            )
        )
        df = df.join(ist, on="date", how="left")

    return df.orderBy("date")


# ── GOLD 6: player_analysis ──────────────────────────────────────────────────

def build_player_analysis(spark: SparkSession, ds: str) -> DataFrame:
    """
    Piyasa katılımcısı bazlı aktivite özeti.

    Kaynak: silver/participants, silver/idm_transactions,
            silver/dpp, silver/injection
    Çıktı:
      organizationId, organizationName, marketTypes,
      idm_buy_mwh, idm_sell_mwh, idm_net_mwh,
      dpp_total_mwh, injection_total_mwh, kgup_deviation_mwh
    """
    parts  = read_silver(spark, "participants",     ds)
    txn    = read_silver(spark, "idm_transactions", ds)
    dpp    = read_silver(spark, "dpp",              ds)
    inject = read_silver(spark, "injection",        ds)

    if parts.rdd.isEmpty():
        return None

    df = parts.select("organizationId", "organizationName", "marketTypes")

    # GİP alış/satış
    if not txn.rdd.isEmpty():
        buy = (
            txn
            .groupBy(F.col("buyerOrganizationId").alias("organizationId"))
            .agg(F.sum("quantity").alias("idm_buy_mwh"))
        )
        sell = (
            txn
            .groupBy(F.col("sellerOrganizationId").alias("organizationId"))
            .agg(F.sum("quantity").alias("idm_sell_mwh"))
        )
        df = df.join(buy,  on="organizationId", how="left")
        df = df.join(sell, on="organizationId", how="left")
        df = df.withColumn(
            "idm_net_mwh",
            F.coalesce(F.col("idm_buy_mwh"), F.lit(0.0))
            - F.coalesce(F.col("idm_sell_mwh"), F.lit(0.0)),
        )

    # KGÜP ve UEVM toplam
    if not dpp.rdd.isEmpty():
        dpp_agg = (
            dpp
            .groupBy("organizationId")
            .agg(F.sum("total").alias("dpp_total_mwh"))
        )
        df = df.join(dpp_agg, on="organizationId", how="left")

    if not inject.rdd.isEmpty():
        inj_agg = (
            inject
            .groupBy("organizationId")
            .agg(F.sum("total").alias("injection_total_mwh"))
        )
        df = df.join(inj_agg, on="organizationId", how="left")

    # KGÜP sapması
    if "dpp_total_mwh" in df.columns and "injection_total_mwh" in df.columns:
        df = df.withColumn(
            "kgup_deviation_mwh",
            F.col("injection_total_mwh") - F.col("dpp_total_mwh"),
        )

    # Tarih partition için ds ekle
    dt = datetime.strptime(ds, "%Y-%m-%d")
    df = (
        df
        .withColumn("year",  F.lit(dt.year))
        .withColumn("month", F.lit(dt.month))
        .withColumn("day",   F.lit(dt.day))
    )
    return df


# ── GOLD 7: imbalance_metrics ─────────────────────────────────────────────────

def build_imbalance_metrics(spark: SparkSession, ds: str) -> DataFrame:
    """
    Dengesizlik özet metrikleri: saatlik dengesizlik, günlük toplam,
    KGÜP vs gerçekleşen karşılaştırması.

    Kaynak: silver/imbalance, silver/system_direction,
            silver/dpp, silver/injection, silver/aic
    Çıktı:
      date, imbalance_mwh, system_direction, cumulative_imbalance,
      dpp_total, injection_total, deviation_mwh, aic_total
    """
    imbal  = read_silver(spark, "imbalance",        ds)
    sdir   = read_silver(spark, "system_direction",  ds)
    dpp    = read_silver(spark, "dpp",               ds)
    inject = read_silver(spark, "injection",         ds)
    aic    = read_silver(spark, "aic",               ds)

    if imbal.rdd.isEmpty():
        return None

    df = imbal.select(
        "date",
        F.col("imbalanceQuantity").alias("imbalance_mwh"),
        "systemDirection",
        F.col("generationTotal").alias("gen_total_mwh"),
        F.col("consumption").alias("consumption_mwh"),
    )

    # Sistem yönü ek detay (API'den)
    if not sdir.rdd.isEmpty() and "systemDirection" not in imbal.columns:
        df = df.join(sdir.select("date", "systemDirection"), on="date", how="left")

    # Kümülatif dengesizlik (gün içi)
    w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("cumulative_imbalance_mwh", F.sum("imbalance_mwh").over(w))

    # Sistem geneli KGÜP vs enjeksiyon
    if not dpp.rdd.isEmpty():
        dpp_total = (
            dpp
            .groupBy("date")
            .agg(F.sum("total").alias("dpp_system_total_mwh"))
        )
        df = df.join(dpp_total, on="date", how="left")

    if not inject.rdd.isEmpty():
        inj_total = (
            inject
            .groupBy("date")
            .agg(F.sum("total").alias("injection_system_total_mwh"))
        )
        df = df.join(inj_total, on="date", how="left")

    if "dpp_system_total_mwh" in df.columns and "injection_system_total_mwh" in df.columns:
        df = df.withColumn(
            "dpp_deviation_mwh",
            F.col("injection_system_total_mwh") - F.col("dpp_system_total_mwh"),
        )

    # Emre amade kapasite
    if not aic.rdd.isEmpty():
        aic_total = (
            aic
            .groupBy("date")
            .agg(F.sum("total").alias("aic_system_total_mw"))
        )
        df = df.join(aic_total, on="date", how="left")

    return df.orderBy("date")


# ── ANA AKIŞ ──────────────────────────────────────────────────────────────────

GOLD_BUILDERS = {
    "market_prices":     build_market_prices,
    "dam_supply_demand": build_dam_supply_demand,
    "idm_activity":      build_idm_activity,
    "dgp_orders":        build_dgp_orders,
    "renewable_impact":  build_renewable_impact,
    "player_analysis":   build_player_analysis,
    "imbalance_metrics": build_imbalance_metrics,
}


def main(ds: str) -> None:
    spark = get_spark_session(f"SilverToGold_{ds}")
    print(f"Gold katmanı oluşturuluyor: {ds}")

    for table, builder in GOLD_BUILDERS.items():
        print(f"\n── {table} ──")
        try:
            df = builder(spark, ds)
            write_gold(df, table, ds)
        except Exception as exc:
            print(f"❌ {table} başarısız: {exc}")
            import traceback
            traceback.print_exc()

    spark.stop()
    print("\n✅ Gold katmanı tamamlandı")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Kullanım: silver_to_gold.py <YYYY-MM-DD>")
        sys.exit(1)
    main(sys.argv[1])