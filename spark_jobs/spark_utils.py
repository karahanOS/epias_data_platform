"""
spark_utils.py — EPIAS Medallion Pipeline Ortak Yardımcı Modül
===============================================================
Tüm bronze_to_silver_*.py ve silver_to_gold.py bu modülü kullanır.

Mimari:
  Bronze : gs://epias-data-lake/bronze/{source}/{ds}.parquet
  Silver : gs://epias-data-lake/silver/{source}/year=YYYY/month=MM/day=DD/
  Gold   : gs://epias-data-lake/gold/{table}/year=YYYY/month=MM/day=DD/
"""

import logging
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, FloatType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

logger = logging.getLogger(__name__)

BUCKET = "epias-data-lake"

# ── SPARK SESSION ─────────────────────────────────────────────────────────────

def get_spark_session(app_name: str) -> SparkSession:
    """
    GCS connector yapılandırılmış SparkSession döner.
    GCP service account kimlik bilgisi GOOGLE_APPLICATION_CREDENTIALS env'den alınır.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/opt/credentials/gcp-key.json")
        # Parquet optimizasyonları
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── GCS PATH YARDIMCILARI ─────────────────────────────────────────────────────

def bronze_path(source: str, ds: str) -> str:
    return f"gs://{BUCKET}/bronze/{source}/{ds}.parquet"

def silver_path(source: str) -> str:
    return f"gs://{BUCKET}/silver/{source}"

def gold_path(table: str) -> str:
    return f"gs://{BUCKET}/gold/{table}"


# ── ŞEMA TANIMLARI ────────────────────────────────────────────────────────────
# Her kaynak için (alan_adı → SparkType) sözlüğü.
# Yalnızca bilinen alanlar cast edilir; API'den gelen ekstra alanlar StringType olarak korunur.

SCHEMAS: dict[str, dict[str, object]] = {

    # ── FİYAT ─────────────────────────────────────────────────────────────────
    "pricing": {
        "date":             TimestampType(),
        "marketTradePrice": DoubleType(),   # PTF (TRY/MWh)
    },
    "smf": {
        "date":                  TimestampType(),
        "systemMarginalPrice":   DoubleType(),   # SMF (TRY/MWh)
        "positiveImbalancePrice":DoubleType(),   # SDF+
        "negativeImbalancePrice":DoubleType(),   # SDF-
    },

    # ── GÖP ───────────────────────────────────────────────────────────────────
    "supply_demand": {
        # Arz-talep eğrisi; her satır bir fiyat kırılımı
        "date":          TimestampType(),
        "demand":        DoubleType(),    # Talep miktarı (MWh)
        "supply":        DoubleType(),    # Arz miktarı (MWh)
        "price":         DoubleType(),    # Fiyat (TRY/MWh)
        "matchPrice":    DoubleType(),    # PTF (eşleşme fiyatı)
    },
    "dam_clearing": {
        "date":                   TimestampType(),
        "matchedBidsQuantity":    DoubleType(),   # Eşleşen alış (MWh)
        "matchedOffersQuantity":  DoubleType(),   # Eşleşen satış (MWh)
        "blockBidQuantity":       DoubleType(),
        "blockOfferQuantity":     DoubleType(),
    },
    "dam_volume": {
        "date":             TimestampType(),
        "marketTradePrice": DoubleType(),
        "volume":           DoubleType(),   # İşlem hacmi (TRY)
    },
    "bid_volume": {
        "date":   TimestampType(),
        "volume": DoubleType(),   # 0 TRY/MWh alış teklif hacmi
    },
    "sales_volume": {
        "date":   TimestampType(),
        "volume": DoubleType(),   # Azami fiyat satış teklif hacmi
    },
    "price_ind_bid": {
        "date":     TimestampType(),
        "quantity": DoubleType(),   # Fiyat bağımsız alış miktarı (MWh)
    },
    "price_ind_offer": {
        "date":     TimestampType(),
        "quantity": DoubleType(),   # Fiyat bağımsız satış miktarı (MWh)
    },

    # ── GİP ───────────────────────────────────────────────────────────────────
    "idm_transactions": {
        "date":                   TimestampType(),
        "price":                  DoubleType(),
        "quantity":               DoubleType(),
        "buyerOrganizationId":    LongType(),
        "sellerOrganizationId":   LongType(),
        "contractName":           StringType(),
    },
    "idm_matching": {
        "date":             TimestampType(),
        "matchedPrice":     DoubleType(),
        "matchedQuantity":  DoubleType(),
        "contractName":     StringType(),
    },
    "idm_wap": {
        "date":                 TimestampType(),
        "weightedAveragePrice": DoubleType(),
        "matchedQuantity":      DoubleType(),
    },
    "idm_bid_offer": {
        "date":          TimestampType(),
        "bidQuantity":   DoubleType(),
        "offerQuantity": DoubleType(),
    },

    # ── DGP / SİSTEM ──────────────────────────────────────────────────────────
    "order_up": {
        "date":                              TimestampType(),
        "upRegulationZeroCodedOfferPrice":   DoubleType(),   # YAL SMF katkısı
        "upRegulationDeliveredAmount":       DoubleType(),   # Gerçekleşen YAL (MWh)
        "upRegulationActivatedAmount":       DoubleType(),
        "organizationId":                    LongType(),
    },
    "order_down": {
        "date":                                TimestampType(),
        "downRegulationZeroCodedBidPrice":     DoubleType(),
        "downRegulationDeliveredAmount":       DoubleType(),
        "downRegulationActivatedAmount":       DoubleType(),
        "organizationId":                      LongType(),
    },
    "system_direction": {
        "date":            TimestampType(),
        "systemDirection": StringType(),   # ENERGY_SURPLUS / ENERGY_DEFICIT
    },

    # ── ÜRETİM ────────────────────────────────────────────────────────────────
    "dpp": {
        "date":           TimestampType(),
        "organizationId": LongType(),
        "uevcbId":        LongType(),
        "total":          DoubleType(),   # KGÜP (MWh)
    },
    "injection": {
        "date":           TimestampType(),
        "organizationId": LongType(),
        "uevcbId":        LongType(),
        "total":          DoubleType(),   # UEVM (MWh)
    },
    "aic": {
        "date":           TimestampType(),
        "organizationId": LongType(),
        "total":          DoubleType(),   # Emre amade kapasite (MW)
    },

    # ── DENGESİZLİK (hesaplamalı) ─────────────────────────────────────────────
    "imbalance": {
        "date":              TimestampType(),
        "generationTotal":   DoubleType(),
        "consumption":       DoubleType(),
        "imbalanceQuantity": DoubleType(),
        "systemDirection":   StringType(),
    },

    # ── YENİLENEBİLİR ─────────────────────────────────────────────────────────
    "unlicensed": {
        "date":       TimestampType(),
        "solar":      DoubleType(),
        "wind":       DoubleType(),
        "biogas":     DoubleType(),
        "biomass":    DoubleType(),
        "canalType":  DoubleType(),
        "river":      DoubleType(),
        "total":      DoubleType(),
    },
    "res_forecast": {
        "date":     TimestampType(),
        "total":    DoubleType(),
        "forecast": DoubleType(),
    },
    "gen_forecast": {
        "date":     TimestampType(),
        "total":    DoubleType(),
    },
    "licensed_gen": {
        "date":       TimestampType(),
        "total":      DoubleType(),
        "solar":      DoubleType(),
        "wind":       DoubleType(),
        "geothermal": DoubleType(),
    },

    # ── YEKDEM MALİYET (aylık) ─────────────────────────────────────────────────
    "unlicensed_cost": {
        "date":       TimestampType(),
        "total":      DoubleType(),
        "solar":      DoubleType(),
        "wind":       DoubleType(),
    },
    "yekdem_total": {
        "date":  TimestampType(),
        "total": DoubleType(),
    },
    "yekdem_unit": {
        "date":     TimestampType(),
        "unitCost": DoubleType(),
    },
    "new_capacity": {
        "date":   TimestampType(),
        "total":  DoubleType(),
        "solar":  DoubleType(),
        "wind":   DoubleType(),
    },

    # ── REFERANS ──────────────────────────────────────────────────────────────
    "participants": {
        "organizationId":   LongType(),
        "organizationName": StringType(),
        "organizationETSOCode": StringType(),
        "marketTypes":      StringType(),
    },
    "renewables_part": {
        "year":             IntegerType(),
        "organizationId":   LongType(),
        "organizationName": StringType(),
        "licenseType":      StringType(),
    },

    # ── HAVA DURUMU ───────────────────────────────────────────────────────────
    "weather": {
        "date":           TimestampType(),
        "city":           StringType(),
        "temperature_2m": DoubleType(),
        "windspeed_10m":  DoubleType(),
        "precipitation":  DoubleType(),
        "cloudcover":     DoubleType(),
        "shortwave_radiation": DoubleType(),
    },
}

# Tarih sütununun adı — bazı endpoint'lerde farklı
DATE_COLUMN: dict[str, str] = {
    "participants":    "organizationId",  # tarih yok, org bazlı
    "renewables_part": "year",
}


# ── ORTAK DÖNÜŞÜMLER ──────────────────────────────────────────────────────────

def apply_schema(df: DataFrame, source: str) -> DataFrame:
    """
    Bilinen sütunları doğru tiplere cast eder.
    Şemada olmayan sütunlar StringType olarak korunur.
    """
    schema = SCHEMAS.get(source, {})
    for col_name, col_type in schema.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    return df


def parse_timestamps(df: DataFrame, source: str) -> DataFrame:
    """
    'date' sütununu TimestampType'a dönüştürür.
    EPIAS formatları: ISO 8601 (2025-01-01T00:00:00+03:00)
    """
    date_col = DATE_COLUMN.get(source, "date")
    if date_col == "date" and "date" in df.columns:
        df = df.withColumn(
            "date",
            F.coalesce(
                F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp(F.col("date")),
            ),
        )
    return df


def add_partition_columns(df: DataFrame, ds: str) -> DataFrame:
    """year, month, day partition sütunları ekler."""
    dt = datetime.strptime(ds, "%Y-%m-%d")
    return (
        df
        .withColumn("year",  F.lit(dt.year))
        .withColumn("month", F.lit(dt.month))
        .withColumn("day",   F.lit(dt.day))
    )


def deduplicate(df: DataFrame, source: str) -> DataFrame:
    """
    Birincil anahtar üzerinden tekrar kayıtları temizler.
    Sıralama: bronze'daki son yazım kazanır.
    """
    pk = _primary_keys(source)
    if not pk:
        return df
    window_spec = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy(*pk)
        .orderBy(F.monotonically_increasing_id().desc())
    )
    return (
        df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _primary_keys(source: str) -> list[str]:
    """Her kaynak için tekrar eden kayıt kontrolü yapılacak alanlar."""
    _PKS = {
        "pricing":          ["date"],
        "smf":              ["date"],
        "supply_demand":    ["date", "price"],
        "dam_clearing":     ["date"],
        "dam_volume":       ["date"],
        "bid_volume":       ["date"],
        "sales_volume":     ["date"],
        "price_ind_bid":    ["date"],
        "price_ind_offer":  ["date"],
        "idm_transactions": ["date", "buyerOrganizationId", "sellerOrganizationId", "contractName"],
        "idm_matching":     ["date", "contractName"],
        "idm_wap":          ["date"],
        "idm_bid_offer":    ["date"],
        "order_up":         ["date", "organizationId"],
        "order_down":       ["date", "organizationId"],
        "system_direction": ["date"],
        "dpp":              ["date", "organizationId", "uevcbId"],
        "injection":        ["date", "organizationId", "uevcbId"],
        "aic":              ["date", "organizationId"],
        "imbalance":        ["date"],
        "unlicensed":       ["date"],
        "res_forecast":     ["date"],
        "gen_forecast":     ["date"],
        "licensed_gen":     ["date"],
        "unlicensed_cost":  ["date"],
        "yekdem_total":     ["date"],
        "yekdem_unit":      ["date"],
        "new_capacity":     ["date"],
        "participants":     ["organizationId"],
        "renewables_part":  ["year", "organizationId"],
        "weather":          ["date", "city"],
    }
    return _PKS.get(source, [])


# ── ANA AKIŞ ─────────────────────────────────────────────────────────────────

def run_bronze_to_silver(source: str, ds: str) -> None:
    """
    Bronze parquet → Silver parquet (partitioned).

    Adımlar:
      1. Bronze'dan oku
      2. Şema uygula (cast)
      3. Timestamp parse
      4. Partition sütunları ekle
      5. Tekilleştir
      6. Silver'a yaz (overwrite partition)
    """
    spark = get_spark_session(f"BronzeToSilver_{source}")

    b_path = bronze_path(source, ds)
    s_path = silver_path(source)

    logger.info("Bronze okunuyor: %s", b_path)
    df = spark.read.parquet(b_path)

    if df.rdd.isEmpty():
        logger.warning("Bronze boş, silver yazımı atlandı: %s / %s", source, ds)
        spark.stop()
        return

    row_count_before = df.count()
    logger.info("Bronze satır sayısı: %d", row_count_before)

    df = apply_schema(df, source)
    df = parse_timestamps(df, source)
    df = add_partition_columns(df, ds)
    df = deduplicate(df, source)

    row_count_after = df.count()
    logger.info(
        "Silver yazılıyor: %s | %d → %d satır (dedup -%d)",
        s_path, row_count_before, row_count_after,
        row_count_before - row_count_after,
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(s_path)
    )
    logger.info("✅ Silver tamamlandı: %s", source)
    spark.stop()