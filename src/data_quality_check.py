"""
data_quality_check.py — EPIAŞ Veri Kalitesi & Kapsam Kontrol Scripti
======================================================================
Altın kural: 2025-01-01'den bugüne her saatlik kayıt var mı?

Kontroller:
  1. Tarih aralığı  — min/max tarih, beklenen vs gerçek satır sayısı
  2. Boşluk tespiti — hangi günlerde eksik saat var?
  3. Değer geçerliliği — negatif fiyat, sıfır üretim, NULL oranı
  4. Mart sağlığı   — gold tabloların durumu
  5. Özet rapor     — trafik ışığı (✅ / ⚠️ / ❌)

Kullanım:
  python src/data_quality_check.py              # Tüm kontroller
  python src/data_quality_check.py --quick      # Sadece özet
  python src/data_quality_check.py --source pricing  # Tek kaynak
"""

from __future__ import annotations  # Python 3.8 uyumlu generic type hints

import os
import sys
import argparse
import logging
from datetime import date, datetime
from typing import Dict, List, Optional
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("DQCheck")

PROJECT   = os.getenv("GCP_PROJECT_ID", "epias-data-platform")
GOLD      = os.getenv("BQ_GOLD_DATASET", "epias_gold")
START_DATE = date(2025, 1, 1)
TODAY      = date.today()
EXPECTED_DAYS  = (TODAY - START_DATE).days + 1
EXPECTED_HOURS = EXPECTED_DAYS * 24

# ── Kontrol edilecek staging modeller ────────────────────────────────────────
# (date, hour) granüllü tablolar — beklenen: EXPECTED_HOURS satır
HOURLY_TABLES = {
    "stg_pricing":           {"key_col": "ptf_try",               "min_val": 0,    "label": "PTF Fiyatı"},
    "stg_smf":               {"key_col": "smf_try",               "min_val": 0,    "label": "SMF"},
    "stg_generation":        {"key_col": "total_generation_mwh",  "min_val": 1000, "label": "Toplam Üretim"},
    "stg_load_estimation":   {"key_col": "forecasted_load_mwh",   "min_val": 1000, "label": "Yük Tahmini"},
    "stg_res_forecast":      {"key_col": "forecasted_res_mwh",    "min_val": 0,    "label": "RES Tahmini"},
    "stg_aic":               {"key_col": "total_aic_mwh",         "min_val": 1000, "label": "AIC"},
    "stg_dam_clearing":      {"key_col": "matched_bids_mwh",      "min_val": 0,    "label": "GÖP Alış"},
    "stg_imbalance":         {"key_col": "net_imbalance_mwh",     "min_val": None, "label": "İmbalans"},
    "stg_system_direction":  {"key_col": "system_direction",      "min_val": None, "label": "Sistem Yönü"},
    "stg_order_up":          {"key_col": "up_regulation_delivered_mwh", "min_val": 0, "label": "YAL"},
    "stg_order_down":        {"key_col": "down_regulation_delivered_mwh", "min_val": 0, "label": "YAT"},
    # stg_outages HOURLY_TABLES'ta değil — olay bazlı (id PK, hour kolonu yok)
}

# Olay bazlı tablolar (tarih var ama saat granüllü değil)
EVENT_TABLES = {
    "stg_outages": {"key_col": "outage_capacity_mwh", "label": "Arıza/Bakım Bildirimleri"},
}

# Günlük granüllü tablolar (date bazlı)
DAILY_TABLES = {
    "stg_dams": {"key_col": "total_storage_mwh", "min_val": 0, "label": "Baraj Depolama"},
}

# Mart tabloları — temel sağlık kontrolü
MART_TABLES = [
    "mart_price_analysis",
    "mart_generation_mix",
    "mart_supply_shock_index",
    "mart_gip_company_activity",
    "mart_cross_market_spread",
    "mart_dgp_company_analysis",
    "mart_ml_features",
    "mart_ptf_extremes",
    "mart_gold_monthly_executive_metrics",
    "mart_production_plan",
    "mart_renewable_deep",
    "mart_forecasted_residual_load",
]

# ── Bağlantı ────────────────────────────────────────────────────────────────
def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT)

def q(client: bigquery.Client, sql: str) -> pd.DataFrame:
    try:
        return client.query(sql).to_dataframe()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})

# ── İkon helper ─────────────────────────────────────────────────────────────
def icon(ok: bool, warn: bool = False) -> str:
    if ok:   return "✅"
    if warn: return "⚠️ "
    return "❌"

# ── 1. Saatlik Tablo Kontrolü ────────────────────────────────────────────────
def check_hourly_table(client, table: str, meta: dict) -> dict:
    sql = f"""
    SELECT
        MIN(date)                                    AS min_date,
        MAX(date)                                    AS max_date,
        COUNT(*)                                     AS total_rows,
        COUNTIF({meta['key_col']} IS NULL)           AS null_count,
        {"COUNTIF(" + meta['key_col'] + " < " + str(meta['min_val']) + ")" if meta['min_val'] is not None else "0"}
                                                     AS below_min_count,
        COUNT(DISTINCT date)                         AS distinct_days,
        COUNT(DISTINCT CONCAT(CAST(date AS STRING),
              CAST(hour AS STRING)))                 AS distinct_hours
    FROM `{PROJECT}.{GOLD}.{table}`
    WHERE date >= '{START_DATE}'
    """
    df = q(client, sql)
    if "error" in df.columns:
        return {"table": table, "status": "ERROR", "error": df["error"][0], **meta}

    row = df.iloc[0]
    total       = int(row["total_rows"])
    completeness = round(total / EXPECTED_HOURS * 100, 1) if EXPECTED_HOURS > 0 else 0
    null_pct    = round(row["null_count"] / total * 100, 2) if total > 0 else 0
    below_pct   = round(row["below_min_count"] / total * 100, 2) if total > 0 else 0
    max_date    = row["max_date"]
    freshness_days = (TODAY - max_date).days if max_date else 999

    status = "OK"
    if completeness < 90:   status = "WARN"
    if completeness < 70:   status = "ERROR"
    if freshness_days > 2:  status = max(status, "WARN") if status != "ERROR" else "ERROR"
    if null_pct > 5:        status = "WARN"

    return {
        "table":            table,
        "label":            meta["label"],
        "min_date":         str(row["min_date"]),
        "max_date":         str(max_date),
        "freshness_days":   freshness_days,
        "total_rows":       total,
        "expected_rows":    EXPECTED_HOURS,
        "completeness_pct": completeness,
        "distinct_days":    int(row["distinct_days"]),
        "null_pct":         null_pct,
        "below_min_pct":    below_pct,
        "status":           status,
    }

# ── 2. Boşluk Tespiti ────────────────────────────────────────────────────────
def find_gaps(client, table: str, top_n: int = 10) -> list[dict]:
    """Günlük saat sayısı < 24 olan günleri döndürür."""
    sql = f"""
    SELECT date, COUNT(*) AS hour_count, 24 - COUNT(*) AS missing_hours
    FROM `{PROJECT}.{GOLD}.{table}`
    WHERE date >= '{START_DATE}'
    GROUP BY date
    HAVING COUNT(*) < 24
    ORDER BY missing_hours DESC, date
    LIMIT {top_n}
    """
    df = q(client, sql)
    if "error" in df.columns or df.empty:
        return []
    return df.to_dict("records")

# ── 3. Tam Tarih Aralığı Kontrolü (takvim boşlukları) ─────────────────────
def find_missing_dates(client, table: str) -> int:
    """Hiç satır olmayan günleri sayar."""
    sql = f"""
    WITH calendar AS (
        SELECT date
        FROM UNNEST(GENERATE_DATE_ARRAY('{START_DATE}', '{TODAY}')) AS date
    ),
    present AS (
        SELECT DISTINCT date FROM `{PROJECT}.{GOLD}.{table}`
        WHERE date >= '{START_DATE}'
    )
    SELECT COUNT(*) AS missing_day_count
    FROM calendar
    LEFT JOIN present USING(date)
    WHERE present.date IS NULL
    """
    df = q(client, sql)
    if "error" in df.columns:
        return -1
    return int(df.iloc[0]["missing_day_count"])

# ── 4. Mart Sağlık Kontrolü ──────────────────────────────────────────────────
def check_mart(client, table: str) -> dict:
    # Bazı mart'lar 'date' yerine farklı kolon kullanır
    DATE_COL_MAP = {
        "mart_gip_company_activity":           "trade_date",
        "mart_gold_monthly_executive_metrics": "year_month",  # aylık mart
    }
    date_col = DATE_COL_MAP.get(table, "date")

    # Aylık mart'lar için DATE dönüşümü
    if date_col == "year_month":
        date_expr = f"PARSE_DATE('%Y-%m', CAST({date_col} AS STRING))"
    else:
        date_expr = date_col

    sql = f"""
    SELECT
        COUNT(*)           AS total_rows,
        MIN({date_expr})   AS min_date,
        MAX({date_expr})   AS max_date
    FROM `{PROJECT}.{GOLD}.{table}`
    """
    df = q(client, sql)
    if "error" in df.columns:
        err = df["error"][0]
        status = "MISSING" if "Not found" in err or "not found" in err else "ERROR"
        return {"table": table, "status": status, "error": err[:80]}

    row = df.iloc[0]
    total = int(row["total_rows"])
    max_date = row["max_date"]
    freshness = (TODAY - max_date).days if max_date else 999

    return {
        "table":          table,
        "status":         "OK" if total > 0 and freshness <= 2 else ("STALE" if freshness > 2 else "EMPTY"),
        "total_rows":     total,
        "min_date":       str(row["min_date"]),
        "max_date":       str(max_date),
        "freshness_days": freshness,
    }

# ── 5. PTF Değer Dağılımı (sanity check) ─────────────────────────────────────
def ptf_sanity(client) -> dict:
    sql = f"""
    SELECT
        AVG(ptf_try)                             AS avg_ptf,
        MIN(ptf_try)                             AS min_ptf,
        MAX(ptf_try)                             AS max_ptf,
        STDDEV(ptf_try)                          AS std_ptf,
        COUNTIF(ptf_try <= 0)                    AS zero_or_negative,
        COUNTIF(ptf_try > 10000)                 AS above_10k,
        COUNT(*)                                 AS total
    FROM `{PROJECT}.{GOLD}.stg_pricing`
    WHERE date >= '{START_DATE}'
    """
    df = q(client, sql)
    if "error" in df.columns:
        return {}
    return df.iloc[0].to_dict()

# ── 6. Hava Verisi Kapsam Kontrolü ───────────────────────────────────────────
def check_weather(client) -> dict:
    sql = f"""
    SELECT
        city_name,
        MIN(CAST(date AS DATE))           AS min_date,
        MAX(CAST(date AS DATE))           AS max_date,
        COUNT(*)                          AS total_rows,
        COUNT(DISTINCT CAST(date AS DATE)) AS days
    FROM `{PROJECT}.{GOLD}.stg_weather`
    WHERE CAST(date AS DATE) >= '{START_DATE}'
    GROUP BY city_name
    ORDER BY city_name
    """
    df = q(client, sql)
    if "error" in df.columns:
        logger.warning(f"stg_weather: {df['error'][0][:80]}")
        return {}
    return df.to_dict("records")

# ── Raporlama ────────────────────────────────────────────────────────────────
def print_section(title: str):
    print(f"\n{'═'*70}")
    print(f"  {title}")
    print(f"{'═'*70}")

def print_hourly_result(r: dict):
    if r.get("status") == "ERROR" and "error" in r:
        print(f"  ❌ {r.get('label', r['table']):<28} — {r['error'][:60]}")
        return

    ic = icon(r["status"] == "OK", r["status"] == "WARN")
    fd = r.get("freshness_days", 999)
    fresh = "bugün" if fd == 0 else f"{fd}g önce"
    comp  = r.get("completeness_pct", 0)

    # 590%+ gibi değerler → "çoklu satır/saat" uyarısı
    comp_str = f"{comp:>6.1f}%" if comp <= 110 else f"~{comp/100:.0f}x ⚠️ "

    print(
        f"  {ic} {r.get('label', r['table']):<28} "
        f"{comp_str:>9}  "
        f"{r.get('total_rows', 0):>7,} satır  "
        f"[{r.get('min_date','?')} → {r.get('max_date','?')}]  "
        f"son güncelleme: {fresh}"
    )
    if r.get("null_pct", 0) > 1:
        print(f"       ⚠️  NULL oranı: {r['null_pct']:.1f}%")
    if 0 < r.get("below_min_pct", 0) <= 100 and r.get("completeness_pct", 200) <= 110:
        print(f"       ⚠️  Min-altı değer: {r['below_min_pct']:.2f}%")
    if comp > 110:
        print(f"       ℹ️  Çoklu satır/saat — kaynak data kategorik olabilir (RES tipi vb.)")

# ── ANA FONKSİYON ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick",  action="store_true", help="Sadece özet satırları")
    parser.add_argument("--source", default=None,       help="Tek kaynak kontrol et")
    parser.add_argument("--gaps",   action="store_true", help="Boşluk detayı göster")
    args = parser.parse_args()

    client = get_client()

    print_section(f"EPIAŞ VERİ KALİTESİ RAPORU  —  {TODAY}")
    print(f"  Kontrol aralığı : {START_DATE} → {TODAY}")
    print(f"  Beklenen gün    : {EXPECTED_DAYS}")
    print(f"  Beklenen saat   : {EXPECTED_HOURS:,}")
    print(f"  Proje           : {PROJECT}.{GOLD}")

    # ── Saatlik tablolar ──────────────────────────────────────────────────
    print_section("1. SAATLİK STAGING TABLOLARI")
    print(f"  {'Tablo':<28} {'Kapsam':>8}  {'Satır':>9}  {'Tarih Aralığı':^30}  Tazelik")
    print(f"  {'-'*28} {'-'*8}  {'-'*9}  {'-'*30}  {'-'*12}")

    results = {}
    tables_to_check = (
        {args.source: HOURLY_TABLES[args.source]}
        if args.source and args.source in HOURLY_TABLES
        else HOURLY_TABLES
    )

    for table, meta in tables_to_check.items():
        r = check_hourly_table(client, table, meta)
        results[table] = r
        print_hourly_result(r)

        if args.gaps and r.get("status") != "ERROR":
            gaps = find_gaps(client, table)
            if gaps:
                print(f"       📅 Eksik saatli günler (ilk {len(gaps)}):")
                for g in gaps:
                    print(f"          {g['date']}  —  {g['missing_hours']} saat eksik")

    # ── Mart tabloları ────────────────────────────────────────────────────
    if not args.quick:
        print_section("2. GOLD MART TABLOLARI")
        print(f"  {'Mart':<45} {'Satır':>9}  {'Tarih Aralığı':^30}  Durum")
        print(f"  {'-'*45} {'-'*9}  {'-'*30}  {'-'*10}")

        for table in MART_TABLES:
            r = check_mart(client, table)
            if r["status"] in ("MISSING", "EMPTY", "ERROR"):
                ic = "❌"
            elif r["status"] == "STALE":
                ic = "⚠️ "
            else:
                ic = "✅"

            if r["status"] in ("MISSING", "ERROR"):
                print(f"  {ic} {table:<45} — {r.get('error', r['status'])}")
            else:
                print(
                    f"  {ic} {table:<45} "
                    f"{r['total_rows']:>9,}  "
                    f"[{r['min_date']} → {r['max_date']}]  "
                    f"{r['freshness_days']}g"
                )

    # ── Olay-bazlı tablolar ───────────────────────────────────────────────
    if not args.quick:
        print_section("2b. OLAY BAZLI TABLOLAR (saatlik değil)")
        for table, meta in EVENT_TABLES.items():
            sql = f"""
            SELECT COUNT(*) AS total_rows, MIN(date) AS min_date, MAX(date) AS max_date,
                   COUNT(DISTINCT date) AS days
            FROM `{PROJECT}.{GOLD}.{table}`
            WHERE date >= '{START_DATE}'
            """
            df = q(client, sql)
            if "error" in df.columns:
                print(f"  ❌ {meta['label']}: {df['error'][0][:60]}")
                continue
            row = df.iloc[0]
            total = int(row["rows"])
            days  = int(row["days"])
            cov   = round(days / EXPECTED_DAYS * 100, 1)
            ic    = icon(cov >= 95, cov >= 80)
            print(f"  {ic} {meta['label']:<35} {total:>7,} kayıt  {days} gün  kapsam: {cov}%")

    # ── PTF sanity ────────────────────────────────────────────────────────
    print_section("3. PTF DEĞER GEÇERLİLİĞİ")
    ptf = ptf_sanity(client)
    if ptf:
        avg, mn, mx = ptf.get("avg_ptf",0), ptf.get("min_ptf",0), ptf.get("max_ptf",0)
        zeros = int(ptf.get("zero_or_negative", 0))
        spikes = int(ptf.get("above_10k", 0))
        total = int(ptf.get("total", 1))
        print(f"  Ortalama PTF : {avg:>10,.2f} TL/MWh")
        print(f"  Min PTF      : {mn:>10,.2f} TL/MWh")
        print(f"  Maks PTF     : {mx:>10,.2f} TL/MWh")
        print(f"  Std sapma    : {ptf.get('std_ptf',0):>10,.2f}")
        print(f"  Sıfır/negatif: {zeros:>10,} saat  ({zeros/total*100:.2f}%)")
        print(f"  >10.000 TL   : {spikes:>10,} saat  ({spikes/total*100:.2f}%)")

    # ── Hava verisi ───────────────────────────────────────────────────────
    print_section("4. HAVA VERİSİ KAPSAMI")
    weather = check_weather(client)
    if weather:
        for w in weather:
            coverage = round(w["days"] / EXPECTED_DAYS * 100, 1)
            ic = icon(coverage >= 95, coverage >= 80)
            print(f"  {ic} {w['city_name']:<12}  "
                  f"{coverage:>6.1f}%  "
                  f"{w['rows']:>7,} satır  "
                  f"[{w['min_date']} → {w['max_date']}]")
    else:
        print("  ❌ stg_weather erişilemiyor")

    # ── Özet skorboard ────────────────────────────────────────────────────
    print_section("5. ÖZET SKOR KARTI")
    ok_count   = sum(1 for r in results.values() if r.get("status") == "OK")
    warn_count = sum(1 for r in results.values() if r.get("status") == "WARN")
    err_count  = sum(1 for r in results.values() if r.get("status") in ("ERROR", "MISSING"))
    total_chk  = len(results)

    print(f"  ✅ Sağlıklı   : {ok_count}/{total_chk}")
    print(f"  ⚠️  Uyarı      : {warn_count}/{total_chk}")
    print(f"  ❌ Hatalı     : {err_count}/{total_chk}")

    # Genel kapsam skoru — %110 üstü multi-row tabloları hariç tut
    completed = [r for r in results.values()
                 if "completeness_pct" in r and r["completeness_pct"] <= 110]
    if completed:
        avg_coverage = sum(r["completeness_pct"] for r in completed) / len(completed)
        stale = [r for r in completed if r.get("freshness_days", 0) > 2]
        print(f"\n  Ortalama veri kapsamı : {avg_coverage:.1f}%")
        if stale:
            print(f"  ⚠️  Güncel olmayan tablo: {', '.join(r['table'] for r in stale)}")

    # Eksik tablo uyarısı
    print(f"\n  📌 Backfill bekleyen  : stg_dpp, stg_sbfgp, stg_res_forecast")
    print(f"  📌 Bu tablolar tamamlanınca DAG --exclude listesinden kaldır")
    print()

if __name__ == "__main__":
    main()
