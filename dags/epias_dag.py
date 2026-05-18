"""
EPIAS Medallion Pipeline v2
============================
Mimari  : Bronze (raw GCS) → Silver (Spark/parquet) → Gold (Spark/parquet) → BigQuery
Zamanlama: Her gün 05:00 UTC (08:00 TR)

Veri gecikmesi politikası:
  - Saatlik piyasa verisi (GÖP/GİP/DGP/üretim/tüketim): T+0 — aynı gün
  - YEKDEM aylık uzlaştırma: T+35 gün (bir önceki aya ait veri çekilir)

Kaynaklar (30 adet):
  Fiyat       : pricing (MCP/PTF), smf
  GÖP         : supply_demand, dam_clearing, dam_volume, bid_volume,
                sales_volume, price_ind_bid, price_ind_offer
  GİP         : idm_transactions, idm_matching, idm_wap, idm_bid_offer
  DGP/Sistem  : order_up, order_down, system_direction
  Üretim      : dpp, injection, aic, imbalance (hesaplamalı)
  Tüketim     : (imbalance içinde)
  Yenilenebilir: unlicensed, res_forecast, gen_forecast, licensed_gen
  YEKDEM maliyet: unlicensed_cost, yekdem_total_cost, yekdem_unit_cost, new_capacity
  Referans    : participants, renewables_part
  Hava        : weather (4 şehir)
"""
from __future__ import annotations
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, Tuple

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── MODÜL YOLU ────────────────────────────────────────────────────────────────
sys.path.insert(0, "/opt/airflow/src")
try:
    from epias_client import EPIASClient
    from weather_client import WeatherClient
except ImportError as exc:
    logging.error(f"Modül yükleme hatası: {exc}")

logger = logging.getLogger(__name__)

# ── AYARLAR ───────────────────────────────────────────────────────────────────
BUCKET_NAME   = "epias-data-lake"
SPARK_CONN_ID = "spark_default"

# ── VERİ GECİKME TABLOSU ─────────────────────────────────────────────────────
# Saatlik/günlük veriler gecikme gerektirmez.
# YEKDEM aylık uzlaştırması ~35 gün gecikmeli yayınlanır;
# execution_date - 35 gün = bir önceki ayın verisi.
DATA_DELAYS: Dict[str, int] = {
    # ── Gecikme yok ──────────────────────────────
    "get_ptf_smf_sdf":                  0,
    "get_smf":                          0,
    "get_supply_demand":                0,
    "get_dam_clearing_quantity":        0,
    "get_dam_trade_volume":             0,
    "get_submitted_bid_volume":         0,
    "get_submitted_sales_volume":       0,
    "get_price_independent_bid":        0,
    "get_price_independent_offer":      0,
    "get_idm_transaction_history":      0,
    "get_idm_matching_quantity":        0,
    "get_idm_weighted_average_price":   0,
    "get_idm_bid_offer_quantities":     0,
    "get_order_summary_up":             0,
    "get_order_summary_down":           0,
    "get_system_direction":             0,
    "get_dpp":                          0,
    "get_injection_quantity":           1,
    "get_aic":                          0,
    "get_imbalance_quantity":           0,  # hesaplamalı, T+2 saatte mevcut
    "get_res_generation_and_forecast":  0,
    "get_generation_forecast":          0,
    "get_licensed_realtime_generation": 0,
    # ── YEKDEM aylık uzlaştırma (+35g) ───────────
    "get_unlicensed_generation":        35,
    "get_unlicensed_generation_cost":   35,
    "get_yekdem_total_cost":            35,
    "get_yekdem_unit_cost":             35,
    "get_new_installed_capacity":       35,
}

# Tarih parametresi almayan metodlar (statik/referans listeler)
NO_DATE_METHODS: frozenset = frozenset({
    "get_market_participants",
    "get_organization_list",
    "get_uevcb_list",
})

# ── HATA YÖNETİMİ ─────────────────────────────────────────────────────────────
def notify_failure(context: dict) -> None:
    """
    on_failure_callback: task başarısız olduğunda çağrılır.
    SMTP bağlantısı kurulduğunda EmailOperator eklenebilir:
      Airflow UI → Admin → Connections → smtp_default
    """
    ti = context["task_instance"]
    logger.error(
        "🚨 TASK HATASI | dag=%s | task=%s | tarih=%s | hata=%s",
        ti.dag_id, ti.task_id, context.get("ds"), context.get("exception"),
    )


# ── CALLABLE'LAR ──────────────────────────────────────────────────────────────

def get_epias_data_callable(method_name: str, **context) -> list:
    """
    EPIAS API'sinden veri çeker.
    DATA_DELAYS tablosuna göre gecikme varsa tarih kaydırılır.
    """
    client = EPIASClient()
    ds     = context["ds"]
    delay  = DATA_DELAYS.get(method_name, 0)

    if delay > 0:
        target = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=delay)).strftime("%Y-%m-%d")
        logger.info("%s: %dg gecikme → %s (exec: %s)", method_name, delay, target, ds)
    else:
        target = ds

    method = getattr(client, method_name)
    result = method() if method_name in NO_DATE_METHODS else method(target, target)

    if not result:
        logger.warning("⚠️  %s(%s) boş döndü (gecikme=%dg)", method_name, target, delay)
    return result


def get_weather_data_callable(**context) -> list:
    """Dört şehir için hava durumu verisi çeker (OpenMeteo)."""
    client = WeatherClient()
    ds     = context["ds"]
    rows: list = []

    for city in ("istanbul", "izmir", "ankara", "konya"):
        try:
            city_df = client.get_weather_for_city(city, ds, ds)
            if hasattr(city_df, "to_dict"):
                # Timezone-aware datetime → string (XCom JSON serialization)
                for col in city_df.select_dtypes(
                    include=["datetime64[ns, UTC]", "datetime64[ns]", "datetimetz"]
                ).columns:
                    city_df[col] = city_df[col].astype(str)
                records = city_df.to_dict(orient="records")
            elif isinstance(city_df, list):
                records = city_df
            else:
                records = [city_df]
            rows.extend(records)
            logger.info("✅ weather/%s: %d satır", city, len(records))
        except Exception as exc:
            logger.error("weather/%s hatası: %s", city, exc)
    return rows


def save_to_gcs_callable(
    task_id: str,
    bucket_path: str,
    allow_empty: bool = False,
    **context,
) -> None:
    """
    Upstream task'ın XCom değerini GCS'e parquet olarak yazar.

    allow_empty=False  Veri boş gelirse ValueError — saatlik piyasa verisi.
    allow_empty=True   Veri boş gelirse sadece uyarı — aylık/statik veriler.
    """
    data = context["ti"].xcom_pull(task_ids=task_id)

    if not data:
        if allow_empty:
            logger.warning("⚠️  %s boş döndü — GCS kaydı atlandı (allow_empty=True)", task_id)
            return
        raise ValueError(f"🚨 {task_id} task'ından veri gelmedi, GCS'e yazılamıyor!")

    # İç içe liste (paralel görev gibi) düzleştirme
    if isinstance(data, list) and data and isinstance(data[0], list):
        flat: list = []
        for sub in data:
            flat.extend(sub)
    else:
        flat = data

    ds       = context["ds"]
    gcs_path = f"gs://{BUCKET_NAME}/{bucket_path}/{ds}.parquet"

    df = pd.DataFrame(flat)
    # Timezone-aware kolon → string (parquet compat)
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].astype(str)

    df.to_parquet(gcs_path, index=False)
    logger.info("✅ GCS: %s | %d satır", gcs_path, len(df))


def load_bigquery_dynamic(table_name: str, **context) -> None:
    """Gold katmanındaki parquet dosyalarını BigQuery'ye yükler."""
    from google.cloud import bigquery

    client   = bigquery.Client()
    ds       = context["ds"]
    y, m, d  = ds.split("-")
    table_id = f"epias-data-platform.epias_gold.gold_{table_name}"
    uri      = f"gs://{BUCKET_NAME}/gold/{table_name}/year={y}/month={m}/day={d}/*.parquet"

    job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        ),
    )
    job.result()
    logger.info("✅ BigQuery: %s", table_id)


def _run_dbt(**_context) -> None:
    logger.info("dbt run tetiklendi")


def _run_ptf_forecast(**_context) -> None:
    logger.info("ML PTF Forecast tetiklendi")


# ── DAG ───────────────────────────────────────────────────────────────────────
default_args = {
    "owner":               "epias_team",
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="epias_medallion_pipeline_v2",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 5 * * *",   # 08:00 TR
    catchup=False,
    max_active_runs=3,               # backfill paralel run sayısı
    max_active_tasks=10,             # LocalExecutor worker limiti
    tags=["epias", "electricity", "market"],
) as dag:

    # =========================================================================
    # BRONZE: raw API verisi → GCS parquet
    # =========================================================================
    #
    # Şema: (dag_task_key, client_method_name, gcs_path, allow_empty)
    #
    # allow_empty=False → veri boş gelirse pipeline durur (saatlik kritik veri)
    # allow_empty=True  → veri boş gelirse sadece uyarı (aylık/yıllık referans)

    HOURLY_SOURCES: dict[str, tuple[str, str, bool]] = {
        # Fiyat
        "pricing":          ("get_ptf_smf_sdf",                "bronze/pricing",          False),
        "smf":              ("get_smf",                        "bronze/smf",              False),
        # GÖP arz-talep
        "supply_demand":    ("get_supply_demand",              "bronze/supply_demand",    False),
        "dam_clearing":     ("get_dam_clearing_quantity",      "bronze/dam_clearing",     False),
        "dam_volume":       ("get_dam_trade_volume",           "bronze/dam_volume",       False),
        "bid_volume":       ("get_submitted_bid_volume",       "bronze/bid_volume",       False),
        "sales_volume":     ("get_submitted_sales_volume",     "bronze/sales_volume",     False),
        "price_ind_bid":    ("get_price_independent_bid",      "bronze/price_ind_bid",    False),
        "price_ind_offer":  ("get_price_independent_offer",    "bronze/price_ind_offer",  False),
        # GİP
        "idm_transactions": ("get_idm_transaction_history",   "bronze/idm_transactions", False),
        "idm_matching":     ("get_idm_matching_quantity",      "bronze/idm_matching",     False),
        "idm_wap":          ("get_idm_weighted_average_price", "bronze/idm_wap",          False),
        "idm_bid_offer":    ("get_idm_bid_offer_quantities",   "bronze/idm_bid_offer",    False),
        # DGP / Sistem yönü
        "order_up":         ("get_order_summary_up",           "bronze/order_up",         False),
        "order_down":       ("get_order_summary_down",         "bronze/order_down",       False),
        "system_direction": ("get_system_direction",           "bronze/system_direction", False),
        # Üretim / Dengesizlik
        "dpp":              ("get_dpp",                        "bronze/dpp",              False),
        "injection":        ("get_injection_quantity",         "bronze/injection",        False),
        "aic":              ("get_aic",                        "bronze/aic",              False),
        "imbalance":        ("get_imbalance_quantity",         "bronze/imbalance",        False),
        # Yenilenebilir
        "unlicensed":       ("get_unlicensed_generation",      "bronze/unlicensed",       False),
        "res_forecast":     ("get_res_generation_and_forecast","bronze/res_forecast",     False),
        "gen_forecast":     ("get_generation_forecast",        "bronze/gen_forecast",     True),  # BUS hatası olası
        "licensed_gen":     ("get_licensed_realtime_generation","bronze/licensed_gen",    False),
        # YEKDEM maliyet (aylık → allow_empty=True)
        "unlicensed_cost":  ("get_unlicensed_generation_cost", "bronze/unlicensed_cost",  True),
        "yekdem_total":     ("get_yekdem_total_cost",          "bronze/yekdem_total",     True),
        "yekdem_unit":      ("get_yekdem_unit_cost",           "bronze/yekdem_unit",      True),
        "new_capacity":     ("get_new_installed_capacity",     "bronze/new_capacity",     True),
    }

    STATIC_SOURCES: Dict[str, Tuple[str, str, bool]] = {
        "participants":    ("get_market_participants",   "bronze/participants",    True),
        "renewables_part": ("get_renewables_participant","bronze/renewables_part", True),
    }

    ALL_SOURCES = {**HOURLY_SOURCES, **STATIC_SOURCES}

    # ── Hava durumu (bağımsız bronze task, silver'a bağlı değil) ─────────────
    get_weather = PythonOperator(
        task_id="get_weather",
        python_callable=get_weather_data_callable,
    )
    save_weather = PythonOperator(
        task_id="save_weather_to_gcs",
        python_callable=save_to_gcs_callable,
        op_kwargs={
            "task_id":    "get_weather",
            "bucket_path":"bronze/weather",
            "allow_empty": False,
        },
    )
    get_weather >> save_weather

    # ── Piyasa + referans kaynakları ──────────────────────────────────────────
    bronze_save_tasks: Dict[str, PythonOperator] = {}

    for key, (method, path, allow_empty) in ALL_SOURCES.items():
        get_t = PythonOperator(
            task_id=f"get_{key}",
            python_callable=get_epias_data_callable,
            op_kwargs={"method_name": method},
        )
        save_t = PythonOperator(
            task_id=f"save_{key}_to_gcs",
            python_callable=save_to_gcs_callable,
            op_kwargs={
                "task_id":     f"get_{key}",
                "bucket_path":  path,
                "allow_empty":  allow_empty,
            },
        )
        get_t >> save_t
        bronze_save_tasks[key] = save_t

    # =========================================================================
    # SILVER: GCS raw → GCS parquet (temizlenmiş, şemalı) via Spark
    # =========================================================================
    silver_tasks: Dict[str, SparkSubmitOperator] = {}

    for key in ALL_SOURCES:
        silver_t = SparkSubmitOperator(
            task_id=f"silver_{key}",
            application=f"/opt/airflow/spark/bronze_to_silver_{key}.py",
            py_files="/opt/airflow/spark/spark_utils.py",
            jars="/opt/spark/jars/gcs-connector.jar",
            conn_id=SPARK_CONN_ID,
            application_args=["{{ ds }}"],
            deploy_mode="client",
            name=f"epias_silver_{key}",
        )
        # 1-to-1 bağımlılık: save_{key} → silver_{key}
        bronze_save_tasks[key] >> silver_t
        silver_tasks[key] = silver_t

    # =========================================================================
    # GOLD: Silver → Gold aggregate tables via Spark
    # =========================================================================
    run_gold = SparkSubmitOperator(
        task_id="run_spark_gold",
        application="/opt/airflow/spark/silver_to_gold.py",
        conn_id=SPARK_CONN_ID,
        application_args=["{{ ds }}"],
        name="epias_run_gold",
        deploy_mode="client",
    )

    for silver_t in silver_tasks.values():
        silver_t >> run_gold

    # =========================================================================
    # BIGQUERY: Gold parquet → BQ analiz tabloları
    # =========================================================================
    BQ_TABLES = [
        "market_prices",        # PTF/SMF günlük + saatlik
        "dam_supply_demand",    # Arz-talep eğrisi, PTF analizi
        "idm_activity",         # GİP hacim ve fiyat aktivitesi
        "dgp_orders",           # YAL/YAT talimatları, DGP analizi
        "renewable_impact",     # Yenilenebilir üretim etkisi
        "player_analysis",      # Katılımcı bazlı piyasa analizi
        "imbalance_metrics",    # Dengesizlik ve sistem yönü
    ]

    bq_loads = []
    for table in BQ_TABLES:
        bq_t = PythonOperator(
            task_id=f"load_bq_{table}",
            python_callable=load_bigquery_dynamic,
            op_kwargs={"table_name": table},
        )
        run_gold >> bq_t
        bq_loads.append(bq_t)

    # =========================================================================
    # DBT & ML FORECAST
    # =========================================================================
    run_dbt = PythonOperator(task_id="run_dbt",         python_callable=_run_dbt)
    run_forecast = PythonOperator(task_id="run_ptf_forecast", python_callable=_run_ptf_forecast)

    for bq_t in bq_loads:
        bq_t >> run_dbt

    run_dbt >> run_forecast