import logging
import os
import sys
import subprocess
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── MODÜL VE YOL TANIMLARI ───────────────────────────────────────────────────
sys.path.insert(0, '/opt/airflow/src')
try:
    from epias_client import EPIASClient
    from weather_client import WeatherClient
except ImportError as e:
    logging.error(f"Modül yükleme hatası: {e}")

logger = logging.getLogger(__name__)

# ── AYARLAR ──────────────────────────────────────────────────────────────────
BUCKET_NAME   = "epias-data-lake"
MY_EMAIL      = "mehmetkarahanc@gmail.com"
SPARK_CONN_ID = "spark_default"
MASTER_URL    = "spark://spark-master:7077"

# ── HATA YÖNETİMİ ────────────────────────────────────────────────────────────
def notify_failure(context):
    task_instance = context['task_instance']
    error_msg = (
        f"🚨 <b>Hata Raporu</b><br><br>"
        f"<b>Görev:</b> {task_instance.task_id}<br>"
        f"<b>DAG:</b> {task_instance.dag_id}<br>"
        f"<b>Tarih:</b> {context['execution_date']}<br>"
        f"<b>Hata:</b> {context.get('exception')}"
    )
    EmailOperator(
        task_id='send_error_email',
        to=MY_EMAIL,
        subject='[KRİTİK] EPIAŞ Pipeline Hatası',
        html_content=error_msg
    ).execute(context)

# ── VERİ GECİKME TABLOSU (Swagger v1.15.11) ─────────────────────────────────
# GÖP/PTF/DGP verileri gün içinde kesinleşir → gecikme yok
# YEKDEM uzlaştırması aylık → 35 gün gecikme
# GİP anlık işlemler → gecikme yok
DATA_DELAYS = {
    "get_ptf_smf_sdf":                  0,
    "get_mcp":                          0,
    "get_dpp":                          0,
    "get_market_participants":          0,
    "get_imbalance_quantity":           0,   # hesaplamalı, T+2 saatte yayınlanır
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
    "get_smf":                          0,
    "get_order_summary_up":             0,
    "get_order_summary_down":           0,
    "get_res_generation_and_forecast":  0,
    "get_generation_forecast":          0,
    "get_licensed_realtime_generation": 0,
    "get_injection_quantity":           0,
    "get_aic":                          0,
    # YEKDEM aylık uzlaştırma — 35 gün gecikme
    "get_unlicensed_generation":        35,
    "get_unlicensed_generation_cost":   35,
    "get_yekdem_total_cost":            35,
    "get_yekdem_unit_cost":             35,
    "get_new_installed_capacity":       35,
}

# Tarih gerektirmeyen (statik/yıllık) metodlar
NO_DATE_METHODS = {
    "get_organization_list",
    "get_market_participants",
    "get_uevcb_list",
    "get_renewables_participant",
}

# ── CALLABLE'LAR ─────────────────────────────────────────────────────────────

def get_epias_data_callable(method_name: str, **context) -> list:
    client    = EPIASClient()
    ds        = context["ds"]
    delay     = DATA_DELAYS.get(method_name, 0)

    if delay > 0:
        target_ds = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=delay)).strftime("%Y-%m-%d")
        logger.info(f"{method_name}: {delay}g gecikme → {target_ds} (exec: {ds})")
    else:
        target_ds = ds

    method = getattr(client, method_name)
    result = method() if method_name in NO_DATE_METHODS else method(target_ds, target_ds)

    if not result:
        logger.warning(f"⚠️ {method_name}({target_ds}) boş döndü (gecikme={delay}g)")
    return result


def get_weather_data_callable(**context) -> list:
    w_client = WeatherClient()
    cities   = ["istanbul", "izmir", "ankara", "konya"]
    ds       = context["ds"]
    rows     = []
    for city in cities:
        try:
            city_df = w_client.get_weather_for_city(city, ds, ds)
            if hasattr(city_df, 'to_dict'):
                for col in city_df.select_dtypes(
                    include=['datetime64[ns, UTC]', 'datetime64[ns]', 'datetimetz']
                ).columns:
                    city_df[col] = city_df[col].astype(str)
                records = city_df.to_dict(orient='records')
            elif isinstance(city_df, list):
                records = city_df
            else:
                records = [city_df]
            rows.extend(records)
            logger.info(f"✅ weather/{city}: {len(records)} satır")
        except Exception as e:
            logger.error(f"weather/{city} hatası: {e}")
    return rows


def save_to_gcs_callable(task_id: str, bucket_path: str,
                          allow_empty: bool = False, **context) -> None:
    """
    XCom'dan veriyi alır, GCS'e parquet olarak yazar.
    allow_empty=True: statik listeler için (participants, org-list vb.)
                      boş gelirse sadece uyarı basar, hata vermez.
    allow_empty=False: saatlik veri için boş gelirse pipeline'ı durdurur.
    """
    data = context["ti"].xcom_pull(task_ids=task_id)

    if not data or len(data) == 0:
        if allow_empty:
            logger.warning(f"⚠️ {task_id} boş döndü — GCS kaydı atlandı (allow_empty=True)")
            return
        raise ValueError(f"🚨 {task_id} task'ından veri gelmedi, GCS'e kayıt yapılamıyor!")

    flat: list = []
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
        for sub in data:
            flat.extend(sub)
    else:
        flat = data

    ds       = context["ds"]
    gcs_path = f"gs://{BUCKET_NAME}/{bucket_path}/{ds}.parquet"

    try:
        df = pd.DataFrame(flat)
        for col in df.select_dtypes(include=['datetimetz']).columns:
            df[col] = df[col].astype(str)
        df.to_parquet(gcs_path, index=False)
        logger.info(f"✅ GCS: {gcs_path} | {len(df)} satır")
    except Exception as e:
        logger.error(f"❌ GCS yazma hatası ({task_id}): {e}")
        raise


def load_bigquery_dynamic(table_name: str, **context) -> None:
    from google.cloud import bigquery
    client   = bigquery.Client()
    ds       = context["ds"]
    y, m, d  = ds.split("-")
    table_id = f"epias-data-platform.epias_gold.gold_{table_name}"
    uri      = f"gs://{BUCKET_NAME}/gold/{table_name}/year={y}/month={m}/day={d}/*.parquet"
    job_cfg  = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    client.load_table_from_uri(uri, table_id, job_config=job_cfg).result()
    logger.info(f"✅ BigQuery: {table_id}")


def run_spark_job(script_name: str, ds: str) -> None:
    script_path = f"/opt/airflow/spark/{script_name}"
    cmd = ["spark-submit", "--master", MASTER_URL, "--deploy-mode", "client", script_path, ds]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Spark job başarısız:\n{result.stderr}")
    logger.info(f"✅ Spark: {script_name}")


def _run_dbt(**context):
    logger.info("dbt run tetiklendi")

def _run_forecast(**context):
    logger.info("ML Forecast tetiklendi")


# ── DAG TANIMI ───────────────────────────────────────────────────────────────
default_args = {
    'owner':             'epias_medallion_pipeline_v2',
    'retries':           1,
    'retry_delay':       timedelta(minutes=5),
    'on_failure_callback': notify_failure,
}

with DAG(
    dag_id="epias_medallion_pipeline_v2",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=3,
    max_active_tasks=10,
) as dag:

    # =========================================================================
    # 1. BRONZE KATMANI
    # Her kaynak için: get_{key} → save_{key}_to_gcs
    # =========================================================================

    # ── Saatlik piyasa verileri (allow_empty=False) ───────────────────────────
    # Değer: DAG task key → (client metod adı, GCS path, allow_empty)
    hourly_sources = {
        # Fiyat
        "pricing":              ("get_ptf_smf_sdf",                 "bronze/pricing",              False),
        # GÖP arz-talep (PTF analizi)
        "supply_demand":        ("get_supply_demand",               "bronze/supply_demand",        False),
        "dam_clearing":         ("get_dam_clearing_quantity",        "bronze/dam_clearing",         False),
        "dam_volume":           ("get_dam_trade_volume",             "bronze/dam_volume",           False),
        "bid_volume":           ("get_submitted_bid_volume",         "bronze/bid_volume",           False),
        "sales_volume":         ("get_submitted_sales_volume",       "bronze/sales_volume",         False),
        "price_ind_bid":        ("get_price_independent_bid",        "bronze/price_ind_bid",        False),
        "price_ind_offer":      ("get_price_independent_offer",      "bronze/price_ind_offer",      False),
        # GİP
        "idm_transactions":     ("get_idm_transaction_history",      "bronze/idm_transactions",     False),
        "idm_matching":         ("get_idm_matching_quantity",        "bronze/idm_matching",         False),
        "idm_wap":              ("get_idm_weighted_average_price",   "bronze/idm_wap",              False),
        "idm_bid_offer":        ("get_idm_bid_offer_quantities",     "bronze/idm_bid_offer",        False),
        # DGP / Sistem yönü
        "smf":                  ("get_smf",                          "bronze/smf",                  False),
        "order_up":             ("get_order_summary_up",             "bronze/order_up",             False),
        "order_down":           ("get_order_summary_down",           "bronze/order_down",           False),
        "system_direction":     ("get_system_direction",             "bronze/system_direction",     False),
        # Üretim / Tüketim
        "dpp":                  ("get_dpp",                          "bronze/dpp",                  False),
        "injection":            ("get_injection_quantity",           "bronze/injection",            False),
        "aic":                  ("get_aic",                          "bronze/aic",                  False),
        "imbalance":            ("get_imbalance_quantity",           "bronze/imbalance",            False),
        # Yenilenebilir
        "unlicensed":           ("get_unlicensed_generation",        "bronze/unlicensed",           False),
        "res_forecast":         ("get_res_generation_and_forecast",  "bronze/res_forecast",         False),
        "gen_forecast":         ("get_generation_forecast",          "bronze/gen_forecast",         False),
        "licensed_gen":         ("get_licensed_realtime_generation", "bronze/licensed_gen",         False),
        # YEKDEM maliyet (aylık, 35g gecikme)
        "unlicensed_cost":      ("get_unlicensed_generation_cost",  "bronze/unlicensed_cost",      True),
        "yekdem_total_cost":    ("get_yekdem_total_cost",            "bronze/yekdem_total_cost",    True),
        "yekdem_unit_cost":     ("get_yekdem_unit_cost",             "bronze/yekdem_unit_cost",     True),
        "new_capacity":         ("get_new_installed_capacity",       "bronze/new_capacity",         True),
    }

    # ── Statik / referans veriler (allow_empty=True) ──────────────────────────
    static_sources = {
        "participants":       ("get_market_participants",   "bronze/participants",     True),
        "renewables_part":    ("get_renewables_participant","bronze/renewables_part",  True),
    }

    bronze_tasks = []

    # Hava durumu
    get_w  = PythonOperator(task_id="get_weather", python_callable=get_weather_data_callable)
    save_w = PythonOperator(
        task_id="save_weather_to_gcs",
        python_callable=save_to_gcs_callable,
        op_kwargs={'task_id': 'get_weather', 'bucket_path': 'bronze/weather', 'allow_empty': False}
    )
    get_w >> save_w
    bronze_tasks.append(save_w)

    # Saatlik piyasa verileri
    for key, (method, path, allow_empty) in hourly_sources.items():
        gt = PythonOperator(
            task_id=f"get_{key}",
            python_callable=get_epias_data_callable,
            op_kwargs={'method_name': method}
        )
        st = PythonOperator(
            task_id=f"save_{key}_to_gcs",
            python_callable=save_to_gcs_callable,
            op_kwargs={'task_id': f"get_{key}", 'bucket_path': path, 'allow_empty': allow_empty}
        )
        gt >> st
        bronze_tasks.append(st)

    # Statik veriler
    for key, (method, path, allow_empty) in static_sources.items():
        gt = PythonOperator(
            task_id=f"get_{key}",
            python_callable=get_epias_data_callable,
            op_kwargs={'method_name': method}
        )
        st = PythonOperator(
            task_id=f"save_{key}_to_gcs",
            python_callable=save_to_gcs_callable,
            op_kwargs={'task_id': f"get_{key}", 'bucket_path': path, 'allow_empty': allow_empty}
        )
        gt >> st
        bronze_tasks.append(st)

    # =========================================================================
    # 2. SILVER KATMANI
    # Her bronze save → ilgili silver Spark job
    # =========================================================================
    all_sources = {**hourly_sources, **static_sources}
    silver_tasks = []

    for key in all_sources:
        silver_t = SparkSubmitOperator(
            task_id=f"silver_{key}",
            application=f"/opt/airflow/spark/bronze_to_silver_{key}.py",
            py_files="/opt/airflow/spark/spark_utils.py",
            jars="/opt/spark/jars/gcs-connector.jar",
            conn_id=SPARK_CONN_ID,
            application_args=["{{ ds }}"],
            deploy_mode="client",
            name=f"epias_silver_{key}"
        )
        silver_tasks.append(silver_t)

    # 1-to-1 bağımlılık: save_{key} → silver_{key}
    for key in all_sources:
        save_task   = next(t for t in bronze_tasks if t.task_id == f"save_{key}_to_gcs")
        silver_task = next(t for t in silver_tasks if t.task_id == f"silver_{key}")
        save_task >> silver_task

    # =========================================================================
    # 3. GOLD KATMANI
    # =========================================================================
    run_gold = SparkSubmitOperator(
        task_id="run_spark_gold",
        application="/opt/airflow/spark/silver_to_gold.py",
        conn_id=SPARK_CONN_ID,
        application_args=["{{ ds }}"],
        name="epias_run_gold",
        deploy_mode="client"
    )

    for s_task in silver_tasks:
        s_task >> run_gold

    # =========================================================================
    # 4. BIGQUERY
    # =========================================================================
    gold_tables = [
        "market_prices",
        "renewable_impact",
        "player_analysis",
        "imbalance_metrics",
        "dam_supply_demand",
        "idm_activity",
        "dgp_orders",
    ]
    bq_loads = []
    for table in gold_tables:
        bt = PythonOperator(
            task_id=f"load_bq_{table}",
            python_callable=load_bigquery_dynamic,
            op_kwargs={'table_name': table}
        )
        bq_loads.append(bt)

    run_gold >> bq_loads

    # =========================================================================
    # 5. DBT & ML FORECAST
    # =========================================================================
    run_dbt      = PythonOperator(task_id="run_dbt",         python_callable=_run_dbt)
    run_forecast = PythonOperator(task_id="run_ptf_forecast", python_callable=_run_forecast)

    for bq_task in bq_loads:
        bq_task >> run_dbt >> run_forecast