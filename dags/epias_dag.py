"""
EPIAS Medallion Pipeline v3
============================
Mimari  : Bronze (raw GCS) → Silver (Spark/parquet) → BigQuery (External) → Gold (dbt) → ML (XGBoost)
Zamanlama: Her gün 05:00 UTC (08:00 TR)

Temizlenmiş, optimize edilmiş ve Predictive Analytics için güncellenmiş üretim hattı.
"""
from __future__ import annotations
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, Tuple

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from epias_sources import EPIAS_SOURCES, DBT_EXCLUDE_PENDING_BACKFILL, SPARK_CONN_ID, make_silver_task

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

# ── VERİ GECİKME TABLOSU ─────────────────────────────────────────────────────
DATA_DELAYS: Dict[str, int] = {
    "get_ptf_smf_sdf":                  0,
    "get_smf":                          0,
    "get_supply_demand":                0,
    "get_dam_clearing_quantity":        0,
    "get_price_independent_bid":        0,
    "get_idm_transaction_history":      0,
    "get_order_summary_up":             0,
    "get_order_summary_down":           0,
    "get_system_direction":             0,
    "get_dpp":                          0,
    "get_injection_quantity":           1,
    "get_aic":                          0,
    "get_imbalance_quantity":           0,
    "get_res_generation_and_forecast":  0,
    "get_licensed_realtime_generation": 0,
    "get_load_estimation_plan":         0,
    "get_unlicensed_generation":        35,
    "get_uevcb_list":                   0,
    "get_outages":                      0,
    "get_dams":                         0
}

NO_DATE_METHODS: frozenset = frozenset({
    "get_market_participants",
    
})

# ── HATA YÖNETİMİ ─────────────────────────────────────────────────────────────
def notify_failure(context: dict) -> None:
    ti = context["task_instance"]
    logger.error(
        "🚨 TASK HATASI | dag=%s | task=%s | tarih=%s | hata=%s",
        ti.dag_id, ti.task_id, context.get("ds"), context.get("exception"),
    )

# ── CALLABLE'LAR ──────────────────────────────────────────────────────────────
def get_epias_data_callable(method_name: str, **context) -> list:
    client = EPIASClient()
    ds     = context["ds"]
    delay  = DATA_DELAYS.get(method_name, 0)

    if delay > 0:
        target = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=delay)).strftime("%Y-%m-%d")
    else:
        target = ds

    method = getattr(client, method_name)
    result = method() if method_name in NO_DATE_METHODS else method(target, target)
    return result

def get_weather_data_callable(**context) -> list:
    client = WeatherClient()
    ds     = context["ds"]
    rows: list = []

    for city in ("istanbul", "izmir", "ankara", "konya"):
        try:
            city_df = client.get_weather_for_city(city, ds, ds)
            if hasattr(city_df, "to_dict"):
                for col in city_df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]", "datetimetz"]).columns:
                    city_df[col] = city_df[col].astype(str)
                records = city_df.to_dict(orient="records")
            elif isinstance(city_df, list):
                records = city_df
            else:
                records = [city_df]
            rows.extend(records)
        except Exception as exc:
            logger.error("weather/%s hatası: %s", city, exc)
    return rows

def save_to_gcs_callable(task_id: str, bucket_path: str, allow_empty: bool = False, **context) -> None:
    data = context["ti"].xcom_pull(task_ids=task_id)

    if not data:
        if allow_empty:
            return
        raise ValueError(f"🚨 {task_id} task'ından veri gelmedi, GCS'e yazılamıyor!")

    if isinstance(data, list) and data and isinstance(data[0], list):
        flat: list = []
        for sub in data:
            flat.extend(sub)
    else:
        flat = data

    ds       = context["ds"]
    gcs_path = f"gs://{BUCKET_NAME}/{bucket_path}/{ds}.parquet"

    df = pd.DataFrame(flat)
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].astype(str)

    df.to_parquet(gcs_path, index=False)

# ── DAG ───────────────────────────────────────────────────────────────────────
default_args = {
    "owner":               "epias_team",
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="epias_medallion_pipeline_v3",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    tags=["epias", "medallion", "dbt", "ml"],
) as dag:

    # =========================================================================
    # BRONZE LAYER (Sadece Yeni Mimarideki Aktif Kaynaklar)
    # =========================================================================
    # v[4] = daily_eligible; v[:3] = (method_name, gcs_path, allow_empty)
    ALL_SOURCES = {k: v[:3] for k, v in EPIAS_SOURCES.items() if v[4]}

    # Hava Durumu Özel Akış
    get_weather = PythonOperator(task_id="get_weather", python_callable=get_weather_data_callable)
    save_weather = PythonOperator(
        task_id="save_weather_to_gcs",
        python_callable=save_to_gcs_callable,
        op_kwargs={"task_id": "get_weather", "bucket_path": "bronze/weather", "allow_empty": False},
    )
    get_weather >> save_weather

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
            op_kwargs={"task_id": f"get_{key}", "bucket_path": path, "allow_empty": allow_empty},
        )
        get_t >> save_t
        bronze_save_tasks[key] = save_t

    # =========================================================================
    # SILVER LAYER (Spark)
    # =========================================================================
    silver_tasks: Dict[str, SparkSubmitOperator] = {}
    
    # Weather için özel silver task
    silver_weather = SparkSubmitOperator(
        task_id="silver_weather",
        application="/opt/airflow/spark/bronze_to_silver_weather.py",
        py_files="/opt/airflow/spark/spark_utils.py",
        jars="/opt/spark/jars/gcs-connector.jar",
        conn_id=SPARK_CONN_ID,
        application_args=["{{ ds }}"],
        deploy_mode="client",
        name="epias_silver_weather",
    )
    save_weather >> silver_weather

    # API'den gelen veriler için silver task'lar
    for key in ALL_SOURCES:
        silver_t = make_silver_task(dag, key)
        bronze_save_tasks[key] >> silver_t
        silver_tasks[key] = silver_t

    # =========================================================================
    # BIGQUERY BRIDGE & DBT (GOLD) & ML
    # =========================================================================
    
    load_to_bq = BashOperator(
        task_id='load_silver_to_bigquery',
        bash_command='python /opt/airflow/src/load_to_bigquery.py',
    )

    run_dbt = BashOperator(
        task_id='run_dbt_gold_models',
        bash_command=(
            'cd /opt/airflow/epias_dbt && dbt run --profiles-dir . '
            '--exclude ' + ' '.join(DBT_EXCLUDE_PENDING_BACKFILL)
        ),
    )
    
    # Training: ağır iş — haftada bir çalışır, dbt sonrası tetiklenir
    run_ptf_trainer = BashOperator(
        task_id='train_ptf_model',
        bash_command='python /opt/airflow/src/ptf_trainer.py',
    )

    # Inference: hafif iş — sadece son 180 satır çeker, önceden eğitilmiş
    # modeli GCS'den yükler ve tek tahmin üretir
    run_ptf_inference = BashOperator(
        task_id='run_ptf_inference',
        bash_command='python /opt/airflow/src/ptf_inference.py',
    )

    # Zinciri Bağlama
    silver_weather >> load_to_bq
    for silver_t in silver_tasks.values():
        silver_t >> load_to_bq

    # dbt → trainer (günlük) → inference (günlük, ama ayrı DAG'da saatlik koşabilir)
    load_to_bq >> run_dbt >> run_ptf_trainer >> run_ptf_inference