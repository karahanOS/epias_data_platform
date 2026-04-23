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
BUCKET_NAME = "epias-data-lake"
MY_EMAIL = "mehmetkarahanc@gmail.com"

# HATA DÜZELTİLDİ: SPARK_CONN_ID ve MASTER_URL tanımlanmamıştı → NameError veriyordu
SPARK_CONN_ID = "spark_default"          # Airflow UI > Admin > Connections'da tanımlı olmalı
MASTER_URL = "spark://spark-master:7077" # docker-compose'daki spark-master servisi

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
    send_email = EmailOperator(
        task_id='send_error_email',
        to=MY_EMAIL,
        subject='[KRİTİK] EPIAŞ Pipeline Hatası',
        html_content=error_msg
    )
    #return send_email.execute(context)

# ── CALLABLE'LAR ─────────────────────────────────────────────────────────────
def get_epias_data_callable(method_name, **context):
    client = EPIASClient()
    ds = context["ds"]
    method = getattr(client, method_name)
    if method_name in ["get_organization_list", "get_market_participants"]:
        return method()
    return method(ds, ds)

def get_weather_data_callable(**context):
    w_client = WeatherClient()
    cities = ["istanbul", "izmir", "ankara", "konya"]
    ds = context["ds"]
    all_weather_rows = []
    
    for city in cities:
        try:
            city_df = w_client.get_weather_for_city(city, ds, ds)
            
            # DataFrame → list of dict dönüşümü
            if hasattr(city_df, 'to_dict'):
                # HATA DÜZELTİLDİ: Timestamp → string (XCom JSON serialize edemez)
                for col in city_df.select_dtypes(include=['datetime64[ns, UTC]', 
                                                           'datetime64[ns]',
                                                           'datetimetz']).columns:
                    city_df[col] = city_df[col].astype(str)
                
                records = city_df.to_dict(orient='records')
            elif isinstance(city_df, list):
                records = city_df
            else:
                records = [city_df]
                
            all_weather_rows.extend(records)
            logger.info(f"✅ {city}: {len(records)} satır alındı")
            
        except Exception as e:
            logger.error(f"{city} için hava durumu hatası: {str(e)}")
            continue
            
    return all_weather_rows

def save_to_gcs_callable(task_id: str, bucket_path: str, **context):
    data = context["ti"].xcom_pull(task_ids=task_id)
    if not data or len*(data)==0:
        raise ValueError(f"🚨 {task_id} taskından veri gelmedi, GCS'e kayıt yapılamıyor!")
        

    flattened_data = []
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
        for sublist in data:
            flattened_data.extend(sublist)
    else:
        flattened_data = data

    ds = context["ds"]
    gcs_path = f"gs://{BUCKET_NAME}/{bucket_path}/{ds}.parquet"

    try:
        df = pd.DataFrame(flattened_data)
        
        # HATA ÖNLEMİ: Timezone-aware datetime kolonlarını string'e çevir
        for col in df.select_dtypes(include=['datetimetz']).columns:
            df[col] = df[col].astype(str)
            
        df.to_parquet(gcs_path, index=False)
        logger.info(f"✅ GCS: {gcs_path} | {len(df)} satır")
    except Exception as e:
        logger.error(f"❌ Hata ({task_id}): {str(e)}")
        raise

def load_bigquery_dynamic(table_name, **context):
    from google.cloud import bigquery
    client = bigquery.Client()
    ds = context["ds"]
    y, m, d = ds.split("-")
    table_id = f"epias-data-platform.epias_gold.gold_{table_name}"
    uri = f"gs://{BUCKET_NAME}/gold/{table_name}/year={y}/month={m}/day={d}/*.parquet"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    client.load_table_from_uri(uri, table_id, job_config=job_config).result()
    logger.info(f"✅ BigQuery yüklendi: {table_id}")

# HATA DÜZELTİLDİ: run_spark_job tanımlanmamıştı → NameError veriyordu
def run_spark_job(script_name: str, ds: str):
    """Spark job'ı spark-submit ile çalıştırır."""
    script_path = f"/opt/airflow/spark/{script_name}"
    cmd = [
        "spark-submit",
        "--master", MASTER_URL,
        "--deploy-mode", "client",
        script_path,
        ds
    ]
    logger.info(f"Spark job başlatılıyor: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Spark job başarısız:\n{result.stderr}")
    logger.info(f"✅ Spark job tamamlandı: {script_name}")

# ── DAG TANIMI ───────────────────────────────────────────────────────────────
default_args = {
    'owner': 'epias_medallion_pipeline_v2',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
}

with DAG(
    dag_id="epias_medallion_pipeline_v2",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    # 1. BRONZE KATMANI
    # HATA DÜZELTİLDİ: "pricing" metodu "get_ptf_smf_sdf" olarak güncellendi
    # (epias_client.py'de get_pricing_data vardı ama DAG get_ptf_smf_sdf çağırıyordu)
    data_sources = {
        "pricing":      "get_ptf_smf_sdf",
        "unlicensed":   "get_unlicensed_generation",
        "imbalance":    "get_imbalance_quantity",
        "dpp":          "get_dpp",
        "participants": "get_market_participants"
    }

    bronze_tasks = []

    # Weather
    get_w  = PythonOperator(task_id="get_weather", python_callable=get_weather_data_callable)
    save_w = PythonOperator(
        task_id="save_weather_to_gcs",
        python_callable=save_to_gcs_callable,
        op_kwargs={'task_id': 'get_weather', 'bucket_path': 'bronze/weather'}
    )
    get_w >> save_w
    bronze_tasks.append(save_w)

    # EPIAŞ kaynakları
    for key, method in data_sources.items():
        gt = PythonOperator(
            task_id=f"get_{key}",
            python_callable=get_epias_data_callable,
            op_kwargs={'method_name': method}
        )
        st = PythonOperator(
            task_id=f"save_{key}_to_gcs",
            python_callable=save_to_gcs_callable,
            op_kwargs={'task_id': f"get_{key}", 'bucket_path': f"bronze/{key}"}
        )
        gt >> st
        bronze_tasks.append(st)

    # Silver Task Tanımı
    silver_tasks = []
    # epias_dag.py içinde Silver Task döngüsü
    for key in data_sources:
        silver_t = SparkSubmitOperator(
            task_id=f"silver_{key}",
            application=f"/opt/airflow/spark/bronze_to_silver_{key}.py",
            py_files="/opt/airflow/spark/spark_utils.py",
            jars="/opt/spark/jars/gcs-connector.jar",
            conn_id="spark_default",
            # HATA DÜZELTME: Master URL'i burada açıkça veya Connection üzerinden protokol ile verin
            master="spark://spark-master:7077", 
            application_args=["{{ ds }}"],
            deploy_mode="client",
            name=f"epias_silver_{key}"
        )
        silver_tasks.append(silver_t)

    run_gold = SparkSubmitOperator(
            task_id="run_spark_gold",
            application="/opt/airflow/spark/silver_to_gold.py",
            conn_id=SPARK_CONN_ID,
            application_args=["{{ ds }}"],
            name="epias_run_gold",
            deploy_mode="client"
        )

    # 4. BIGQUERY
    gold_tables = ["market_prices", "renewable_impact", "player_analysis", "imbalance_metrics"]
    bq_loads = []
    for table in gold_tables:
        bt = PythonOperator(
            task_id=f"load_bq_{table}",
            python_callable=load_bigquery_dynamic,
            op_kwargs={'table_name': table}
        )
        bq_loads.append(bt)

    # 5. DBT & ML FORECAST
    run_dbt      = PythonOperator(task_id="run_dbt",          python_callable=lambda **c: logger.info("dbt run"))
    run_forecast = PythonOperator(task_id="run_ptf_forecast",  python_callable=lambda **c: logger.info("ML Forecast tetiklendi"))

    # ⛓️ ZİNCİRLEME
    # HATA DÜZELTİLDİ: "b_task >> silver_tasks" list'e >> uygulanamaz
    # Her bronze task her silver task'ı tetiklemeli
    for b_task in bronze_tasks:
        for s_task in silver_tasks:
            b_task >> s_task

    for s_task in silver_tasks:
        s_task >> run_gold

    run_gold >> bq_loads

    for bq_task in bq_loads:
        bq_task >> run_dbt >> run_forecast