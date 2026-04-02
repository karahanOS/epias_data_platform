import logging
from datetime import datetime, timedelta
import pytz
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

import sys
sys.path.insert(0, '/opt/airflow/src')
from epias_client import EPIASClient
from weather_client import WeatherClient




logger = logging.getLogger(__name__)

# ── YARDIMCI FONKSİYONLAR ────────────────────────────────────────────────────
def notify_failure(context):
    """Veri kalitesi testi patladığında e-posta gönderir."""
    task_instance = context['task_instance']
    error_msg = f"Dikkat! {task_instance.task_id} veri kalitesi testini geçemedi. Tahminleme durduruldu!"
    
    send_email = EmailOperator(
        task_id='send_error_email',
        to='mehmetkarahanc@gmail.com',
        subject='[KRİTİK] Veri Kalitesi Hatası - EPIAS Pipeline',
        html_content=error_msg
    )
    return send_email.execute(context)

# DAG tanımında:
default_args = {
    'on_failure_callback': notify_failure,
    # ...
}


def on_failure(context):
    task_id = context["task_instance"].task_id
    exception = context["exception"]
    execution_date = context["execution_date"]
    logger.error(
        f"Task başarısız! "
        f"task_id={task_id}, "
        f"tarih={execution_date}, "
        f"hata={exception}"
    )


def get_context_params(**context):
    tgt = context["ti"].xcom_pull(task_ids="fetch_tgt")
    execution_date = context["execution_date"]
    tz = pytz.timezone("Europe/Istanbul")
    start = execution_date.replace(hour=0, minute=0, second=0, tzinfo=tz)
    end = execution_date.replace(hour=23, minute=0, second=0, tzinfo=tz)
    return (
        tgt,
        start.strftime("%Y-%m-%dT%H:%M:%S+03:00"),
        end.strftime("%Y-%m-%dT%H:%M:%S+03:00"),
    )


# ── CALLABLE'LAR: VERİ ÇEK ───────────────────────────────────────────────────

def fetch_tgt_callable():
    client = EPIASClient()
    tgt = client._fetch_tgt()
    logger.info("TGT başarıyla alındı.")
    return tgt


def get_ptf_callable(**context):
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_ptf(start, end)
    logger.info(f"PTF verisi çekildi: {len(data)} kayıt")
    return data


def get_smf_callable(**context):
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_smf(start, end)
    logger.info(f"SMF verisi çekildi: {len(data)} kayıt")
    return data


def get_generation_callable(**context):
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_realtime_generation(start, end)
    logger.info(f"Üretim verisi çekildi: {len(data)} kayıt")
    return data


def get_consumption_callable(**context):
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_realtime_consumption(start, end)
    logger.info(f"Tüketim verisi çekildi: {len(data)} kayıt")
    return data


def get_load_estimation_callable(**context):
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_load_estimation_plan(start, end)
    logger.info(f"Yük tahmin verisi çekildi: {len(data)} kayıt")
    return data


def get_weather_callable(**context):
    execution_date = context["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")
    client = WeatherClient()
    data = client.get_weighted_weather(date_str, date_str)
    logger.info(f"Hava durumu verisi çekildi: {len(data)} kayıt")
    return data


# ── CALLABLE'LAR: GCS'E KAYDET ───────────────────────────────────────────────

def save_to_gcs(task_id: str, bucket_path: str, **context):
    import pandas as pd
    data = context["ti"].xcom_pull(task_ids=task_id)
    if not data:
        logger.warning(f"{task_id} için veri boş, GCS'e yazılmıyor.")
        return
    execution_date = context["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")
    gcs_path = f"gs://epias-data-lake/{bucket_path}/{date_str}.parquet"
    df = pd.DataFrame(data)
    df.to_parquet(gcs_path, index=False)
    logger.info(f"GCS'e yazıldı: {gcs_path} ({len(df)} kayıt)")


def save_ptf_callable(**context):
    save_to_gcs("get_ptf", "bronze/ptf", **context)

def save_smf_callable(**context):
    save_to_gcs("get_smf", "bronze/smf", **context)

def save_generation_callable(**context):
    save_to_gcs("get_generation", "bronze/generation", **context)

def save_consumption_callable(**context):
    save_to_gcs("get_consumption", "bronze/consumption", **context)

def save_load_estimation_callable(**context):
    save_to_gcs("get_load_estimation", "bronze/load_estimation", **context)

def save_weather_callable(**context):
    save_to_gcs("get_weather", "bronze/weather", **context)


# ── CALLABLE'LAR: SPARK PIPELINE ─────────────────────────────────────────────

def run_spark_job(script_name: str, target_date: str):
    """Spark container'ında bir job çalıştırır ve tarihi parametre olarak yollar."""
    cmd = [
        "docker", "exec", "epias_data_platform-spark-1",
        "/opt/spark/bin/spark-submit",
        "--jars", "/opt/spark_jobs/gcs-connector-hadoop3-2.2.22.jar",
        f"/opt/spark_jobs/{script_name}",
        target_date  # <-- İŞTE BURASI! Spark scriptindeki sys.argv[1]'e gidecek değer
    ]
    logger.info(f"Spark job başlatılıyor: {script_name} (Tarih: {target_date})")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

    if result.returncode != 0:
        logger.error(f"Spark job başarısız: {result.stderr[-3000:]}")
        raise Exception(f"Spark job başarısız: {script_name}\n{result.stderr[-2000:]}")

    logger.info(f"Spark job tamamlandı: {script_name}")
    return result.stdout[-1000:]


def run_silver_pipeline_callable(**context):
    """Tüm Bronze → Silver dönüşümlerini sırayla çalıştırır."""
    target_date = context["ds"] 
    
    # LİSTEYİ EKSİKSİZ DOLDURUYORUZ:
    jobs = [
        "bronze_to_silver_ptf.py",
        "bronze_to_silver_smf.py",
        "bronze_to_silver_generation.py",
        "bronze_to_silver_consumption.py",
        "bronze_to_silver_load_estimation.py",
        "bronze_to_silver_weather.py"
    ]
    for job in jobs:
        logger.info(f"Silver job: {job} çalıştırılıyor...")
        run_spark_job(job, target_date) 
        
    logger.info(f"{target_date} için tüm Silver job'ları tamamlandı!")


def run_gold_pipeline_callable(**context):
    """Silver → Gold dönüşümünü çalıştırır."""
    target_date = context["ds"] # Airflow'dan tarihi çek
    logger.info(f"Gold pipeline {target_date} tarihi için başlatılıyor...")
    
    # Tarihi script'e gönder
    run_spark_job("silver_to_gold.py", target_date)
    
    logger.info("Gold pipeline tamamlandı!")


# ── CALLABLE'LAR: BİGQUERY & DBT ─────────────────────────────────────────────

def load_bigquery_callable(**context):
    """Gold tablolarını BigQuery'e yükler."""
    import os
    from google.cloud import bigquery, storage

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/credentials/gcp-key.json"

    bq_client = bigquery.Client()
    gcs_client = storage.Client()

    BUCKET_NAME = "epias-data-lake"
    DATASET = "epias_gold"

    tables = [
        "price_spread_analysis",
        "generation_mix_price_impact",
        "supply_demand_summary",
        "load_vs_actual",
        "renewable_deep_analysis",
        "ml_features",
    ]

    def list_parquet_files(prefix):
        bucket = gcs_client.bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=prefix)
        return [
            f"gs://{BUCKET_NAME}/{blob.name}"
            for blob in blobs
            if blob.name.endswith(".parquet")
        ]

    for table_name in tables:
        uris = list_parquet_files(f"gold/{table_name}/")
        if not uris:
            logger.warning(f"{table_name} için dosya bulunamadı, atlanıyor.")
            continue

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        load_job = bq_client.load_table_from_uri(
                    source_uris=uris,
                    destination=f"{DATASET}.gold_{table_name}", # <-- DÜZELTİLDİ
                    job_config=job_config,
        )
        load_job.result()
        logger.info(f"{table_name} BigQuery'e yüklendi!")


def run_dbt_callable(**context):
    """dbt modellerini çalıştırır."""
    dbt_dir = "/opt/airflow/epias_dbt"
    
    # dbt'nin Python içindeki kesin yolunu veriyoruz ki kafası karışmasın
    dbt_executable = "/home/airflow/.local/bin/dbt"
    
    # --profiles-dir olarak /root/ yerine, dbt projesinin kendi klasörünü gösteriyoruz
    cmd = [
        dbt_executable, "run", 
        "--project-dir", dbt_dir, 
        "--profiles-dir", dbt_dir
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

    if result.returncode != 0:
        logger.error(f"dbt başarısız: {result.stderr}")
        raise Exception(f"dbt run başarısız:\n{result.stdout[-1000:]}\n{result.stderr[-1000:]}")

    logger.info(f"dbt tamamlandı:\n{result.stdout[-500:]}")


def run_ptf_forecast_callable(**context):
    """PTF forecasting modelini çalıştırır ve tahminleri kaydeder."""
    forecast_script = "/opt/airflow/src/ptf_forecaster.py"
    cmd = ["python3", forecast_script]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600,
        cwd="/opt/airflow"
    )

    if result.returncode != 0:
        logger.error(f"PTF forecast başarısız: {result.stderr}")
        raise Exception(f"PTF forecast başarısız:\n{result.stderr[-500:]}")

    logger.info(f"PTF forecast tamamlandı:\n{result.stdout[-500:]}")


# ── DAG TANIMI ────────────────────────────────────────────────────────────────

default_args = {
    "owner": "epias_pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="epias_daily_ingestion",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["epias", "ingestion"],
) as dag:

    # ── TASK 1: TGT AL ────────────────────────────────────────────────────────
    fetch_tgt = PythonOperator(
        task_id="fetch_tgt",
        python_callable=fetch_tgt_callable,
    )

    # ── TASK 2-7: VERİ ÇEK ───────────────────────────────────────────────────
    get_ptf = PythonOperator(
        task_id="get_ptf",
        python_callable=get_ptf_callable,
        trigger_rule="all_done",
    )
    get_smf = PythonOperator(
        task_id="get_smf",
        python_callable=get_smf_callable,
        trigger_rule="all_done",
    )
    get_generation = PythonOperator(
        task_id="get_generation",
        python_callable=get_generation_callable,
        trigger_rule="all_done",
    )
    get_consumption = PythonOperator(
        task_id="get_consumption",
        python_callable=get_consumption_callable,
        trigger_rule="all_done",
    )
    get_load_estimation = PythonOperator(
        task_id="get_load_estimation",
        python_callable=get_load_estimation_callable,
        trigger_rule="all_done",
    )
    get_weather = PythonOperator(
        task_id="get_weather",
        python_callable=get_weather_callable,
        trigger_rule="all_done",
    )

    # ── TASK 8-13: GCS'E KAYDET ──────────────────────────────────────────────
    save_ptf = PythonOperator(
        task_id="save_ptf_to_gcs",
        python_callable=save_ptf_callable,
    )
    save_smf = PythonOperator(
        task_id="save_smf_to_gcs",
        python_callable=save_smf_callable,
    )
    save_generation = PythonOperator(
        task_id="save_generation_to_gcs",
        python_callable=save_generation_callable,
    )
    save_consumption = PythonOperator(
        task_id="save_consumption_to_gcs",
        python_callable=save_consumption_callable,
    )
    save_load_estimation = PythonOperator(
        task_id="save_load_estimation_to_gcs",
        python_callable=save_load_estimation_callable,
    )
    save_weather = PythonOperator(
        task_id="save_weather_to_gcs",
        python_callable=save_weather_callable,
    )

    # ── TASK 14: SPARK SILVER ─────────────────────────────────────────────────
    run_silver = PythonOperator(
        task_id="run_spark_silver",
        python_callable=run_silver_pipeline_callable,
        trigger_rule="all_done",
    )

    # ── TASK 15: SPARK GOLD ───────────────────────────────────────────────────
    run_gold = PythonOperator(
        task_id="run_spark_gold",
        python_callable=run_gold_pipeline_callable,
    )

    # ── TASK 16: BIGQUERY YÜKLE ───────────────────────────────────────────────
    load_bq = PythonOperator(
        task_id="load_bigquery",
        python_callable=load_bigquery_callable,
    )

    # ── TASK 17: DBT RUN ──────────────────────────────────────────────────────
    run_dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt_callable,
    )

    # ── TASK 18: PTF FORECAST ─────────────────────────────────────────────────
    run_forecast = PythonOperator(
        task_id="run_ptf_forecast",
        python_callable=run_ptf_forecast_callable,
    )

    # ── BAĞIMLILIKLAR ─────────────────────────────────────────────────────────
    #
    # fetch_tgt
    #     ├── get_ptf ──── save_ptf_to_gcs ──┐
    #     ├── get_smf ──── save_smf_to_gcs ──┤
    #     ├── get_generation ── save_gen ────┤
    #     ├── get_consumption ── save_con ───┤── run_spark_silver
    #     ├── get_load_estimation ── save ───┤       │
    #     └── get_weather ──── save_weather ─┘   run_spark_gold
    #                                                 │
    #                                           load_bigquery
    #                                                 │
    #                                             run_dbt
    #                                                 │
    #                                          run_ptf_forecast

    fetch_tgt >> [get_ptf, get_smf, get_generation,
                  get_consumption, get_load_estimation, get_weather]

    get_ptf >> save_ptf
    get_smf >> save_smf
    get_generation >> save_generation
    get_consumption >> save_consumption
    get_load_estimation >> save_load_estimation
    get_weather >> save_weather

    [save_ptf, save_smf, save_generation,
     save_consumption, save_load_estimation, save_weather] >> run_silver

    run_silver >> run_gold >> load_bq >> run_dbt >> run_forecast