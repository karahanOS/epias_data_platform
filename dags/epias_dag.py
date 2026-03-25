import logging
from datetime import datetime, timedelta
import pytz

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, '/opt/airflow/src')
from epias_client import EPIASClient
sys.path.insert(0, '/opt/airflow/src')
from weather_client import WeatherClient

logger = logging.getLogger(__name__)

# ── YARDIMCI FONKSİYONLAR ────────────────────────────────────────────────────

def on_failure(context):
    """Herhangi bir task çökünce çalışır, hatayı loglar."""
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
    """
    Her callable'da tekrar eden TGT + tarih hesaplamasını tek yerden yönetir.
    DRY prensibi: Don't Repeat Yourself.
    """
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
def get_weather_callable(**context):
    """4 şehir için saatlik hava durumu verisini çeker."""
    execution_date = context["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")
    client = WeatherClient()
    data = client.get_weighted_weather(date_str, date_str)
    logger.info(f"Hava durumu verisi çekildi: {len(data)} kayıt")
    return data



def fetch_tgt_callable():
    """
    TGT token alır ve return eder.
    Airflow return edilen değeri otomatik olarak XCom'a yazar.
    """
    client = EPIASClient()
    tgt = client._fetch_tgt()
    logger.info("TGT başarıyla alındı.")
    return tgt


def get_ptf_callable(**context):
    """PTF (Piyasa Takas Fiyatı) verisini çeker."""
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_ptf(start, end)
    logger.info(f"PTF verisi çekildi: {len(data)} kayıt")
    return data


def get_smf_callable(**context):
    """SMF (Sistem Marjinal Fiyatı) verisini çeker."""
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_smf(start, end)
    logger.info(f"SMF verisi çekildi: {len(data)} kayıt")
    return data


def get_generation_callable(**context):
    """Gerçek zamanlı üretim verisini çeker."""
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_realtime_generation(start, end)
    logger.info(f"Üretim verisi çekildi: {len(data)} kayıt")
    return data


def get_consumption_callable(**context):
    """Gerçek zamanlı tüketim verisini çeker."""
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_realtime_consumption(start, end)
    logger.info(f"Tüketim verisi çekildi: {len(data)} kayıt")
    return data


def get_load_estimation_callable(**context):
    """Saatlik yük tahmin planı verisini çeker."""
    tgt, start, end = get_context_params(**context)
    client = EPIASClient()
    client._tgt = tgt
    data = client.get_load_estimation_plan(start, end)
    logger.info(f"Yük tahmin verisi çekildi: {len(data)} kayıt")
    return data


# ── CALLABLE'LAR: GCS'E KAYDET ───────────────────────────────────────────────

def save_to_gcs(task_id: str, bucket_path: str, **context):
    """
    XCom'dan veriyi okur, Parquet'e çevirir ve GCS'e yazar.
    Tüm save task'ları bu fonksiyonu kullanır — DRY.
    """
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
    catchup=True,
    max_active_runs=3,
    tags=["epias", "ingestion"],
) as dag:

    # ── TASK 1: TGT AL ────────────────────────────────────────────────────────
    fetch_tgt = PythonOperator(
        task_id="fetch_tgt",
        python_callable=fetch_tgt_callable,
    )

    # ── TASK 2-6: VERİ ÇEK ───────────────────────────────────────────────────
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

    # ── TASK 7-11: GCS'E KAYDET ──────────────────────────────────────────────
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
    # ── BAĞIMLILIKLAR ─────────────────────────────────────────────────────────
    #
    # fetch_tgt
    #     │
    #     ├── get_ptf ──────────── save_ptf_to_gcs
    #     ├── get_smf ──────────── save_smf_to_gcs
    #     ├── get_generation ───── save_generation_to_gcs
    #     ├── get_consumption ──── save_consumption_to_gcs
    #     └── get_load_estimation ─ save_load_estimation_to_gcs

    fetch_tgt >> [get_ptf, get_smf, get_generation, get_consumption, get_load_estimation, get_weather]
    get_weather >> save_weather

    get_ptf >> save_ptf
    get_smf >> save_smf
    get_generation >> save_generation
    get_consumption >> save_consumption
    get_load_estimation >> save_load_estimation