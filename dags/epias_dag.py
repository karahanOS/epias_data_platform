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
# Silver layer runs on Dataproc Serverless (ADR-0002), consolidated into
# DATAPROC_POOL_SIZE batches instead of one per source (ADR-0003) — see
# epias_sources.group_sources / make_silver_batch_task.
from epias_sources import (
    EPIAS_SOURCES,
    DBT_EXCLUDE_PENDING_BACKFILL,
    group_sources,
    make_silver_batch_task,
)

# ── MODÜL YOLU ────────────────────────────────────────────────────────────────
sys.path.insert(0, "/opt/airflow/src")
try:
    from epias_client import EPIASClient
    from weather_client import WeatherClient
    from fx_client import FXClient
    from silver_lookback_fix import fix_smf_partition, fix_system_direction_partition
except ImportError as exc:
    logging.error(f"Modül yükleme hatası: {exc}")

logger = logging.getLogger(__name__)

# ── AYARLAR ───────────────────────────────────────────────────────────────────
BUCKET_NAME   = "epias-data-lake"

# ── VERİ GECİKME TABLOSU ─────────────────────────────────────────────────────
DATA_DELAYS: Dict[str, int] = {
    "get_ptf_smf_sdf":                  0,
    # idm_transaction_history/outages: EPIAS API rejects endDate >= today for
    # these two ("endDate must be in the past"). Under the daily schedule this
    # never surfaced (ds was effectively yesterday by the time the DAG ran).
    # Under hourly scheduling ds is always "today" intraday, so these failed
    # on every single hourly run once the hourly schedule went live
    # (2026-07-24) — cascading into silver_batch failures and blocking
    # load_silver_to_bigquery/run_dbt_gold_models for the whole pipeline.
    # Fix: delay=1, same pattern as get_injection_quantity.
    #
    # get_smf used to be in this same delay=1 group, but the actual root
    # cause (2026-08-15 investigation, SMF forecaster plan) turned out to be
    # narrower than "same-day is rejected": EPIAS errorCode (VAL)SEF1116
    # rejects any endDate *timestamp* that's still in the future, and
    # epias_client.py's _date_body() always requested through end-of-day
    # 23:00 — trivially future for any same-day query issued before midnight.
    # get_smf now internally caps endDate at "now - 1h" for same-day requests
    # (EPIASClient._safe_end_iso) and was live-verified to return real SMF
    # data up to ~5-6h old, matching the official S+5 publish lag (EPİAŞ
    # Kurul Kararı 10711, row 53) — so delay=0 is now correct and lets every
    # hourly run pick up the freshest available SMF as the day progresses,
    # same as most other sources (see schedule_interval comment below).
    "get_smf":                          0,
    "get_supply_demand":                0,
    "get_dam_clearing_quantity":        0,
    "get_price_independent_bid":        0,
    # get_idm_transaction_history/get_outages: also used to be in the same
    # delay=1 "endDate must be in the past" group as get_smf above, based on
    # the same original (overly broad) diagnosis. Re-verified live 2026-08-19
    # against EPİAŞ Kurul Kararı 10711 (Şeffaflık Platformunda Yayımlanacak
    # Veri Listesi): GİP anlık işlemler ~ row 76 "Gün İçi Piyasası İşlem Akışı"
    # (Saatlik, G — same-day/near-real-time), outages = row 24 "Arıza &
    # Plansız Bakım" (Saatlik, Anlık — real-time). Both live-tested with
    # end_date=today: get_idm_transaction_history returned 18,404 rows,
    # get_outages returned 68 rows, neither raised the past-date validation
    # error. delay=0 is correct per the official schedule; allow_empty=True
    # (epias_sources.py) covers the edge case of a very-early-hour run before
    # any of today's rows exist yet, same safety net as smf/system_direction.
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
    "get_dams":                         0,
    # get_interim_mcp: negative delay = LEAD. K.PTF (itiraz-öncesi PTF)
    # teslim gününden ~1 gün önce, GÖP açık artırması kapanır kapanmaz
    # mevcut oluyor — final PTF'nin aksine, teslim gününün kendi 14:00'ini
    # beklemiyor. -1 ile her saatlik run "ds+1" (yarın) için K.PTF ister;
    # açık artırma henüz kapanmadıysa allow_empty=True zaten boş dönüşü
    # sorunsuz karşılıyor (pricing'in "veri saat 14 öncesinde mevcut değil"
    # davranışıyla aynı mekanizma).
    "get_interim_mcp":                  -1,
}

# ADR-0006 (2026-08-08): get_res_generation_and_forecast/get_aic were both
# called with a single-day (target, target) range — same as everything else
# — but unlike get_load_estimation_plan (whose API generously returns
# tomorrow's data even for a single-day query, confirmed empirically), these
# two respect the requested range strictly. A direct query for tomorrow
# specifically DOES return real data (confirmed: EPİAŞ already publishes
# both a day ahead), so the gap was purely our own request window, not an
# EPİAŞ-side publish-timing limit. Consequence: mart_ptf_forward_features
# always saw forecasted_res_mwh=0 / capacity_utilization_ratio=NULL for
# genuinely-future dates, which actively misled the forward-forecast model
# (0 RES ≠ neutral — a real price crash usually means *high* renewable
# output, the opposite signal). LOOKAHEAD_DAYS widens just these two
# sources' end date to target+1 so tomorrow's already-published data is
# captured same-day instead of only becoming visible once "tomorrow"
# becomes "today".
LOOKAHEAD_DAYS: Dict[str, int] = {
    "get_res_generation_and_forecast": 1,
    "get_aic":                         1,
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

    # delay=0 -> target=ds (unchanged). delay>0 -> look back N days (unchanged).
    # delay<0 -> look AHEAD |delay| days (lead) — e.g. get_interim_mcp's -1
    # fetches ds+1 ("tomorrow"), since K.PTF for tomorrow is already
    # available today once the day-ahead auction clears (~14:00 TRT).
    target = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=delay)).strftime("%Y-%m-%d")

    # LOOKAHEAD_DAYS (ADR-0006): widen the END date only, so the fetch range
    # becomes [target, target+N] instead of a single day — captures
    # already-published future rows (e.g. tomorrow's RES forecast/AIC) in
    # the same daily fetch instead of waiting for "tomorrow" to become "today".
    lookahead = LOOKAHEAD_DAYS.get(method_name, 0)
    end_target = (datetime.strptime(target, "%Y-%m-%d") + timedelta(days=lookahead)).strftime("%Y-%m-%d")

    method = getattr(client, method_name)
    result = method() if method_name in NO_DATE_METHODS else method(target, end_target)
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

def get_fx_data_callable(**context) -> list:
    client = FXClient()
    ds = context["ds"]
    # T-1: TCMB ~15:30 TR'de yayınlar, DAG 08:00 TR'de çalışır — bir önceki günün kuru alınır
    target = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    return client.get_usdtry(target)

def smf_lookback_silver_fix_callable(**context) -> None:
    """Corrects yesterday's smf/system_direction Silver partition once
    EPİAŞ's S+5 settlement lag has fully cleared (2026-08-18 investigation).

    Hours 22-23 (Istanbul) settle at 03:00/04:00 the NEXT Istanbul day — after
    the last hourly run for `ds` already executed (see epias_client.py's
    _safe_end_iso, which narrows same-day requests to "now - 1h"). Since
    DATA_DELAYS["get_smf"]/["get_system_direction"] = 0 means the DAG's `ds`
    never revisits a past date, Bronze permanently froze at whatever was
    available in that narrow window — confirmed missing exactly hours 22-23
    every day since same-day SMF fetch went live (2026-08-15).

    Runs every hour rather than once daily: cheap (a few small API calls +
    a GCS parquet overwrite, no Dataproc/Spark involved — see
    src/silver_lookback_fix.py) and self-heals within ~1h of settlement
    instead of waiting for a once-a-day window, with no meaningful added
    GCP cost either way.
    """
    ds = context["ds"]
    yesterday = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    client = EPIASClient()

    n_smf = fix_smf_partition(client, yesterday)
    n_dir = fix_system_direction_partition(client, yesterday)
    logger.info(f"Silver lookback fix ({yesterday}): smf={n_smf} rows, system_direction={n_dir} rows")


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

    # Coerce integer columns to float64 so every daily Bronze file uses DOUBLE
    # physical type in Parquet.  Without this, pandas infers int64 when all API
    # values for a column happen to be round numbers (e.g. downRegulationOneCoded=0
    # or 1 all day).  Spark / BigQuery external tables registered as DOUBLE then
    # refuse to read those partitions — same fix already present in backfill_chunk.
    int_cols = df.select_dtypes(include=["int32", "int64"]).columns.tolist()
    if int_cols:
        df[int_cols] = df[int_cols].astype("float64")

    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].astype(str)

    df.to_parquet(gcs_path, index=False)

# ── DAG ───────────────────────────────────────────────────────────────────────
# ADR-0004 (2026-07-27): on_failure_callback only ever logged an error line --
# nobody was actually notified unless they opened the Airflow UI. email_on_failure
# uses Airflow's built-in SMTP support (see docker-compose.yml's AIRFLOW__SMTP__*
# env vars) to send a real email on any task failure, at zero added cost.
default_args = {
    "owner":               "epias_team",
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "email_on_failure":    True,
    "email":               ["mehmetkarahanc@gmail.com"],
}

with DAG(
    dag_id="epias_medallion_pipeline_v3",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    # Hourly (ADR-0002 action item 9 + ADR-0003): EPIAS publishes most sources
    # hour-by-hour, so re-fetching "today" (ds stays constant intra-day under
    # Airflow's hourly data-interval semantics) each hour naturally picks up
    # newly-published hours as the day progresses — Silver's overwrite mode
    # reflects the growing day. Made viable by ADR-0003's batch consolidation
    # (~4 min Silver layer instead of ~40 min) fitting well inside the hour.
    schedule_interval="0 * * * *",
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

    # Döviz Kuru Özel Akışı (TCMB EVDS)
    get_fx = PythonOperator(task_id="get_fx_rates", python_callable=get_fx_data_callable)
    save_fx = PythonOperator(
        task_id="save_fx_to_gcs",
        python_callable=save_to_gcs_callable,
        op_kwargs={"task_id": "get_fx_rates", "bucket_path": "bronze/fx_rates", "allow_empty": False},
    )
    get_fx >> save_fx

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
    # SILVER LAYER (Dataproc Serverless, consolidated — ADR-0002 + ADR-0003)
    # =========================================================================
    # All daily sources (incl. weather/fx, which have their own bronze flow but
    # the same bronze_to_silver_<key>.py job pattern) are grouped into
    # DATAPROC_POOL_SIZE batches — one shared Spark session per group instead
    # of one Dataproc batch per source — to amortize cold start (~3-4 min)
    # across many sources rather than paying it ~24 times. See ADR-0003.
    all_silver_keys = list(ALL_SOURCES.keys()) + ["weather", "fx_rates"]
    bronze_dep_tasks = {**bronze_save_tasks, "weather": save_weather, "fx_rates": save_fx}

    silver_batch_tasks = []
    for i, group in enumerate(group_sources(all_silver_keys)):
        batch_t = make_silver_batch_task(f"silver_batch_{i}", group, ["{{ ds }}"])
        for key in group:
            bronze_dep_tasks[key] >> batch_t
        silver_batch_tasks.append(batch_t)

    # =========================================================================
    # BIGQUERY BRIDGE & DBT (GOLD) & ML
    # =========================================================================
    
    load_to_bq = BashOperator(
        task_id='load_silver_to_bigquery',
        bash_command='python /opt/airflow/src/load_to_bigquery.py',
    )

    # smf/system_direction lookback correction (2026-08-18, see
    # smf_lookback_silver_fix_callable docstring) — independent of the
    # Bronze/Silver Dataproc chain (writes directly to Silver's GCS Hive
    # partition via src/silver_lookback_fix.py), only needs to complete
    # before dbt build reads Silver this hour.
    smf_lookback_fix = PythonOperator(
        task_id='smf_lookback_silver_fix',
        python_callable=smf_lookback_silver_fix_callable,
    )

    # ADR-0004 (2026-07-27): dbt run never ran dbt test in production -- the
    # 2026-07-25/26 Silver dedup investigation found ~12 tables silently
    # duplicated for months because schema.yml's tests were never actually
    # executed. A separate run_dbt_tests task closed the *detection* gap
    # (failures become visible within the hour) but not the *prevention* gap
    # -- dbt run had already written the (possibly bad) data before dbt test
    # got a chance to catch it. `dbt build` interleaves each model with its
    # own tests in dependency order; `--fail-fast` aborts the rest of the run
    # on the first real failure, so a bad stg_pricing row (say) now stops
    # mart_ml_features from building on top of it that
    # hour, instead of the bad data quietly propagating downstream. The 2
    # known pre-existing, non-duplication test failures (assert_no_hourly_gaps,
    # mart_gop_volume_analysis's ptf_try not_null -- both reflect the old
    # Faz-0 laptop-uptime gap) are set to severity=warn in schema.yml /
    # assert_no_hourly_gaps.sql specifically so they don't trip --fail-fast.
    run_dbt = BashOperator(
        task_id='run_dbt_gold_models',
        bash_command=(
            'cd /opt/airflow/epias_dbt && dbt build --fail-fast --profiles-dir . --target prod '
            '--exclude ' + ' '.join(DBT_EXCLUDE_PENDING_BACKFILL)
        ),
    )

    # Zinciri Bağlama
    for batch_t in silver_batch_tasks:
        batch_t >> load_to_bq
    load_to_bq >> run_dbt
    smf_lookback_fix >> run_dbt