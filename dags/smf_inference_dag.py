"""
smf_inference_dag.py — Hourly SMF Inference DAG
==================================================
Runs every hour. Loads pre-trained direction+price models from GCS, predicts
SMF (and system direction) for every newly-settled and genuinely-future hour,
writes results to BigQuery (gold_smf_predictions / gold_smf_forward_predictions).
Completely independent from the main medallion pipeline DAG — mirrors
ptf_inference_dag.py exactly.

2026-08-21: added run_smf_trading_signal (ADR-08's smf_trading_signal.py),
chained after run_smf_inference. Found completely un-scheduled — a
manual-only script (`python src/smf_trading_signal.py`), apparently run
once when ADR-08 was built and never since, so gold_smf_trading_backtest
had gone 4 days stale before anyone noticed via the dashboard. It only
reads BigQuery (gold_smf_predictions/stg_smf/stg_pricing/
mart_gip_hourly_reference, no GCS/Dataproc dependency) and WRITE_TRUNCATEs
the whole backtest table every run (~9s observed) — cheap enough to run
every hour right after the predictions it depends on are refreshed,
rather than on some coarser, easier-to-forget-about schedule.
"""
import logging
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

logger = logging.getLogger(__name__)

# 2026-09-01: a stalled/hung inference run isn't the only way this pipeline
# goes stale silently -- a task can also complete "successfully" while
# genuinely producing nothing new (e.g. an upstream EPIAS outage). execution_
# timeout/dagrun_timeout (below) catch the hang case; this data-level check
# catches the other one, independent of task status. Threshold is 3x the
# normal ~1h write cadence -- wide enough that one missed/retried run doesn't
# false-positive, tight enough to alert within ~3h instead of the ~8h it took
# a human to notice the 2026-08-30/09-01 outage via the dashboard.
FRESHNESS_THRESHOLD_MINUTES = 180


def check_smf_freshness_callable(**context) -> None:
    """Raises (-> task fails -> email_on_failure fires) if gold_smf_forward_snapshot
    hasn't been written to recently. Deliberately reads the table directly
    rather than trusting upstream task state -- the whole point is to catch
    staleness that isn't a task failure/hang."""
    from config import get_bq_client, GCP_PROJECT_ID as PROJECT, BQ_GOLD_DATASET as GOLD

    client = get_bq_client()
    df = client.query(f"""
        SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(snapshotted_at), MINUTE) AS lag_minutes
        FROM `{PROJECT}.{GOLD}.gold_smf_forward_snapshot`
    """).to_dataframe()
    lag_minutes = df["lag_minutes"].iloc[0]
    logger.info(f"gold_smf_forward_snapshot freshness: last write {lag_minutes} min ago "
                f"(threshold {FRESHNESS_THRESHOLD_MINUTES} min)")

    if lag_minutes is None or lag_minutes > FRESHNESS_THRESHOLD_MINUTES:
        raise RuntimeError(
            f"🚨 gold_smf_forward_snapshot is stale: last write {lag_minutes} min ago "
            f"(threshold {FRESHNESS_THRESHOLD_MINUTES} min). smf_hourly_inference may be "
            f"stuck, failing silently, or blocked upstream (check EPIAS SMF publication, "
            f"GCS billing status, Airflow scheduler health)."
        )

# 2026-09-01: added execution_timeout/dagrun_timeout after a real ~8h outage
# — run_smf_trading_signal hung (process alive, ~0s CPU accumulated) right
# after a transient GCS billing-account suspension, blocking every scheduled
# run behind it via max_active_runs=1. retries alone never helped here: a
# hung task never actually fails, so retry logic never triggers. Without an
# execution_timeout Airflow has no way to know a task is stuck vs. just slow.
# Sizing: run_smf_inference normally completes in well under 20 min (model
# load + BigQuery + prediction for the day's remaining hours); run_smf_trading_signal
# is ~9s observed (see its own docstring above) — 10 min is a generous margin,
# not a realistic expected runtime. dagrun_timeout is the second line of
# defense: even if a future task's own timeout is missed or misconfigured,
# the whole DagRun self-fails after 1h (this DAG is hourly-scheduled, so
# anything still running into the next scheduled slot is already broken) and
# frees the max_active_runs=1 slot for the next run instead of blocking it
# indefinitely.
# email_on_failure was missing here entirely before 2026-09-01 -- unlike
# epias_dag.py (which has had SMTP alerting since ADR-0004), a task failure
# in this DAG never actually notified anyone. Reuses the same SMTP config
# (docker-compose.yml's AIRFLOW__SMTP__*) epias_dag.py already relies on.
default_args = {
    "owner":             "epias_team",
    "retries":           2,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
    "email_on_failure":  True,
    "email":             ["mehmetkarahanc@gmail.com"],
}

with DAG(
    dag_id="smf_hourly_inference",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",   # Every hour on the hour
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["epias", "ml", "inference", "hourly"],
) as dag:

    run_inference = BashOperator(
        task_id="run_smf_inference",
        bash_command="python /opt/airflow/src/smf_inference.py",
    )

    run_trading_signal = BashOperator(
        task_id="run_smf_trading_signal",
        bash_command="python /opt/airflow/src/smf_trading_signal.py",
        execution_timeout=timedelta(minutes=10),
    )

    check_freshness = PythonOperator(
        task_id="check_smf_freshness",
        python_callable=check_smf_freshness_callable,
        execution_timeout=timedelta(minutes=5),
    )

    run_inference >> run_trading_signal >> check_freshness
