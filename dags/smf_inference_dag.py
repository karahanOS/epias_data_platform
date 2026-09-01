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
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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
default_args = {
    "owner":             "epias_team",
    "retries":           2,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
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

    run_inference >> run_trading_signal
