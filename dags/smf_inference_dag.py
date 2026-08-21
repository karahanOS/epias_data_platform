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

default_args = {
    "owner":           "epias_team",
    "retries":         2,
    "retry_delay":     timedelta(minutes=2),
}

with DAG(
    dag_id="smf_hourly_inference",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",   # Every hour on the hour
    catchup=False,
    max_active_runs=1,
    tags=["epias", "ml", "inference", "hourly"],
) as dag:

    run_inference = BashOperator(
        task_id="run_smf_inference",
        bash_command="python /opt/airflow/src/smf_inference.py",
    )

    run_trading_signal = BashOperator(
        task_id="run_smf_trading_signal",
        bash_command="python /opt/airflow/src/smf_trading_signal.py",
    )

    run_inference >> run_trading_signal
