"""
smf_inference_dag.py — Hourly SMF Inference DAG
==================================================
Runs every hour. Loads pre-trained direction+price models from GCS, predicts
SMF (and system direction) for every newly-settled and genuinely-future hour,
writes results to BigQuery (gold_smf_predictions / gold_smf_forward_predictions).
Completely independent from the main medallion pipeline DAG — mirrors
ptf_inference_dag.py exactly.
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
