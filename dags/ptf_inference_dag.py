"""
ptf_inference_dag.py — Hourly PTF Inference DAG
================================================
Runs every hour. Loads pre-trained model from GCS, predicts next hour PTF,
writes result to BigQuery gold_ptf_predictions table.
Completely independent from the main medallion pipeline DAG.
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
    dag_id="ptf_hourly_inference",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",   # Every hour on the hour
    catchup=False,
    max_active_runs=1,
    tags=["epias", "ml", "inference", "hourly"],
) as dag:

    run_inference = BashOperator(
        task_id="run_ptf_inference",
        bash_command="python /opt/airflow/src/ptf_inference.py",
    )
