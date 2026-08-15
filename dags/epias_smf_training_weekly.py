"""
epias_smf_training_weekly.py — Weekly 2-Stage XGBoost SMF Model Retraining
=============================================================================
Mirrors epias_ptf_training_weekly.py's structure and rationale exactly — see
that file's docstring. smf_trainer.py is the same weekly-cadence, ~10-30+
minute full retrain (now trains two models — direction classifier and price
regressor — so allow more headroom than PTF's single-model job).

Offset one hour after epias_ptf_training_weekly (03:00 UTC) to avoid resource
contention between the two weekly training jobs.

Reads whatever is currently in epias_gold (mart_smf_lag_features) — no direct
task dependency on epias_medallion_pipeline_v3 is wired here, same as PTF's
training DAG.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "epias_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="epias_smf_training_weekly",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 4 * * 1",  # Monday 04:00 UTC — 1h after PTF's training run
    catchup=False,
    max_active_runs=1,
    tags=["epias", "ml", "training"],
) as dag:

    train_smf_model = BashOperator(
        task_id="train_smf_model",
        bash_command="python /opt/airflow/src/smf_trainer.py",
    )
