"""
epias_ptf_training_weekly.py — Weekly XGBoost PTF Model Retraining
====================================================================
Split out of epias_medallion_pipeline_v3 (see ADR-0002 action item 9 /
plans/03-dataproc-batch-consolidation.md validation notes) when that DAG's
schedule moved from daily to hourly for fresher intraday data. ptf_trainer.py
itself is documented as a weekly-cadence, 10-30 minute full retrain — running
it on every hourly medallion-pipeline run would execute it 24x/day, which
contradicts its own stated cadence and wastes compute for no benefit (the
model doesn't need retraining every hour, only inference does).

Reads whatever is currently in epias_gold — no direct task dependency on
epias_medallion_pipeline_v3 is wired here; the weekly cadence gives ample
margin for the hourly pipeline to have refreshed the gold tables already.
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
    dag_id="epias_ptf_training_weekly",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 3 * * 1",  # Monday 03:00 UTC — off-peak, ahead of the hourly pipeline's data needs
    catchup=False,
    max_active_runs=1,
    tags=["epias", "ml", "training"],
) as dag:

    train_ptf_model = BashOperator(
        task_id="train_ptf_model",
        bash_command="python /opt/airflow/src/ptf_trainer.py",
    )
