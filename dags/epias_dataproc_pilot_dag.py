"""
epias_dataproc_pilot_dag.py — Dataproc Serverless Pilot (dams source)
=======================================================================
One-off, manually-triggered DAG to validate running an existing PySpark
Silver job (bronze_to_silver_dams.py) via Dataproc Serverless for Spark,
instead of the always-on docker-compose spark-master/spark-worker
containers that currently run 24/7 regardless of load.

See plans/02-spark-airflow-hosting-migration.md (ADR-0002) for the full
migration rationale and plan. This DAG covers action items 2-5: pick a
low-risk pilot job, submit it as a Dataproc Batch, and compare its output
against the existing Spark-on-docker-compose output for the same date.

The job script itself (bronze_to_silver_dams.py) is UNCHANGED — only how
it gets submitted differs. It still reads/writes plain gs:// paths;
Dataproc Serverless's runtime images ship with the GCS connector built
in, so the manual gcs-connector.jar mount used by the current
docker-compose Spark setup is not needed here.

──────────────────────────────────────────────────────────────────────
Prerequisites — NOT done by this file, require gcloud/Console access
this session did not have (see ADR-0002 action item 1):
──────────────────────────────────────────────────────────────────────
  1. Enable the Dataproc API on the project:
       gcloud services enable dataproc.googleapis.com --project=epias-data-platform

  2. Create a dedicated Dataproc service account (least privilege —
     do NOT reuse a broad Editor/Owner account):
       gcloud iam service-accounts create dataproc-pilot \
         --project=epias-data-platform \
         --display-name="Dataproc Serverless pilot runner"
       gcloud projects add-iam-policy-binding epias-data-platform \
         --member="serviceAccount:dataproc-pilot@epias-data-platform.iam.gserviceaccount.com" \
         --role="roles/dataproc.worker"
       gsutil iam ch \
         serviceAccount:dataproc-pilot@epias-data-platform.iam.gserviceaccount.com:roles/storage.objectAdmin \
         gs://epias-data-lake

  3. Upload this job's dependencies to GCS (Dataproc Batches read the
     PySpark entrypoint and its dependencies from GCS, not a local mount):
       gsutil cp spark_jobs/bronze_to_silver_dams.py gs://epias-data-lake/dataproc/pilot/
       gsutil cp spark_jobs/spark_utils.py           gs://epias-data-lake/dataproc/pilot/

  4. Add an Airflow connection named 'google_cloud_default' (Admin ->
     Connections in the Airflow UI) using the dataproc-pilot service
     account's key, or set GOOGLE_APPLICATION_CREDENTIALS in the
     Airflow container environment to that key's path.

──────────────────────────────────────────────────────────────────────
Usage
──────────────────────────────────────────────────────────────────────
Trigger manually from the Airflow UI ("Trigger DAG w/ config") passing a
date already processed by the current Spark path, e.g.:
    {"ds": "2026-05-29"}
so the resulting gs://epias-data-lake/silver/dams/year=2026/month=05/day=29/
output can be diffed against what the existing docker-compose Spark job
already wrote for that date, to confirm parity before touching any
production DAG.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# ── AYARLAR ───────────────────────────────────────────────────────────────────
GCP_PROJECT_ID = "epias-data-platform"
REGION         = "europe-west1"   # matches the epias-data-lake bucket's region
BUCKET_NAME    = "epias-data-lake"
PILOT_PREFIX   = f"gs://{BUCKET_NAME}/dataproc/pilot"

# Dataproc batch IDs must be unique and DNS-safe; ts_nodash is unique per run.
BATCH_ID_TEMPLATE = "epias-pilot-dams-{{ ts_nodash | lower }}"

BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": f"{PILOT_PREFIX}/bronze_to_silver_dams.py",
        "python_file_uris": [f"{PILOT_PREFIX}/spark_utils.py"],
        # bronze_to_silver_dams.py takes one positional arg: the run date (ds).
        # Pass a date via {"ds": "..."} in "Trigger DAG w/ config" to target a
        # specific historical date for comparison against the existing output.
        "args": ["{{ dag_run.conf.get('ds', ds) }}"],
    },
    "runtime_config": {
        # Verify the latest available Dataproc Serverless runtime version at
        # submission time (gcloud dataproc batches list-runtime-versions) —
        # this is a placeholder pending actual GCP access.
        "version": "2.2",
    },
    "environment_config": {
        "execution_config": {
            # Run the batch as our dedicated least-privilege service account
            # instead of defaulting to the project's default Compute Engine
            # service account (which epias-dataproc has no rights to
            # impersonate, and which is broader than this pilot needs).
            "service_account": f"epias-dataproc@{GCP_PROJECT_ID}.iam.gserviceaccount.com",
        },
    },
}

with DAG(
    dag_id="epias_dataproc_pilot",
    description="One-off pilot: run bronze_to_silver_dams via Dataproc Serverless instead of docker-compose Spark",
    schedule_interval=None,        # manual trigger only — this is a pilot, not a scheduled job
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["epias", "pilot", "dataproc", "adr-0002"],
) as dag:

    submit_dams_pilot = DataprocCreateBatchOperator(
        task_id="submit_dams_pilot_batch",
        project_id=GCP_PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID_TEMPLATE,
    )
