# Handoff Prompts — epias_data_platform

Generated: 2026-06-06

Copy any of these directly into `/make-plan` to start implementation.

---

## Prompt 1: U4 — Move timestamp parsing into BaseEpiasSparkJob

```
/make-plan

Target: Add `BaseEpiasSparkJob.parse_epias_timestamp()` static method to
`spark_jobs/spark_utils.py` and replace the duplicated `F.to_timestamp()`
calls across all bronze_to_silver_*.py jobs.

## Background
`F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")` is copy-pasted
identically into 18+ Spark job files (representative locations: 
bronze_to_silver_pricing.py:28, bronze_to_silver_consumption.py:22, 
bronze_to_silver_generation.py:35, bronze_to_silver_smf.py:18, 
bronze_to_silver_order_up.py:32). See 02-duplication-report.md#D06.

## What to do
1. In `spark_jobs/spark_utils.py`, add a static method to `BaseEpiasSparkJob`:
   ```python
   @staticmethod
   def parse_epias_timestamp(col_name: str = "date"):
       return F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ssXXX")
   ```
2. In every `spark_jobs/bronze_to_silver_*.py` file, replace:
   `F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")`
   with:
   `self.parse_epias_timestamp()`
   (or `BaseEpiasSparkJob.parse_epias_timestamp()` for static call)

## Anti-pattern guards
- Do NOT change the timestamp format string — it matches EPIAS API ISO format exactly.
- Do NOT modify `bronze_to_silver_market.py` — it's legacy code that doesn't 
  extend BaseEpiasSparkJob (see spark_utils.py note).
- Do NOT add parameters to control format — one format for all EPIAS sources.

## Flowchart reference
See 01-flowcharts/F03-spark-etl.md for full Bronze→Silver flow.
```

---

## Prompt 2: U5 — Centralize source config and dbt exclude list

```
/make-plan

Target: Create `dags/epias_sources.py` containing a single EPIAS_SOURCES dict
and DBT_EXCLUDE_PENDING_BACKFILL list. Update `dags/epias_dag.py` and 
`dags/epias_backfill_dag.py` to import from it instead of maintaining 
independent inline dicts.

## Background
EPIAS source lists in the two DAGs have diverged:
- epias_dag.py:154-180 includes "injection" and "uevcb_list"; excludes "sbfgp"
- epias_backfill_dag.py:51-71 excludes "injection" and "uevcb_list"; includes "sbfgp"
The dbt exclude list is copy-pasted:
- epias_dag.py:258-261
- epias_backfill_dag.py:310-313
See 02-duplication-report.md#D03 and #D04.

## What to do
1. Create `dags/epias_sources.py` with:
   - `EPIAS_SOURCES` dict: each entry has (method_name, gcs_path, allow_empty,
     backfill_eligible, is_static). Populate from the union of both existing
     source dicts. Mark "injection" as backfill_eligible=False, "participants"
     as is_static=True, etc.
   - `DBT_EXCLUDE_PENDING_BACKFILL` list: `["stg_dpp", "stg_sbfgp", 
     "stg_res_forecast", "mart_production_plan"]`

2. In `dags/epias_dag.py`:
   - Replace HOURLY_SOURCES/STATIC_SOURCES inline dicts with:
     `ALL_SOURCES = {k: v for k, v in EPIAS_SOURCES.items() if not v[4]}`
   - Replace dbt exclude string at line 258-261 with:
     `" ".join(DBT_EXCLUDE_PENDING_BACKFILL)`

3. In `dags/epias_backfill_dag.py`:
   - Replace BACKFILL_SOURCES dict with:
     `BACKFILL_SOURCES = {k: v for k, v in EPIAS_SOURCES.items() if v[3]}`
   - Replace dbt exclude string at lines 310-313 same way.

## Anti-pattern guards
- Do NOT change the logic of either DAG — only the source of the data 
  (inline → imported).
- Do NOT try to merge the two DAGs into one — they serve different schedules 
  and execution contexts.
- Do NOT remove the backfill_eligible=False entries from EPIAS_SOURCES — 
  they still run daily and need to exist in the dict.
- Verify that the final set of sources in each DAG is identical to current 
  behavior after the change.

## Flowchart reference
See 01-flowcharts/F10-daily-dag.md and 01-flowcharts/F12-backfill-dag.md.
```

---

## Prompt 3: U2 — Extract shared PTF feature engineering

```
/make-plan

Target: Create `src/ptf_features.py` with a single `build_ptf_features(df)`
function. Update `src/ptf_trainer.py` and `src/ptf_inference.py` to use it,
eliminating the duplicated feature construction.

## Background
ptf_trainer.py:63-81 and ptf_inference.py:77-85 independently implement the
same lag/rolling/temporal features. If a feature is changed in one place
(e.g., ptf_lag_24h shift window), inference silently diverges from training.
The forward-fill logic for outage columns (ptf_trainer.py:77-79) is also 
missing from ptf_inference.py, which is a latent bug.
See 02-duplication-report.md#D02.

## What to do
1. Create `src/ptf_features.py`:
   ```python
   def build_ptf_features(df: pd.DataFrame) -> pd.DataFrame:
       df = df.copy()
       df["hour"] = df.index.hour
       df["day_of_week"] = df.index.dayofweek
       df["month"] = df.index.month
       df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
       df["ptf_lag_24h"] = df["ptf_try"].shift(24)
       df["ptf_lag_168h"] = df["ptf_try"].shift(168)
       for col in ["supply_shock_index", "total_outage_mwh", "total_available_capacity_mwh"]:
           if col in df.columns:
               df[col] = df[col].ffill().fillna(0)
       df["supply_shock_trend_7d"] = df["supply_shock_index"].rolling(168).mean()
       return df
   ```

2. In `src/ptf_trainer.py`, replace lines 63-81:
   - `from ptf_features import build_ptf_features`
   - Call `df = build_ptf_features(df)` where the individual feature lines were.
   - Keep the existing `dropna()` call immediately after.

3. In `src/ptf_inference.py`, replace lines 77-85:
   - `from ptf_features import build_ptf_features`
   - Call `df = build_ptf_features(df)` in `build_inference_features()`.
   - Keep the existing `dropna()` and `iloc[[-1]]` selection after.

## Anti-pattern guards
- Do NOT change the feature names or window sizes — the trained model artifact 
  on GCS expects exactly these feature names and the inference must match.
- Do NOT remove the `dropna()` in either caller — the shared function 
  intentionally returns NaN rows (lag warmup); callers decide when to drop.
- Do NOT add cyclic encoding or F08-style features here — this module is 
  ONLY the shared F06/F07 feature set.

## Flowchart reference
See 01-flowcharts/F06-ptf-trainer.md and 01-flowcharts/F07-ptf-inference.md.
```

---

## Prompt 4: U3 — Shared config and BQ client factory

```
/make-plan

Target: Create `src/config.py` with shared env-var constants and a
`get_bq_client()` factory. Update all 4 remaining src/*.py files to import
from it instead of reading env vars independently.

## Background
Five files read GCP_PROJECT_ID, BQ_GOLD_DATASET, GCS_BUCKET independently:
- src/ptf_trainer.py:20-23
- src/ptf_inference.py:21-23
- src/load_to_bigquery.py:22
- src/data_quality_check.py:33-34
(ptf_forecaster.py will be deleted by U1)
See 02-duplication-report.md#D05.

## What to do
1. Create `src/config.py`:
   ```python
   import os
   from google.cloud import bigquery

   GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "epias-data-platform")
   BQ_GOLD_DATASET = os.getenv("BQ_GOLD_DATASET", "epias_gold")
   GCS_BUCKET = os.getenv("GCS_BUCKET", "epias-data-lake")

   def get_bq_client() -> bigquery.Client:
       return bigquery.Client(project=GCP_PROJECT_ID)
   ```

2. In each of the 4 files:
   - Replace local os.getenv() calls for these three vars with imports.
   - Replace `bigquery.Client(project=...)` with `get_bq_client()`.
   - Keep EPIAS-specific env vars (EPIAS_USERNAME, EPIAS_PASSWORD) in 
     epias_client.py — those are auth concerns, not GCP concerns.

## Anti-pattern guards
- Do NOT make get_bq_client() a singleton with module-level state — just a 
  simple factory; callers may need independent clients for concurrent queries.
- Do NOT move EPIAS_USERNAME/EPIAS_PASSWORD here — epias_client.py owns auth 
  for its own API.
- Do NOT rename the constants — use the same names as in the existing 
  os.getenv() calls to minimize diff.

## Flowchart reference
No single flowchart — this is a horizontal concern across F04, F06, F07, F09.
```

---

## Prompt 5: U1 — Retire ptf_forecaster.py (dead code + parallel pipeline)

```
/make-plan

Target: Delete `src/ptf_forecaster.py`. Optionally backport walk-forward CV
from it into `src/ptf_trainer.py` if the modeling team wants it.

## Background
ptf_forecaster.py:197-203 (run() entrypoint) is never called from any Airflow 
DAG. The model it produces (models/ptf_advanced_xgb_model.joblib, local disk)
is never read by any other file. It is a dead training pipeline running in 
parallel with the production pipeline in ptf_trainer.py.

Key differences that would be lost on deletion:
- Walk-forward 3-fold CV (ptf_forecaster.py:138-166) — better than F06's 
  single holdout at ptf_trainer.py:108-112
- Cyclic sin/cos encoding (ptf_forecaster.py:100-105) — different from F06's
  integer encoding; NOT compatible with existing production model artifact
- 48h lag feature (ptf_forecaster.py:80) — additional lag not in F06/F07

See 02-duplication-report.md#D01.
See 01-flowcharts/F06-ptf-trainer.md, 01-flowcharts/F08-ptf-forecaster.md.

## What to do — Option A (recommended: delete only)
1. Confirm `src/ptf_forecaster.py` is not imported anywhere:
   `grep -r "ptf_forecaster" .` — should return zero matches outside the file itself.
2. Confirm `models/ptf_advanced_xgb_model.joblib` is not referenced:
   `grep -r "ptf_advanced_xgb_model" .` — should return zero matches.
3. Delete `src/ptf_forecaster.py`.
4. If a `models/` directory exists with local artifacts, document that they
   are safe to delete too.

## What to do — Option B (delete + backport walk-forward CV)
After step 3 above:
5. In `src/ptf_trainer.py`, replace the single train/test split (lines 108-112)
   with walk-forward 3-fold CV. Use the same fold logic from 
   ptf_forecaster.py:138-166 but applied to F06's feature set.
   Keep the final model serialization to GCS (ptf_trainer.py:184-185) unchanged.
   Keep integer encoding for hour/dow/month (NOT cyclic) to preserve 
   compatibility with the existing production model artifact.

## Anti-pattern guards
- Do NOT switch to cyclic encoding in ptf_trainer.py unless you retrain from 
  scratch and update ptf_inference.py feature construction simultaneously 
  (feature names would change).
- Do NOT add the 48h lag to ptf_trainer.py unless ptf_inference.py is updated 
  to build it too — train/serve feature mismatch is a silent failure.
- After deletion, run the existing Airflow DAG dry-run to confirm no tasks 
  reference ptf_forecaster.py.
```

---

## Prompt 6: U6 — SparkSubmit factory function (optional cleanup)

```
/make-plan

Target: Add `make_silver_task(dag, source_key, is_backfill=False)` to
`dags/epias_sources.py` and replace the near-identical SparkSubmitOperator
loops in both DAGs.

## Background
dags/epias_dag.py:228-240 and dags/epias_backfill_dag.py:237-257 define 
SparkSubmitOperator instances in near-identical loops, differing only in 
application_args and task_id suffix.
See 02-duplication-report.md#D07.

## What to do
1. In `dags/epias_sources.py` (created by U5), add:
   ```python
   from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

   SPARK_CONN_ID = "spark_default"
   GCS_CONNECTOR_JAR = "/opt/spark/jars/gcs-connector.jar"
   SPARK_UTILS_PATH = "/opt/airflow/spark/spark_utils.py"

   def make_silver_task(dag, source_key: str, is_backfill: bool = False):
       suffix = "_backfill" if is_backfill else ""
       args = ["1970-01-01", "--backfill"] if is_backfill else ["{{ ds }}"]
       return SparkSubmitOperator(
           task_id=f"silver_{source_key}{suffix}",
           application=f"/opt/airflow/spark/bronze_to_silver_{source_key}.py",
           py_files=SPARK_UTILS_PATH,
           jars=GCS_CONNECTOR_JAR,
           conn_id=SPARK_CONN_ID,
           application_args=args,
           deploy_mode="client",
           name=f"epias_silver_{source_key}{suffix}",
           dag=dag,
       )
   ```

2. In `dags/epias_dag.py:228-240`: replace the loop body with 
   `silver_t = make_silver_task(dag, key)`

3. In `dags/epias_backfill_dag.py:237-257`: replace the loop body with
   `silver_t = make_silver_task(dag, source_key, is_backfill=True)`

## Anti-pattern guards
- Do NOT merge the two loops themselves — they chain dependencies differently 
  (daily: parallel; backfill: sequential chunks per source).
- Implement U5 first — this function belongs in epias_sources.py.
- Do NOT parameterize the Spark worker memory or executor config here — 
  those are cluster-level settings, not per-task.

## Flowchart reference
See 01-flowcharts/F10-daily-dag.md and 01-flowcharts/F12-backfill-dag.md.
## Prerequisite: U5 must be completed first.
```
