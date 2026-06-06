# Plan 01 — Unify Duplicated Concerns
## epias_data_platform

**Created:** 2026-06-06  
**Source:** `PATHFINDER-2026-06-06/04-handoff-prompts.md`  
**Duplication evidence:** `PATHFINDER-2026-06-06/02-duplication-report.md`

---

## Context

The Pathfinder analysis identified 7 duplicated concerns across the codebase. This plan addresses 6 of them in the safest execution order — each phase is self-contained and verified before proceeding to the next.

**Execution order (lowest risk first):**

| Phase | Unit | Files touched | Risk |
|---|---|---|---|
| 1 | U4 — Timestamp base method | `spark_utils.py` + 25 Spark jobs | Very low |
| 2 | U5 — Source config + dbt exclude list | `dags/epias_sources.py` (new) + 2 DAGs | Low |
| 3 | U6 — SparkSubmit factory | `dags/epias_sources.py` + 2 DAGs | Low — prereq: Phase 2 |
| 4 | U3 — Shared `config.py` + BQ factory | `src/config.py` (new) + 4 `src/*.py` | Low |
| 5 | U2 — Shared PTF feature engineering | `src/ptf_features.py` (new) + 2 `src/*.py` | Medium — affects inference |
| 6 | U1 — Retire `ptf_forecaster.py` | `src/ptf_forecaster.py` (delete) | Low |

---

## Phase 0: Pre-flight Checks (run before anything else)

These greps confirm the discovery findings hold. Run in the repo root before starting Phase 1.

```bash
# Confirm ptf_forecaster.py is not imported anywhere (should return 0 matches outside the file)
grep -rn "ptf_forecaster\|PredictivePTFForecaster" C:\epias_data_platform\src C:\epias_data_platform\dags

# Confirm the abandoned model path is not referenced
grep -rn "ptf_advanced_xgb_model" C:\epias_data_platform\src C:\epias_data_platform\dags

# Count timestamp call sites (expect 25+ matches across bronze_to_silver_*.py)
grep -rn "yyyy-MM-dd'T'HH:mm:ssXXX" C:\epias_data_platform\spark_jobs

# Confirm dbt exclude strings are identical
grep -rn "stg_dpp stg_sbfgp" C:\epias_data_platform\dags
```

**Expected results:**
- No imports of `ptf_forecaster` outside the file itself
- No references to `ptf_advanced_xgb_model` outside `ptf_forecaster.py`
- 25+ hits on the timestamp format string, all in `spark_jobs/`
- Exactly 2 hits on the dbt exclude string (one per DAG)

If any check fails, stop and investigate before proceeding.

---

## Phase 1: U4 — Add `parse_epias_timestamp()` to `BaseEpiasSparkJob`

**Goal:** One method owns the EPIAS ISO timestamp format string. All 25+ jobs call it.

### Step 1.1 — Add static method to `BaseEpiasSparkJob`

**File:** `spark_jobs/spark_utils.py`

**Where to insert:** Immediately before `add_partition_columns()` (which is at approximately line 118). The new method should live in the `BaseEpiasSparkJob` class body.

**What to add:**
```python
@staticmethod
def parse_epias_timestamp(col_name: str = "date"):
    """Parse EPIAS ISO-8601 timestamp column to Spark TimestampType."""
    return F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ssXXX")
```

**Do NOT touch:** `spark_utils.py:130` — that line is `ts = F.to_timestamp(F.col(date_col))` with **no format string**, used in the backfill `add_partition_columns()` path. It is a different call and must stay as-is.

### Step 1.2 — Replace call sites in each job file

For each file below, find the exact line shown, and replace the full `F.to_timestamp(...)` expression with `self.parse_epias_timestamp()`.

| File | Line | Current expression |
|---|---|---|
| `spark_jobs/bronze_to_silver_pricing.py` | 28 | `F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")` |
| `spark_jobs/bronze_to_silver_generation.py` | 35 | `F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")` |
| `spark_jobs/bronze_to_silver_smf.py` | 18 | `F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")` |
| `spark_jobs/bronze_to_silver_consumption.py` | 23 | `F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")` |
| `spark_jobs/bronze_to_silver_order_up.py` | 32 | `F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")` |
| `spark_jobs/bronze_to_silver_order_down.py` | ~32 | same pattern |
| (all remaining `bronze_to_silver_*.py` files) | varies | same pattern |

**Exception — `bronze_to_silver_weather.py`:** Has **two calls** with potentially different column names or formats. Read it first before replacing. Apply `parse_epias_timestamp(col_name=...)` with the appropriate column name if different from `"date"`.

**Exception — `bronze_to_silver_outages.py`:** Has **four calls**. Read the file to identify which columns each call targets. Use `parse_epias_timestamp(col_name=...)` for each.

**Exception — `bronze_to_silver_market.py`:** **Do NOT touch.** This file does not extend `BaseEpiasSparkJob` and uses a legacy pattern. Leave it unchanged.

Full replacement pattern for all standard jobs:
```python
# Before:
df = df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))

# After:
df = df.withColumn("date", self.parse_epias_timestamp())
```

### Step 1.3 — Verification

```bash
# Must return 0 matches (all replaced)
grep -rn "yyyy-MM-dd'T'HH:mm:ssXXX" C:\epias_data_platform\spark_jobs

# Must return exactly 1 match (the new method definition in spark_utils.py)
grep -rn "parse_epias_timestamp" C:\epias_data_platform\spark_jobs
```

**Anti-pattern guards:**
- Do NOT change the format string `"yyyy-MM-dd'T'HH:mm:ssXXX"` — it matches EPIAS API exactly.
- Do NOT replace the unformatted call at `spark_utils.py:130`.
- Do NOT touch `bronze_to_silver_market.py`.

---

## Phase 2: U5 — Centralize Source Config and dbt Exclude List

**Goal:** One file owns all EPIAS source definitions and the dbt model exclusion list. Both DAGs import from it.

### Step 2.1 — Read exact current source dicts

Before writing the new file, read both DAGs to capture the exact current source definitions:
- `dags/epias_dag.py:154-180` — `HOURLY_SOURCES` and `STATIC_SOURCES`
- `dags/epias_backfill_dag.py:51-71` — `BACKFILL_SOURCES`

### Step 2.2 — Create `dags/epias_sources.py`

Create a new file `dags/epias_sources.py`. The `EPIAS_SOURCES` dict is the **union** of both DAG source dicts. Each entry has 5 fields as a named tuple or plain tuple: `(method_name, gcs_path, allow_empty, backfill_eligible, is_static)`.

Template structure (populate exact values from the DAG files read in Step 2.1):

```python
# dags/epias_sources.py
"""
Single source of truth for EPIAS data source configuration.
Imported by epias_dag.py and epias_backfill_dag.py.

Tuple fields: (method_name, gcs_path, allow_empty, backfill_eligible, is_static)
  - backfill_eligible: include in epias_backfill_dag.py
  - is_static: reference data with no date dimension (e.g. participants)
"""

EPIAS_SOURCES = {
    # --- backfill_eligible=True sources (run in both daily + backfill DAGs) ---
    "pricing":        ("<method>", "<path>", False, True,  False),
    "smf":            ("<method>", "<path>", False, True,  False),
    "consumption":    ("<method>", "<path>", False, True,  False),
    "supply_demand":  ("<method>", "<path>", False, True,  False),
    "dam_clearing":   ("<method>", "<path>", False, True,  False),
    "price_ind_bid":  ("<method>", "<path>", False, True,  False),
    "idm_transactions": ("<method>", "<path>", False, True, False),
    "order_up":       ("<method>", "<path>", False, True,  False),
    "order_down":     ("<method>", "<path>", False, True,  False),
    "system_direction": ("<method>", "<path>", False, True, False),
    "dpp":            ("<method>", "<path>", False, True,  False),
    "sbfgp":          ("<method>", "<path>", False, True,  False),  # backfill only
    "aic":            ("<method>", "<path>", False, True,  False),
    "imbalance":      ("<method>", "<path>", False, True,  False),
    "res_forecast":   ("<method>", "<path>", False, True,  False),
    "generation":     ("<method>", "<path>", False, True,  False),
    "load_estimation": ("<method>", "<path>", False, True, False),
    "outages":        ("<method>", "<path>", False, True,  False),
    "dams":           ("<method>", "<path>", False, True,  False),
    # --- daily-only sources (backfill_eligible=False) ---
    "injection":      ("<method>", "<path>", True,  False, False),
    "uevcb_list":     ("<method>", "<path>", True,  False, False),
    # --- static / dimension sources ---
    "participants":   ("<method>", "<path>", False, False, True),
}

# Models excluded from dbt runs while their Silver backfill is incomplete.
# Remove a model from this list once its backfill is complete and dbt builds cleanly.
DBT_EXCLUDE_PENDING_BACKFILL = [
    "stg_dpp",
    "stg_sbfgp",
    "stg_res_forecast",
    "mart_production_plan",
]
```

Fill in `<method>` and `<path>` values by copying them verbatim from `dags/epias_dag.py:154-180` and `dags/epias_backfill_dag.py:51-71`.

### Step 2.3 — Update `dags/epias_dag.py`

**Replace lines 154-180** (the `HOURLY_SOURCES` / `STATIC_SOURCES` / `ALL_SOURCES` definitions) with:

```python
from epias_sources import EPIAS_SOURCES, DBT_EXCLUDE_PENDING_BACKFILL

ALL_SOURCES = {k: v for k, v in EPIAS_SOURCES.items() if not v[4]}  # exclude is_static
```

**Replace the dbt exclude string** at lines 258-261. The bash command currently has `--exclude stg_dpp stg_sbfgp stg_res_forecast mart_production_plan` hardcoded. Change it to:

```python
bash_command=(
    f'cd /opt/airflow/epias_dbt && dbt run --profiles-dir . '
    f'--exclude {" ".join(DBT_EXCLUDE_PENDING_BACKFILL)}'
),
```

### Step 2.4 — Update `dags/epias_backfill_dag.py`

**Replace lines 51-71** (the `BACKFILL_SOURCES` definition) with:

```python
from epias_sources import EPIAS_SOURCES, DBT_EXCLUDE_PENDING_BACKFILL

BACKFILL_SOURCES = {k: v for k, v in EPIAS_SOURCES.items() if v[3]}  # backfill_eligible
```

**Replace the dbt exclude string** at lines 310-313 with the same f-string pattern as Step 2.3.

### Step 2.5 — Verification

```bash
# The inline source dicts should be gone from both DAGs
grep -n "HOURLY_SOURCES\|STATIC_SOURCES\|BACKFILL_SOURCES\s*=" C:\epias_data_platform\dags\epias_dag.py C:\epias_data_platform\dags\epias_backfill_dag.py

# The hardcoded exclude string should be gone from both DAGs
grep -n "stg_dpp stg_sbfgp" C:\epias_data_platform\dags\epias_dag.py C:\epias_data_platform\dags\epias_backfill_dag.py

# epias_sources.py should exist and define both symbols
grep -n "EPIAS_SOURCES\|DBT_EXCLUDE_PENDING_BACKFILL" C:\epias_data_platform\dags\epias_sources.py

# Source counts must match original behavior
python -c "
import sys; sys.path.insert(0, 'dags')
from epias_sources import EPIAS_SOURCES
daily = {k: v for k, v in EPIAS_SOURCES.items() if not v[4]}
backfill = {k: v for k, v in EPIAS_SOURCES.items() if v[3]}
print('Daily sources:', len(daily), list(daily.keys()))
print('Backfill sources:', len(backfill), list(backfill.keys()))
"
```

Expected: daily count matches the original `len(ALL_SOURCES)` from `epias_dag.py`; backfill count matches original `len(BACKFILL_SOURCES)`.

**Anti-pattern guards:**
- Do NOT merge the two DAGs.
- Do NOT remove `backfill_eligible=False` entries from `EPIAS_SOURCES` — daily DAG still needs them.
- Do NOT change the logic of either DAG loop — only the source of the data.
- The `sbfgp` source must be `backfill_eligible=True` to preserve backfill DAG behavior.
- `injection` must be `backfill_eligible=False` (it was never in `BACKFILL_SOURCES`).

---

## Phase 3: U6 — `make_silver_task()` Factory Function

**Goal:** The near-identical `SparkSubmitOperator` loops in both DAGs call a shared factory instead of repeating boilerplate.

**Prerequisite:** Phase 2 complete (`dags/epias_sources.py` exists).

### Step 3.1 — Add factory function to `dags/epias_sources.py`

Append to the bottom of `dags/epias_sources.py` (after the `DBT_EXCLUDE_PENDING_BACKFILL` list):

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

_SPARK_CONN_ID = "spark_default"
_GCS_CONNECTOR_JAR = "/opt/spark/jars/gcs-connector.jar"
_SPARK_UTILS_PATH = "/opt/airflow/spark/spark_utils.py"


def make_silver_task(dag, source_key: str, is_backfill: bool = False) -> SparkSubmitOperator:
    """Create a SparkSubmitOperator for a bronze→silver job."""
    suffix = "_backfill" if is_backfill else ""
    args = ["1970-01-01", "--backfill"] if is_backfill else ["{{ ds }}"]
    return SparkSubmitOperator(
        task_id=f"silver_{source_key}{suffix}",
        application=f"/opt/airflow/spark/bronze_to_silver_{source_key}.py",
        py_files=_SPARK_UTILS_PATH,
        jars=_GCS_CONNECTOR_JAR,
        conn_id=_SPARK_CONN_ID,
        application_args=args,
        deploy_mode="client",
        name=f"epias_silver_{source_key}{suffix}",
        dag=dag,
    )
```

Verify the constant values (`spark_default`, `/opt/spark/jars/gcs-connector.jar`, `/opt/airflow/spark/spark_utils.py`) match what is hardcoded in `dags/epias_dag.py:228-240` and `dags/epias_backfill_dag.py:237-257` before adding them.

### Step 3.2 — Update `dags/epias_dag.py`

Add to the `from epias_sources import` line: `make_silver_task`

In the Spark task creation loop (`epias_dag.py:228-240`), replace the `SparkSubmitOperator(...)` call with:

```python
silver_t = make_silver_task(dag, key)
```

Keep the surrounding loop structure and dependency wiring (`save_t >> silver_t`) unchanged.

### Step 3.3 — Update `dags/epias_backfill_dag.py`

Add `make_silver_task` to the import line.

In the Spark task creation loop (`epias_backfill_dag.py:237-257`), replace the `SparkSubmitOperator(...)` call with:

```python
silver_t = make_silver_task(dag, source_key, is_backfill=True)
```

Keep the surrounding loop structure and dependency wiring unchanged.

### Step 3.4 — Verification

```bash
# SparkSubmitOperator constructor should no longer appear in either DAG file
grep -n "SparkSubmitOperator(" C:\epias_data_platform\dags\epias_dag.py C:\epias_data_platform\dags\epias_backfill_dag.py

# make_silver_task must be used in both DAGs
grep -n "make_silver_task" C:\epias_data_platform\dags\epias_dag.py C:\epias_data_platform\dags\epias_backfill_dag.py
```

**Anti-pattern guards:**
- Do NOT merge the two loop structures — they chain dependencies differently.
- Do NOT add Spark worker memory or executor config to `make_silver_task` — cluster-level settings stay in the Spark connection config.
- The weather silver task (hardcoded, not from a loop) may need separate treatment — check whether `epias_dag.py:215-224` also uses `SparkSubmitOperator` and apply the factory there too if it matches the pattern.

---

## Phase 4: U3 — Shared `src/config.py` and BQ Client Factory

**Goal:** One file owns the three GCP environment variables and the BigQuery client constructor. All `src/*.py` files import from it.

### Step 4.1 — Create `src/config.py`

```python
# src/config.py
import os
from google.cloud import bigquery

GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "epias-data-platform")
BQ_GOLD_DATASET: str = os.getenv("BQ_GOLD_DATASET", "epias_gold")
GCS_BUCKET: str = os.getenv("GCS_BUCKET", "epias-data-lake")


def get_bq_client() -> bigquery.Client:
    """Return a BigQuery client for the configured project."""
    return bigquery.Client(project=GCP_PROJECT_ID)
```

### Step 4.2 — Update `src/ptf_trainer.py`

**Remove lines 20-22** (the three `os.getenv()` calls for GCP vars). The `GCS_BUCKET` constant at line 22 is also used at lines 154 and 156 — update those references.

**Add at top** (after existing imports):
```python
from config import GCP_PROJECT_ID, BQ_GOLD_DATASET, GCS_BUCKET, get_bq_client
```

**Replace** `PROJECT_ID` with `GCP_PROJECT_ID` and `DATASET_ID` with `BQ_GOLD_DATASET` throughout the file (both are used in the BQ query string).

**Replace** the `bigquery.Client(project=PROJECT_ID)` call at line 33 with `get_bq_client()`.

Keep line 23 (`MODEL_GCS_PATH`) and line 24 (`IMPORTANCE_GCS_PATH`) — those are module-specific constants, not GCP config.

### Step 4.3 — Update `src/ptf_inference.py`

**Remove lines 21-23** (same three `os.getenv()` calls).

**Add import:** `from config import GCP_PROJECT_ID, BQ_GOLD_DATASET, GCS_BUCKET, get_bq_client`

**Update line 25** — `PREDICTIONS_TABLE` uses `PROJECT_ID` and `DATASET_ID` — update those references to `GCP_PROJECT_ID` and `BQ_GOLD_DATASET`.

**Replace** `bigquery.Client(project=PROJECT_ID)` at line 49 with `get_bq_client()`.

Keep `MODEL_GCS_PATH` (line 24) and `LOOKBACK_HOURS` (line 28) unchanged.

### Step 4.4 — Update `src/load_to_bigquery.py`

**Remove line 22** (`self.project_id = os.getenv("GCP_PROJECT_ID", ...)`).

**Add import:** `from config import GCP_PROJECT_ID, get_bq_client`

**Replace** `os.getenv("GCP_PROJECT_ID", "epias-data-platform")` with `GCP_PROJECT_ID`.

**Replace** `bigquery.Client(project=self.project_id)` at line 27 with `get_bq_client()`. The `self.project_id` instance variable can be removed if not used elsewhere in the class.

### Step 4.5 — Update `src/data_quality_check.py`

**Remove lines 33-34** (`PROJECT` and `GOLD` os.getenv calls).

**Add import:** `from config import GCP_PROJECT_ID, BQ_GOLD_DATASET, get_bq_client`

**Replace** all uses of `PROJECT` with `GCP_PROJECT_ID` and `GOLD` with `BQ_GOLD_DATASET` throughout the file.

**Replace** the `bigquery.Client(project=PROJECT)` call at line 85 with `get_bq_client()`.

### Step 4.6 — Verification

```bash
# No remaining inline os.getenv calls for GCP vars in src/ (except epias_client.py which is intentional)
grep -n "os.getenv.*GCP_PROJECT_ID\|os.getenv.*BQ_GOLD_DATASET\|os.getenv.*GCS_BUCKET" C:\epias_data_platform\src

# config.py must be importable
python -c "import sys; sys.path.insert(0, 'src'); from config import GCP_PROJECT_ID, BQ_GOLD_DATASET, GCS_BUCKET, get_bq_client; print('OK', GCP_PROJECT_ID)"

# All four files must import from config
grep -rn "from config import" C:\epias_data_platform\src
```

**Anti-pattern guards:**
- Do NOT make `get_bq_client()` a singleton — it must return a new client each call.
- Do NOT move `EPIAS_USERNAME`/`EPIAS_PASSWORD` from `epias_client.py` — auth for a different API, different module's concern.
- Do NOT rename the exported constants (`GCP_PROJECT_ID` etc.) — keep them identical to the `os.getenv()` key names to minimize diff.
- `data_quality_check.py` uses `PROJECT` and `GOLD` as local names for the constants — replace all occurrences, not just the definition lines.

---

## Phase 5: U2 — Extract Shared PTF Feature Engineering

**Goal:** `src/ptf_features.py` owns all lag/rolling/temporal feature construction. Both `ptf_trainer.py` and `ptf_inference.py` call it, eliminating train/serve skew risk.

**Why this is the most impactful change:** The forward-fill for outage columns (`ptf_trainer.py:77-79`) is currently **missing** from `ptf_inference.py`. After this phase, inference correctly applies the same preprocessing as training.

### Step 5.1 — Create `src/ptf_features.py`

```python
# src/ptf_features.py
import pandas as pd


def build_ptf_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build shared temporal, lag, and supply-shock features for PTF price prediction.

    The returned DataFrame may contain NaN rows in lag/rolling columns (lag warmup
    period). Callers are responsible for calling dropna() after this function.

    Input df must have:
    - A DatetimeIndex
    - Column 'ptf_try' (₺/MWh hourly price)
    - Optionally: 'supply_shock_index', 'total_outage_mwh', 'total_available_capacity_mwh'
    """
    df = df.copy()

    # Temporal features
    df["hour"] = df.index.hour
    df["day_of_week"] = df.index.dayofweek
    df["month"] = df.index.month
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

    # Lag features
    df["ptf_lag_24h"] = df["ptf_try"].shift(24)
    df["ptf_lag_168h"] = df["ptf_try"].shift(168)

    # Supply shock features — forward-fill outage data before rolling
    for col in ["supply_shock_index", "total_outage_mwh", "total_available_capacity_mwh"]:
        if col in df.columns:
            df[col] = df[col].ffill().fillna(0)

    if "supply_shock_index" in df.columns:
        df["supply_shock_trend_7d"] = df["supply_shock_index"].rolling(168).mean()

    return df
```

### Step 5.2 — Update `src/ptf_trainer.py`

**Add import** (after existing imports):
```python
from ptf_features import build_ptf_features
```

**In `engineer_features()` (lines 59-98):** Replace lines 63-81 (the feature construction block) with:
```python
df = build_ptf_features(df)
```

The `dropna()` call that follows (approximately line 96) must remain in place — do not move it into `ptf_features.py`.

### Step 5.3 — Update `src/ptf_inference.py`

**Add import:**
```python
from ptf_features import build_ptf_features
```

**In `build_inference_features()` (lines 75-92):** Replace lines 77-85 (the feature construction lines) with:
```python
df = build_ptf_features(df)
```

The `dropna()` call and the `iloc[[-1]]` row selection that follow must remain in place.

### Step 5.4 — Verification

```bash
# Feature construction lines should be gone from both files
grep -n "ptf_lag_24h\|ptf_lag_168h\|supply_shock_trend_7d\|day_of_week\|is_weekend" C:\epias_data_platform\src\ptf_trainer.py C:\epias_data_platform\src\ptf_inference.py

# Both files must import from ptf_features
grep -n "from ptf_features import" C:\epias_data_platform\src\ptf_trainer.py C:\epias_data_platform\src\ptf_inference.py

# ptf_features.py must be importable and have the right function
python -c "import sys; sys.path.insert(0,'src'); import pandas as pd; from ptf_features import build_ptf_features; print('OK')"
```

**End-to-end smoke test** (if the BQ/GCS environment is available):
```bash
python src/ptf_inference.py
# Should run without error and write one row to gold_ptf_predictions
```

**Anti-pattern guards:**
- Do NOT change the feature names or shift windows (`shift(24)`, `shift(168)`) — the GCS model artifact (`ptf_xgb_model.joblib`) was trained on these exact names and expects them at inference.
- Do NOT remove `dropna()` from either caller — `build_ptf_features()` intentionally returns NaN rows.
- Do NOT add F08-style features (cyclic encoding, 48h lag, GÖP imbalance) to `ptf_features.py` — this module serves only F06/F07.
- Do NOT call `build_ptf_features()` inside the function; call it before and pass the result in — the trainer's `engineer_features()` returns a modified df, not a new object.

---

## Phase 6: U1 — Retire `src/ptf_forecaster.py`

**Goal:** Remove the dead parallel training pipeline. The local-disk model it produces is never consumed.

### Step 6.1 — Final confirmation greps

```bash
# Must return 0 matches in DAG files or any src file other than ptf_forecaster.py itself
grep -rn "ptf_forecaster\|PredictivePTFForecaster" C:\epias_data_platform\src C:\epias_data_platform\dags

# Must return 0 matches outside ptf_forecaster.py
grep -rn "ptf_advanced_xgb_model" C:\epias_data_platform\src C:\epias_data_platform\dags
```

If either grep returns matches outside `ptf_forecaster.py` itself, stop and investigate.

### Step 6.2 — Delete the file

```bash
git rm C:\epias_data_platform\src\ptf_forecaster.py
```

### Step 6.3 — Check for orphaned local model artifacts

```bash
# Check if a local models/ directory has artifacts
ls C:\epias_data_platform\models\ 2>/dev/null || echo "No models/ directory"
```

If `models/ptf_advanced_xgb_model.joblib` or `models/ptf_shap_importance.csv` exist locally, document them and delete if confirmed unneeded.

### Step 6.4 — Optional: backport walk-forward CV to `ptf_trainer.py`

**Only if the modeling team confirms they want this.** The walk-forward CV in `ptf_forecaster.py:138-166` is strictly better validation than the single 30-day holdout in `ptf_trainer.py:108-112`.

If backporting:
- Copy the 3-fold walk-forward loop from `ptf_forecaster.py:138-166`
- Apply it to the F06 feature set (not the F08 feature set)
- Keep integer encoding for `hour`/`day_of_week`/`month` — do NOT switch to sin/cos
- The final model training (lines 174-184 in `ptf_forecaster.py`) and GCS upload (lines 184-185 in `ptf_trainer.py`) must remain unchanged
- Do NOT add the 48h lag unless `ptf_inference.py` is updated simultaneously

### Step 6.5 — Verification

```bash
# File must be gone
ls C:\epias_data_platform\src\ptf_forecaster.py 2>&1

# Daily DAG must still be importable
python -c "import sys; sys.path.insert(0,'dags'); import epias_dag; print('DAG import OK')"
```

**Anti-pattern guards:**
- Do NOT switch to cyclic encoding unless retrained model artifact is deployed and `ptf_inference.py` is updated simultaneously.
- Do NOT add 48h lag without updating `ptf_inference.py` (train/serve mismatch).

---

## Phase 7: Final Verification

Run all checks in sequence to confirm the entire set of changes is consistent.

```bash
# 1. No timestamp format string left in Spark jobs
grep -rn "yyyy-MM-dd'T'HH:mm:ssXXX" C:\epias_data_platform\spark_jobs
# Expected: 0 matches (all replaced with self.parse_epias_timestamp())

# 2. parse_epias_timestamp defined and used
grep -rn "parse_epias_timestamp" C:\epias_data_platform\spark_jobs
# Expected: 1 definition (spark_utils.py) + N usages

# 3. Inline source dicts gone from DAGs
grep -n "HOURLY_SOURCES\s*=\|BACKFILL_SOURCES\s*=" C:\epias_data_platform\dags\epias_dag.py C:\epias_data_platform\dags\epias_backfill_dag.py
# Expected: 0 matches

# 4. Hardcoded dbt exclude strings gone from DAGs
grep -n "stg_dpp stg_sbfgp" C:\epias_data_platform\dags\epias_dag.py C:\epias_data_platform\dags\epias_backfill_dag.py
# Expected: 0 matches

# 5. No inline GCP env-var reads left in src/ (except epias_client.py)
grep -n "os.getenv.*GCP_PROJECT_ID\|os.getenv.*BQ_GOLD_DATASET\|os.getenv.*GCS_BUCKET" C:\epias_data_platform\src
# Expected: 0 matches (epias_client.py doesn't use these vars so it won't appear)

# 6. Feature engineering gone from trainer and inference
grep -n "ptf_lag_24h\s*=\|ptf_lag_168h\s*=" C:\epias_data_platform\src\ptf_trainer.py C:\epias_data_platform\src\ptf_inference.py
# Expected: 0 matches (moved to ptf_features.py)

# 7. ptf_forecaster.py is deleted
ls C:\epias_data_platform\src\ptf_forecaster.py 2>&1
# Expected: file not found

# 8. New files exist and are importable
python -c "
import sys
sys.path.insert(0, 'src')
sys.path.insert(0, 'dags')
from config import GCP_PROJECT_ID, get_bq_client
from ptf_features import build_ptf_features
from epias_sources import EPIAS_SOURCES, DBT_EXCLUDE_PENDING_BACKFILL, make_silver_task
print('All imports OK')
print('Sources:', len(EPIAS_SOURCES))
print('dbt excludes:', DBT_EXCLUDE_PENDING_BACKFILL)
"
```

---

## Quick Reference

| Phase | New file | Modified files | Deleted |
|---|---|---|---|
| 1 | — | `spark_utils.py` + 25 `bronze_to_silver_*.py` | — |
| 2 | `dags/epias_sources.py` | `epias_dag.py`, `epias_backfill_dag.py` | — |
| 3 | — | `dags/epias_sources.py`, `epias_dag.py`, `epias_backfill_dag.py` | — |
| 4 | `src/config.py` | `ptf_trainer.py`, `ptf_inference.py`, `load_to_bigquery.py`, `data_quality_check.py` | — |
| 5 | `src/ptf_features.py` | `ptf_trainer.py`, `ptf_inference.py` | — |
| 6 | — | optionally `ptf_trainer.py` | `src/ptf_forecaster.py` |
