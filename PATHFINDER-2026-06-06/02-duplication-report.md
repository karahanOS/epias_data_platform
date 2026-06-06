# Duplication Report — epias_data_platform

Generated: 2026-06-06

---

## D01 · Parallel ML Training Pipelines (F06 vs F08)

**Concern:** Two independent XGBoost PTF training pipelines exist side-by-side. F06 feeds the production hourly inference loop; F08 is an offline forecaster with no downstream consumer.

| Aspect | F06 `ptf_trainer.py` | F08 `ptf_forecaster.py` |
|---|---|---|
| BQ source | `mart_forecasted_residual_load` + `mart_supply_shock_index` (join) | `mart_ml_features` only |
| Feature encoding | Categorical (hour, dow, month as integers) | Cyclic sin/cos + interaction |
| Lags | 24h, 168h | 24h, **48h**, 168h |
| Rolling | `supply_shock_trend_7d` only | avg 24h/168h + std + cap_util |
| CV strategy | Single 30-day holdout | Walk-forward 3-fold |
| Model artifact | → **GCS** (`gs://epias-data-lake/models/ptf_xgb_model.joblib`) | → **local disk** (`models/ptf_advanced_xgb_model.joblib`) |
| Used by inference (F07) | **Yes** — F07 loads from the same GCS path | **No** — nothing reads the local artifact |
| Called from any DAG | Yes — `dags/epias_dag.py:265` | Not called from any DAG |

**Evidence (file:line):**
- `src/ptf_trainer.py:20-24` — GCS bucket/paths hardcoded
- `src/ptf_forecaster.py:25` — local model path `models/ptf_advanced_xgb_model.joblib`
- `src/ptf_inference.py:24` — reads same path as F06 (`ptf_xgb_model.joblib`)
- `src/ptf_forecaster.py:197-203` — `run()` entry — no DAG calls this

**XGBoost hyperparameters are copy-pasted across all three locations:**
- `src/ptf_trainer.py:114-123`
- `src/ptf_forecaster.py:150-158`
- `src/ptf_forecaster.py:175-183`

**Why they diverged:** F08 appears to have been developed as an R&D improvement over F06 (more features, walk-forward CV) but was never promoted to production. The model it trains is never consumed — it's dead output.

**Legitimate specialization?** No. Two training pipelines that train the same target (`ptf_try`) with overlapping features are not intentional specializations — one is an abandoned experiment. The divergent feature encoding (categorical vs sin/cos) means models from F06 and F08 are not interchangeable, which is not documented or intentional.

---

## D02 · Feature Engineering Reimplemented in F06 and F07 (trainer↔inference skew risk)

**Concern:** `ptf_trainer.py::engineer_features()` and `ptf_inference.py::build_inference_features()` independently implement the same feature construction. If one is updated, the other may silently diverge, causing train/serve skew.

**Evidence (file:line):**

| Feature | F06 `ptf_trainer.py` | F07 `ptf_inference.py` |
|---|---|---|
| `hour` | line 63 | line 77 |
| `day_of_week` | line 64 | line 78 |
| `month` | line 65 | line 79 |
| `is_weekend` | line 66 | line 80 |
| `ptf_lag_24h` | line 68 | line 82 |
| `ptf_lag_168h` | line 69 | line 83 |
| `supply_shock_trend_7d` | line 81 | line 85 |
| `forward-fill outage cols` | lines 77-79 | **missing** (relies on BQ source) |

**Why they diverged:** Inference was written separately from training; the author replicated the feature list rather than extracting a shared function.

**Legitimate specialization?** Partially. The inference function applies feature construction only on recent rows (180-row window), not full history, and returns only `iloc[-1]` — that wrapping logic is legitimately different. But the **feature construction itself** (lag/rolling/temporal) should be one shared function.

---

## D03 · DAG Source Lists Out of Sync (F10 vs F12)

**Concern:** The EPIAS data source dictionaries in the daily DAG and backfill DAG are maintained independently and have already diverged.

**Evidence (file:line):**

| Source | Daily DAG (`epias_dag.py:154-180`) | Backfill DAG (`epias_backfill_dag.py:51-71`) |
|---|---|---|
| `injection` | ✅ included | ❌ excluded |
| `uevcb_list` | ✅ included | ❌ excluded |
| `sbfgp` | ❌ excluded from dbt (line 260) | ✅ in `BACKFILL_SOURCES` |
| `participants` | ✅ (STATIC) | ❌ explicitly excluded |

**Why they diverged:** Both files maintain their own source dictionaries with documented rationale in comments, but the comments reference "pending backfills" that may never resolve. Copy-paste with ad-hoc edits.

**Legitimate specialization?** Partially. The backfill DAG legitimately excludes static/slowly-changing sources (participants, uevcb_list) that don't need historical backfill. But `injection` appears accidentally omitted and `sbfgp` is confusingly present in backfill but excluded from dbt — indicating confusion about completion status.

---

## D04 · dbt Exclude List Copy-Pasted (F10 vs F12)

**Concern:** The list of temporarily-excluded dbt models appears identically in both DAGs. If one is updated (e.g., when backfill for `stg_dpp` completes), the other is not.

**Evidence (file:line):**
- `dags/epias_dag.py:258-261` — `--exclude stg_dpp stg_sbfgp stg_res_forecast mart_production_plan`
- `dags/epias_backfill_dag.py:310-313` — identical string

**Why they diverged:** Copy-paste when backfill DAG was created from daily DAG template.

**Legitimate specialization?** No. Identical strings. Zero tolerance for drift.

---

## D05 · BigQuery Client Initialization Scattered (F06, F07, F08, F04, F09)

**Concern:** Five Python files independently instantiate `bigquery.Client(project=...)` after reading the same environment variables, with no shared connection factory.

**Evidence (file:line):**
- `src/ptf_trainer.py:20-23` + line 33 — reads `GCP_PROJECT_ID`, `BQ_GOLD_DATASET`, `GCS_BUCKET`; creates client
- `src/ptf_inference.py:21-23` + line 49 — same three vars; creates client
- `src/ptf_forecaster.py:22-23` + line 24 — same three vars; creates client
- `src/load_to_bigquery.py:22` + line 27 — reads `GCP_PROJECT_ID`; creates client
- `src/data_quality_check.py:33-34` + line 85 — reads `GCP_PROJECT_ID`, `BQ_GOLD_DATASET`; creates client

**Why they diverged:** No shared config/client module; each author re-implemented env-var reading.

**Legitimate specialization?** No. Same env vars, same `bigquery.Client()` call pattern.

---

## D06 · Spark Date-Timestamp Parsing Boilerplate in 18+ Jobs (within F03)

**Concern:** `F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")` appears identically in 18+ Spark job files. Any change to the timestamp format requires editing every job individually.

**Evidence (file:line — representative sample):**
- `spark_jobs/bronze_to_silver_pricing.py:28`
- `spark_jobs/bronze_to_silver_consumption.py:22`
- `spark_jobs/bronze_to_silver_generation.py:35`
- `spark_jobs/bronze_to_silver_smf.py:18`
- `spark_jobs/bronze_to_silver_order_up.py:32`
- `spark_jobs/bronze_to_silver_imbalance.py:35`
- `spark_jobs/bronze_to_silver_injection.py:32`
- (+ 11 more jobs)

**Why they diverged:** Copy-paste boilerplate from first job implementation.

**Legitimate specialization?** No. The format string is identical in all jobs.

---

## D07 · Spark Silver Task Boilerplate Duplicated Across Two DAGs (F10 vs F12)

**Concern:** `SparkSubmitOperator` definitions for Silver jobs appear in near-identical loops in both the daily and backfill DAGs.

**Evidence (file:line):**
- `dags/epias_dag.py:228-240` — `SparkSubmitOperator` per source, `application_args=["{{ ds }}"]`
- `dags/epias_backfill_dag.py:237-257` — identical operator config, `application_args=["1970-01-01", "--backfill"]`

The only runtime difference is the `application_args` value.

**Why they diverged:** Backfill DAG was templated from daily DAG with args modified.

**Legitimate specialization?** Single-parameter difference. A factory function would eliminate the boilerplate without changing semantics.

---

## Summary Table

| ID | Concern | Files | Locations | Legitimate? |
|---|---|---|---|---|
| **D01** | Two parallel ML training pipelines (F06 dead, F08 active) | `ptf_trainer.py`, `ptf_forecaster.py`, `ptf_inference.py` | trainer:114, forecaster:150,175 | **No** — F08 produces no consumed artifact |
| **D02** | Feature engineering in trainer vs inference (skew risk) | `ptf_trainer.py`, `ptf_inference.py` | trainer:63-81, inference:77-85 | **Partial** — wrapper differs, core features don't |
| **D03** | DAG source lists out of sync | `epias_dag.py`, `epias_backfill_dag.py` | dag:154-180, backfill:51-71 | **Partial** — some exclusions intentional, `injection` probably not |
| **D04** | dbt exclude list copy-pasted | `epias_dag.py`, `epias_backfill_dag.py` | dag:258-261, backfill:310-313 | **No** — identical strings |
| **D05** | BigQuery client scattered | 5 `src/*.py` files | 5 `bigquery.Client()` call sites | **No** — same pattern, no variation |
| **D06** | Timestamp format string copy-pasted in 18+ Spark jobs | `spark_jobs/bronze_to_silver_*.py` | 18+ `:~28` lines | **No** — should be in base class |
| **D07** | SparkSubmitOperator loop duplicated in two DAGs | `epias_dag.py`, `epias_backfill_dag.py` | dag:228-240, backfill:237-257 | **Partial** — single-param difference |
