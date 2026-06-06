# Feature Inventory — epias_data_platform

Generated: 2026-06-06

## Boundary decisions

- **EPIAS Client** and **Weather Client** are kept separate — different APIs, different auth models, different downstream consumers.
- **PTF Trainer**, **PTF Inference**, and **PTF Forecaster** are kept separate because two parallel ML implementations exist (GCS-backed vs. local-disk); this divergence is a primary duplication target.
- **Airflow DAGs** are kept as three separate features (Daily, Hourly, Backfill) because they have distinct schedules and partial code duplication.

---

## F01 · EPIAS API Client

| | |
|---|---|
| Entry point | `src/epias_client.py:24` — `EPIASClient` class |
| Core files | `src/epias_client.py` |
| Purpose | Wraps EPIAS transparency platform REST API (CAS auth, retry, token caching). Provides 50+ methods for Turkish electricity market data: PTF/SMF prices, real-time generation by source, DAM/IDM volumes, supply-demand curves, outages, dam levels, imbalance. |

## F02 · Weather API Client

| | |
|---|---|
| Entry point | `src/weather_client.py:50` — `WeatherClient` class |
| Core files | `src/weather_client.py` |
| Purpose | Fetches hourly weather from Open-Meteo for 4 Turkish cities (Istanbul, Izmir, Konya, Ankara) with regional weighting. Returns per-city and weighted-average metrics. |

## F03 · Daily Bronze→Silver ETL (Spark)

| | |
|---|---|
| Entry point | `spark_jobs/spark_utils.py:13` — `BaseEpiasSparkJob` |
| Core files | `spark_jobs/spark_utils.py`, 25× `spark_jobs/bronze_to_silver_<source>.py` |
| Purpose | OOP Spark framework for daily incremental loads from GCS Bronze parquet to GCS Silver parquet (Hive-partitioned). Features: schema inference, nested-JSON flattening, deduplication on primary keys, idempotent partition overwrite, optional backfill mode. |

## F04 · BigQuery External Table Bridge

| | |
|---|---|
| Entry point | `src/load_to_bigquery.py:18` — `BQExternalTableManager` |
| Core files | `src/load_to_bigquery.py` |
| Purpose | Creates/updates BigQuery external tables pointing to GCS Silver Parquet. No data copy — dbt reads these directly. Supports per-table or bulk invocation. |

## F05 · dbt Gold Transformation Layer

| | |
|---|---|
| Entry point | `epias_dbt/dbt_project.yml` |
| Core files | `epias_dbt/models/staging/stg_*.sql` (~20), `epias_dbt/models/marts/mart_*.sql` (~25) |
| Purpose | BigQuery SQL transformations. Staging views clean Silver external tables. Mart layer builds analytics, ML feature store, supply shock index, forecasted residual load, and price analysis tables. |

## F06 · PTF Trainer (GCS-backed XGBoost)

| | |
|---|---|
| Entry point | `src/ptf_trainer.py:161` — `run()` |
| Core files | `src/ptf_trainer.py` |
| Purpose | Weekly XGBoost training on Gold `mart_forecasted_residual_load` + `mart_supply_shock_index`. Time-series train/test split (last 30 days held out), saves model + feature importance to GCS as joblib artifact. |

## F07 · PTF Inference (Hourly, GCS-backed)

| | |
|---|---|
| Entry point | `src/ptf_inference.py:114` — `run()` |
| Core files | `src/ptf_inference.py` |
| Purpose | Hourly inference job: loads XGBoost from GCS, pulls last 180 rows from BigQuery for lag warm-up, predicts next-hour PTF, writes single row to `gold_ptf_predictions`. Runs independently via dedicated DAG. |

## F08 · PTF Forecaster (Local-disk XGBoost)

| | |
|---|---|
| Entry point | `src/ptf_forecaster.py:20` — `PredictivePTFForecaster` class |
| Core files | `src/ptf_forecaster.py` |
| Purpose | Alternative XGBoost implementation using `mart_ml_features` (GÖP volumes, AIC, renewable ratios, GİP-GÖP spread). Walk-forward cross-validation, cyclic time encoding, hour×day interactions. Saves model to local disk (not GCS). |

## F09 · Data Quality Monitor

| | |
|---|---|
| Entry point | `src/data_quality_check.py:302` — `main()` |
| Core files | `src/data_quality_check.py` |
| Purpose | BigQuery-based completeness and sanity checks on staging and mart tables. Validates hourly coverage, gap detection, NULL rates, outlier detection (negative prices, zero generation), freshness monitoring, color-coded status scoreboard. |

## F10 · Daily Medallion DAG (Airflow)

| | |
|---|---|
| Entry point | `dags/epias_dag.py:140` — DAG definition |
| Core files | `dags/epias_dag.py` |
| Purpose | Orchestrates full daily Bronze→Silver→Gold→ML pipeline (05:00 UTC). 20+ source ingestors + weather path, Spark jobs, BQ external table refresh, dbt Gold builds, weekly trainer, and inference hooks. |

## F11 · Hourly PTF Inference DAG (Airflow)

| | |
|---|---|
| Entry point | `dags/ptf_inference_dag.py:18` — DAG definition |
| Core files | `dags/ptf_inference_dag.py` |
| Purpose | Independent hourly DAG running `ptf_inference.py`. Decoupled from daily pipeline; enables real-time PTF prediction updates. |

## F12 · Historical Backfill DAG (Airflow)

| | |
|---|---|
| Entry point | `dags/epias_backfill_dag.py:40` — BACKFILL parameters |
| Core files | `dags/epias_backfill_dag.py` |
| Purpose | One-time manual backfill for historical data gaps. Chunks into 7-day windows to respect EPIAS 80 req/min rate limit. Bronze chunks saved separately; Spark jobs detect `--backfill` flag and append rather than overwrite Silver partitions. |

## F13 · Streamlit Analytics Dashboard

| | |
|---|---|
| Entry point | `dashboard.py:33` — `st.set_page_config` |
| Core files | `dashboard.py` |
| Purpose | Multi-page interactive dashboard consuming BigQuery Gold marts. Visualizes executive metrics, price analysis, generation mix, market volumes, supply-demand, imbalance risk, PTF predictions, and YEKDEM impact. Uses Plotly. |
