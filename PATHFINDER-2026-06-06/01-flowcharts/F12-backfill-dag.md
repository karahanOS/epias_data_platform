# F12 · Historical Backfill DAG (Airflow)

Entry: `dags/epias_backfill_dag.py:40` — DAG `epias_historical_backfill`

Schedule: `None` — **manual trigger only**

```mermaid
flowchart TD
    subgraph PHASE1 ["PHASE 1: BRONZE (parallel, 16 sources × N chunks + weather)"]
        B1["bronze_{source}_{chunk_start}<br/>PythonOperator × 16src × ~76chunks<br/>epias_backfill_dag.py:213-226"]
        BW["bronze_weather_{chunk_start}<br/>PythonOperator × ~76chunks<br/>epias_backfill_dag.py:265-272"]
        B1 --- B1["backfill_chunk() line:89-132<br/>→ gs://epias-data-lake/bronze/{src}/backfill_*.parquet"]
        BW --- BW["backfill_weather_chunk() line:135-175<br/>→ gs://epias-data-lake/bronze/weather/backfill_*.parquet"]
    end

    subgraph PHASE2 ["PHASE 2: SILVER (Spark, per source)"]
        S1["silver_{source}_backfill<br/>SparkSubmitOperator ×16<br/>epias_backfill_dag.py:237-257<br/>flag: --backfill (append mode)"]
        SW["silver_weather_backfill<br/>SparkSubmitOperator<br/>epias_backfill_dag.py:274-283"]
    end

    BQ["register_silver_external_tables<br/>BashOperator: load_to_bigquery.py<br/>epias_backfill_dag.py:290-297"]
    DBT["run_dbt_full_refresh<br/>BashOperator: dbt run --full-refresh<br/>--exclude stg_dpp stg_sbfgp stg_res_forecast mart_production_plan<br/>epias_backfill_dag.py:302-314"]
    TRAIN["train_initial_model<br/>BashOperator: ptf_trainer.py<br/>epias_backfill_dag.py:319-322"]
    End["Historical dataset ready"]

    B1 --> S1
    BW --> SW
    S1 & SW --> BQ --> DBT --> TRAIN --> End
```

## Scale (2025-01-01 to 2026-06-06)
- ~76 weekly chunks × 16 sources = ~1,216 bronze tasks
- 16 EPIAS + 1 weather silver tasks
- 3 sequential gate tasks
- **~1,235+ dynamic tasks total**

## Key Overlaps with Daily DAG (F10)
| Task | Backfill DAG | Daily DAG | Same script? |
|---|---|---|---|
| BQ bridge | `register_silver_external_tables` line:292 | `load_silver_to_bigquery` line:248 | YES — `load_to_bigquery.py` |
| dbt run | `run_dbt_full_refresh` line:310 | `run_dbt_gold_models` line:258 | YES — same dbt project; different flags (`--full-refresh` vs incremental) |
| PTF train | `train_initial_model` line:321 | `train_ptf_model` line:267 | YES — `ptf_trainer.py` |
| Spark jobs | `silver_{source}_backfill` line:239 | `silver_{key}` line:228 | YES — same scripts; different args |
| Bronze ingest | `bronze_{source}_{chunk}` line:213 | `get_{key}` + `save_{key}_to_gcs` | SAME METHODS — different chunk/save pattern |

## dbt Model Exclusions (identical in both DAGs)
`stg_dpp`, `stg_sbfgp`, `stg_res_forecast`, `mart_production_plan`
- Backfill: `epias_backfill_dag.py:310-313`
- Daily: `epias_dag.py:258-261`
