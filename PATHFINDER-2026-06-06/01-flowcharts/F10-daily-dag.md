# F10 · Daily Medallion DAG (Airflow)

Entry: `dags/epias_dag.py:140` — DAG `epias_medallion_pipeline_v3`

Schedule: `0 5 * * *` (05:00 UTC daily)

```mermaid
flowchart TD
    Start["DAG Start<br/>epias_dag.py:140"]

    subgraph BRONZE ["BRONZE LAYER (parallel, 22+1 paths)"]
        get1["get_pricing<br/>epias_dag.py:196"]
        get2["get_smf ... get_dams<br/>epias_dag.py:196<br/>(21 more sources)"]
        getW["get_weather<br/>epias_dag.py:185"]
        save1["save_pricing_to_gcs<br/>epias_dag.py:201"]
        save2["save_{source}_to_gcs ×21<br/>epias_dag.py:201"]
        saveW["save_weather_to_gcs<br/>epias_dag.py:186"]
        get1 --> save1
        get2 --> save2
        getW --> saveW
    end

    subgraph SILVER ["SILVER LAYER (Spark, parallel)"]
        silver1["silver_pricing<br/>SparkSubmitOperator<br/>epias_dag.py:228"]
        silver2["silver_{source} ×21<br/>SparkSubmitOperator<br/>epias_dag.py:228"]
        silverW["silver_weather<br/>SparkSubmitOperator<br/>epias_dag.py:215"]
        save1 --> silver1
        save2 --> silver2
        saveW --> silverW
    end

    loadBQ["load_silver_to_bigquery<br/>BashOperator: load_to_bigquery.py<br/>epias_dag.py:246"]
    runDBT["run_dbt_gold_models<br/>BashOperator: dbt run<br/>epias_dag.py:251"]
    trainPTF["train_ptf_model<br/>BashOperator: ptf_trainer.py<br/>epias_dag.py:265"]
    infPTF["run_ptf_inference<br/>BashOperator: ptf_inference.py<br/>epias_dag.py:272"]
    End["DAG Complete<br/>epias_dag.py:283"]

    Start --> BRONZE --> SILVER
    silver1 & silver2 & silverW --> loadBQ --> runDBT --> trainPTF --> infPTF --> End
```

## Task Count
- 22 `get_*` tasks (dynamically generated from `ALL_SOURCES`)
- 22 `save_*_to_gcs` tasks
- 22 `silver_*` SparkSubmitOperator tasks
- 1 `get_weather` + 1 `save_weather_to_gcs` + 1 `silver_weather` (hardcoded)
- 4 terminal tasks
- **Total: ~73 tasks**

## Dynamic Generation
`ALL_SOURCES = HOURLY_SOURCES ∪ {participants}` (lines 154–182)

For each key: `get_{key}` (PythonOperator) → `save_{key}_to_gcs` (PythonOperator) → `silver_{key}` (SparkSubmitOperator)

## External Module Calls
- `EPIASClient` — `src/epias_client.py:24`
- `WeatherClient` — `src/weather_client.py:50`
- `load_to_bigquery.py` — `src/load_to_bigquery.py:109`
- `ptf_trainer.py` — `src/ptf_trainer.py:161`
- `ptf_inference.py` — `src/ptf_inference.py:114`
- GCS Connector JAR (line 219)
