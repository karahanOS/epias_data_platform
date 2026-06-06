# F04 · BigQuery External Table Bridge

Entry: `src/load_to_bigquery.py:18` — `BQExternalTableManager`

```mermaid
flowchart TD
    Entry["__main__ entry<br/>load_to_bigquery.py:109"]
    InitMgr["BQExternalTableManager.__init__<br/>load_to_bigquery.py:19"]
    ReadEnv["Read GCP_PROJECT_ID env<br/>load_to_bigquery.py:22"]
    CreateClient["Create BigQuery Client<br/>load_to_bigquery.py:27"]
    EnsureDataset["_ensure_dataset_exists<br/>load_to_bigquery.py:31-41"]
    GetDataset{"get_dataset()<br/>load_to_bigquery.py:34"}
    CreateDataset["create_dataset()<br/>load_to_bigquery.py:40"]
    CheckArgv{"sys.argv[1]?<br/>load_to_bigquery.py:115-118"}
    RunAll["run_all_tables()<br/>load_to_bigquery.py:76"]
    Iterate["Iterate 20 silver tables<br/>load_to_bigquery.py:79-101"]
    CreateOrUpdate["create_or_update_external_table(table)<br/>load_to_bigquery.py:43"]
    BuildID["Construct table_id<br/>{project}.silver.{name}<br/>load_to_bigquery.py:48"]
    BuildURI["Construct GCS URI<br/>gs://epias-data-lake/silver/{name}/*<br/>load_to_bigquery.py:49"]
    CreateExtConfig["ExternalConfig('PARQUET')<br/>load_to_bigquery.py:55"]
    SetSourceURIs["Set source_uris<br/>load_to_bigquery.py:56"]
    HivePartition["HivePartitioningOptions AUTO<br/>load_to_bigquery.py:59-62"]
    CreateTableObj["Create Table object<br/>load_to_bigquery.py:65-66"]
    DeleteTable["delete_table(not_found_ok=True)<br/>load_to_bigquery.py:70"]
    CreateTable["create_table()<br/>load_to_bigquery.py:71"]
    LogSuccess["Log success<br/>load_to_bigquery.py:72"]
    LogError["Log error (continue)<br/>load_to_bigquery.py:74"]
    LogDone["Log completion<br/>load_to_bigquery.py:107"]

    Entry --> InitMgr --> ReadEnv --> CreateClient --> EnsureDataset
    EnsureDataset --> GetDataset
    GetDataset -->|NotFound| CreateDataset --> CheckArgv
    GetDataset -->|Found| CheckArgv
    CheckArgv -->|No args| RunAll --> Iterate --> CreateOrUpdate
    CheckArgv -->|Single table| CreateOrUpdate
    CreateOrUpdate --> BuildID --> BuildURI --> CreateExtConfig --> SetSourceURIs --> HivePartition --> CreateTableObj --> DeleteTable --> CreateTable
    CreateTable -->|success| LogSuccess --> Iterate
    CreateTable -->|Exception| LogError --> Iterate
    Iterate -->|all done| LogDone
```

## Idempotency
Delete-before-create ensures schema changes are always applied. `not_found_ok=True` safe on first run.

## External Dependencies
- `google.cloud.bigquery` — Client, ExternalConfig, HivePartitioningOptions
- `google.api_core.exceptions.NotFound`
- GCS bucket: `epias-data-lake/silver/{table}/*`
- Env: `GCP_PROJECT_ID` (default: `epias-data-platform`)

## Called From
- `dags/epias_dag.py:248` (daily, BashOperator)
- `dags/epias_backfill_dag.py:292` (backfill, BashOperator)
