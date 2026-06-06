# F07 · PTF Inference (Hourly, GCS-backed)

Entry: `src/ptf_inference.py:114` — `run()`

```mermaid
flowchart TD
    Start["run()<br/>ptf_inference.py:114"]
    LoadModel["load_model_from_gcs()<br/>ptf_inference.py:33"]
    GCSBlob["storage.Client.bucket → download blob<br/>ptf_inference.py:36-38"]
    JoblibLoad["joblib.load model artifact<br/>ptf_inference.py:39"]
    ExtractArtifact["Extract model + features list<br/>ptf_inference.py:115-117"]
    FetchData["extract_recent_data()<br/>ptf_inference.py:46"]
    BQJoin["BigQuery: LEFT JOIN<br/>mart_forecasted_residual_load<br/>+ mart_supply_shock_index<br/>LIMIT 180<br/>ptf_inference.py:49-66"]
    SortDF["to_dataframe + sort asc<br/>ptf_inference.py:66-68"]
    BuildFeatures["build_inference_features()<br/>ptf_inference.py:75"]
    Temporal["hour, day_of_week, month, is_weekend<br/>ptf_inference.py:77-80"]
    Lags["ptf_lag_24h, ptf_lag_168h<br/>ptf_inference.py:82-83"]
    Rolling["supply_shock_trend_7d (7-day mean)<br/>ptf_inference.py:85"]
    Dropna["dropna on feature cols<br/>ptf_inference.py:87"]
    SelectLatest["Select latest row + required_features<br/>ptf_inference.py:90"]
    Predict["model.predict(X_latest)<br/>ptf_inference.py:122"]
    ExtractPred["Extract predicted_ptf + timestamp<br/>ptf_inference.py:122-123"]
    WriteBQ["write_prediction_to_bq()<br/>ptf_inference.py:97"]
    FormatRow["Format: predicted_date, hour,<br/>predicted_ptf, predicted_at UTC<br/>ptf_inference.py:100-105"]
    InsertRows["client.insert_rows_json<br/>ptf_inference.py:106"]
    Done["Inference complete<br/>ptf_inference.py:130"]

    Start --> LoadModel --> GCSBlob --> JoblibLoad --> ExtractArtifact
    Start --> FetchData --> BQJoin --> SortDF
    ExtractArtifact & SortDF --> BuildFeatures
    BuildFeatures --> Temporal --> Lags --> Rolling --> Dropna --> SelectLatest
    SelectLatest --> Predict --> ExtractPred --> WriteBQ
    WriteBQ --> FormatRow --> InsertRows --> Done
```

## GCS Read
- `gs://epias-data-lake/models/ptf_xgb_model.joblib` (line 24)

## BQ Read
- `epias_gold.mart_forecasted_residual_load` (join left)
- `epias_gold.mart_supply_shock_index` (join right, on `date`)
- Lookback: 180 rows (line 28)

## BQ Write
- `epias_gold.gold_ptf_predictions` — schema: `predicted_date`, `hour`, `predicted_ptf`, `predicted_at`

## Feature Overlap with F06
Same 12 features as F06 trainer. Model artifact is the bridge.
