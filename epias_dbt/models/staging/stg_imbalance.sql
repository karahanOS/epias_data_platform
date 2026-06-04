{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

SELECT
    CAST(date AS DATE) AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP)) AS hour,
    CAST(generationTotal AS NUMERIC) AS total_generation_mwh,
    CAST(consumption AS NUMERIC) AS total_consumption_mwh,
    CAST(imbalanceQuantity AS NUMERIC) AS net_imbalance_mwh
FROM {{ source('silver', 'imbalance') }}

{% if is_incremental() %} WHERE date >= (SELECT MAX(date) FROM {{ this }}) {% endif %}