{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- Silver imbalance.date is a UTC TIMESTAMP (parse_epias_timestamp).
-- Convert to Turkish local date/hour to align with all other hourly staging models.
SELECT
    DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(generationTotal  AS NUMERIC) AS total_generation_mwh,
    CAST(consumption      AS NUMERIC) AS total_consumption_mwh,
    CAST(imbalanceQuantity AS NUMERIC) AS net_imbalance_mwh
FROM {{ source('silver', 'imbalance') }}

{% if is_incremental() %}
  WHERE DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}