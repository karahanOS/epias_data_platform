{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

SELECT
    CAST(date AS DATE) AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP)) AS hour,
    CAST(systemMarginalPrice AS FLOAT64) AS smf_try
FROM {{ source('silver', 'smf') }}

{% if is_incremental() %} WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}