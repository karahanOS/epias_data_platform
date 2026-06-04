{{ config(materialized='incremental', unique_key=['date', 'hour'], partition_by={"field": "date", "data_type": "date"}) }}

WITH ptf AS (
    SELECT date, hour, ptf_try FROM {{ ref('stg_pricing') }}
),
smf AS (
    SELECT date, hour, smf_try FROM {{ ref('stg_smf') }}
)
SELECT 
    p.date, 
    p.hour, 
    p.ptf_try, 
    s.smf_try, 
    (p.ptf_try - s.smf_try) AS price_spread_try
FROM ptf p
LEFT JOIN smf s ON p.date = s.date AND p.hour = s.hour
{% if is_incremental() %} WHERE p.date >= (SELECT MAX(date) FROM {{ this }}) {% endif %}