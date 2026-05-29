{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH pricing AS (
    SELECT * FROM {{ ref('stg_pricing') }}
)

SELECT
    date,
    hour,
    ptf_try,
    smf_try,
    (ptf_try - smf_try) AS price_spread,
    system_direction,
    CASE 
        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Kış'
        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'İlkbahar'
        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Yaz'
        ELSE 'Sonbahar'
    END AS season
FROM pricing