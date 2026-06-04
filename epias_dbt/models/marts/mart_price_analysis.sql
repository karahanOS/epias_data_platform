{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

SELECT 
    p.date,
    p.hour,
    p.ptf_try,
    s.smf_try,
    (p.ptf_try - s.smf_try) AS price_spread,
    CASE 
        WHEN EXTRACT(MONTH FROM p.date) IN (12, 1, 2) THEN 'Kış'
        WHEN EXTRACT(MONTH FROM p.date) IN (3, 4, 5) THEN 'İlkbahar'
        WHEN EXTRACT(MONTH FROM p.date) IN (6, 7, 8) THEN 'Yaz'
        ELSE 'Sonbahar'
    END AS season
FROM {{ ref('stg_pricing') }} p
LEFT JOIN {{ ref('stg_smf') }} s ON p.date = s.date AND p.hour = s.hour