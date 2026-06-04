{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

SELECT
    i.date,
    i.hour,
    i.net_imbalance_mwh,
    CASE 
        WHEN i.net_imbalance_mwh > 0 THEN 'Enerji Fazlası (Pozitif)'
        WHEN i.net_imbalance_mwh < 0 THEN 'Enerji Açığı (Negatif)'
        ELSE 'Dengede' 
    END AS deviation_direction,
    p.ptf_try,
    s.smf_try,
    (i.net_imbalance_mwh * s.smf_try) AS total_imbalance_cost_try
FROM {{ ref('stg_imbalance') }} i
LEFT JOIN {{ ref('stg_pricing') }} p ON i.date = p.date AND i.hour = p.hour
LEFT JOIN {{ ref('stg_smf') }} s ON i.date = s.date AND i.hour = s.hour