{{ config(
    materialized='table', 
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH hourly_system AS (
    SELECT
        smf.date,
        smf.hour,
        smf.smf_try,
        p.ptf_try,
        (smf.smf_try - p.ptf_try) AS smf_ptf_spread_try,
        sd.system_direction,
        imb.net_imbalance_mwh
    FROM {{ ref('stg_smf') }} smf
    LEFT JOIN {{ ref('stg_pricing') }} p ON smf.date = p.date AND smf.hour = p.hour
    LEFT JOIN {{ ref('stg_system_direction') }} sd ON smf.date = sd.date AND smf.hour = sd.hour
    LEFT JOIN {{ ref('stg_imbalance') }} imb ON smf.date = imb.date AND smf.hour = imb.hour
),
system_yal AS (
    SELECT 
        date, 
        hour, 
        CAST(SUM(up_regulation_delivered_mwh) AS FLOAT64) AS total_yal_mwh 
    FROM {{ ref('stg_order_up') }}
    GROUP BY 1, 2
),
system_yat AS (
    SELECT 
        date, 
        hour, 
        CAST(SUM(down_regulation_delivered_mwh) AS FLOAT64) AS total_yat_mwh 
    FROM {{ ref('stg_order_down') }}
    GROUP BY 1, 2
)

SELECT
    hs.date,
    hs.hour,
    hs.system_direction,
    hs.net_imbalance_mwh,
    hs.smf_try,
    hs.ptf_try,
    hs.smf_ptf_spread_try,
    COALESCE(u.total_yal_mwh, 0.0) AS total_yal_mwh,
    COALESCE(d.total_yat_mwh, 0.0) AS total_yat_mwh
FROM hourly_system hs
LEFT JOIN system_yal u ON hs.date = u.date AND hs.hour = u.hour
LEFT JOIN system_yat d ON hs.date = d.date AND hs.hour = d.hour