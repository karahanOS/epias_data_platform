{{ config(
    materialized='table', 
    partition_by={"field": "date", "data_type": "date"}
) }}

SELECT
    p.date,
    p.hour,
    p.ptf_try,
    smf.smf_try,
    l.forecasted_load_mwh,
    r.forecasted_res_mwh,
    (l.forecasted_load_mwh - r.forecasted_res_mwh) AS forecasted_residual_load_mwh
FROM {{ ref('stg_pricing') }} p
LEFT JOIN {{ ref('stg_smf') }} smf ON p.date = smf.date AND p.hour = smf.hour
LEFT JOIN {{ ref('stg_load_estimation') }} l ON p.date = l.date AND p.hour = l.hour
LEFT JOIN {{ ref('stg_res_forecast') }} r ON p.date = r.date AND p.hour = r.hour