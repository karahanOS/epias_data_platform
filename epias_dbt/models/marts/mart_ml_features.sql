{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

WITH pricing AS (
    SELECT date, hour, ptf_try FROM {{ ref('stg_pricing') }}
),
load_est AS (
    SELECT date, hour, forecasted_load_mwh FROM {{ ref('stg_load_estimation') }}
),
res_forecast AS (
    SELECT date, hour, forecasted_res_mwh FROM {{ ref('stg_res_forecast') }}
),
outages AS (
    -- Arızalarda ayrı bir saat kolonu olmadığı için gün bazında toplam arıza kapasitesini alıyoruz
    SELECT date, SUM(outage_capacity_mwh) AS total_outage_mwh
    FROM {{ ref('stg_outages') }}
    GROUP BY 1
)

SELECT
    p.date,
    p.hour,
    p.ptf_try,
    smf.smf_try,
    l.forecasted_load_mwh,
    r.forecasted_res_mwh,
    COALESCE(o.total_outage_mwh, 0) AS total_outage_mwh,
    (l.forecasted_load_mwh - r.forecasted_res_mwh) AS forecasted_residual_load_mwh
FROM pricing p
LEFT JOIN {{ ref('stg_smf') }} smf ON p.date = smf.date AND p.hour = smf.hour
LEFT JOIN load_est l ON p.date = l.date AND p.hour = l.hour
LEFT JOIN res_forecast r ON p.date = r.date AND p.hour = r.hour
LEFT JOIN outages o ON p.date = o.date