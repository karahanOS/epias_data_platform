{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH lep AS (
    SELECT date, forecasted_load_mwh FROM {{ ref('stg_load_estimation') }}
),
res_forecast AS (
    SELECT date, forecasted_total_res_mwh, forecasted_wind_mwh, forecasted_solar_mwh FROM {{ ref('stg_res_forecast') }}
),
pib AS (
    SELECT date, price_independent_bid_mwh FROM {{ ref('stg_price_ind_bid') }}
),
pricing AS (
    SELECT date, ptf_try FROM {{ ref('stg_pricing') }}
)

SELECT
    l.date,
    p.ptf_try,
    l.forecasted_load_mwh,
    r.forecasted_total_res_mwh,
    r.forecasted_wind_mwh,
    r.forecasted_solar_mwh,
    pb.price_independent_bid_mwh,
    (l.forecasted_load_mwh - r.forecasted_total_res_mwh) AS forecasted_residual_load_mwh
FROM lep l
LEFT JOIN res_forecast r ON l.date = r.date
LEFT JOIN pricing p ON l.date = p.date
LEFT JOIN pib pb ON l.date = pb.date