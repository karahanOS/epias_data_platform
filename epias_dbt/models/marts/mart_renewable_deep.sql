{{ config(materialized='table') }}

WITH gen AS (
    SELECT * FROM {{ ref('stg_generation') }}
),
forecast AS (
    SELECT * FROM {{ ref('stg_res_forecast') }}
)

SELECT
    g.date,
    g.hour,
    g.wind_generation_mwh,
    g.solar_generation_mwh,
    f.forecasted_wind_mwh,
    f.forecasted_solar_mwh,
    (g.wind_generation_mwh - f.forecasted_wind_mwh) AS wind_forecast_error
FROM gen g
LEFT JOIN forecast f ON g.date = f.date AND g.hour = f.hour