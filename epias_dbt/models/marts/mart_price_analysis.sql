{{ config(materialized='table') }}

SELECT
    date,
    hour,
    mcp_usd,
    smp_usd,
    price_spread_usd,
    system_direction,
    season
FROM {{ ref('stg_price_spread') }}