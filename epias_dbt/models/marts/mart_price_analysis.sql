{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_price_spread') }}
)

SELECT
    date,
    hour,
    mcp_usd, -- ptf yerine
    smp_usd, -- smf yerine
    price_spread_usd,
    system_direction,
    season,
    -- Gerekirse TL karşılıklarını da buraya ekleyebilirsin
    ptf AS mcp_tl,
    smf AS smp_tl
FROM base