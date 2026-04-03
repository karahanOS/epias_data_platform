SELECT
    date,
    hour,
    ptf as mcp_tl,
    smf as smp_tl,
    mcpUsd as mcp_usd,
    smpUsd as smp_usd,
    price_spread_usd,
    system_direction,
    season
FROM {{ source('epias_gold', 'gold_price_spread_analysis') }}