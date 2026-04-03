SELECT
    date,
    hour,
    ptf as mcp_tl,
    smf as smp_tl,
    mcpUsd as mcp_usd,
    smpUsd as smp_usd,
    price_spread_usd,
    system_direction,
    -- Yine date üzerinden extract
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month,
    CASE 
        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Kış'
        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'İlkbahar'
        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Yaz'
        ELSE 'Sonbahar'
    END AS season
FROM {{ source('epias_gold', 'gold_price_spread_analysis') }}