SELECT
    date,
    hour,
    ptf, -- TL bazlı kalsın (opsiyonel)
    mcpUsd AS mcp_usd, -- Global MCP ismi
    smpUsd AS smp_usd, -- Global SMP ismi
    (smpUsd - mcpUsd) AS price_spread_usd,
    system_direction,
    -- Season hesaplamasını buraya ekleyelim ki Mart modelleri hata vermesin
    CASE 
        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Kış'
        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'İlkbahar'
        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Yaz'
        ELSE 'Sonbahar'
    END AS season
FROM {{ source('epias_gold', 'gold_price_spread_analysis') }}