{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_ptf_usd: PTF Dolar Cinsinden Normalize Fiyat
--
-- Kaynak: EPIAS /v1/markets/dam/data/mcp → mcpUsd alanı
--   stg_pricing.ptf_usd = EPIAS'ın o saatte kullandığı resmi kur
--   stg_pricing.ptf_eur = EUR karşılığı
--
-- TCMB kuruna gerek yok — EPIAS piyasa saatine ait USD değerini doğrudan verir.
--
-- Kullanım alanları:
--   1. Enflasyon analizi — TL değer kaybından arındırılmış PTF karşılaştırması
--   2. Uluslararası karşılaştırma — enerji raporlarında $/MWh standarttır
--   3. Implicit kur çıkarımı — ptf_try / ptf_usd = EPIAS'ın kullandığı kur
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    date,
    hour,

    -- TL bazlı
    ptf_try,

    -- USD bazlı (EPIAS'tan direkt)
    ptf_usd,

    -- EUR bazlı
    ptf_eur,

    -- EPIAS'ın kullandığı implicit USD/TRY kuru
    SAFE_DIVIDE(ptf_try, ptf_usd)                      AS implicit_usdtry,

    -- Günlük ortalamalar
    AVG(ptf_try) OVER (PARTITION BY date)               AS ptf_try_daily_avg,
    AVG(ptf_usd) OVER (PARTITION BY date)               AS ptf_usd_daily_avg,

    -- Yıllık karşılaştırma için YYYY-MM granülü
    FORMAT_DATE('%Y-%m', date)                          AS year_month

FROM {{ ref('stg_pricing') }}
WHERE ptf_usd IS NOT NULL
