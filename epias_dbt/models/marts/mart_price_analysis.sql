{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_price_analysis: Saatlik PTF / SMF fiyat analizi
--
-- Yeni kolonlar (Sprint A):
--   price_block              — Puant/Gündüz/Gece (EPİAŞ haftalık rapor segmentleri)
--   daily_ptf_range          — O günün MAX(PTF) − MIN(PTF) (pencere fonksiyonu)
--   deficit_settlement_price — GREATEST(PTF, SMF) × 1.03  (DUY Açık takas)
--   surplus_settlement_price — LEAST(PTF, SMF) × 0.97     (DUY Fazla takas)
--   cap_proximity_pct        — PTF / 4.500 × 100           (tavan yakınlık %)
--   is_zero_price            — PTF = 0 saati (güneş fazlası / düşük talep sinyali)
--
-- Tavan: 4.500 TL/MWh (EPDK Karar 14459, 04 Nisan 2026)
-- Blok tanımları (EPİAŞ): Gece 22–05, Gündüz 06–16, Puant 17–21
-- ─────────────────────────────────────────────────────────────────────────────

WITH base AS (
    SELECT
        p.date,
        p.hour,
        p.ptf_try,
        s.smf_try,
        (p.ptf_try - COALESCE(s.smf_try, p.ptf_try)) AS price_spread,

        CASE
            WHEN EXTRACT(MONTH FROM p.date) IN (12, 1, 2) THEN 'Kış'
            WHEN EXTRACT(MONTH FROM p.date) IN (3, 4, 5)  THEN 'İlkbahar'
            WHEN EXTRACT(MONTH FROM p.date) IN (6, 7, 8)  THEN 'Yaz'
            ELSE 'Sonbahar'
        END AS season,

        -- EPİAŞ fiyat blokları (Türkiye yerel saati — UTC+3, DST yok)
        CASE
            WHEN p.hour BETWEEN 17 AND 21 THEN 'Puant'
            WHEN p.hour BETWEEN 6  AND 16 THEN 'Gündüz'
            ELSE 'Gece'                          -- 0–5 ve 22–23
        END AS price_block,

        -- DUY takas fiyatları (k=ℓ=0.03)
        ROUND(
            GREATEST(p.ptf_try, COALESCE(s.smf_try, p.ptf_try)) * 1.03,
            4
        ) AS deficit_settlement_price,

        ROUND(
            LEAST(p.ptf_try, COALESCE(s.smf_try, p.ptf_try)) * 0.97,
            4
        ) AS surplus_settlement_price,

        -- Tavan yakınlığı — 4.500 TL/MWh (Nisan 2026+)
        ROUND(p.ptf_try / 4500.0 * 100.0, 2) AS cap_proximity_pct,

        -- Sıfır fiyat saati (güneş fazlası / düşük talep sinyali)
        (p.ptf_try = 0) AS is_zero_price

    FROM {{ ref('stg_pricing') }} p
    LEFT JOIN {{ ref('stg_smf') }} s ON p.date = s.date AND p.hour = s.hour
)

SELECT
    b.*,

    -- Günlük PTF aralığı (MAX − MIN pencere fonksiyonu)
    ROUND(
        MAX(b.ptf_try) OVER (PARTITION BY b.date)
        - MIN(b.ptf_try) OVER (PARTITION BY b.date),
        4
    ) AS daily_ptf_range

FROM base b
