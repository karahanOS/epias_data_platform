{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_imbalance_cost: Saatlik dengesizlik maliyeti (DUY takas fiyat kuralları)
--
-- DUY Madde 28 — Takas fiyatı formülü:
--   Enerji Açığı  (net_imbalance_mwh < 0) → GREATEST(PTF, SMF) × 1.03
--   Enerji Fazlası (net_imbalance_mwh > 0) → LEAST(PTF, SMF)    × 0.97
--   k = ℓ = 0.03  (EPDK, Mayıs 2015 — yön bağımsız katsayılar)
--   Tavan fiyat: 4.500 TL/MWh (EPDK Karar 14459, 04 Nisan 2026;
--                               öncesi 3.400 TL/MWh, Nisan 2025'ten itibaren)
--
-- NOT: Eski formül (net_imbalance_mwh × smf_try) YANLIŞTIR.
--      SMF tek taraflı tavan fiyatı değildir; DUY katsayılı çift-taraflı
--      hesaplama gerektirir.
--
-- Kolon açıklamaları:
--   imbalance_direction        — Açık / Fazla / Dengede
--   deficit_settlement_price   — Açık takas fiyatı  (TL/MWh)
--   surplus_settlement_price   — Fazla takas fiyatı (TL/MWh)
--   settlement_price_try       — Geçerli takas fiyatı (yöne göre)
--   total_imbalance_cost_try   — Net maliyet (açık: negatif = kayıp;
--                                 fazla: pozitif = düşük gelir)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    i.date,
    i.hour,
    i.net_imbalance_mwh,

    -- ── Dengesizlik Yönü ────────────────────────────────────────────────────
    CASE
        WHEN i.net_imbalance_mwh < 0 THEN 'Enerji Açığı (Negatif)'
        WHEN i.net_imbalance_mwh > 0 THEN 'Enerji Fazlası (Pozitif)'
        ELSE 'Dengede'
    END AS imbalance_direction,

    p.ptf_try,
    s.smf_try,

    -- ── DUY Takas Fiyatları ─────────────────────────────────────────────────
    -- SMF NULL ise (veri yok) PTF'yi hem altta hem üstte kullan → %3 fark korunur.
    ROUND(
        GREATEST(p.ptf_try, COALESCE(s.smf_try, p.ptf_try)) * 1.03,
        4
    ) AS deficit_settlement_price_try,

    ROUND(
        LEAST(p.ptf_try, COALESCE(s.smf_try, p.ptf_try)) * 0.97,
        4
    ) AS surplus_settlement_price_try,

    -- Yöne göre geçerli takas fiyatı
    CASE
        WHEN i.net_imbalance_mwh < 0
            THEN ROUND(GREATEST(p.ptf_try, COALESCE(s.smf_try, p.ptf_try)) * 1.03, 4)
        WHEN i.net_imbalance_mwh > 0
            THEN ROUND(LEAST(p.ptf_try,    COALESCE(s.smf_try, p.ptf_try)) * 0.97, 4)
        ELSE p.ptf_try
    END AS settlement_price_try,

    -- ── Toplam Dengesizlik Maliyeti (TL) ───────────────────────────────────
    -- Açık: negatif MWh × daha yüksek fiyat → büyük negatif sayı (maliyet)
    -- Fazla: pozitif MWh × daha düşük fiyat → daha az gelir
    CASE
        WHEN i.net_imbalance_mwh < 0
            THEN ROUND(
                i.net_imbalance_mwh
                * GREATEST(p.ptf_try, COALESCE(s.smf_try, p.ptf_try)) * 1.03,
                2
            )
        WHEN i.net_imbalance_mwh > 0
            THEN ROUND(
                i.net_imbalance_mwh
                * LEAST(p.ptf_try, COALESCE(s.smf_try, p.ptf_try)) * 0.97,
                2
            )
        ELSE 0
    END AS total_imbalance_cost_try

FROM {{ ref('stg_imbalance') }} i
LEFT JOIN {{ ref('stg_pricing') }} p ON i.date = p.date AND i.hour = p.hour
LEFT JOIN {{ ref('stg_smf') }}    s ON i.date = s.date AND i.hour = s.hour
