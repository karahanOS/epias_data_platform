{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_shift_optimizer: Vardiya bazlı elektrik maliyeti optimizasyonu
--
-- Enerji yoğun sanayi tesisleri için: klasik 3 vardiyanın (00-08/08-16/16-24)
-- ortalama PTF'si, ve günün en ucuz/en pahalı kesintisiz 8 saatlik penceresi
-- (vardiya başlangıcı esnek olsaydı nereye konulmalıydı sorusuna cevap).
--
-- max_shift_saving_per_mwh = enerji yoğun prosesi en pahalı yerine en ucuz
-- 8 saatlik pencereye kaydırmanın MWh başına potansiyel tasarrufu.
-- ─────────────────────────────────────────────────────────────────────────────

WITH hourly AS (
    SELECT date, hour, ptf_try FROM {{ ref('mart_price_analysis') }}
),

shifts AS (
    SELECT
        date,
        AVG(CASE WHEN hour BETWEEN 0  AND 7  THEN ptf_try END) AS vardiya_00_08_avg_ptf,
        AVG(CASE WHEN hour BETWEEN 8  AND 15 THEN ptf_try END) AS vardiya_08_16_avg_ptf,
        AVG(CASE WHEN hour BETWEEN 16 AND 23 THEN ptf_try END) AS vardiya_16_24_avg_ptf
    FROM hourly
    GROUP BY date
),

-- Her saat için "bu saatten başlayan 8 saatlik pencere"nin ort. PTF'si.
-- window_hour_count ile sadece TAM 8 saat içeren pencereler tutulur — 17-23
-- arası başlangıçlar günün sonuna taşıp eksik pencere üretir, aşağıda elenir.
windows AS (
    SELECT
        date,
        hour AS window_start_hour,
        AVG(ptf_try) OVER (
            PARTITION BY date ORDER BY hour
            ROWS BETWEEN CURRENT ROW AND 7 FOLLOWING
        ) AS window_avg_ptf,
        COUNT(*) OVER (
            PARTITION BY date ORDER BY hour
            ROWS BETWEEN CURRENT ROW AND 7 FOLLOWING
        ) AS window_hour_count
    FROM hourly
),

full_windows AS (
    SELECT date, window_start_hour, window_avg_ptf
    FROM windows
    WHERE window_hour_count = 8
),

cheapest AS (
    SELECT date, window_start_hour AS cheapest_8h_start_hour, window_avg_ptf AS cheapest_8h_avg_ptf
    FROM full_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY window_avg_ptf ASC, window_start_hour ASC) = 1
),

priciest AS (
    SELECT date, window_start_hour AS priciest_8h_start_hour, window_avg_ptf AS priciest_8h_avg_ptf
    FROM full_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY window_avg_ptf DESC, window_start_hour ASC) = 1
)

SELECT
    s.date,
    s.vardiya_00_08_avg_ptf,
    s.vardiya_08_16_avg_ptf,
    s.vardiya_16_24_avg_ptf,
    c.cheapest_8h_start_hour,
    c.cheapest_8h_avg_ptf,
    p.priciest_8h_start_hour,
    p.priciest_8h_avg_ptf,
    ROUND(p.priciest_8h_avg_ptf - c.cheapest_8h_avg_ptf, 2) AS max_shift_saving_per_mwh

FROM shifts s
LEFT JOIN cheapest c ON c.date = s.date
LEFT JOIN priciest p ON p.date = s.date
