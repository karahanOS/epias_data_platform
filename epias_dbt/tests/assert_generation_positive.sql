-- assert_generation_positive.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Toplam üretim 0 MWh altına düşmemeli (gece bile bazal yük var).
-- 100 MWh eşiği: Türkiye sistemi hiçbir saatte bu kadar düşmez.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT date, hour, total_generation_mwh
FROM {{ ref('stg_generation') }}
WHERE total_generation_mwh < 100
  AND date >= '2025-01-01'
