-- assert_ptf_positive.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- PTF değerleri negatif olmamalı.
-- Sıfır PTF kabul edilebilir (RES fazlası dönemleri) ama < 0 veri hatası.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT date, hour, ptf_try
FROM {{ ref('stg_pricing') }}
WHERE ptf_try < 0
  AND date >= '2025-01-01'
