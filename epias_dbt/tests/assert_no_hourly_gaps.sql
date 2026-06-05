-- assert_no_hourly_gaps.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- 2025-01-01'den bu yana stg_pricing'de hiç saat boşluğu olmamalı.
-- dbt test başarılı = sorgu 0 satır döner (gap yok)
-- dbt test başarısız = boşluk olan (date, hour) çiftleri döner
--
-- Kullanım: dbt test --select assert_no_hourly_gaps
-- ─────────────────────────────────────────────────────────────────────────────

WITH expected_hours AS (
    SELECT
        cal_date  AS date,
        h         AS hour
    FROM
        UNNEST(GENERATE_DATE_ARRAY('2025-01-01', CURRENT_DATE())) AS cal_date,
        UNNEST(GENERATE_ARRAY(0, 23)) AS h
),
actual_hours AS (
    SELECT DISTINCT date, hour
    FROM {{ ref('stg_pricing') }}
    WHERE date >= '2025-01-01'
)

SELECT
    e.date,
    e.hour,
    'stg_pricing' AS source_table
FROM expected_hours e
LEFT JOIN actual_hours a USING(date, hour)
WHERE a.date IS NULL
  -- Son 2 günü hariç tut (henüz API'den gelmemiş olabilir)
  AND e.date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
ORDER BY e.date, e.hour
