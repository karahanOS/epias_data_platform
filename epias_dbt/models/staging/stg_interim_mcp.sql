{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

-- K.PTF (Kesinleşmemiş Piyasa Takas Fiyatı) — itiraz süreci tamamlanmamış GÖP
-- fiyatı. Available roughly a day BEFORE the final PTF (stg_pricing): the
-- day-ahead auction result is published here as soon as it clears (~14:00
-- TRT the day before delivery), while stg_pricing withholds the same hour
-- until delivery day's own ~14:00 (appeal window). Empirically (6 sampled
-- historical dates, 2026-05 to 2026-08) interim always matches final exactly
-- once settled — see mart_ptf_realized.sql, which is the intended consumer.
--
-- Column mapping (EPIAS API actual field name → dbt alias):
--   marketTradePrice → interim_ptf_try (named differently from stg_pricing's
--   ptf_try on purpose — this value hasn't cleared the appeal window yet and
--   must never be silently substituted for it in training features).
--
-- Same UTC->Turkish-local (date,hour) conversion as stg_pricing.sql — see
-- that file's header comment for why this matters for every downstream JOIN.
SELECT
    DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(marketTradePrice AS FLOAT64) AS interim_ptf_try
FROM {{ source('silver', 'interim_mcp') }} AS s

{% if is_incremental() %}
  WHERE DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul'),
               EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul')
  ORDER BY s.date DESC
) = 1
