{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_gip_hourly_reference: hourly GİP (intraday market) volume-weighted
-- average price — the reference "true up now" cost/value used by
-- mart_smf_trading_signal.sql's expected-value comparison against letting an
-- imbalance settle at (predicted) SMF. Sourced from stg_idm_transactions,
-- already ingested at per-transaction granularity (ADR-08).
--
-- VWAP, not a bid/ask quote: this is a realized-average proxy for "what GİP
-- cost this hour," not a live order-book price. Adequate for backtesting
-- (it's exactly what actually happened), understates real execution cost for
-- a live signal (no spread/slippage) — see ADR-08's Consequences section.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    date,
    hour,
    SAFE_DIVIDE(SUM(price_try * quantity_mwh), SUM(quantity_mwh)) AS gip_vwap_try,
    SUM(quantity_mwh)                                             AS gip_volume_mwh,
    COUNT(*)                                                      AS gip_transaction_count
FROM {{ ref('stg_idm_transactions') }}
{% if is_incremental() %}
  WHERE date >= DATE_SUB((SELECT MAX(date) FROM {{ this }}), INTERVAL 1 DAY)
{% endif %}
GROUP BY date, hour
