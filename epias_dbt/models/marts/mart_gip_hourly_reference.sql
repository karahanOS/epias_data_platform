{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "date", "data_type": "date"}
) }}

{% set cutoff = (run_started_at.date() - modules.datetime.timedelta(days=7)).strftime('%Y-%m-%d') %}

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
--
-- Fixed 2026-08-25 (cost investigation): was incremental_strategy='merge',
-- which — like stg_idm_transactions before it — scans the ENTIRE
-- stg_idm_transactions table every run regardless of predicate (confirmed:
-- BigQuery's MERGE doesn't prune, full stop, same finding as that earlier
-- fix). Switched to insert_overwrite, the same fix already validated there.
-- Unlike that fix, no separate intermediate model is needed here —
-- stg_idm_transactions is now a NATIVE partitioned table (not external), and
-- insert_overwrite's read step is a plain SELECT, not a MERGE, so a literal
-- WHERE filter prunes correctly on its own.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    date,
    hour,
    SAFE_DIVIDE(SUM(price_try * quantity_mwh), SUM(quantity_mwh)) AS gip_vwap_try,
    SUM(quantity_mwh)                                             AS gip_volume_mwh,
    COUNT(*)                                                      AS gip_transaction_count
FROM {{ ref('stg_idm_transactions') }}
{% if is_incremental() %}
WHERE date >= DATE('{{ cutoff }}')
{% endif %}
GROUP BY date, hour
