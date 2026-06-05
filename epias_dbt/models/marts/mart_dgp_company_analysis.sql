{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- DGP (Dengeleme Güç Piyasası) şirket bazlı YAL / YAT analizi.
-- Hangi şirket sisteme ne kadar yük alımı (YAL) ve yük atımı (YAT) sağladı?
-- SMF'yi kimin belirlediğini ve DGP'den kimin kazandığını gösterir.

WITH yal AS (
    SELECT
        date,
        hour,
        organization_id,
        up_regulation_delivered_mwh   AS yal_delivered_mwh,
        up_regulation_zero_mwh        AS yal_zero_price_mwh,
        up_regulation_one_mwh         AS yal_one_coded_mwh,
        net_mwh                       AS yal_net_mwh
    FROM {{ ref('stg_order_up') }}
    WHERE organization_id IS NOT NULL
),
yat AS (
    SELECT
        date,
        hour,
        organization_id,
        down_regulation_delivered_mwh AS yat_delivered_mwh,
        down_regulation_zero_mwh      AS yat_zero_price_mwh,
        net_mwh                       AS yat_net_mwh
    FROM {{ ref('stg_order_down') }}
    WHERE organization_id IS NOT NULL
),
companies AS (
    SELECT organization_id, organization_name, organization_code
    FROM {{ ref('stg_participants') }}
)

SELECT
    COALESCE(u.date, d.date)                    AS date,
    COALESCE(u.hour, d.hour)                    AS hour,
    COALESCE(u.organization_id, d.organization_id) AS organization_id,
    c.organization_name,
    c.organization_code,
    COALESCE(u.yal_delivered_mwh, 0.0)          AS yal_delivered_mwh,
    COALESCE(u.yal_zero_price_mwh, 0.0)         AS yal_zero_price_mwh,
    COALESCE(u.yal_net_mwh, 0.0)                AS yal_net_mwh,
    COALESCE(d.yat_delivered_mwh, 0.0)          AS yat_delivered_mwh,
    COALESCE(d.yat_zero_price_mwh, 0.0)         AS yat_zero_price_mwh,
    COALESCE(d.yat_net_mwh, 0.0)                AS yat_net_mwh,
    -- Net DGP pozisyonu: YAL - YAT (pozitif = net YAL sağlayıcı)
    COALESCE(u.yal_delivered_mwh, 0.0)
        - COALESCE(d.yat_delivered_mwh, 0.0)   AS net_dgp_mwh
FROM yal u
FULL OUTER JOIN yat d
    ON u.date = d.date AND u.hour = d.hour AND u.organization_id = d.organization_id
LEFT JOIN companies c
    ON COALESCE(u.organization_id, d.organization_id) = c.organization_id
