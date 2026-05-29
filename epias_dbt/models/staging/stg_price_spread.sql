{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH pricing AS (
    SELECT * FROM {{ ref('stg_pricing') }}
)

SELECT
    date,
    hour,
    ptf_try AS ptf,
    smf_try AS smf,
    (ptf_try - smf_try) AS price_spread,
    system_direction
FROM pricing

{% if is_incremental() %}
  WHERE date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
