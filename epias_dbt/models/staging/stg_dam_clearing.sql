{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

WITH raw_dam AS (
    SELECT * FROM {{ source('silver', 'dam_clearing') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(hour AS INT64) AS hour,
    CAST(date AS TIMESTAMP) AS date_timestamp,
    CAST(matchedBidsQuantity AS FLOAT64) AS matched_bids_mwh,
    CAST(matchedOffersQuantity AS FLOAT64) AS matched_offers_mwh,
    CAST(blockBidQuantity AS FLOAT64) AS block_bid_mwh,
    CAST(blockOfferQuantity AS FLOAT64) AS block_offer_mwh
FROM raw_dam

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}