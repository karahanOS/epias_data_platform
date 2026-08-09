{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- DPP kaynağı aslında KGÜP'tür (Kesinleşmiş Günlük Üretim Planı), BGÜP değil —
-- EPİAŞ'ın kendi başlığı "5.71. Kesinleşmiş Günlük Üretim Planı (KGÜP) Listeleme
-- Servisi" (bkz. plans/07-company-level-market-activity-kgup.md). Model adı
-- (stg_dpp) ve kaynak tablo adı (silver.dpp) API endpoint adını yansıtıyor,
-- değiştirilmedi — sadece bu yorum ve get_dpp()'nin docstring'i düzeltildi.
-- Uzlaştırma dönemine ait SONRAKİ katman (KUDÜP) için stg_sbfgp.sql kullanılmalıdır.
WITH deduped AS (
    SELECT
        CAST(date AS DATE)                                    AS date,
        CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64)    AS hour,
        CAST(toplam      AS FLOAT64) AS total_planned_mwh,
        CAST(dogalgaz    AS FLOAT64) AS natural_gas_mwh,
        CAST(ruzgar      AS FLOAT64) AS wind_mwh,
        CAST(linyit      AS FLOAT64) AS lignite_mwh,
        CAST(tasKomur    AS FLOAT64) AS hard_coal_mwh,
        CAST(ithalKomur  AS FLOAT64) AS imported_coal_mwh,
        CAST(fuelOil     AS FLOAT64) AS fueloil_mwh,
        CAST(jeotermal   AS FLOAT64) AS geothermal_mwh,
        CAST(barajli     AS FLOAT64) AS dam_hydro_mwh,
        CAST(nafta       AS FLOAT64) AS naphtha_mwh,
        CAST(biokutle    AS FLOAT64) AS biomass_mwh,
        CAST(akarsu      AS FLOAT64) AS river_hydro_mwh,
        CAST(gunes       AS FLOAT64) AS solar_mwh,
        CAST(diger       AS FLOAT64) AS other_mwh,
        -- Cross-Hive-partition duplication (same class as stg_generation/stg_pricing etc.):
        -- raw `date` is a full UTC timestamp and `time` is the Turkish-local hour label,
        -- so the last few UTC hours of a day land in the NEXT day's local-hour partition
        -- too, duplicating (date,hour) 00/01/02 across two adjacent GCS partitions.
        ROW_NUMBER() OVER(
            PARTITION BY CAST(date AS DATE), CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64)
            ORDER BY _record_hash DESC
        ) AS rn
    FROM {{ source('silver', 'dpp') }}
)
SELECT * EXCEPT(rn) FROM deduped WHERE rn = 1

{% if is_incremental() %}
  AND date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
