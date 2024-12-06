{{ config(materialized='incremental', unique_key='link_id') }}

WITH source AS (
    SELECT
        MD5(CONCAT(CAST(ll.LISTING_ID AS STRING), CAST(ri.RETURN_ITEM_ID AS STRING), CAST(ps.PARTNER_SKU_ID AS STRING))) AS link_id,
        ll.LISTING_ID,
        ri.RETURN_ITEM_ID,
        ps.PARTNER_SKU_ID,
        CURRENT_TIMESTAMP() AS load_date,
        'telelink_dbt' AS record_source
    FROM {{ source('telelink_bronze', 'liquidation_listings') }} ll
    JOIN {{ ref('hub_return_items') }} ri ON ll.RETURN_ITEM_ID = ri.RETURN_ITEM_ID
    JOIN {{ ref('hub_partner_skus') }} ps ON ll.PARTNER_SKU_ID = ps.PARTNER_SKU_ID
)

SELECT
    link_id,
    LISTING_ID,
    RETURN_ITEM_ID,
    PARTNER_SKU_ID,
    load_date,
    record_source
FROM source

{% if is_incremental() %}
  WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}