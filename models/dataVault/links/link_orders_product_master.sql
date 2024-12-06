{{ config(materialized='incremental', unique_key='link_id') }}

WITH source AS (
    SELECT
        MD5(CONCAT(CAST(o.ORDER_EXTERNAL_ID AS STRING), CAST(p.SKU AS STRING))) AS link_id,
        h_o.ORDER_ID,
        h_p.SKU,
        CURRENT_TIMESTAMP() AS load_date,
        'telelink_dbt' AS record_source
    FROM {{ source('telelink_bronze', 'order_items') }} o
    JOIN {{ ref('hub_orders') }} h_o ON o.ORDER_EXTERNAL_ID = h_o.ORDER_EXTERNAL_ID
    JOIN {{ ref('hub_product_master') }} h_p ON o.SKU = h_p.SKU
)

SELECT
    link_id,
    ORDER_ID,
    SKU,
    load_date,
    record_source
FROM source

{% if is_incremental() %}
  WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}