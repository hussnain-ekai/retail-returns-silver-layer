{{ config(materialized='incremental', unique_key='link_id') }}

WITH source AS (
    SELECT
        MD5(CONCAT(CAST(r.RETURN_ID AS STRING), CAST(oi.ORDER_ITEM_ID AS STRING), CAST(c.GRADE_ID AS STRING))) AS link_id,
        h_r.RETURN_ID,
        h_oi.ORDER_ITEM_ID,
        h_c.GRADE_ID,
        CURRENT_TIMESTAMP() AS load_date,
        'telelink_dbt' AS record_source
    FROM {{ source('telelink_bronze', 'return_items') }} r
    JOIN {{ ref('hub_returns') }} h_r ON r.RETURN_ID = h_r.RETURN_ID
    JOIN {{ ref('hub_order_items') }} h_oi ON r.ORDER_ITEM_ID = h_oi.ORDER_ITEM_ID
    JOIN {{ ref('hub_condition_grades') }} h_c ON r.GRADE_ID = h_c.GRADE_ID
)

SELECT
    link_id,
    RETURN_ID,
    ORDER_ITEM_ID,
    GRADE_ID,
    load_date,
    record_source
FROM source

{% if is_incremental() %}
  WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}