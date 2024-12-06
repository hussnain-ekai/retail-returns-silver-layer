{{ config(materialized='incremental', unique_key='link_id') }}

WITH source AS (
    SELECT
        MD5(CONCAT(CAST(dr.RULE_ID AS STRING), CAST(pm.SKU AS STRING), CAST(cg.GRADE_ID AS STRING))) AS link_id,
        dr.RULE_ID,
        pm.SKU,
        cg.GRADE_ID,
        CURRENT_TIMESTAMP() AS load_date,
        'telelink_dbt' AS record_source
    FROM {{ source('telelink_bronze', 'disposition_rules') }} dr
    JOIN {{ ref('hub_product_master') }} pm ON dr.SKU = pm.SKU
    JOIN {{ ref('hub_condition_grades') }} cg ON dr.CONDITION_GRADE_ID = cg.GRADE_ID
)

SELECT
    link_id,
    RULE_ID,
    SKU,
    GRADE_ID,
    load_date,
    record_source
FROM source

{% if is_incremental() %}
  WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}