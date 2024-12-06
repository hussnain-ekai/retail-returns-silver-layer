{{ config(materialized='incremental', unique_key='link_id') }}

WITH source AS (
    SELECT
        MD5(CONCAT(CAST(ft.TRANSACTION_ID AS STRING), CAST(r.RETURN_ID AS STRING))) AS link_id,
        ft.TRANSACTION_ID,
        r.RETURN_ID,
        CURRENT_TIMESTAMP() AS load_date,
        'telelink_dbt' AS record_source
    FROM {{ source('telelink_bronze', 'financial_transactions') }} ft
    JOIN {{ ref('hub_returns') }} r ON ft.RETURN_ID = r.RETURN_ID
)

SELECT
    link_id,
    TRANSACTION_ID,
    RETURN_ID,
    load_date,
    record_source
FROM source

{% if is_incremental() %}
  WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}