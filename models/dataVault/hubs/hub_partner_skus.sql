{{ config(materialized='incremental', unique_key='partner_sku_id') }}

with source as (
    select 
        partner_sku_id,
        load_date,
        record_source
    from {{ ref('stg_partner_skus') }}
),

deduped as (
    select 
        partner_sku_id,
        max(load_date) as load_date,
        max(record_source) as record_source
    from source
    group by partner_sku_id
)

select 
    partner_sku_id,
    load_date,
    record_source
from deduped