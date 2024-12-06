{{ config(materialized='incremental', unique_key='sku') }}

with source as (
    select 
        sku,
        load_date,
        record_source
    from {{ ref('stg_product_master') }}
),

deduped as (
    select 
        sku,
        max(load_date) as load_date,
        max(record_source) as record_source
    from source
    group by sku
)

select 
    sku,
    load_date,
    record_source
from deduped