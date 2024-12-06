{{ config(materialized='incremental', unique_key='order_external_id') }}

with source as (
    select 
        order_external_id,
        load_date,
        record_source
    from {{ ref('stg_orders') }}
),

deduped as (
    select 
        order_external_id,
        max(load_date) as load_date,
        max(record_source) as record_source
    from source
    group by order_external_id
)

select 
    order_external_id,
    load_date,
    record_source
from deduped