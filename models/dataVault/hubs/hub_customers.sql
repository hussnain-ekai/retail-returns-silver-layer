{{ config(materialized='incremental', unique_key='customer_id') }}

with source as (
    select 
        customer_id,
        load_date,
        record_source
    from {{ ref('stg_customers') }}
),

deduped as (
    select 
        customer_id,
        max(load_date) as load_date,
        max(record_source) as record_source
    from source
    group by customer_id
)

select 
    customer_id,
    load_date,
    record_source
from deduped