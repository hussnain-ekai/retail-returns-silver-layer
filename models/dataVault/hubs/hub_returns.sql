{{ config(materialized='incremental', unique_key='return_id') }}

with source as (
    select 
        return_id,
        load_date,
        record_source
    from {{ ref('stg_returns') }}
),

deduped as (
    select 
        return_id,
        max(load_date) as load_date,
        max(record_source) as record_source
    from source
    group by return_id
)

select 
    return_id,
    load_date,
    record_source
from deduped