{{ config(materialized='incremental', unique_key='location_id') }}

with source as (
    select 
        location_id,
        load_date,
        record_source
    from {{ ref('stg_inventory_locations') }}
),

deduped as (
    select 
        location_id,
        max(load_date) as load_date,
        max(record_source) as record_source
    from source
    group by location_id
)

select 
    location_id,
    load_date,
    record_source
from deduped