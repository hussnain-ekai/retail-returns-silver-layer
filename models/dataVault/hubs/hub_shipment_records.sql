{{ config(materialized='incremental', unique_key='shipment_id') }}

with source as (
    select 
        shipment_id,
        load_date,
        record_source
    from {{ ref('stg_shipment_records') }}
),

deduped as (
    select 
        shipment_id,
        max(load_date) as load_date,
        max(record_source) as record_source
    from source
    group by shipment_id
)

select 
    shipment_id,
    load_date,
    record_source
from deduped