{{ config(
    materialized='incremental',
    unique_key='batch_id'
) }}

with source as (
    select
        batch_id,
        processing_time,
        batch_status,
        batch_timestamp,
        disposal_cost,
        load_date,
        record_source
    from {{ ref('stg_recycling_batches') }}
)

select
    batch_id,
    processing_time,
    batch_status,
    batch_timestamp,
    disposal_cost,
    load_date,
    record_source
from source
{% if is_incremental() %}
    where batch_id not in (select batch_id from {{ this }})
{% endif %}