{{ config(
    materialized='incremental',
    unique_key='disposition_outcome_id'
) }}

with source as (
    select
        disposition_outcome_id,
        disposition_outcome,
        last_update_timestamp,
        load_date,
        record_source
    from {{ ref('stg_disposition_outcomes') }}
)

select
    disposition_outcome_id,
    disposition_outcome,
    last_update_timestamp,
    load_date,
    record_source
from source
{% if is_incremental() %}
    where disposition_outcome_id not in (select disposition_outcome_id from {{ this }})
{% endif %}