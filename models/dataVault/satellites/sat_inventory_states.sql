{{ config(
    materialized='incremental',
    unique_key='inventory_state_id'
) }}

with source as (
    select
        inventory_state_id,
        location_id,
        current_condition_grade_id,
        current_status,
        last_update_timestamp,
        load_date,
        record_source
    from {{ ref('stg_inventory_states') }}
)

select
    inventory_state_id,
    location_id,
    current_condition_grade_id,
    current_status,
    last_update_timestamp,
    load_date,
    record_source
from source
{% if is_incremental() %}
    where inventory_state_id not in (select inventory_state_id from {{ this }})
{% endif %}