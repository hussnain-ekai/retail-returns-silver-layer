-- models/staging/stg_inventory_states.sql

{{ config(
    materialized='incremental',
    unique_key='INVENTORY_STATE_ID'
) }}

with source as (

    select 
        trim(upper(INVENTORY_STATE_ID)) as INVENTORY_STATE_ID,
        trim(upper(LOCATION_ID)) as LOCATION_ID,
        trim(upper(CURRENT_CONDITION_GRADE_ID)) as CURRENT_CONDITION_GRADE_ID,
        trim(upper(CURRENT_STATUS)) as CURRENT_STATUS,
        to_timestamp(UPDATED_AT) as LAST_UPDATE_TIMESTAMP,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'inventory_states') }}

),

validated as (

    select 
        INVENTORY_STATE_ID,
        LOCATION_ID,
        CURRENT_CONDITION_GRADE_ID,
        CURRENT_STATUS,
        LAST_UPDATE_TIMESTAMP,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where INVENTORY_STATE_ID is not null
      and regexp_like(INVENTORY_STATE_ID, '^IS-[0-9]+$')  -- Example format
)

select * from validated
