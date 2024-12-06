-- models/staging/stg_disposition_outcomes.sql

{{ config(
    materialized='incremental',
    unique_key='DISPOSITION_OUTCOME_ID'
) }}

with source as (

    select 
        trim(upper(DISPOSITION_OUTCOME_ID)) as DISPOSITION_OUTCOME_ID,
        trim(upper(DISPOSITION_OUTCOME)) as DISPOSITION_OUTCOME,
        trim(upper(RETURN_ITEM_ID)) as RETURN_ITEM_ID,
        to_timestamp(UPDATED_AT) as LAST_UPDATE_TIMESTAMP,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'disposition_outcomes') }}

),

validated as (

    select 
        DISPOSITION_OUTCOME_ID,
        DISPOSITION_OUTCOME,
        RETURN_ITEM_ID,
        LAST_UPDATE_TIMESTAMP,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where DISPOSITION_OUTCOME_ID is not null
      and regexp_like(DISPOSITION_OUTCOME_ID, '^DO-[0-9]+$')  -- Example format
      and DISPOSITION_OUTCOME in ('APPROVED', 'REJECTED', 'REPAIRED')  -- Predefined standards
)

select * from validated
