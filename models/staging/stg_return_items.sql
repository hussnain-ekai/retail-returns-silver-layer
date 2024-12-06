-- models/staging/stg_return_items.sql

{{ config(
    materialized='incremental',
    unique_key='RETURN_ITEM_ID'
) }}

with source as (

    select 
        trim(upper(RETURN_ITEM_ID)) as RETURN_ITEM_ID,
        trim(upper(RETURN_ID)) as RETURN_ID,
        trim(upper(ORDER_ITEM_ID)) as ORDER_ITEM_ID,
        trim(upper(CONDITION_RECEIVED)) as CONDITION_RECEIVED,
        trim(upper(REASON_CODE)) as REASON_CODE,
        trim(upper(GRADE_ID)) as GRADE_ID,
        trim(upper(DISPOSITION_OUTCOME_ID)) as DISPOSITION_OUTCOME_ID,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'return_items') }}

),

validated as (

    select 
        RETURN_ITEM_ID,
        RETURN_ID,
        ORDER_ITEM_ID,
        CONDITION_RECEIVED,
        REASON_CODE,
        GRADE_ID,
        DISPOSITION_OUTCOME_ID,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where RETURN_ITEM_ID is not null
      and regexp_like(RETURN_ITEM_ID, '^RI-[0-9]+$')  -- Example format
)

select * from validated
