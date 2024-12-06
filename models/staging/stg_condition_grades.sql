-- models/staging/stg_condition_grades.sql

{{ config(
    materialized='incremental',
    unique_key='GRADE_ID'
) }}

with source as (

    select 
        trim(upper(GRADE_ID)) as GRADE_ID,
        trim(upper(GRADE_CODE)) as GRADE_CODE,
        trim(upper(DESCRIPTION)) as DESCRIPTION,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'condition_grades') }}

),

validated as (

    select 
        GRADE_ID,
        GRADE_CODE,
        DESCRIPTION,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where GRADE_ID is not null
      and regexp_like(GRADE_ID, '^[0-9]+$')  -- Example format
)

select * from validated
