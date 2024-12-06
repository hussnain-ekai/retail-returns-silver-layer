-- models/staging/stg_disposition_rules.sql

{{ config(
    materialized='incremental',
    unique_key='RULE_ID'
) }}

with source as (

    select 
        trim(upper(RULE_ID)) as RULE_ID,
        trim(upper(SKU)) as SKU,
        trim(upper(CONDITION_GRADE_ID)) as CONDITION_GRADE_ID,
        trim(upper(PREFERRED_CHANNEL)) as PREFERRED_CHANNEL,
        CAST(DECISION_PRIORITY AS int) as DECISION_PRIORITY,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'disposition_rules') }}

),

validated as (

    select 
        RULE_ID,
        SKU,
        CONDITION_GRADE_ID,
        PREFERRED_CHANNEL,
        DECISION_PRIORITY,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where RULE_ID is not null
      and regexp_like(RULE_ID, '^DR-[0-9]+$')  -- Example format
)

select * from validated
