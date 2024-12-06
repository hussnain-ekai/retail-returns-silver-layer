-- models/staging/stg_recycling_batches.sql

{{ config(
    materialized='incremental',
    unique_key='BATCH_ID'
) }}

with source as (

    select 
        trim(upper(BATCH_ID)) as BATCH_ID,
        trim(upper(PROCESSOR_NAME)) as PROCESSOR_NAME,
        trim(upper(MATERIAL_TYPE)) as MATERIAL_TYPE,
        CAST(WEIGHT_KG AS decimal(10,2)) as WEIGHT_KG,
        to_date(PROCESSING_DATE) as PROCESSING_DATE,
        CAST(DISPOSAL_COST AS decimal(10,2)) as DISPOSAL_COST,
        CAST(CERTIFICATE_OF_DESTRUCTION AS boolean) as CERTIFICATE_OF_DESTRUCTION,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'recycling_batches') }}

),

validated as (

    select 
        BATCH_ID,
        PROCESSOR_NAME,
        MATERIAL_TYPE,
        WEIGHT_KG,
        PROCESSING_DATE,
        DISPOSAL_COST,
        CERTIFICATE_OF_DESTRUCTION,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where BATCH_ID is not null
      and PROCESSING_DATE <= current_date()  -- Ensure no future dates
)

select * from validated
