-- models/staging/stg_refurbishment_records.sql

{{ config(
    materialized='incremental',
    unique_key='REFURB_ID'
) }}

with source as (

    select 
        trim(upper(REFURB_ID)) as REFURB_ID,
        trim(upper(RETURN_ITEM_ID)) as RETURN_ITEM_ID,
        trim(upper(PARTNER_SKU_ID)) as PARTNER_SKU_ID,
        trim(upper(REFURB_GRADE)) as REFURB_GRADE,
        CAST(COST_OF_REFURB AS decimal(10,2)) as COST_OF_REFURB,
        to_date(REFURB_DATE) as REFURB_DATE,
        CAST(WARRANTY_PERIOD_DAYS AS int) as WARRANTY_PERIOD_DAYS,
        trim(upper(NOTES)) as NOTES,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'refurbishment_records') }}

),

validated as (

    select 
        REFURB_ID,
        RETURN_ITEM_ID,
        PARTNER_SKU_ID,
        REFURB_GRADE,
        COST_OF_REFURB,
        REFURB_DATE,
        WARRANTY_PERIOD_DAYS,
        NOTES,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where REFURB_ID is not null
      and regexp_like(REFURB_ID, '^RF-[0-9]+$')  -- Example format
)

select * from validated
