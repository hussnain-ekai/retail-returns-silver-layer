-- models/staging/stg_partner_skus.sql

{{ config(
    materialized='incremental',
    unique_key='PARTNER_SKU_ID'
) }}

with source as (

    select 
        trim(upper(PARTNER_SKU_ID)) as PARTNER_SKU_ID,
        trim(upper(PARTNER_NAME)) as PARTNER_NAME,
        trim(upper(PARTNER_SKU)) as PARTNER_SKU,
        trim(upper(INTERNAL_SKU)) as INTERNAL_SKU,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'partner_skus') }}

),

validated as (

    select 
        PARTNER_SKU_ID,
        PARTNER_NAME,
        PARTNER_SKU,
        INTERNAL_SKU,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where PARTNER_SKU_ID is not null
      and length(PARTNER_SKU_ID) <= 20  -- Example length constraint
)

select * from validated
