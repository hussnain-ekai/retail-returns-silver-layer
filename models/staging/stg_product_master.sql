-- models/staging/stg_product_master.sql

{{ config(
    materialized='incremental',
    unique_key='SKU'
) }}

with source as (

    select 
        trim(upper(SKU)) as SKU,
        trim(upper(PRODUCT_NAME)) as PRODUCT_NAME,
        trim(upper(CATEGORY)) as CATEGORY,
        trim(upper(BRAND)) as BRAND,
        CAST(COST_BASIS AS decimal(10,2)) as COST_BASIS,
        trim(upper(SUPPLIER_ID)) as SUPPLIER_ID,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'product_master') }}

),

validated as (

    select 
        SKU,
        PRODUCT_NAME,
        CATEGORY,
        BRAND,
        COST_BASIS,
        SUPPLIER_ID,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where SKU is not null
      and length(SKU) <= 50  -- Example length constraint
)

select * from validated
