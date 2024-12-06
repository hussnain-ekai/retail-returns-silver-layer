-- models/staging/stg_liquidation_listings.sql

{{ config(
    materialized='incremental',
    unique_key='LISTING_ID'
) }}

with source as (

    select 
        trim(upper(LISTING_ID)) as LISTING_ID,
        trim(upper(RETURN_ITEM_ID)) as RETURN_ITEM_ID,
        trim(upper(PARTNER_SKU_ID)) as PARTNER_SKU_ID,
        trim(upper(CONDITION)) as CONDITION,
        CAST(SOLD_PRICE AS decimal(10,2)) as SOLD_PRICE,
        to_date(LISTING_DATE) as LISTING_DATE,
        trim(upper(BUYER_ID)) as BUYER_ID,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'liquidation_listings') }}

),

validated as (

    select 
        LISTING_ID,
        RETURN_ITEM_ID,
        PARTNER_SKU_ID,
        CONDITION,
        SOLD_PRICE,
        LISTING_DATE,
        BUYER_ID,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where LISTING_ID is not null
      and regexp_like(LISTING_ID, '^LL-[0-9]+$')  -- Example format
)

select * from validated
