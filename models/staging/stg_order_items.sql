-- models/staging/stg_order_items.sql

{{ config(
    materialized='incremental',
    unique_key='ORDER_ITEM_ID'
) }}

with source as (

    select 
        trim(upper(ORDER_ITEM_ID)) as ORDER_ITEM_ID,
        trim(upper(ORDER_ID)) as ORDER_ID,
        trim(upper(SKU)) as SKU,
        CAST(QUANTITY AS int) as QUANTITY,
        CAST(UNIT_PRICE AS decimal(10,2)) as UNIT_PRICE,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'order_items') }}

),

validated as (

    select 
        ORDER_ITEM_ID,
        ORDER_ID,
        SKU,
        QUANTITY,
        UNIT_PRICE,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where ORDER_ITEM_ID is not null
      and regexp_like(ORDER_ITEM_ID, '^OI-[0-9]+$')  -- Example format
)

select * from validated
