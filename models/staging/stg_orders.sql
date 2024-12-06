-- models/staging/stg_orders.sql

{{ config(
    materialized='incremental',
    unique_key='ORDER_EXTERNAL_ID'
) }}

with source as (

    select 
        trim(upper(ORDER_EXTERNAL_ID)) as ORDER_EXTERNAL_ID,
        trim(upper(ORDER_ID)) as ORDER_ID,
        trim(upper(CUSTOMER_ID)) as CUSTOMER_ID,
        to_date(ORDER_DATE) as ORDER_DATE,
        CAST(ORDER_TOTAL AS decimal(10,2)) as ORDER_TOTAL,
        trim(upper(SALES_CHANNEL)) as SALES_CHANNEL,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'orders') }}

),

validated as (

    select 
        ORDER_EXTERNAL_ID,
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_DATE,
        ORDER_TOTAL,
        SALES_CHANNEL,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where ORDER_EXTERNAL_ID is not null
      and regexp_like(ORDER_EXTERNAL_ID, '^ORD-[0-9]+$')  -- Example format
)

select * from validated
