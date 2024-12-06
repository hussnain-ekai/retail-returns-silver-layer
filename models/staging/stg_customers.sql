-- models/staging/stg_customers.sql

{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID'
) }}

with source as (

    select 
        trim(upper(CUSTOMER_ID)) as CUSTOMER_ID,
        trim(upper(CUSTOMER_EXTERNAL_ID)) as CUSTOMER_EXTERNAL_ID,
        trim(upper(FIRST_NAME)) as FIRST_NAME,
        trim(upper(LAST_NAME)) as LAST_NAME,
        trim(upper(EMAIL)) as EMAIL,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'customers') }}

),

validated as (

    select 
        CUSTOMER_ID,
        CUSTOMER_EXTERNAL_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where CUSTOMER_ID is not null
      and regexp_like(CUSTOMER_ID, '^[A-Za-z0-9]{5,}$')  -- Example format
)

select * from validated
