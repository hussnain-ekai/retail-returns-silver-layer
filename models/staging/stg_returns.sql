-- models/staging/stg_returns.sql

{{ config(
    materialized='incremental',
    unique_key='RETURN_ID'
) }}

with source as (

    select 
        trim(upper(RETURN_ID)) as RETURN_ID,
        trim(upper(RETURN_EXTERNAL_ID)) as RETURN_EXTERNAL_ID,
        trim(upper(ORDER_ID)) as ORDER_ID,
        to_date(RETURN_INITIATED_DATE) as RETURN_INITIATED_DATE,
        to_date(RETURN_RECEIVED_DATE) as RETURN_RECEIVED_DATE,
        to_date(REFUND_ISSUED_DATE) as REFUND_ISSUED_DATE,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'returns') }}

),

validated as (

    select 
        RETURN_ID,
        RETURN_EXTERNAL_ID,
        ORDER_ID,
        RETURN_INITIATED_DATE,
        RETURN_RECEIVED_DATE,
        REFUND_ISSUED_DATE,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where RETURN_ID is not null
      and regexp_like(RETURN_ID, '^RET-[0-9]+$')  -- Example format
)

select * from validated
