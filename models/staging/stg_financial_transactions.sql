-- models/staging/stg_financial_transactions.sql

{{ config(
    materialized='incremental',
    unique_key='TRANSACTION_ID'
) }}

with source as (

    select 
        trim(upper(TRANSACTION_ID)) as TRANSACTION_ID,
        trim(upper(RETURN_ID)) as RETURN_ID,
        trim(upper(TRANSACTION_TYPE)) as TRANSACTION_TYPE,
        CAST(AMOUNT AS decimal(10,2)) as AMOUNT,
        to_date(TRANSACTION_DATE) as TRANSACTION_DATE,
        trim(upper(NOTES)) as NOTES,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'financial_transactions') }}

),

validated as (

    select 
        TRANSACTION_ID,
        RETURN_ID,
        TRANSACTION_TYPE,
        AMOUNT,
        TRANSACTION_DATE,
        NOTES,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where TRANSACTION_ID is not null
      and TRANSACTION_TYPE = 'Refund'
)

select * from validated
