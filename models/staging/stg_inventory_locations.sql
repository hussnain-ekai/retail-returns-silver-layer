-- models/staging/stg_inventory_locations.sql

{{ config(
    materialized='incremental',
    unique_key='LOCATION_ID'
) }}

with source as (

    select 
        trim(upper(LOCATION_ID)) as LOCATION_ID,
        trim(upper(LOCATION_CODE)) as LOCATION_CODE,
        trim(upper(LOCATION_NAME)) as LOCATION_NAME,
        trim(upper(ADDRESS)) as ADDRESS,
        trim(upper(CITY)) as CITY,
        trim(upper(STATE)) as STATE,
        trim(upper(COUNTRY)) as COUNTRY,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'inventory_locations') }}

),

validated as (

    select 
        LOCATION_ID,
        LOCATION_CODE,
        LOCATION_NAME,
        ADDRESS,
        CITY,
        STATE,
        COUNTRY,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where LOCATION_ID is not null
      and COUNTRY = 'USA'  -- Based on source DBML
)

select * from validated
