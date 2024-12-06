-- models/staging/stg_shipment_records.sql

{{ config(
    materialized='incremental',
    unique_key='SHIPMENT_ID'
) }}

with source as (

    select 
        trim(upper(SHIPMENT_ID)) as SHIPMENT_ID,
        trim(upper(TRACKING_NUMBER)) as TRACKING_NUMBER,
        trim(upper(ORIGIN_LOCATION_ID)) as ORIGIN_LOCATION_ID,
        trim(upper(DESTINATION_LOCATION_ID)) as DESTINATION_LOCATION_ID,
        to_date(PICKUP_DATE) as PICKUP_DATE,
        to_date(DELIVERY_DATE) as DELIVERY_DATE,
        trim(upper(STATUS)) as STATUS,
        CAST(FREIGHT_COST AS decimal(10,2)) as FREIGHT_COST,
        to_timestamp(CREATED_AT) as CREATED_AT,
        to_timestamp(UPDATED_AT) as UPDATED_AT,
        current_timestamp() as LOAD_DATE,
        'telelink_dbt' as RECORD_SOURCE

    from {{ source('telelink_bronze', 'shipment_records') }}

),

validated as (

    select 
        SHIPMENT_ID,
        TRACKING_NUMBER,
        ORIGIN_LOCATION_ID,
        DESTINATION_LOCATION_ID,
        PICKUP_DATE,
        DELIVERY_DATE,
        STATUS,
        FREIGHT_COST,
        CREATED_AT,
        UPDATED_AT,
        LOAD_DATE,
        RECORD_SOURCE

    from source

    where SHIPMENT_ID is not null
      and STATUS = 'Delivered'  -- Example filter condition
)

select * from validated
