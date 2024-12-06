{{ config(materialized='incremental', unique_key='grade_id') }}

with source as (
    select 
        grade_id,
        load_date,
        record_source
    from {{ ref('stg_condition_grades') }}
),

deduped as (
    select 
        grade_id,
        max(load_date) as load_date,
        max(record_source) as record_source
    from source
    group by grade_id
)

select 
    grade_id,
    load_date,
    record_source
from deduped