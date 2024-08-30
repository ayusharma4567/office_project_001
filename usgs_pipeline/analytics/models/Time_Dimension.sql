{{config(materialized='table')}}

with raw_table as (
    select
        Time as event_time
    from {{ source('landing', 'extraction_stage') }}
)

select 
    ROW_NUMBER() OVER() AS Time_ID,
    Date(event_time) as date,
    Time(event_time) as time,
    extract(year from event_time) as year,
    extract(month from event_time) as month,
    extract(day from event_time) as day
from
    raw_table
group by
    date, time, year, month, day
