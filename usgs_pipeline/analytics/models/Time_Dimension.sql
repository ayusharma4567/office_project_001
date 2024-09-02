{{ config(materialized='table') }}

WITH raw_table AS (
    SELECT
        Time AS event_time
    FROM {{ source('landing', 'extraction_stage') }}
),
daily_summary AS (
    SELECT 
        Date(event_time) AS date,
        EXTRACT(year FROM event_time) AS year,
        EXTRACT(month FROM event_time) AS month,
        EXTRACT(day FROM event_time) AS day,
        COUNT(*) AS event_count -- Example aggregation
    FROM raw_table
    GROUP BY date, year, month, day
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY date) AS Time_ID,
    date,
    year,
    month,
    day
FROM daily_summary
