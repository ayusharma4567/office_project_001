-- models/earthquake_facts.sql

{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        *,
        DATE(TIME) AS event_date,
        TIME(TIME) AS event_time
    FROM {{ source('landing', 'extraction_stage') }} -- Reference to the source table
)

SELECT
    ROW_NUMBER() OVER() AS Earthquake_ID,  -- Creates a unique ID for the fact table
    td.Time_ID,
    ld.Location_ID,
    mtd.Magnitude_Type_ID,  -- Use Magnitude_Type_ID from magnitude_type_dimension
    sd.Depth,
    sd.Magnitude,
    sd.NST,
    sd.GAP,
    sd.DMIN,
    sd.RMS,
    sd.Horizontal_Error,
    sd.Depth_Error,
    sd.Magnitude_Error,
    sd.Magnitude_ST,
    sd.Status,
    sd.Location_Source,
    sd.Magnitude_Source
FROM
    source_data sd
LEFT JOIN
    {{ ref('time_dimension') }} td
    ON DATE(sd.Time) = td.date AND TIME(sd.Time) = td.time
LEFT JOIN
    {{ ref('location_dimension') }} ld
    ON sd.Latitude = ld.Latitude AND sd.Longitude = ld.Longitude
LEFT JOIN
    {{ ref('magnitude_type_dimension') }} mtd
    ON sd.Magnitude_Type = mtd.Magnitude_Type;

