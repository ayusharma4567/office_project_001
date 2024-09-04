{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        *,
        DATE(TIME) AS event_date,
        TIME(TIME) AS event_time
    FROM {{ source('landing', 'extraction_stage') }} -- Reference to the source table
)

SELECT
    ROW_NUMBER() OVER (ORDER BY sd.TIME) AS Earthquake_ID,  -- Creates a unique ID for the fact table
    td.Time_ID,
    ld.Location_ID,
    mtd.Magnitude_Type_ID,  -- Use Magnitude_Type_ID from magnitude_type_dimension
    sd.Depth,
    sd.NST,
    sd.GAP,
    sd.DMIN,
    sd.RMS,
    sd.HORIZONTALERROR as Horizontal_Error,
    sd.DEPTHERROR as Depth_Error,
    sd.MAGERROR as Magnitude_Error,
    sd.MAGNST as Magnitude_ST
FROM
    source_data sd
LEFT JOIN
    {{ ref('Time_Dimension') }} td
    ON DATE(sd.Time) = td.date
LEFT JOIN
    {{ ref('Location_Dimension') }} ld
    ON sd.Latitude = ld.Latitude AND sd.Longitude = ld.Longitude
LEFT JOIN
    {{ ref('Magnitude_Type_Dimension') }} mtd
    ON sd.MAGTYPE = mtd.MAGTYPE