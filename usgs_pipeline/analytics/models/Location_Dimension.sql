
{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY Latitude, Longitude, Place) AS Location_ID,  -- Generate a unique Location_ID
    Latitude,
    Longitude,
    Place,
    NULL AS Region -- Region can be derived or added as needed
FROM
    (
        SELECT DISTINCT
            Latitude,
            Longitude,
            Place
        FROM
            {{ source('landing', 'extraction_stage') }} -- Reference to the source table
        WHERE
            Latitude IS NOT NULL AND Longitude IS NOT NULL
    ) AS distinct_locations