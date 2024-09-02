{{ config(materialized='table') }}

WITH distinct_magnitude_types AS (
    SELECT DISTINCT
        MAGTYPE  -- Corrected column name
    FROM
        {{ source('landing', 'extraction_stage') }} -- Reference to the source table
    WHERE
        MAGTYPE IS NOT NULL  -- Ensure this column exists and is not null
)

SELECT
    ROW_NUMBER() OVER (ORDER BY MAGTYPE) AS Magnitude_Type_ID,  -- Generate a unique Magnitude_Type_ID
    MAGTYPE
FROM
    distinct_magnitude_types

