-- models/magnitude_type_dimension.sql

{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER() AS Magnitude_Type_ID,  -- Generate a unique Magnitude_Type_ID
    Magnitude_Type
FROM
    (
        SELECT DISTINCT
            Magnitude_Type
        FROM
            {{ source('landing', 'extraction_stage') }} -- Reference to the source table
        WHERE
            Magnitude_Type IS NOT NULL
    ) AS distinct_magnitude_types;
