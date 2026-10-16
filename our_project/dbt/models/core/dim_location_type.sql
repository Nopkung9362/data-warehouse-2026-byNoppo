{{
  config(
    materialized='table',
    schema='core_dw'
  )
}}

WITH distinct_locations AS (
    SELECT DISTINCT
        TRIM(UPPER(location_description)) AS location_desc
    FROM {{ ref('stg_crimes') }}
    WHERE location_description IS NOT NULL AND TRIM(location_description) != ''
)

SELECT
    -1 AS location_type_key,
    'UNKNOWN' AS location_desc

UNION ALL

SELECT
    ROW_NUMBER() OVER(ORDER BY location_desc) AS location_type_key,
    location_desc
FROM distinct_locations
