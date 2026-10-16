{{
  config(
    materialized='table',
    schema='core_dw'
  )
}}

WITH seed_iucr AS (
    SELECT
        LPAD(TRIM("IUCR"), 4, '0') AS iucr_code,
        UPPER(TRIM("PRIMARY DESCRIPTION")) AS primary_type,
        TRIM("SECONDARY DESCRIPTION") AS description,
        CASE WHEN UPPER(TRIM("INDEX CODE")) = 'I' THEN TRUE ELSE FALSE END AS is_index_crime
    FROM {{ ref('iucr_codes') }}
)

SELECT
    -1 AS crime_type_key,
    '-1' AS iucr_code,
    'UNKNOWN' AS primary_type,
    'UNKNOWN' AS description,
    FALSE AS is_index_crime

UNION ALL

SELECT
    ROW_NUMBER() OVER(ORDER BY iucr_code) AS crime_type_key,
    iucr_code,
    primary_type,
    description,
    is_index_crime
FROM seed_iucr

