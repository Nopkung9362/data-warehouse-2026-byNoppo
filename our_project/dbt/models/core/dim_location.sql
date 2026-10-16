{{
  config(
    materialized='table',
    schema='core_dw'
  )
}}

WITH distinct_boundaries AS (
    SELECT DISTINCT
        COALESCE(LPAD(TRIM(beat::TEXT), 4, '0'), '-1') AS beat_code,
        NULLIF(TRIM(beat::TEXT), '')::INTEGER AS beat_num,
        CASE
            WHEN LENGTH(TRIM(beat::TEXT)) >= 2 THEN SUBSTRING(TRIM(beat::TEXT), 1, 2)
            ELSE 'UNK'
        END AS sector,
        COALESCE(LPAD(TRIM(district::TEXT), 2, '0'), '-1') AS district_id,
        CASE
            WHEN district IS NOT NULL AND TRIM(district::TEXT) != ''
            THEN 'District ' || LPAD(TRIM(district::TEXT), 2, '0')
            ELSE 'Unknown District'
        END AS district_name,
        COALESCE(TRIM(community_area::TEXT), '-1') AS community_area_id,
        CASE
            WHEN community_area IS NOT NULL AND TRIM(community_area::TEXT) != ''
            THEN 'Community Area ' || TRIM(community_area::TEXT)
            ELSE 'Unknown Community'
        END AS community_name,
        COALESCE(ward, -1) AS ward_num,
        '2001-01-01'::DATE AS effective_start_date,
        '9999-12-31'::DATE AS effective_end_date,
        TRUE AS is_current
    FROM {{ ref('stg_crimes') }}
    WHERE beat IS NOT NULL OR district IS NOT NULL OR ward IS NOT NULL
)

SELECT
    -1 AS location_key,
    '-1' AS beat_code,
    -1 AS beat_num,
    'UNK' AS sector,
    '-1' AS district_id,
    'Unknown District' AS district_name,
    '-1' AS community_area_id,
    'Unknown Community' AS community_name,
    -1 AS ward_num,
    '1970-01-01'::DATE AS effective_start_date,
    '9999-12-31'::DATE AS effective_end_date,
    TRUE AS is_current

UNION ALL

SELECT
    ROW_NUMBER() OVER(ORDER BY beat_code, district_id, ward_num, community_area_id) AS location_key,
    beat_code,
    beat_num,
    sector,
    district_id,
    district_name,
    community_area_id,
    community_name,
    ward_num,
    effective_start_date,
    effective_end_date,
    is_current
FROM distinct_boundaries
