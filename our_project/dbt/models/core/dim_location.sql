{{
  config(
    materialized='table',
    schema='core_dw'
  )
}}

WITH seed_districts AS (
    SELECT DISTINCT
        LPAD(TRIM("DIST_NUM"::TEXT), 2, '0') AS district_id,
        UPPER(TRIM("DIST_LABEL"::TEXT)) AS district_label
    FROM {{ ref('police_districts') }}
    WHERE "DIST_NUM" IS NOT NULL
),

seed_communities AS (
    SELECT DISTINCT
        TRIM("AREA_NUMBE"::TEXT) AS community_area_id,
        UPPER(TRIM("COMMUNITY"::TEXT)) AS community_name
    FROM {{ ref('community_areas') }}
    WHERE "AREA_NUMBE" IS NOT NULL
),

seed_beats AS (
    SELECT DISTINCT
        LPAD(TRIM("BEAT_NUM"::TEXT), 4, '0') AS beat_code,
        TRIM("SECTOR"::TEXT) AS seed_sector
    FROM {{ ref('police_beats') }}
    WHERE "BEAT_NUM" IS NOT NULL
),

seed_wards AS (
    SELECT DISTINCT
        NULLIF(TRIM("WARD"::TEXT), '')::INTEGER AS ward_num
    FROM {{ ref('chicago_wards') }}
    WHERE "WARD" IS NOT NULL
),

distinct_boundaries AS (
    SELECT DISTINCT
        COALESCE(LPAD(TRIM(beat::TEXT), 4, '0'), '-1') AS beat_code,
        NULLIF(TRIM(beat::TEXT), '')::INTEGER AS beat_num,
        CASE
            WHEN LENGTH(TRIM(beat::TEXT)) >= 2 THEN SUBSTRING(TRIM(beat::TEXT), 1, 2)
            ELSE 'UNK'
        END AS fallback_sector,
        COALESCE(LPAD(TRIM(district::TEXT), 2, '0'), '-1') AS district_id,
        COALESCE(TRIM(community_area::TEXT), '-1') AS community_area_id,
        COALESCE(ward, -1) AS ward_num,
        '2001-01-01'::DATE AS effective_start_date,
        '9999-12-31'::DATE AS effective_end_date,
        TRUE AS is_current
    FROM {{ ref('stg_crimes') }}
    WHERE beat IS NOT NULL OR district IS NOT NULL OR ward IS NOT NULL
),

enriched AS (
    SELECT
        b.beat_code,
        b.beat_num,
        COALESCE(pb.seed_sector, b.fallback_sector) AS sector,
        b.district_id,
        CASE
            WHEN b.district_id != '-1' THEN 'District ' || b.district_id
            ELSE 'Unknown District'
        END AS district_name,
        b.community_area_id,
        COALESCE(
            ca.community_name,
            CASE 
                WHEN b.community_area_id != '-1' THEN 'Community Area ' || b.community_area_id 
                ELSE 'Unknown Community' 
            END
        ) AS community_name,
        b.ward_num,
        b.effective_start_date,
        b.effective_end_date,
        b.is_current
    FROM distinct_boundaries b
    LEFT JOIN seed_districts pd ON b.district_id = pd.district_id
    LEFT JOIN seed_communities ca ON b.community_area_id = ca.community_area_id
    LEFT JOIN seed_beats pb ON b.beat_code = pb.beat_code
    LEFT JOIN seed_wards cw ON b.ward_num = cw.ward_num
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
FROM enriched
