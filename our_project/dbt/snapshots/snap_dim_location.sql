{% snapshot snap_dim_location %}

{{
    config(
      target_schema='core_dw',
      strategy='check',
      unique_key="beat_code || '_' || district_id || '_' || ward_num || '_' || community_area_id",
      check_cols=['district_name', 'community_name', 'sector'],
      invalidate_hard_deletes=True
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
        COALESCE(ward, -1) AS ward_num
    FROM {{ ref('stg_crimes') }}
    WHERE beat IS NOT NULL OR district IS NOT NULL OR ward IS NOT NULL
)

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
    b.ward_num
FROM distinct_boundaries b
LEFT JOIN seed_districts pd ON b.district_id = pd.district_id
LEFT JOIN seed_communities ca ON b.community_area_id = ca.community_area_id
LEFT JOIN seed_beats pb ON b.beat_code = pb.beat_code
LEFT JOIN seed_wards cw ON b.ward_num = cw.ward_num

{% endsnapshot %}
