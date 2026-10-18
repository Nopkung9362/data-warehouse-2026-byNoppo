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
    COALESCE(ward, -1) AS ward_num
FROM {{ ref('stg_crimes') }}
WHERE beat IS NOT NULL OR district IS NOT NULL OR ward IS NOT NULL

{% endsnapshot %}
