{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_id',
    schema='core_dw',
    indexes=[
      {'columns': ['date_key']},
      {'columns': ['location_key']},
      {'columns': ['crime_type_key']},
      {'columns': ['source_updated_at']}
    ]
  )
}}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_crimes') }}
    {% if is_incremental() %}
    -- 3-Day Lookback Window เพื่อดักจับ late-arriving arrest updates ตามข้อกำหนด V3
    WHERE updated_on >= (SELECT COALESCE(MAX(source_updated_at) - INTERVAL '3 DAYS', '1970-01-01'::TIMESTAMPTZ) FROM {{ this }})
    {% endif %}
),

validated_data AS (
    SELECT s.*
    FROM source_data s
    -- คัดกรองข้อมูลที่ไม่ผ่านเกณฑ์คุณภาพวิกฤตออก (ถูกกักกันไว้ที่ quarantine_crime_records)
    WHERE s.id IS NOT NULL
      AND s.date IS NOT NULL
      AND s.primary_type IS NOT NULL
      AND s.iucr IS NOT NULL
      AND s.date <= CURRENT_TIMESTAMP
      AND (
          (s.latitude BETWEEN 41.60 AND 42.05 AND s.longitude BETWEEN -87.94 AND -87.52)
          OR (s.latitude IS NULL AND s.longitude IS NULL)
      )
)

SELECT
    -- แปลง hash MD5 เป็น BIGINT แบบ Deterministic ตามไวยากรณ์ PostgreSQL
    ('x' || substr(md5(v.id::text), 1, 16))::bit(64)::bigint AS incident_key,
    COALESCE(TO_CHAR(v.date::TIMESTAMP, 'YYYYMMDD')::INTEGER, -1) AS date_key,
    COALESCE(TO_CHAR(v.date::TIMESTAMP, 'HH24MI')::INTEGER, -1) AS time_key,
    COALESCE(l.location_key, -1) AS location_key,
    COALESCE(c.crime_type_key, -1) AS crime_type_key,
    COALESCE(lt.location_type_key, -1) AS location_type_key,
    v.id AS source_id,
    v.case_number,
    v.block,
    v.latitude,
    v.longitude,
    v.x_coordinate,
    v.y_coordinate,
    1::SMALLINT AS incident_count,
    CASE WHEN v.arrest THEN 1::SMALLINT ELSE 0::SMALLINT END AS arrest_flag,
    CASE WHEN v.domestic THEN 1::SMALLINT ELSE 0::SMALLINT END AS domestic_flag,
    v.updated_on AS source_updated_at,
    CURRENT_TIMESTAMP AS dw_load_timestamp
FROM validated_data v
-- Temporal Join สำหรับ SCD Type 2 ของ dim_location ด้วย Composite NK ทั้ง 4 ฟิลด์ (V3 PDF)
LEFT JOIN {{ ref('dim_location') }} l
    ON COALESCE(LPAD(TRIM(v.beat::TEXT), 4, '0'), '-1') = l.beat_code
   AND COALESCE(LPAD(TRIM(v.district::TEXT), 2, '0'), '-1') = l.district_id
   AND COALESCE(v.ward::INTEGER, -1) = l.ward_num
   AND COALESCE(TRIM(v.community_area::TEXT), '-1') = l.community_area_id
   AND v.date::DATE BETWEEN l.effective_start_date AND l.effective_end_date
LEFT JOIN {{ ref('dim_crime_type') }} c
    ON LPAD(TRIM(v.iucr::TEXT), 4, '0') = c.iucr_code
LEFT JOIN {{ ref('dim_location_type') }} lt
    ON TRIM(UPPER(v.location_description)) = lt.location_desc
