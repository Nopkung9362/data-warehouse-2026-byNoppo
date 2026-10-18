{{
  config(
    materialized='view',
    schema='raw_staging'
  )
}}

SELECT
    id::BIGINT AS id,
    TRIM(case_number) AS case_number,
    date::TIMESTAMP AS date,
    TRIM(block) AS block,
    LPAD(TRIM(iucr), 4, '0') AS iucr,
    UPPER(TRIM(primary_type)) AS primary_type,
    TRIM(description) AS description,
    UPPER(TRIM(location_description)) AS location_description,
    CASE WHEN UPPER(TRIM(arrest::TEXT)) IN ('TRUE', '1', 'T') THEN TRUE ELSE FALSE END AS arrest,
    CASE WHEN UPPER(TRIM(domestic::TEXT)) IN ('TRUE', '1', 'T') THEN TRUE ELSE FALSE END AS domestic,
    LPAD(TRIM(beat::TEXT), 4, '0') AS beat,
    LPAD(NULLIF(TRIM(district::TEXT), '')::INTEGER::TEXT, 2, '0') AS district,
    NULLIF(TRIM(ward::TEXT), '')::INTEGER AS ward,
    TRIM(community_area::TEXT) AS community_area,
    TRIM(fbi_code) AS fbi_code,
    NULLIF(TRIM(x_coordinate::TEXT), '')::NUMERIC(12,3) AS x_coordinate,
    NULLIF(TRIM(y_coordinate::TEXT), '')::NUMERIC(12,3) AS y_coordinate,
    NULLIF(TRIM(year::TEXT), '')::INTEGER AS year,
    updated_on::TIMESTAMPTZ AS updated_on,
    NULLIF(TRIM(latitude::TEXT), '')::NUMERIC(10,7) AS latitude,
    NULLIF(TRIM(longitude::TEXT), '')::NUMERIC(10,7) AS longitude,
    _loaded_at,
    _batch_id
FROM {{ source('raw_staging', 'raw_crimes') }}
