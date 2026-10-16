{{
  config(
    materialized='table',
    schema='data_marts'
  )
}}

SELECT
    f.date_key,
    d.full_date,
    l.beat_code,
    l.district_name,
    c.primary_type,
    d.is_weekend,
    SUM(f.incident_count) AS total_incidents,
    SUM(f.arrest_flag) AS total_arrests,
    SUM(f.domestic_flag) AS total_domestic,
    SUM(CASE WHEN c.is_index_crime THEN 1 ELSE 0 END) AS total_index_crimes,
    ROUND(100.0 * SUM(f.arrest_flag) / NULLIF(SUM(f.incident_count), 0), 2) AS arrest_rate_pct
FROM {{ ref('fact_crime_incident') }} f
JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
JOIN {{ ref('dim_location') }} l ON f.location_key = l.location_key
JOIN {{ ref('dim_crime_type') }} c ON f.crime_type_key = c.crime_type_key
GROUP BY
    f.date_key,
    d.full_date,
    l.beat_code,
    l.district_name,
    c.primary_type,
    d.is_weekend
