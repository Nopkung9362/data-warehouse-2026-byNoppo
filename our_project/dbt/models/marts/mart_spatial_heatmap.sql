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
    l.community_name,
    ROUND(AVG(f.latitude), 7) AS centroid_latitude,
    ROUND(AVG(f.longitude), 7) AS centroid_longitude,
    t.hour_24,
    t.shift_name,
    c.primary_type,
    SUM(f.incident_count) AS incident_count,
    SUM(f.incident_count * (
        CASE
            WHEN c.is_index_crime THEN 3
            WHEN c.primary_type IN ('BATTERY', 'ASSAULT') THEN 2
            ELSE 1
        END
    )) AS total_density_weight
FROM {{ ref('fact_crime_incident') }} f
JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
JOIN {{ ref('dim_time') }} t ON f.time_key = t.time_key
JOIN {{ ref('dim_location') }} l ON f.location_key = l.location_key
JOIN {{ ref('dim_crime_type') }} c ON f.crime_type_key = c.crime_type_key
WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL
GROUP BY
    f.date_key,
    d.full_date,
    l.beat_code,
    l.district_name,
    l.community_name,
    t.hour_24,
    t.shift_name,
    c.primary_type
