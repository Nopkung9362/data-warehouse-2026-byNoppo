{{
  config(
    materialized='incremental',
    schema='raw_staging',
    alias='quarantine_crime_records'
  )
}}

WITH raw_source AS (
    SELECT * FROM {{ source('raw_staging', 'raw_crimes') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT COALESCE(MAX(quarantined_at), '1970-01-01'::timestamptz) FROM {{ this }})
    {% endif %}
)

SELECT
    nextval('raw_staging.quarantine_crime_records_quarantine_id_seq'::regclass) AS quarantine_id,
    to_jsonb(r.*) AS raw_payload,
    r.id AS source_id,
    r.case_number,
    CASE
        WHEN r.id IS NULL OR r.date IS NULL OR r.primary_type IS NULL OR r.iucr IS NULL THEN 'MISSING_MANDATORY_FIELD'
        WHEN r.date::TIMESTAMP > CURRENT_TIMESTAMP THEN 'FUTURE_INCIDENT_DATE'
        WHEN r.latitude IS NOT NULL AND (r.latitude::NUMERIC < 41.60 OR r.latitude::NUMERIC > 42.05 OR r.longitude::NUMERIC < -87.94 OR r.longitude::NUMERIC > -87.52) THEN 'COORDINATES_OUT_OF_BOUNDS'
        ELSE 'UNKNOWN_DATA_ANOMALY'
    END AS reject_reason,
    COALESCE(r._batch_id, 'MANUAL_RUN') AS batch_id,
    CURRENT_TIMESTAMP AS quarantined_at
FROM raw_source r
WHERE r.id IS NULL
   OR r.date IS NULL
   OR r.primary_type IS NULL
   OR r.iucr IS NULL
   OR r.date::TIMESTAMP > CURRENT_TIMESTAMP
   OR (r.latitude IS NOT NULL AND (r.latitude::NUMERIC < 41.60 OR r.latitude::NUMERIC > 42.05 OR r.longitude::NUMERIC < -87.94 OR r.longitude::NUMERIC > -87.52))
