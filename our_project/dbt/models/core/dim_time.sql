{{
  config(
    materialized='table',
    schema='core_dw'
  )
}}

WITH minute_series AS (
    SELECT i AS minute_val
    FROM generate_series(0, 1439) i
)

SELECT
    -1 AS time_key,
    -1::SMALLINT AS hour_24,
    -1::SMALLINT AS minute,
    'UNK' AS period_name,
    'Unknown Shift' AS shift_name

UNION ALL

SELECT
    (LPAD((minute_val / 60)::TEXT, 2, '0') || LPAD((minute_val % 60)::TEXT, 2, '0'))::INTEGER AS time_key,
    (minute_val / 60)::SMALLINT AS hour_24,
    (minute_val % 60)::SMALLINT AS minute,
    CASE WHEN (minute_val / 60) < 12 THEN 'AM' ELSE 'PM' END AS period_name,
    CASE
        WHEN (minute_val / 60) >= 8 AND (minute_val / 60) < 16 THEN 'Morning Shift (08:00-16:00)'
        WHEN (minute_val / 60) >= 16 THEN 'Afternoon Shift (16:00-24:00)'
        ELSE 'Night Shift (00:00-08:00)'
    END AS shift_name
FROM minute_series
