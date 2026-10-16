{{
  config(
    materialized='table',
    schema='core_dw'
  )
}}

WITH date_series AS (
    SELECT '2001-01-01'::DATE + i AS full_date
    FROM generate_series(0, 10957) i -- 2001-01-01 to 2030-12-31 (~30 years)
)

SELECT
    -1 AS date_key,
    '1970-01-01'::DATE AS full_date,
    0::SMALLINT AS day_of_week,
    'Unknown' AS day_name,
    0::SMALLINT AS day_of_month,
    0::SMALLINT AS month_no,
    'Unknown' AS month_name,
    0::SMALLINT AS quarter,
    0::SMALLINT AS year,
    FALSE AS is_weekend,
    FALSE AS is_holiday

UNION ALL

SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_key,
    full_date,
    EXTRACT(ISODOW FROM full_date)::SMALLINT AS day_of_week,
    TO_CHAR(full_date, 'Day') AS day_name,
    EXTRACT(DAY FROM full_date)::SMALLINT AS day_of_month,
    EXTRACT(MONTH FROM full_date)::SMALLINT AS month_no,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(QUARTER FROM full_date)::SMALLINT AS quarter,
    EXTRACT(YEAR FROM full_date)::SMALLINT AS year,
    CASE WHEN EXTRACT(ISODOW FROM full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE
        WHEN (EXTRACT(MONTH FROM full_date) = 1 AND EXTRACT(DAY FROM full_date) = 1) THEN TRUE -- New Year
        WHEN (EXTRACT(MONTH FROM full_date) = 7 AND EXTRACT(DAY FROM full_date) = 4) THEN TRUE -- Independence Day
        WHEN (EXTRACT(MONTH FROM full_date) = 12 AND EXTRACT(DAY FROM full_date) = 25) THEN TRUE -- Christmas
        ELSE FALSE
    END AS is_holiday
FROM date_series
