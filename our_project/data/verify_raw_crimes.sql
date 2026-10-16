\timing on

-- 1. Total row count
SELECT COUNT(*) AS total_raw_rows FROM raw_staging.raw_crimes;

-- 2. Stamp batch id for traceability
UPDATE raw_staging.raw_crimes 
SET _batch_id = 'INITIAL_BULK_2001_2026',
    _loaded_at = CURRENT_TIMESTAMP
WHERE _batch_id IS NULL OR _batch_id = 'INITIAL_LOAD';

-- 3. Temporal span and distinct years
SELECT 
    MIN(date) AS min_date_str,
    MAX(date) AS max_date_str,
    COUNT(DISTINCT year) AS distinct_years,
    MIN(year) AS min_year,
    MAX(year) AS max_year
FROM raw_staging.raw_crimes;

-- 4. Mandatory fields completeness and null audit
SELECT 
    COUNT(*) FILTER (WHERE id IS NULL) AS null_ids,
    COUNT(*) FILTER (WHERE case_number IS NULL) AS null_cases,
    COUNT(*) FILTER (WHERE date IS NULL) AS null_dates,
    COUNT(*) FILTER (WHERE primary_type IS NULL) AS null_primary_types,
    COUNT(*) FILTER (WHERE iucr IS NULL) AS null_iucrs,
    COUNT(*) FILTER (WHERE latitude IS NULL) AS unmapped_coords,
    COUNT(*) FILTER (WHERE latitude IS NOT NULL) AS mapped_coords
FROM raw_staging.raw_crimes;

-- 5. Storage footprint
SELECT pg_size_pretty(pg_total_relation_size('raw_staging.raw_crimes')) AS raw_table_size;
