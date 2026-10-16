-- ==============================================================================
-- 001_init_schemas.sql
-- Chicago Spatial-Temporal Crime Data Warehouse Initialization Script
-- Derived from Appendix A of Data Warehouse Architecture Design Document V3
-- ==============================================================================

-- 1. Create Data Warehouse Layers (Postgres Schemas)
CREATE SCHEMA IF NOT EXISTS raw_staging;
CREATE SCHEMA IF NOT EXISTS core_dw;
CREATE SCHEMA IF NOT EXISTS data_marts;

-- 2. Create Raw Staging Ingestion Table
CREATE TABLE IF NOT EXISTS raw_staging.raw_crimes (
    id BIGINT,
    case_number VARCHAR(50),
    date VARCHAR(50),
    block VARCHAR(150),
    iucr VARCHAR(10),
    primary_type VARCHAR(100),
    description VARCHAR(255),
    location_description VARCHAR(150),
    arrest VARCHAR(10),
    domestic VARCHAR(10),
    beat VARCHAR(20),
    district VARCHAR(20),
    ward VARCHAR(20),
    community_area VARCHAR(20),
    fbi_code VARCHAR(20),
    x_coordinate VARCHAR(50),
    y_coordinate VARCHAR(50),
    year VARCHAR(10),
    updated_on VARCHAR(50),
    latitude VARCHAR(50),
    longitude VARCHAR(50),
    location VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(100) DEFAULT 'INITIAL_LOAD'
);

-- 3. Create Quarantine Table for Malformed / Out-of-Bounds Records
CREATE TABLE IF NOT EXISTS raw_staging.quarantine_crime_records (
    quarantine_id BIGSERIAL PRIMARY KEY,
    raw_payload JSONB NOT NULL,
    source_id BIGINT,
    case_number VARCHAR(50),
    reject_reason VARCHAR(255) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    quarantined_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 4. Create Declarative Partitioned Fact Table
CREATE TABLE IF NOT EXISTS core_dw.fact_crime_incident (
    incident_key BIGINT NOT NULL,
    date_key INTEGER NOT NULL,
    time_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    crime_type_key INTEGER NOT NULL,
    location_type_key INTEGER NOT NULL,
    source_id BIGINT NOT NULL,
    case_number VARCHAR(50) NOT NULL,
    block VARCHAR(150),
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    x_coordinate NUMERIC(12,3),
    y_coordinate NUMERIC(12,3),
    incident_count SMALLINT NOT NULL DEFAULT 1,
    arrest_flag SMALLINT NOT NULL DEFAULT 0,
    domestic_flag SMALLINT NOT NULL DEFAULT 0,
    source_updated_at TIMESTAMPTZ NOT NULL,
    dw_load_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fact_crime_incident PRIMARY KEY (incident_key, date_key),
    CONSTRAINT uk_fact_crime_source_id UNIQUE (source_id, date_key)
) PARTITION BY RANGE (date_key);

-- Yearly Partition Ranges
CREATE TABLE IF NOT EXISTS core_dw.fact_crime_incident_y2021
    PARTITION OF core_dw.fact_crime_incident
    FOR VALUES FROM (20210101) TO (20220101);

CREATE TABLE IF NOT EXISTS core_dw.fact_crime_incident_y2022
    PARTITION OF core_dw.fact_crime_incident
    FOR VALUES FROM (20220101) TO (20230101);

CREATE TABLE IF NOT EXISTS core_dw.fact_crime_incident_y2023
    PARTITION OF core_dw.fact_crime_incident
    FOR VALUES FROM (20230101) TO (20240101);

CREATE TABLE IF NOT EXISTS core_dw.fact_crime_incident_y2024
    PARTITION OF core_dw.fact_crime_incident
    FOR VALUES FROM (20240101) TO (20250101);

CREATE TABLE IF NOT EXISTS core_dw.fact_crime_incident_y2025
    PARTITION OF core_dw.fact_crime_incident
    FOR VALUES FROM (20250101) TO (20260101);

CREATE TABLE IF NOT EXISTS core_dw.fact_crime_incident_y2026
    PARTITION OF core_dw.fact_crime_incident
    FOR VALUES FROM (20260101) TO (20270101);

CREATE TABLE IF NOT EXISTS core_dw.fact_crime_incident_default
    PARTITION OF core_dw.fact_crime_incident DEFAULT;

-- 5. Role-Based Access Control (RBAC) - Principle of Least Privilege
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bi_reader') THEN
        CREATE ROLE bi_reader WITH LOGIN PASSWORD 'bi_secure_pass';
    END IF;
END $$;

GRANT USAGE ON SCHEMA data_marts TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA data_marts TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_marts GRANT SELECT ON TABLES TO bi_reader;
