# 🏙️ Chicago Spatial-Temporal Crime Data Warehouse

> **Course:** Data Warehousing (06026212) — Faculty of Information Technology, KMITL  
> **Source of Truth:** [Data_Warehouse_Architecture_Design_Document_Chicago_V3.pdf](./Docs/Data_Warehouse_Architecture_Design_Document_Chicago_V3.pdf) (Version 1.2, 14 ตุลาคม 2569)  
> **Tech Stack:** PostgreSQL 16, dbt Core, Apache Airflow 3.x, Astronomer Cosmos, Metabase, Docker Compose  

---

## 🏗️ Architecture & Project Structure

```text
our_project/
├── docker-compose.yaml             # Complete stack (Postgres, Airflow, Cosmos, dbt, Metabase, pgAdmin)
├── dockerfile.airflow              # Custom Airflow image with dbt-postgres, cosmos, and ingestion packages
├── postgresql.conf                 # Tuned PostgreSQL config for analytical loads
├── .env.example                    # Environment template
├── .env                            # Active environment configuration
├── Docs/                           # Formal Architecture Design Document (V3 PDF/DOCX) & ERD scripts
├── data/
│   ├── raw/                        # Raw incident CSV files (includes crimes_sample_1000.csv sample)
│   └── seeds/                      # Historical boundary CSVs
├── scripts/
│   ├── 001_init_schemas.sql        # Database initialization (schemas, partition fact, quarantine, RBAC)
│   ├── fetch_daily_crimes.py       # SODA API incremental fetcher with 3-day lookback window
│   └── refresh_metabase_cache.py   # Automated Metabase cache refresh
├── airflow/
│   ├── dags/
│   │   └── crime_dw_daily.py       # Daily pipeline DAG: Ingest -> Quarantine -> Cosmos DbtTaskGroup -> Metabase
│   ├── logs/
│   ├── plugins/
│   └── config/
└── dbt/
    ├── dbt_project.yml             # dbt project definition with layer schemas
    ├── profiles.yml                # Connections for prod (container) and dev (local)
    ├── packages.yml                # dbt_utils package dependency
    ├── macros/
    │   └── generate_schema_name.sql # Custom schema name override
    ├── models/
    │   ├── staging/
    │   │   ├── _sources.yml        # Raw source contract
    │   │   ├── stg_crimes.sql      # Type casting & field cleaning
    │   │   └── stg_quarantined_crimes.sql # Data Quality Gate quarantine model
    │   ├── core/
    │   │   ├── _core_models.yml    # Uniqueness, completeness & FK tests
    │   │   ├── dim_date.sql        # Calendar date dimension (2001-2030)
    │   │   ├── dim_time.sql        # 1,440 minute dimension + CPD Shifts
    │   │   ├── dim_crime_type.sql  # IUCR master dimension + Index crime severity
    │   │   ├── dim_location_type.sql # Location description dimension
    │   │   ├── dim_location.sql    # Spatial boundary dimension (SCD Type 2)
    │   │   └── fact_crime_incident.sql # Incremental fact table with delete-and-insert
    │   └── marts/
    │       ├── _marts_models.yml   # Marts schema validation
    │       ├── mart_daily_beat_crime_summary.sql # Beat x Day x Crime summary
    │       └── mart_spatial_heatmap.sql          # Density weights for Metabase heatmaps
    ├── snapshots/
    │   └── snap_dim_location.sql   # SCD Type 2 tracking for boundary redistricting
    ├── seeds/
    └── tests/
```

---

## 🚀 Quick Start Guide

### 1. Initialize Environment
```bash
cp .env.example .env
```

### 2. Start Services via Docker Compose
```bash
docker compose up -d
docker compose ps
```

### 3. Verify Service Endpoints
* **Apache Airflow UI:** [http://localhost:8080](http://localhost:8080) (`admin` / `admin`)
* **Metabase BI Dashboard:** [http://localhost:3000](http://localhost:3000)
* **pgAdmin 4 Web Console:** [http://localhost:5050](http://localhost:5050) (`admin@crime-dw.local` / `admin_secret_pass`)
* **PostgreSQL 16:** `localhost:5432` (`postgres` / `postgres`, DB: `crime_dw`)

---

## 🛡️ Data Quality Framework & Quality Gates
Derived from Table 25 of the V3 Design Document:
1. **Uniqueness:** `source_id` must be unique in `core_dw.fact_crime_incident`.
2. **Completeness:** Mandatory fields (`case_number`, `date`, `primary_type`, `iucr`) must not be null.
3. **Validity:** Incident date cannot be in the future (`date <= CURRENT_TIMESTAMP`). Coordinates must stay inside Chicago boundary (`41.60 <= lat <= 42.05`, `-87.94 <= lon <= -87.52`).
4. **Quarantine:** All anomalous rows are routed to `raw_staging.quarantine_crime_records` with audit reasons.
5. **Quality Gates:** Astronomer Cosmos halts downstream Marts execution (`TestBehavior.AFTER_EACH`) if critical Core DW tests fail.
