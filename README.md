# Data Warehouse 2026 - IT KMITL

Repository for Data Warehouse Course (06026212), Faculty of Information Technology, King Mongkut's Institute of Technology Ladkrabang.

---

## 🏙️ Featured Project: Chicago Crime Data Warehouse
The complete enterprise-grade Data Warehouse project is located in the [`our_project/`](./our_project/) directory.

* **Full Documentation & User Guide:** 👉 **[`our_project/README.md`](./our_project/README.md)**
* **Formal Architecture Design Document:** [`our_project/Docs/Data_Warehouse_Architecture_Design_Document_Chicago_V3.pdf`](./our_project/Docs/Data_Warehouse_Architecture_Design_Document_Chicago_V3.pdf)
* **Technical Checkpoint Report:** [`our_project/Docs/DWH_V01.md`](./our_project/Docs/DWH_V01.md)

### Tech Stack:
- **Relational Storage & Partitioning:** PostgreSQL 16 (Star Schema, Partitioned Fact Table)
- **Transformations & Quality Gates:** dbt Core 1.8.x (28 Automated Tests, 100% Pass)
- **Orchestration & Workflow:** Apache Airflow 3.x + Astronomer Cosmos
- **Business Intelligence:** Metabase BI (Executive Decision & Tactical Patrol Dashboards)
- **Database Administration:** pgAdmin 4
- **Containerization:** Docker Compose (8 Interconnected Microservices)

---

## 📚 Coursework & Weekly Labs
- [`week01-data-warehouse-setup/`](./week01-data-warehouse-setup/) — Data Warehouse Infrastructure Setup
- [`week02-olap-metabase/`](./week02-olap-metabase/) — OLAP & Metabase BI Explorations
- [`week03-kpi-layer/`](./week03-kpi-layer/) — Enterprise KPI Metric Layer Design
- [`week04-star-schema/`](./week04-star-schema/) — Dimensional Modeling & Star Schema
- [`week05-snowflake-schema-with-dbt/`](./week05-snowflake-schema-with-dbt/) — Snowflake Schema Transformation with dbt
- [`week06-scd-with-dbt/`](./week06-scd-with-dbt/) — Slowly Changing Dimensions (SCD Type 1 & 2)
- [`week07-fact-table-design-with-dbt/`](./week07-fact-table-design-with-dbt/) — Fact Table Partitioning & Grain Design
- [`week08-aggregation-summary-table-with-dbt/`](./week08-aggregation-summary-table-with-dbt/) — Summary Aggregations & Data Marts
- [`week09-etl-incremental-scd-with-dbt/`](./week09-etl-incremental-scd-with-dbt/) — Incremental Loading & Idempotency
- [`week10-airflow-orchestration-with-dbt/`](./week10-airflow-orchestration-with-dbt/) — Apache Airflow Pipeline Scheduling
- [`week11-modern-data-stack-cosmos/`](./week11-modern-data-stack-cosmos/) — Modern Data Stack with Astronomer Cosmos
