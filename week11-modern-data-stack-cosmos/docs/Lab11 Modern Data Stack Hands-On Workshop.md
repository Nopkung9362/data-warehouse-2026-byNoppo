# LAB SHEET: MODERN DATA STACK HANDS-ON WORKSHOP

## Data Warehouse Transformation & Orchestration with dbt and Apache Airflow

| ข้อมูลการฝึกอบรม (Workshop Overview) |  |  |  |
| --- | --- | --- | --- |
| **ระยะเวลา:** | 2 ชั่วโมง (120 นาที) | **รูปแบบ:** | Hands-on Intensive Workshop (Fill-in & Observe) |
| **กลุ่มเป้าหมาย:** | ผู้มีพื้นฐาน DWH, SQL และเคยลองใช้ Airflow/dbt | **เครื่องมือหลัก:** | Docker, Apache Airflow 3.2.2, dbt Core, PostgreSQL 16, Astronomer Cosmos |

> **เป้าหมายหลักของ Workshop (Core Goals)**
> * **Hybrid Data Ingestion Strategy:** เข้าใจสถาปัตยกรรม ELT ในชีวิตจริง โดยใช้ Native Bulk Load สำหรับ Raw Transactions ขนาดใหญ่ และใช้ `dbt seed` สำหรับ Reference Lookup Data ขนาดเล็ก
> * **Star & Snowflake Schema Data Modeling:** ออกแบบและสร้าง Fact & Dimension Tables ด้วยสถาปัตยกรรม Star Schema และ Snowflake Schema บนชุดข้อมูล **Brazilian E-Commerce Public Dataset by Olist**
> * **Dedicated Database Setup:** สร้างและจัดการ Data Warehouse บน PostgreSQL Database อิสระ (`olist_db`) พร้อมจัดการ Custom Schema Naming ผ่าน dbt Macro
> * **Advanced Airflow Orchestration & Task Flow Patterns:** เชี่ยวชาญการเขียน DAG รูปแบบต่างๆ เช่น Parallel Execution (Fan-out/Fan-in), Dynamic Branching และ Astronomer Cosmos Integration
> * **Production Failure Investigation & Debugging:** ฝึกทักษะการตรวจสอบ Stack Trace, การไล่อ่าน Log บน Airflow UI/dbt และการ Recovery ระบบเมื่อเกิด Data Pipeline Failure
> 
> 

---

## โครงสร้างโฟลเดอร์โครงการ (Project Directory Structure)

```text
.
├── docker-compose.yaml
├── dockerfile.airflow
├── raw_data/                               # โฟลเดอร์เก็บไฟล์ CSV ข้อมูลดิบ Olist (8 ไฟล์)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   └── olist_geolocation_dataset.csv
├── dags/                                   # Airflow DAGs
│   ├── lab_airflow_task_patterns.py
│   └── lab_olist_cosmos_pipeline.py
├── dbt_root/                               # โฟลเดอร์เก็บ dbt profile (Mounts เข้า /root/.dbt)
│   └── profiles.yml                        # เชื่อมต่อไปยัง olist_db บน PostgreSQL
└── dbt/                                    # โฟลเดอร์ dbt หลัก (Mounts เข้า /usr/app ใน dw_dbt และ /opt/airflow/dbt)
    └── olist_dbt/                          # dbt project (โฟลเดอร์ย่อย)
        ├── dbt_project.yml
        ├── seeds/                          # โฟลเดอร์สำหรับ dbt seed (Reference Lookup Data)
        │   └── product_category_name_translation.csv
        ├── macros/                         # โฟลเดอร์เก็บ dbt Custom Macros
        │   └── generate_schema_name.sql    # Macro ปรับแต่งชื่อ Schema ให้ตรงตามกำหนด
        └── models/
            ├── staging/
            │   ├── schema.yml              # ประกาศ Sources (olist_raw)
            │   ├── stg_olist_orders.sql
            │   ├── stg_olist_order_items.sql
            │   ├── stg_olist_customers.sql
            │   ├── stg_olist_products.sql
            │   ├── stg_product_category_translation.sql
            │   └── stg_olist_sellers.sql
            └── marts/
                ├── schema.yml              # ประกาศ Data Tests (unique, not_null)
                ├── dim_category.sql        # Snowflake Schema Layer ( Normalized Category )
                ├── dim_products.sql        # Snowflake Schema Layer ( Products -> Category )
                ├── dim_customers.sql       # Star Schema Dimension
                ├── dim_sellers.sql         # Star Schema Dimension
                └── fct_daily_sales.sql     # Star Schema Fact Table

```

---

## ขั้นตอนที่ 0: การเตรียม Environment & Dedicated Database (Hybrid Ingestion Setup)

### 1. สร้างโครงสร้างไฟล์และโฟลเดอร์ใน dbt

#### Windows (PowerShell)

เปิด PowerShell ที่ Root Directory ของโครงการ แล้วคัดลอกคำสั่งด้านล่างนี้ไปรันได้ทันที:

```powershell
# 1. สร้างโฟลเดอร์ทั้งหมด
New-Item -ItemType Directory -Force -Path `
    "dbt/olist_dbt/seeds", `
    "dbt/olist_dbt/macros", `
    "dbt/olist_dbt/models/staging", `
    "dbt/olist_dbt/models/marts"

# 2. สร้างไฟล์เปล่าทั้งหมด
New-Item -ItemType File -Force -Path `
    "dags/lab_airflow_task_patterns.py", `
    "dags/lab_olist_cosmos_pipeline.py", `
    "dbt/olist_dbt/dbt_project.yml", `
    "dbt/olist_dbt/macros/generate_schema_name.sql", `
    "dbt/olist_dbt/models/staging/schema.yml", `
    "dbt/olist_dbt/models/staging/stg_olist_orders.sql", `
    "dbt/olist_dbt/models/staging/stg_olist_order_items.sql", `
    "dbt/olist_dbt/models/staging/stg_olist_customers.sql", `
    "dbt/olist_dbt/models/staging/stg_olist_products.sql", `
    "dbt/olist_dbt/models/staging/stg_product_category_translation.sql", `
    "dbt/olist_dbt/models/staging/stg_olist_sellers.sql", `
    "dbt/olist_dbt/models/marts/schema.yml", `
    "dbt/olist_dbt/models/marts/dim_category.sql", `
    "dbt/olist_dbt/models/marts/dim_products.sql", `
    "dbt/olist_dbt/models/marts/dim_customers.sql", `
    "dbt/olist_dbt/models/marts/dim_sellers.sql", `
    "dbt/olist_dbt/models/marts/fct_daily_sales.sql"

```

---

#### macOS / Linux (Terminal / Bash / Zsh)

เปิด Terminal ที่ Root Directory ของโครงการ แล้วคัดลอกคำสั่งด้านล่างนี้ไปรันได้ทันที:

```bash
# 1. สร้างโฟลเดอร์ทั้งหมด
mkdir -p dbt/olist_dbt/seeds \
         dbt/olist_dbt/macros \
         dbt/olist_dbt/models/staging \
         dbt/olist_dbt/models/marts

# 2. สร้างไฟล์เปล่าทั้งหมด
touch dbt/olist_dbt/dbt_project.yml \
      dags/lab_airflow_task_patterns.py \
      dags/lab_olist_cosmos_pipeline.py \
      dbt/olist_dbt/macros/generate_schema_name.sql \
      dbt/olist_dbt/models/staging/schema.yml \
      dbt/olist_dbt/models/staging/stg_olist_orders.sql \
      dbt/olist_dbt/models/staging/stg_olist_order_items.sql \
      dbt/olist_dbt/models/staging/stg_olist_customers.sql \
      dbt/olist_dbt/models/staging/stg_olist_products.sql \
      dbt/olist_dbt/models/staging/stg_product_category_translation.sql \
      dbt/olist_dbt/models/staging/stg_olist_sellers.sql \
      dbt/olist_dbt/models/marts/schema.yml \
      dbt/olist_dbt/models/marts/dim_category.sql \
      dbt/olist_dbt/models/marts/dim_products.sql \
      dbt/olist_dbt/models/marts/dim_customers.sql \
      dbt/olist_dbt/models/marts/dim_sellers.sql \
      dbt/olist_dbt/models/marts/fct_daily_sales.sql

```

### 2. ปรับแก้ไฟล์ `dockerfile.airflow`

```dockerfile
FROM apache/airflow:3.2.2

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-postgres \
    astronomer-cosmos

```

### 3. ปรับแก้ไฟล์ `docker-compose.yaml`

```dockerfile
...
environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://dw_user:dw_pass@postgres/airflow
    AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'    # <-- เพิ่มบรรทัดนี้

...
services:
  postgres:
    ...
    volumes:
      - pg_data:/var/lib/postgresql/data
      - ./postgresql.conf:/etc/postgresql/postgresql.conf
      - ${AIRFLOW_PROJ_DIR:-.}/raw_data:/home/raw_data    # <-- เพิ่มบรรทัดนี้

```

### 4. สั่ง Build Image และสั่งรัน Container

```bash
docker compose build
docker compose up -d

```

### 5. สร้าง Dedicated Database (`olist_db`) และ Raw Schema ใน PostgreSQL

```bash
# 1. สร้าง Database olist_db
docker exec -it dw_postgres psql -U dw_user -d airflow -c "CREATE DATABASE olist_db;"

# 2. สร้าง Schema olist_raw ไว้รองรับ Raw Data
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "CREATE SCHEMA IF NOT EXISTS olist_raw;"

```

### 6. Hybrid Data Ingestion Execution (Bulk Load vs dbt seed)

* **การนำเข้า Raw Transactions:** ทำการ Bulk Import เข้าไปยัง Schema `olist_raw` ผ่านคำสั่ง PostgreSQL `COPY`:
```bash
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "
CREATE TABLE IF NOT EXISTS olist_raw.olist_customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS olist_raw.olist_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS olist_raw.olist_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS olist_raw.olist_products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE IF NOT EXISTS olist_raw.olist_sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5)
);

TRUNCATE TABLE 
    olist_raw.olist_customers,
    olist_raw.olist_orders,
    olist_raw.olist_order_items,
    olist_raw.olist_products,
    olist_raw.olist_sellers;
"

docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_customers FROM '/home/raw_data/olist_customers_dataset.csv' WITH (FORMAT csv, HEADER true);"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_orders FROM '/home/raw_data/olist_orders_dataset.csv' WITH (FORMAT csv, HEADER true);"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_order_items FROM '/home/raw_data/olist_order_items_dataset.csv' WITH (FORMAT csv, HEADER true);"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_products FROM '/home/raw_data/olist_products_dataset.csv' WITH (FORMAT csv, HEADER true);"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_sellers FROM '/home/raw_data/olist_sellers_dataset.csv' WITH (FORMAT csv, HEADER true);"

```


* **การนำเข้า Reference Lookup Data (`product_category_name_translation.csv`):** คัดลอกไฟล์นี้ไปไว้ในโฟลเดอร์ `dbt/olist_dbt/seeds/` เพื่อรอการประมวลผลด้วย `dbt seed`

---

## โครงสร้างเวลาและหัวข้อการเรียนรู้ (Agenda Timeline)

| ช่วงเวลา | หัวข้อหลัก | กิจกรรมภาคปฏิบัติ & ความรู้สอดแทรก |
| --- | --- | --- |
| **00:00 - 00:40** | **Part 1: Star & Snowflake Data Modeling in dbt** | `dbt seed`, Staging & Sources Schema, Snowflake (`dim_category` $\rightarrow$ `dim_products`), Star Schema (`dim_customers`, `dim_sellers`, `fct_daily_sales`), `dbt test` |
| **00:40 - 01:15** | **Part 2: Advanced Airflow DAGs & Task Flow Patterns** | Parallel Execution (Fan-out/Fan-in), Branching (`BranchPythonOperator`), Sequential Flow & Detailed Component Explanations |
| **01:15 - 01:50** | **Part 3: End-to-End Orchestration & Failure Lab** | Astronomer Cosmos (`DbtTaskGroup`), Failure Simulation, Airflow UI Debugging & Recovery Workflow |
| **01:50 - 02:00** | **Part 4: สรุปภาพรวมและ Best Practices** | Division of Duties (Airflow vs dbt vs DWH), Environment Separation & Production Checklist |

---

## Part 1: Star & Snowflake Data Modeling in dbt (40 นาที)

### 00:00 - 00:10 | การสร้าง dbt Configurations & Custom Schema Macro

#### 1. สร้างไฟล์ `profiles.yml` ใน `dbt_root/`

```yaml
# dbt_root/profiles.yml
olist_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      user: dw_user
      pass: dw_pass
      port: 5432
      dbname: olist_db
      schema: olist
      threads: 4

```

#### 2. ปรับแต่งไฟล์ `dbt_project.yml`

```yaml
# dbt/olist_dbt/dbt_project.yml
name: 'olist_dbt'
version: '1.0.0'
config-version: 2

profile: 'olist_dbt'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

seeds:
  olist_dbt:
    +schema: raw
    +quote_columns: false

models:
  olist_dbt:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts

```

#### 3. รันสั่งโหลด Reference Data ด้วย `dbt seed`

```bash
docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt seed"

```

---

### 00:10 - 00:28 | Staging Layer & Sources Schema Definition

#### 1. กำหนดตารางข้อมูลดิบใน `dbt/olist_dbt/models/staging/schema.yml`

นี่คือจุดที่เชื่อมโยงฟังก์ชัน `{{ source('raw_olist', '...') }}` เข้ากับชื่อ Schema/Table จริงใน PostgreSQL:

```yaml
# dbt/olist_dbt/models/staging/schema.yml
version: 2

sources:
  - name: raw_olist          # ชื่ออ้างอิงของ Source
    schema: olist_raw        # ชื่อ Schema จริงใน PostgreSQL
    tables:
      - name: olist_orders
      - name: olist_order_items
      - name: olist_customers
      - name: olist_products
      - name: olist_sellers

```

#### 2. สร้าง Staging Models (`dbt/olist_dbt/models/staging/`)

```sql
-- dbt/olist_dbt/models/staging/stg_product_category_translation.sql
{{ config(materialized='view') }}

SELECT
    TRIM(product_category_name) AS category_name_portuguese,
    TRIM(product_category_name_english) AS category_name_english
FROM {{ ref('product_category_name_translation') }}

```

```sql
-- dbt/olist_dbt/models/staging/stg_olist_products.sql
{{ config(materialized='view') }}

SELECT
    CAST(product_id AS VARCHAR) AS product_id,
    COALESCE(TRIM(product_category_name), 'unknown') AS product_category_name,
    CAST(product_weight_g AS INT) AS product_weight_g
FROM {{ source('raw_olist', 'olist_products') }}

```

```sql
-- dbt/olist_dbt/models/staging/stg_olist_orders.sql
{{ config(materialized='view') }}

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_status AS VARCHAR) AS order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp
FROM {{ source('raw_olist', 'olist_orders') }}

```

```sql
-- dbt/olist_dbt/models/staging/stg_olist_order_items.sql
{{ config(materialized='view') }}

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(order_item_id AS INT) AS order_item_id,
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(seller_id AS VARCHAR) AS seller_id,
    CAST(price AS NUMERIC(10,2)) AS price,
    CAST(freight_value AS NUMERIC(10,2)) AS freight_value
FROM {{ source('raw_olist', 'olist_order_items') }}

```

```sql
-- dbt/olist_dbt/models/staging/stg_olist_customers.sql
{{ config(materialized='view') }}

SELECT
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(customer_unique_id AS VARCHAR) AS customer_unique_id,
    CAST(customer_city AS VARCHAR) AS customer_city,
    CAST(customer_state AS VARCHAR) AS customer_state
FROM {{ source('raw_olist', 'olist_customers') }}

```

```sql
-- dbt/olist_dbt/models/staging/stg_olist_sellers.sql
{{ config(materialized='view') }}

SELECT
    CAST(seller_id AS VARCHAR) AS seller_id,
    CAST(seller_city AS VARCHAR) AS seller_city,
    CAST(seller_state AS VARCHAR) AS seller_state
FROM {{ source('raw_olist', 'olist_sellers') }}

```

---

### 00:28 - 00:35 | Data Modeling: Snowflake & Star Schema (`dbt/olist_dbt/models/marts/`)

#### 1. Snowflake Schema Layer: Product Dimension Normalization

> **แนวคิด Snowflake Schema:** ทำการ Normalize ข้อมูลหมวดหมู่สินค้าแยกออกมาเป็น `dim_category` แล้วเชื่อมกับ `dim_products` ช่วยลดความซ้ำซ้อนของข้อมูลและรองรับลำดับชั้น (Hierarchy) ในอนาคต

```sql
-- dbt/olist_dbt/models/marts/dim_category.sql
{{ config(materialized='table') }}

SELECT
    MD5(category_name_portuguese) AS category_key,
    category_name_portuguese,
    category_name_english
FROM {{ ref('stg_product_category_translation') }}

```

```sql
-- dbt/olist_dbt/models/marts/dim_products.sql
{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_olist_products') }}
),
categories AS (
    SELECT * FROM {{ ref('dim_category') }}
)

SELECT
    p.product_id,
    c.category_key, -- Foreign Key อ้างอิงไปยัง dim_category (Snowflake Structure)
    c.category_name_english,
    p.product_weight_g
FROM products p
LEFT JOIN categories c ON p.product_category_name = c.category_name_portuguese

```

#### 2. Star Schema Layer: Dimensions & Fact Table

```sql
-- dbt/olist_dbt/models/marts/dim_customers.sql
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ ref('stg_olist_customers') }}

```

```sql
-- dbt/olist_dbt/models/marts/dim_sellers.sql
{{ config(materialized='table') }}

SELECT
    seller_id,
    seller_city,
    seller_state
FROM {{ ref('stg_olist_sellers') }}

```

```sql
-- dbt/olist_dbt/models/marts/fct_daily_sales.sql
{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_olist_orders') }}
),
items AS (
    SELECT * FROM {{ ref('stg_olist_order_items') }}
),
customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),
products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    DATE(o.order_purchase_timestamp) AS sale_date,
    c.customer_state,
    p.category_name_english,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS total_customers,
    SUM(i.price) AS total_revenue,
    SUM(i.freight_value) AS total_freight_cost
FROM orders o
JOIN items i ON o.order_id = i.order_id
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON i.product_id = p.product_id
GROUP BY DATE(o.order_purchase_timestamp), c.customer_state, p.category_name_english

```

---

### 00:35 - 00:40 | Data Quality Gate Definition & Testing

#### 1. กำหนด Data Quality Tests ใน `dbt/olist_dbt/models/marts/schema.yml`

สร้างไฟล์นี้เพื่อเปิดใช้งานคำสั่ง `dbt test` ซึ่งจะนำไปทดสอบสถานการณ์ Data Failure ใน Part 3:

```yaml
# dbt/olist_dbt/models/marts/schema.yml
version: 2

models:
  - name: dim_products
    columns:
      - name: product_id
        tests:
          - unique
          - not_null

  - name: dim_customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null

  - name: fct_daily_sales
    columns:
      - name: sale_date
        tests:
          - not_null
      - name: total_revenue
        tests:
          - not_null

```

#### 2. ทดสอบสั่ง Run และ Test บน dbt CLI

```bash
docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt run && dbt test"

```

---

## Part 2: Advanced Airflow DAGs & Task Flow Patterns (35 นาที)

### 1. สร้างไฟล์ `dags/lab_airflow_task_patterns.py`:

```python
# dags/lab_airflow_task_patterns.py
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os

def notify_failure_callback(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    logical_date = context.get('data_interval_start')
    exception = context.get('exception')
    
    print(f"[ALERT FAILURE] DAG: '{dag_id}' | Task: '{task_id}'")
    print(f"Execution Logical Date: {logical_date}")
    print(f"Error Cause: {exception}")

def check_data_freshness_branch(**kwargs):
    file_exists = os.path.exists('/home/raw_data/olist_orders_dataset.csv')
    if file_exists:
        # คืนค่าเป็น list ของทุก task ภายใน ingestion_group เพื่อให้ทำงานขนานกันทั้งหมด
        return [
            'ingestion_group.ingest_orders',
            'ingestion_group.ingest_items',
            'ingestion_group.ingest_customers'
        ]
    else:
        return 'skip_pipeline_notice'

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': notify_failure_callback,
}

with DAG(
    dag_id='lab_airflow_task_patterns',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2017, 1, 1),
    catchup=False,
    tags=['production', 'task_patterns', 'olist'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # Pattern 1: Branching Pattern
    branch_check = BranchPythonOperator(
        task_id='branch_check_data_freshness',
        python_callable=check_data_freshness_branch,
    )

    skip_pipeline_notice = EmptyOperator(
        task_id='skip_pipeline_notice'
    )

    # Pattern 2: Parallel Execution Pattern (Fan-out / Fan-in) ใน TaskGroup
    with TaskGroup(group_id='ingestion_group') as ingestion_group:
        ingest_orders = SQLExecuteQueryOperator(
            task_id='ingest_orders',
            conn_id='postgres_dwh',
            sql="SELECT 1; -- Ingest Orders Process"
        )

        ingest_items = SQLExecuteQueryOperator(
            task_id='ingest_items',
            conn_id='postgres_dwh',
            sql="SELECT 1; -- Ingest Items Process"
        )

        ingest_customers = SQLExecuteQueryOperator(
            task_id='ingest_customers',
            conn_id='postgres_dwh',
            sql="SELECT 1; -- Ingest Customers Process"
        )

    # Pattern 3: Sequential Flow
    audit_raw_data = SQLExecuteQueryOperator(
        task_id='audit_raw_data_quality',
        conn_id='postgres_dwh',
        sql="SELECT COUNT(*) FROM olist_raw.olist_orders;"
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule='none_failed_min_one_success'
    )

    start_pipeline >> branch_check
    branch_check >> skip_pipeline_notice >> end_pipeline
    branch_check >> ingestion_group >> audit_raw_data >> end_pipeline

```

---

#### คำอธิบายโดยละเอียดขององค์ประกอบใน DAG

| องค์ประกอบ (Component) | หน้าที่และหลักการทำงานเชิงลึก |
| --- | --- |
| **`start_date` & `catchup=False**` | `start_date` กำหนดจุดเริ่มต้นตรรกะเวลาของ DAG ส่วน `catchup=False` ป้องกันไม่ให้ Airflow สั่งรัน DAG ย้อนหลังรวบยอดนับร้อยรอบโดยไม่ตั้งใจเมื่อเพิ่งเปิดใช้งาน DAG |
| **`data_interval_start`** | Jinja Macro ที่สะท้อน Logical Date เริ่มต้นของรอบ Schedule จริง **ห้ามใช้ `CURRENT_DATE()**` เพราะจะทำให้การสั่ง Re-run หรือ Backfill ข้อมูลย้อนหลังผิดพลาด |
| **`BranchPythonOperator`** | ทำหน้าที่ตัดสินใจเลือกเส้นทางประมวลผล โดยฟังก์ชัน Python ต้อง Return คืนค่า `task_id` หรือ `group_id` ที่ต้องการให้ระบบทำงานต่อ Task ในสายที่ไม่ถูกเลือกจะกลายเป็นสถานะ `skipped` |
| **`TaskGroup`** | รวมกลุ่ม Tasks ที่เกี่ยวข้องกัน (เช่น การ Ingest ข้อมูลจากหลายตาราง) ให้แสดงผลเป็นกล่องเดียวที่สามารถยุบ/ขยายได้บน Airflow UI ช่วยให้ Graph View สะอาดตา |
| **`trigger_rule`** | กฎการตัดสินใจรัน Task ถัดไป โดยปกติคือ `all_success` แต่เมื่อมี Branching ต้องเปลี่ยนเป็น `none_failed_min_one_success` เพื่อให้ Task ปลายทางทำงานได้แม้มียอดสายถูก Skip ไป |
| **`on_failure_callback`** | Event Listener ที่ขอนำฟังก์ชัน Python ไปผูกไว้ เมื่อมี Task ใดใน DAG เกิด Failure Airflow จะส่ง Context Object เข้ามาในฟังก์ชันเพื่อประมวลผลการแจ้งเตือน alert ทันที |

---

### 2. สร้าง Connection `postgres_dwh` ใน Airflow

เนื่องจาก DAG มีการเรียกใช้ `conn_id='postgres_dwh'` ต้องเข้าไปตั้งค่าการเชื่อมต่อใน Airflow Web UI ก่อน:

1. เปิดเบราว์เซอร์ไปที่ **Airflow UI** (`http://localhost:28080` หรือ Port ที่ตั้งค่าไว้)
2. ไปที่เมนู **Admin** $\rightarrow$ **Connections**
3. กดปุ่ม **+ (Add a new record)** แล้วกรอกข้อมูล:
* **Connection Id:** `postgres_dwh`
* **Connection Type:** `Postgres`
* **Host:** `postgres` (หรือชื่อ service ของ postgres ใน `docker-compose.yaml`)
* **Database:** `olist_db`
* **Login:** `dw_user`
* **Password:** `dw_pass`
* **Port:** `5432`


4. กด **Save** แล้วกด **Test** เพื่อทดสอบการเชื่อมต่อ 

---

### 3. ตรวจสอบ Syntax และ DAG Import Errors

---

### 4. เปิดใช้งานและสั่งรัน DAG (Unpause & Trigger)

1. กลับไปที่หน้า **DAGs** บน Airflow UI
2. ค้นหา DAG ชื่อ `lab_airflow_task_patterns`
3. สับสวิตช์หน้าชื่อ DAG จาก **Off** เป็น **On** (Unpause)
4. กดปุ่ม **Trigger DAG** (ไอคอนปุ่ม Play ด้านขวา) เพื่อสั่งรันทันที

---

### 5. ตรวจสอบผลการประมวลผล (Graph View & Logs)

1. คลิกเข้าไปที่ชื่อ DAG `lab_airflow_task_patterns` แล้วเลือกมุมมอง **Graph**
2. **สังเกตพฤติกรรมของ Task Flow Patterns:**
* **Branching:** ดูว่าสาย `ingestion_group` ทำงาน และสาย `skip_pipeline_notice` ถูกสคิป (เปลี่ยนเป็นสีชมพู/Skipped) หรือไม่
* **Parallel Execution:** ดูการทำงานพร้อมกันของ Task ภายใน `ingestion_group`
* **Trigger Rule:** ดูว่า `end_pipeline` ทำงานสำเร็จ (สีเขียว/Success) แม้จะมีบางสายถูก Skip ไป


3. คลิกที่ Task ใดก็ได้ $\rightarrow$ เลือก **Logs** เพื่อดูรายละเอียดการพิมพ์ Log ของ Python Callback หรือ SQL Query

---

## Part 3: End-to-End Orchestration & Failure Lab (35 นาที)

### 01:15 - 01:30 | เขียน DAG เชื่อมต่อ dbt ด้วย Astronomer Cosmos

### 1. สร้างไฟล์ `dags/lab_olist_cosmos_pipeline.py`:

```python
# dags/lab_olist_cosmos_pipeline.py
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime

profile_config = ProfileConfig(
    profile_name="olist_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dwh",
        profile_args={"schema": "olist_marts"},
    ),
)

with DAG(
    dag_id='lab_olist_cosmos_integration',
    start_date=datetime(2017, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['dbt', 'cosmos', 'olist'],
) as dag:

    dbt_transformation = DbtTaskGroup(
        group_id="dbt_transformation_layer",
        project_config=ProjectConfig("/opt/airflow/dbt/olist_dbt"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="dbt"),
    )

```

---

### 2. เปิดใช้งานและสั่งรัน DAG บน Airflow UI

1. เปิดหน้า **Airflow UI** (`http://localhost:28080`)
2. ค้นหา DAG ชื่อ `lab_olist_cosmos_integration`
3. สับสวิตช์เปิดใช้งาน (Unpause) จาก **Off** เป็น **On**
4. กดปุ่ม **Trigger DAG** (ไอคอน Play) เพื่อสั่งรันการประมวลผล

---

### 3. สังเกตผลลัพธ์ dbt Task Group บน Airflow UI

1. คลิกเข้าไปที่ชื่อ DAG `lab_olist_cosmos_integration` แล้วเลือกมุมมอง **Graph**
2. คลี่กล่อง **`dbt_transformation_layer`** ออกมา
3. **สังเกตโครงสร้างที่ Cosmos สร้างให้อัตโนมัติ:**
* จะเห็น Lineage Dependency ระหว่าง Staging Models และ Marts Models โดยตรง เช่น `stg_olist_customers` $\rightarrow$ `dim_customers`
* แต่ละ Model จะถูกรันด้วยคำสั่ง `dbt run` ในรูปของ Airflow Task แยกกันโดยอัตโนมัติ



> **วิธีตรวจสอบความถูกต้อง:** ทุก Task ภายใน TaskGroup เปลี่ยนสถานะเป็น **สีเขียว (Success)** และข้อมูลในตาราง `olist_marts` บน PostgreSQL ถูกสร้างขึ้นครบถ้วน

---

### 01:30 - 01:50 | Hands-on Lab: Failure Simulation, Investigation & Recovery

#### ขั้นตอนที่ 1: การจำลอง Data Failure

แอบแทรกข้อมูลเน่า (`customer_id` เป็น `NULL`) ลงในตาราง `olist_raw.olist_customers`:

```bash
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "
INSERT INTO olist_raw.olist_customers (customer_id, customer_unique_id, customer_city, customer_state)
VALUES (NULL, 'invalid_id', 'Sao Paulo', 'SP');
"

```

#### ขั้นตอนที่ 2: การสืบสวนและตรวจสอบข้อผิดพลาด (Debugging Workflow)

1. **เปิด Airflow UI (`http://localhost:28080`):** สั่ง Trigger DAG `lab_olist_cosmos_integration`
2. **สังเกตผล:** Task ในกลุ่ม Cosmos ชื่อ `not_null_dim_customers_customer_id` จะเปลี่ยนเป็น **สีแดง (Failed)**
3. **ตรวจสอบ Log:** คลิกดู Log จะพบข้อความจาก `schema.yml` ที่ตรวจเจอค่า NULL:
> `GOT 1 result, configured to fail if != 0`
> `Failure in test not_null_dim_customers_customer_id`
> `Table dim_customers has 1 null value in column customer_id`



#### ขั้นตอนที่ 3: การแก้ไขข้อมูลและการสั่ง Re-run (Recovery)

1. **Clean ข้อมูลเน่าออกจาก DWH:**
```bash
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "
DELETE FROM olist_raw.olist_customers WHERE customer_unique_id = 'invalid_id';
"

```


2. **สั่ง Clear Task บน Airflow UI:**
* คลิก Task dim_customer สีแดงที่ Fail $\rightarrow$ เลือกเมนู **Clear Task** (เลือก `Downstream`) $\rightarrow$ กด **Clear**
* Task จะเปลี่ยนสถานะกลับเป็น **สีเขียว (Success)** โดยไม่ต้องสั่งรันใหม่ทั้งหมดตั้งแต่ต้น



---

## Part 4: สรุปภาพรวมและ Best Practices (10 นาที)

| องค์ประกอบ (Component) | หน้าที่หลัก (Primary Role) | แนวคิดสำคัญ (Key Concept) |
| --- | --- | --- |
| **Apache Airflow** | Workflow Orchestrator | ดูแลเรื่อง **WHEN & ORDER**: บริหารจัดการจังหวะเวลา, Branching, Parallel Pipeline และ Alerting |
| **dbt (data build tool)** | Transformation Engine | ดูแลเรื่อง **HOW TO TRANSFORM**: แปลงข้อมูลแบบ Star/Snowflake Schema, ควบคุม Data Quality Gate (`dbt test`) และโหลด Reference Data (`dbt seed`) |
| **PostgreSQL (`olist_db`)** | Compute & Storage Engine | ดูแลเรื่อง **COMPUTE POWER**: เป็นที่เก็บข้อมูลดิบและ Data Marts อิสระจาก Airflow Metadata |

> **Production Checklist สำหรับนำไปใช้งานจริง**
> 1. **Hybrid Ingestion Strategy:** ใช้ Bulk Loading Engine สำหรับ Raw Transactions ขนาดใหญ่ และใช้ `dbt seed` สำหรับ Reference Lookups ไม่เกิน 10,000 แถว
> 2. **Isolated Data Warehouses:** แยก Database สำหรับสถิติธุรกิจ (`olist_db`) ออกจาก Database ระบบงานอื่นเสมอ
> 3. **Never Process Big Data in Airflow Python:** ให้ใช้ Airflow สั่ง Query หรือเรียก dbt ไปประมวลผลที่ Data Warehouse เสมอ ห้ามดึงข้อมูลมาคำนวณใน Memory ของ Airflow
> 4. **Smart Recovery with Clear Task:** เมื่อเกิด Failure ในระบบ Production ให้สืบค้นสาเหตุจาก Task Logs แก้ไขที่ข้อมูลต้นทาง แล้วใช้คำสั่ง `Clear Task` เพื่อสั่ง Re-run เฉพาะส่วนที่ล้มเหลวเพื่อประหยัดเวลาและ Compute Cost
> 


---

## Part 5: การส่งงาน
ส่ง screenshot หน้า graph ของ DAG lab_olist_cosmos_integration ที่ success ทั้งหมด
