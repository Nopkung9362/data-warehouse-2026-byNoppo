# 📦 Week 11: Modern Data Stack — Cosmos, Task Patterns & Failure Lab

> **Course:** Data Warehousing (การสร้างคลังข้อมูล)  
> **Topic:** Hybrid Ingestion, Star & Snowflake Modeling, Advanced Airflow Patterns และ Astronomer Cosmos  
> **Duration:** 2 Hours

> 💡 **Lab concept / แนวคิดหลัก:** Lab 10 ให้ Airflow เรียก `dbt run` ผ่าน **BashOperator หนึ่งตัว** —
> ทั้งโปรเจกต์ dbt กลายเป็น Task เดียวบน Graph ถ้าพังก็เห็นแค่ว่า "dbt พัง" สัปดาห์นี้เปลี่ยนไปใช้
> **Astronomer Cosmos** ซึ่งอ่านโปรเจกต์ dbt แล้ว **แตกทุก Model และทุก Test ออกเป็น Airflow Task ของตัวเอง**
> พร้อม Lineage ครบ — พอ Test ตัวไหน Fail คุณจะเห็น **ชื่อ Test นั้นเป็นสีแดงบน Graph** และ Clear
> เฉพาะสายที่พังได้ ซึ่งคือหัวใจของ Part 3: **Failure Simulation → Investigation → Recovery**

> 📷 The screenshot blocks below point at `docs/screenshots/`. Capture each output as you run the
> lab and drop the PNGs there — filenames already match.

---

## 🎯 Learning Objectives / วัตถุประสงค์

1. อธิบายและลงมือทำ **Hybrid Data Ingestion** — ใช้ Native Bulk Load (`COPY`) สำหรับ Raw Transaction ขนาดใหญ่ และใช้ `dbt seed` สำหรับ Reference Lookup Data ขนาดเล็กได้
2. ออกแบบและสร้าง **Star Schema** และ **Snowflake Schema** บนชุดข้อมูล **Brazilian E-Commerce Public Dataset by Olist** ด้วย dbt ได้
3. จัดการ **Dedicated Database** (`olist_db`) และควบคุมชื่อ Schema ด้วย **dbt Custom Macro** (`generate_schema_name`) ได้
4. เขียน Airflow DAG ด้วย **Task Flow Patterns** ขั้นสูง — Parallel Execution (Fan-out / Fan-in), Branching, `TaskGroup`, `trigger_rule` และ `on_failure_callback`
5. เชื่อม dbt เข้ากับ Airflow ด้วย **Astronomer Cosmos** (`DbtTaskGroup`) เพื่อให้แต่ละ Model / Test กลายเป็น Task แยกกันพร้อม Lineage
6. ทำ **Failure Investigation & Recovery** — จำลองข้อมูลเสีย ไล่อ่าน Log บน Airflow UI แก้ที่ต้นทาง แล้วใช้ **Clear Task** สั่งรันใหม่เฉพาะส่วนที่พัง

---

## 🧰 Tools & Stack Overview / เครื่องมือที่ใช้

| Tool | What is it? | หน้าที่ใน Lab |
|---|---|---|
| **Apache Airflow 3.2.2** | Workflow orchestrator | จัดลำดับงาน, Branching, Parallel Task, Log และ Recovery |
| **Astronomer Cosmos** | Airflow ⇄ dbt integration | อ่านโปรเจกต์ dbt แล้วแตกเป็น Airflow Task รายตัวพร้อม Lineage (`DbtTaskGroup`) |
| **dbt Core + dbt-postgres** | Transformation framework | `dbt seed`, Staging, Star / Snowflake Marts และ Data Quality Gate (`dbt test`) |
| **PostgreSQL 16** | Relational database | เก็บ Airflow metadata ใน `airflow` และ Data Warehouse ของ Lab ใน **`olist_db`** แยกกันคนละฐานข้อมูล |
| **pgAdmin 4** | DB management UI | ตรวจสอบ Schema / ตาราง / จำนวนแถว |
| **Docker Compose** | Containerization | เปิด services `postgres`, `airflow-*`, `pgadmin`, `metabase`, `dbt` |
| **VS Code / Text Editor** | Editor | สร้างไฟล์ `.py`, `.sql`, `.yml` |

**Dataset / ชุดข้อมูล**

| Dataset | Rows | รายละเอียด |
|---|---:|---|
| [`olist.zip`](./data/olist.zip) | — | Brazilian E-Commerce Public Dataset by Olist — บีบอัดไว้ 8 ไฟล์ (34 MB) |
| └ `olist_customers_dataset.csv` | 99,441 | ลูกค้า (1 แถว = 1 คำสั่งซื้อ ดู ⚠️ ด้านล่าง) |
| └ `olist_orders_dataset.csv` | 99,441 | คำสั่งซื้อ `2016-09-04` → `2018-09-03` |
| └ `olist_order_items_dataset.csv` | 112,650 | รายการสินค้าในคำสั่งซื้อ (Grain ของ Fact) |
| └ `olist_products_dataset.csv` | 32,951 | สินค้า |
| └ `olist_sellers_dataset.csv` | 3,095 | ผู้ขาย |
| └ `product_category_name_translation.csv` | 71 | **dbt seed** — แปลชื่อหมวดหมู่ ปอร์ตุเกส → อังกฤษ |
| └ `olist_order_payments_dataset.csv` | 103,886 | **ไม่ได้ใช้ใน Lab นี้** — ใบ Lab ลิสต์ไว้ในผังโฟลเดอร์เฉย ๆ |
| └ `olist_geolocation_dataset.csv` | 1,000,163 | **ไม่ได้ใช้ใน Lab นี้** — ใบ Lab ลิสต์ไว้ในผังโฟลเดอร์เฉย ๆ |

> 📝 **ทำไมถึงเป็นไฟล์ `.zip` ไม่ใช่ `.csv` เปล่า ๆ เหมือนสัปดาห์ก่อน** — ชุดนี้แตกไฟล์แล้วรวม **111 MB**
> (เฉพาะ `olist_geolocation_dataset.csv` ไฟล์เดียว 61 MB และ Lab ไม่ได้ใช้เลย) รีโปทั้งรีโปตอนนี้ยังไม่ถึง
> 25 MB จึงเก็บเป็น `.zip` ตามที่ผู้สอนส่งมา แล้วให้แตกไฟล์เองใน Part 0.5 — ได้ครบทั้ง 8 ไฟล์เหมือนเดิม

> 📝 **ผังโฟลเดอร์ในใบ Lab เขียนว่า "8 ไฟล์" แต่ลิสต์มาแค่ 7** (ตก `product_category_name_translation.csv`
> ซึ่งไม่ได้อยู่ใน `raw_data/` แต่ไปอยู่ใน `seeds/` ของ dbt) — ไฟล์จริงมี 8 ไฟล์ตามตารางด้านบน

**สองชั้นของการนำเข้าข้อมูล (Hybrid Ingestion)**

| ชั้น | วิธี | ใช้กับ | เหตุผล |
|---|---|---|---|
| **Raw Transactions** | PostgreSQL `COPY` (Native Bulk Load) | 5 ไฟล์ ~350,000 แถว | เร็วที่สุด ไม่ต้องผ่าน Python ทีละแถว |
| **Reference Lookup** | `dbt seed` | 71 แถว | เล็กพอจะ Version Control ใน Git ได้ และให้ dbt วาด Lineage ต่อได้ |

**แผนเวลาโดยประมาณ**

| ช่วงเวลา | หัวข้อ |
|---|---|
| **00:00 – 00:20** | Part 0 — Environment, Dedicated DB และ Hybrid Ingestion |
| **00:20 – 01:00** | Part 1 — Star & Snowflake Modeling ด้วย dbt |
| **01:00 – 01:25** | Part 2 — Advanced Airflow DAGs & Task Flow Patterns |
| **01:25 – 01:50** | Part 3 — Cosmos Integration + Failure Lab |
| **01:50 – 02:00** | Part 4 — สรุปและ Best Practices |

---

## 📁 Files in This Week / ไฟล์ในสัปดาห์นี้

| File / Folder | Description |
|---|---|
| 📂 [docs/](./docs/) | Lab instructions |
| ├── 📝 [Lab11 Modern Data Stack Hands-On Workshop.md](./docs/Lab11%20Modern%20Data%20Stack%20Hands-On%20Workshop.md) | Lab instruction (Markdown — สัปดาห์นี้ผู้สอนส่งมาเป็น `.md` ไม่มี `.docx` / `.pdf`) |
| └── 📂 [screenshots/](./docs/screenshots/) | Images referenced by this README |
| 📂 [data/](./data/) | Dataset |
| └── 📦 [olist.zip](./data/olist.zip) | 8 CSV ของ Olist — แตกไฟล์ลง `raw_data/` ใน Part 0.5 |
| 📂 [script/](./script/) | **ไฟล์ `.py` / `.sql` / `.yml` ทั้งหมดของ Lab เตรียมไว้ให้** — ใช้กับ "คำสั่งลัด" ในแต่ละหัวข้อ |
| ├── 📂 [dags/](./script/dags/) | Airflow DAG ทั้ง 2 ไฟล์ พร้อมคัดลอก |
| ├── 📂 [sql/](./script/sql/) | `create_raw_tables.sql` — DDL ของ `olist_raw` (Part 0.6) |
| ├── 📂 [dbt/olist_dbt/](./script/dbt/olist_dbt/) | dbt project พร้อมคัดลอกทั้งชุด (13 ไฟล์) |
| └── 📂 [dbt_root/](./script/dbt_root/) | `profiles.yml` — **อ้างอิงเท่านั้น ห้ามคัดลอกทับ** (Part 1.1) |
| 📂 [lab-week11/](./lab-week11/) | **Lab working directory** |
| ├── 📂 [dbt_root/](./lab-week11/dbt_root/) | Holds `profiles.yml` — the dbt connection profile |
| │&nbsp;&nbsp;&nbsp;└── ⚙️ [profiles.yml](./lab-week11/dbt_root/profiles.yml) | Connects dbt to PostgreSQL, database `olist_db`, schema `olist` |
| └── 📂 [dbt/olist_dbt/seeds/](./lab-week11/dbt/olist_dbt/seeds/) | dbt seed |
| &nbsp;&nbsp;&nbsp;&nbsp;└── 📊 [product_category_name_translation.csv](./lab-week11/dbt/olist_dbt/seeds/product_category_name_translation.csv) | 71 แถว — Reference Lookup สำหรับ `dbt seed` |

---

## 🔧 Part 0: Environment, Dedicated Database & Hybrid Ingestion

### 0.1 root ของ Lab อยู่ที่ไหน

> ⚠️ **ใบ Lab เรียก root ว่า "Root Directory ของโครงการ" แต่ในรีโปนี้ชื่อ `lab-week01`**
> `docker-compose.yaml` mount แบบ **relative กับตำแหน่งของตัวมันเอง** (`./dags`, `./raw_data`,
> `./dbt`, `./dbt_root`) ดังนั้นทุกโฟลเดอร์ของ Lab ต้องอยู่ **ข้าง ๆ** `docker-compose.yaml`
> เสมอ ไม่ว่าโฟลเดอร์นั้นจะชื่ออะไร
>
> | สถานะของคุณ | root คือ |
> |---|---|
> | ทำต่อจาก Week 1 มาเรื่อย ๆ | `dsba8-data-warehouse/week01-data-warehouse-setup/lab-week01/` |
> | เริ่มจาก 0 (คอมโดนล้าง / เครื่องใหม่) | `DWH_Lab/` — ได้จากการแตกไฟล์ [`DWH_Lab.zip`](../week01-data-warehouse-setup/lab-week01/DWH_Lab.zip) |
>
> ทั้งสองแบบใช้คำสั่งเดียวกันทุกบรรทัดหลังจาก `cd` เข้า root แล้ว

> 💡 **เช็กว่ายืนถูกที่หรือยัง** — สั่งแล้วต้องเห็น `docker-compose.yaml`
>
> **Mac / Linux:** `ls docker-compose.yaml` · **Windows (PowerShell):** `Test-Path docker-compose.yaml`

---

### 0.2 สร้างโครงสร้างโฟลเดอร์

โครงสร้างที่ต้องได้ **ในโฟลเดอร์เดียวกับ `docker-compose.yaml`**:

```text
lab-week01/                                     ← root: โฟลเดอร์ที่มี docker-compose.yaml
│                                                 (ถ้าเริ่มจาก 0 โดยแตกไฟล์ DWH_Lab.zip → root ชื่อ DWH_Lab/)
├── docker-compose.yaml                         ← มีอยู่แล้ว (ต้องแก้ใน Part 0.4)
├── dockerfile.airflow                          ← มีอยู่แล้ว (ต้องแก้ใน Part 0.4 — เพิ่ม astronomer-cosmos)
├── raw_data/                                   ← แตก olist.zip ลงที่นี่ (Part 0.5)
│   ├── olist_customers_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   ├── olist_order_payments_dataset.csv        ← ไม่ได้ใช้ใน Lab นี้
│   └── olist_geolocation_dataset.csv           ← ไม่ได้ใช้ใน Lab นี้
├── dags/
│   ├── lab_airflow_task_patterns.py            ← Part 2
│   └── lab_olist_cosmos_pipeline.py            ← Part 3
├── dbt_root/
│   └── profiles.yml                            ← ไฟล์ใช้ร่วมทุก Lab — "เพิ่ม" block เท่านั้น
└── dbt/
    └── olist_dbt/
        ├── dbt_project.yml
        ├── seeds/
        │   └── product_category_name_translation.csv
        ├── macros/
        │   └── generate_schema_name.sql
        └── models/
            ├── staging/
            │   ├── schema.yml                  ← ประกาศ Sources (olist_raw)
            │   ├── stg_olist_orders.sql
            │   ├── stg_olist_order_items.sql
            │   ├── stg_olist_customers.sql
            │   ├── stg_olist_products.sql
            │   ├── stg_olist_sellers.sql
            │   └── stg_product_category_translation.sql
            └── marts/
                ├── schema.yml                  ← ประกาศ Data Tests
                ├── dim_category.sql            ← Snowflake: หมวดหมู่ที่ Normalize ออกมา
                ├── dim_products.sql            ← Snowflake: Products → Category
                ├── dim_customers.sql           ← Star Dimension
                ├── dim_sellers.sql             ← Star Dimension
                └── fct_daily_sales.sql         ← Star Fact
```

สร้างโฟลเดอร์ (ต่างกันแค่ไวยากรณ์ของ shell):

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01     # เริ่มจาก 0: cd DWH_Lab
mkdir -p dags raw_data dbt_root \
  dbt/olist_dbt/seeds dbt/olist_dbt/macros \
  dbt/olist_dbt/models/staging dbt/olist_dbt/models/marts
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01     # เริ่มจาก 0: cd DWH_Lab
New-Item -ItemType Directory -Force dags, raw_data, dbt_root, `
  dbt/olist_dbt/seeds, dbt/olist_dbt/macros, `
  dbt/olist_dbt/models/staging, dbt/olist_dbt/models/marts
```

> 💡 Tip: paste as a single line if the line breaks cause errors. คำสั่ง `cd` ด้านบนนับจาก
> root ของรีโป — ถ้าอยู่ที่อื่นให้ใช้ path เต็มแทน

> ⚠️ **ต้อง `mkdir` ให้ครบ *ก่อน* สั่ง `docker compose up` ใน Part 0.4** — ถ้าโฟลเดอร์ยังไม่มี
> Docker จะสร้างให้เองโดยเป็นของ `root` ทำให้ dbt เขียน `target/` ไม่ได้ตอนรัน

<details>
<summary><b>⚡ คำสั่งลัด — วางไฟล์ DAG / SQL / YAML ทั้งหมดของ Lab นี้ในครั้งเดียว</b></summary>

ทุกไฟล์ `.py`, `.sql` และ `.yml` ของ Lab นี้เตรียมไว้แล้วใน [`script/`](./script/) ถ้าไม่อยากสร้างไฟล์
ทีละไฟล์แล้ว copy-paste จาก README ให้คัดลอกทั้งชุดทีเดียว — วางทั้งสามบรรทัดได้เลย:

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp -r ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/. dbt/olist_dbt/
cp    ../../week11-modern-data-stack-cosmos/script/dags/*.py dags/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item -Recurse -Force ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\* dbt\olist_dbt\
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dags\*.py dags\
```

> ⚠️ คำสั่งนี้แตะเฉพาะ `dbt/olist_dbt/` และ `dags/` เท่านั้น — **ไม่รวม `dbt_root/profiles.yml`** ซึ่งใช้ร่วมกัน
> ทุก Lab และต้องเพิ่ม block เองตาม Part 1.1 ส่วน seed CSV ที่จะคัดลอกในขั้นถัดไปจะไม่ถูกลบ
> เพราะเป็นการ merge ไม่ใช่ replace

> 📝 ถึงจะใช้คำสั่งลัด ก็ยัง**ควรอ่านคำอธิบายของแต่ละไฟล์ใน Part 1–3** เพราะข้อสอบและ
> Google Form ถามจากเหตุผลเบื้องหลัง ไม่ใช่แค่ผลลัพธ์ที่รันได้

> 💡 **บรรทัด `cd` นับจาก root ของรีโป** ถ้าคุณอยู่ใน `lab-week01/` อยู่แล้ว `cd` จะขึ้น error
> แต่บรรทัด copy ถัดไป**ยังทำงานถูกต้อง** เพราะยืนอยู่ที่เดิมอยู่แล้ว — ข้าม error นี้ได้เลย

> 📝 **คนที่เริ่มจาก 0 และวาง `DWH_Lab/` ไว้นอกรีโป** ให้ `cd` เข้า `DWH_Lab` ของตัวเองแทน
> แล้วชี้ต้นทางด้วย path เต็มไปยัง `week11-modern-data-stack-cosmos/script/`

</details>

> ⚠️ **`dbt_root/profiles.yml` เป็นไฟล์ที่ใช้ร่วมกันทุก Lab** — ถ้าคุณทำ Lab 3–10 มาแล้ว ไฟล์นี้
> มี profile ของสัปดาห์ก่อนอยู่ ให้ **เพิ่ม** block `olist_dbt:` ต่อท้าย (Part 1.1) **ห้ามเขียนทับทั้งไฟล์**
> ไม่งั้น Lab เก่าจะรันไม่ได้

---

### 0.3 คัดลอก dbt seed

Reference Lookup Data ไม่ได้ไปอยู่ใน `raw_data/` แต่ไปอยู่ใน `seeds/` ของโปรเจกต์ dbt เพื่อรอคำสั่ง `dbt seed`:

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/lab-week11/dbt/olist_dbt/seeds/product_category_name_translation.csv dbt/olist_dbt/seeds/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\lab-week11\dbt\olist_dbt\seeds\product_category_name_translation.csv dbt\olist_dbt\seeds\
```

---

### 0.4 แก้ `dockerfile.airflow` และ `docker-compose.yaml`

สัปดาห์นี้ต้องแก้ **3 จุด** — ทั้งหมดเป็นการ **เพิ่ม** ไม่ใช่แทนที่

#### (1) `dockerfile.airflow` — ติดตั้ง Astronomer Cosmos

Airflow container ต้องมี `astronomer-cosmos` เพิ่มจาก `dbt-core` + `dbt-postgres` ที่มีอยู่แล้ว:

**`dockerfile.airflow` — หลังแก้แล้ว**

```dockerfile
FROM apache/airflow:3.2.2

# Switch to root to install any required system binaries
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Switch back to the airflow user to install python libraries safely
USER airflow

# Install dbt core, the postgres adapter, and the Airflow <-> dbt integration
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-postgres \
    astronomer-cosmos
```

#### (2) `docker-compose.yaml` — เปิดปุ่ม Test Connection

ใต้ `x-airflow-common` ➡️ `environment:` เพิ่มบรรทัดเดียว เพื่อให้ปุ่ม **Test** ในหน้า Admin → Connections
(Part 2.2) กดได้:

**`docker-compose.yaml` — ส่วน `environment:` ของ `x-airflow-common`**

```yaml
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://dw_user:dw_pass@postgres/airflow
    AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'    # <-- เพิ่มบรรทัดนี้
```

#### (3) `docker-compose.yaml` — Mount สองจุด

**`docker-compose.yaml` — ส่วน `volumes:` ของ service `postgres`**

```yaml
    volumes:
      - pg_data:/var/lib/postgresql/data
      - ./postgresql.conf:/etc/postgresql/postgresql.conf
      - ${AIRFLOW_PROJ_DIR:-.}/raw_data:/home/raw_data    # <-- เพิ่มบรรทัดนี้
```

> 💡 **ทำไม `postgres` ถึงต้องเห็น `raw_data/` ด้วย** — คำสั่ง `\copy` ใน Part 0.6 เป็น
> **meta-command ฝั่ง client** ของ `psql` ไม่ใช่ `COPY` ฝั่ง server ดังนั้นมันอ่านไฟล์จาก
> **เครื่องที่ `psql` รันอยู่** ซึ่งในที่นี้คือภายใน container `dw_postgres` (เพราะเราสั่งผ่าน
> `docker exec -it dw_postgres psql ...`) ถ้าไม่ mount จะขึ้น `No such file or directory`

**`docker-compose.yaml` — ส่วน `volumes:` ของ `x-airflow-common` หลังแก้แล้ว**

```yaml
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - ${AIRFLOW_PROJ_DIR:-.}/raw_data:/home/raw_data
    - ${AIRFLOW_PROJ_DIR:-.}/dbt/lab10:/opt/airflow/dbt
    - ${AIRFLOW_PROJ_DIR:-.}/dbt:/opt/airflow/dbt_projects    # <-- เพิ่มบรรทัดนี้
    - ${AIRFLOW_PROJ_DIR:-.}/dbt_root:/opt/airflow/dbt_root
```

> ⚠️ **จุดที่ต่างจากใบ Lab — และเป็นจุดเดียวที่ DAG ของสัปดาห์นี้ไม่ตรงใบ Lab เป๊ะ**
> ใบ Lab สมมติว่า `dbt/` ทั้งโฟลเดอร์ถูก mount ไปที่ `/opt/airflow/dbt` แล้วโปรเจกต์อยู่ที่
> `/opt/airflow/dbt/olist_dbt` — แต่ในรีโปนี้ **`/opt/airflow/dbt` ถูกจองไว้ให้ `dbt/lab10` ตั้งแต่ Lab 10**
> ถ้าไปแก้บรรทัดนั้น DAG ของ Lab 10 (`--project-dir /opt/airflow/dbt`) จะพังทันที
> เราจึง **เพิ่ม** mount ใหม่ `dbt:/opt/airflow/dbt_projects` แทน แล้ว DAG ของ Part 3 ชี้ไปที่
> `/opt/airflow/dbt_projects/olist_dbt` — ต่างจากใบ Lab แค่ **คำเดียว** และ Lab 10 ยังรันได้เหมือนเดิม
> (ผลพลอยได้: Lab 12–15 เพิ่มโปรเจกต์ใหม่ได้เลย ไม่ต้องแก้ `docker-compose.yaml` อีก)

#### (4) Build ใหม่แล้วสั่งรัน

```bash
docker compose build
docker compose up -d --force-recreate
docker compose ps
```

> ⚠️ **ไม่ต้องใช้ `docker compose down -v`** เพราะจะลบ Named Volume และข้อมูลเดิมทั้งหมด
> (รวมถึงฐานข้อมูลของ Lab 3–10)

> ⚠️ **Lab นี้ต้องใช้ Airflow จริง ๆ** — ตรวจว่า `airflow-apiserver`, `airflow-scheduler` และ
> `airflow-init` ขึ้นครบใน `docker compose ps` ครั้งแรกจะใช้เวลา build นาน เพราะต้องติดตั้ง
> `astronomer-cosmos` เพิ่ม

**ตรวจว่า container เห็นไฟล์จริงหรือยัง** (เหมือนกันทุก OS):

```bash
docker compose exec airflow-scheduler ls /opt/airflow/dbt_projects /opt/airflow/dbt_root
```

> ✅ **ผลที่ต้องได้:** เห็น `olist_dbt` ใน `/opt/airflow/dbt_projects` และเห็น `profiles.yml` ใน `/opt/airflow/dbt_root`

---

### 0.5 แตกไฟล์ข้อมูลลง `raw_data/`

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
unzip -o ../../week11-modern-data-stack-cosmos/data/olist.zip -d raw_data/
ls raw_data/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Expand-Archive -Path ..\..\week11-modern-data-stack-cosmos\data\olist.zip -DestinationPath raw_data\ -Force
Get-ChildItem raw_data\
```

> ✅ **ผลที่ต้องได้:** 8 ไฟล์ `.csv` ใน `raw_data/` รวมประมาณ **111 MB**

> 📝 `product_category_name_translation.csv` จะถูกแตกลง `raw_data/` ไปด้วย — **ไม่เป็นไร** ปล่อยไว้ได้
> เพราะตัวที่ `dbt seed` ใช้คือไฟล์ใน `dbt/olist_dbt/seeds/` ที่คัดลอกไว้แล้วใน Part 0.3

---

### 0.6 สร้าง Dedicated Database และ Bulk Load

#### (1) สร้าง `olist_db` และ Schema `olist_raw`

```bash
docker exec -it dw_postgres psql -U dw_user -d airflow -c "CREATE DATABASE olist_db;"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "CREATE SCHEMA IF NOT EXISTS olist_raw;"
```

> 📝 **หมายเหตุ:** หาก PostgreSQL แจ้งว่า `database "olist_db" already exists` แสดงว่าเคยสร้างไว้แล้ว
> ใช้ต่อได้เลย

#### (2) สร้างตาราง Raw

`script/sql/create_raw_tables.sql`

```sql
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
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แล้วรันด้วย <code>psql -f</code> แทนการพิมพ์ยาว ๆ</b></summary>

ต้นทาง: [`script/sql/create_raw_tables.sql`](./script/sql/create_raw_tables.sql) — คัดลอกเข้า `raw_data/`
(ซึ่ง mount เข้า `dw_postgres` แล้วใน Part 0.4) จากนั้นสั่งรันทีเดียว โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/sql/create_raw_tables.sql raw_data/
docker exec -it dw_postgres psql -U dw_user -d olist_db -f /home/raw_data/create_raw_tables.sql
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\sql\create_raw_tables.sql raw_data\
docker exec -it dw_postgres psql -U dw_user -d olist_db -f /home/raw_data/create_raw_tables.sql
```

</details>

> 💡 **แบบที่ใบ Lab เขียนไว้** คือยัด SQL ทั้งก้อนเข้า `-c "..."` ซึ่งใช้ได้ทั้ง bash และ PowerShell
> (ทั้งสอง shell รองรับ string หลายบรรทัด) แต่ถ้า paste แล้วเพี้ยน ให้ใช้ `psql -f` ในกล่องลัดด้านบนแทน

#### (3) Bulk Load ด้วย `\copy`

คำสั่งชุดนี้เหมือนกันทุก OS:

```bash
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_customers FROM '/home/raw_data/olist_customers_dataset.csv' WITH (FORMAT csv, HEADER true);"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_orders FROM '/home/raw_data/olist_orders_dataset.csv' WITH (FORMAT csv, HEADER true);"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_order_items FROM '/home/raw_data/olist_order_items_dataset.csv' WITH (FORMAT csv, HEADER true);"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_products FROM '/home/raw_data/olist_products_dataset.csv' WITH (FORMAT csv, HEADER true);"
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\copy olist_raw.olist_sellers FROM '/home/raw_data/olist_sellers_dataset.csv' WITH (FORMAT csv, HEADER true);"
```

**ผลที่ต้องได้** — `psql` จะพิมพ์ `COPY <n>` ทีละบรรทัด:

| ตาราง | `COPY` |
|---|---:|
| `olist_raw.olist_customers` | 99,441 |
| `olist_raw.olist_orders` | 99,441 |
| `olist_raw.olist_order_items` | 112,650 |
| `olist_raw.olist_products` | 32,951 |
| `olist_raw.olist_sellers` | 3,095 |

<details>
<summary><b>Show Output — <code>\copy</code> ทั้ง 5 คำสั่ง</b></summary>

![psql printing COPY 99441, COPY 99441, COPY 112650, COPY 32951 and COPY 3095](./docs/screenshots/bulk-load-copy.png)

</details>

> 💡 **ตรวจซ้ำด้วยคำสั่งเดียว** (เหมือนกันทุก OS):

```bash
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "select 'customers' t, count(*) from olist_raw.olist_customers union all select 'orders', count(*) from olist_raw.olist_orders union all select 'order_items', count(*) from olist_raw.olist_order_items union all select 'products', count(*) from olist_raw.olist_products union all select 'sellers', count(*) from olist_raw.olist_sellers;"
```

> 📝 **ทำไม DDL ถึงสะกดผิดว่า `product_name_lenght` / `product_description_lenght`** — เพราะ
> **หัวคอลัมน์ในไฟล์ CSV ต้นฉบับสะกดผิดมาแบบนั้น** การตั้งชื่อคอลัมน์ให้ตรงกับไฟล์ทำให้ `\copy`
> ตามลำดับคอลัมน์ได้ถูกต้อง — อย่า "แก้คำผิด" ให้ถูกหลักภาษา ไม่งั้นข้อมูลจะเลื่อนช่อง

---

## 🧱 Part 1: Star & Snowflake Data Modeling in dbt

### 1.1 Profile สำหรับเชื่อมต่อ `olist_db`

`dbt_root/profiles.yml`

> 📝 **นี่คือ block ที่ต้อง _เพิ่ม_ ไม่ใช่ทั้งไฟล์** — ถ้าเคยทำ Lab 3–10 ไฟล์นี้จะมี profile
> ของสัปดาห์ก่อน (`dvd_kpi`, `coffee_dw`, `coffee_dw_snowflake`, `coffee_dw_scd`, `lab7`, `lab8`,
> `lab9`, `lab10`) อยู่แล้ว ให้วาง `olist_dbt:` ต่อท้ายโดยไม่ลบของเดิม
> (ถ้าเริ่มจาก 0 ไฟล์ยังว่าง — ใส่เฉพาะ block นี้ได้เลย)

```yaml
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

> ⚠️ **จำให้แม่น — ชื่อ host:** เมื่อเชื่อมต่อจาก container หนึ่งไปอีก container หนึ่ง ต้องใช้
> **ชื่อ service คือ `postgres`** และพอร์ตภายใน `5432` — **ไม่ใช้** `localhost`, **ไม่ใช้** พอร์ต `25432`
> และ **ไม่ใช้** ชื่อ container `dw_postgres`

> 📝 **`schema: olist` ไม่ใช่ schema ปลายทางของตารางใด ๆ** — มันเป็นแค่ *ฐาน* ที่ Macro ใน Part 1.3
> เอาไปประกอบเป็น `olist_raw` / `olist_staging` / `olist_marts` อีกที

---

### 1.2 กำหนดค่า dbt Project

`dbt/olist_dbt/dbt_project.yml`

```yaml
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

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/dbt_project.yml`](./script/dbt/olist_dbt/dbt_project.yml) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/dbt_project.yml dbt/olist_dbt/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\dbt_project.yml dbt\olist_dbt\
```

</details>

> 💡 **`+schema` ไม่ใช่ชื่อ schema จริง แต่เป็น "ส่วนท้าย"** — dbt เอาไปต่อกับ `schema:` ใน
> `profiles.yml` ผ่าน Macro ชื่อ `generate_schema_name` ซึ่งเราจะเขียนทับเองในขั้นถัดไป

---

### 1.3 Custom Macro: ล็อกชื่อ Schema ให้นิ่ง

`dbt/olist_dbt/macros/generate_schema_name.sql`

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        olist_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/macros/generate_schema_name.sql`](./script/dbt/olist_dbt/macros/generate_schema_name.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/macros/generate_schema_name.sql dbt/olist_dbt/macros/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\macros\generate_schema_name.sql dbt\olist_dbt\macros\
```

</details>

> ⚠️ **ใบ Lab ระบุไฟล์นี้ไว้ในผังโฟลเดอร์ แต่ไม่ได้ให้เนื้อหามา** — และถ้าไม่มีไฟล์นี้ **Lab จะพังใน Part 3**
> นี่คือเหตุผล:
>
> | ตอนรัน | `target.schema` เป็นอะไร | ถ้าใช้ Macro **default** ของ dbt | ถ้าใช้ Macro ข้างบน |
> |---|---|---|---|
> | **Part 1** — `dbt run` ผ่าน `dw_dbt` | `olist` (จาก `profiles.yml`) | `olist_raw` / `olist_staging` / `olist_marts` ✅ | เหมือนกัน ✅ |
> | **Part 3** — Cosmos บน Airflow | `olist_marts` (Cosmos บังคับผ่าน `profile_args`) | `olist_marts_raw` / `olist_marts_staging` / `olist_marts_marts` ❌ | `olist_raw` / `olist_staging` / `olist_marts` ✅ |
>
> Macro **default** ของ dbt คืนค่า `{{ target.schema }}_{{ custom_schema_name }}` — พอ Cosmos
> เปลี่ยน `target.schema` เป็น `olist_marts` ชื่อ schema จะบวมเป็น `olist_marts_marts` ทันที
> แล้ว `source('raw_olist', ...)` ที่ชี้ไป `olist_raw` ก็จะหาตารางไม่เจอ
> Macro ข้างบน **ไม่สนใจ `target.schema` เลย** เมื่อมี `+schema` — บังคับให้ขึ้นต้นด้วย `olist_` เสมอ
> ทั้ง dbt CLI และ Cosmos จึงเขียนลง schema ชุดเดียวกัน

**Schema ที่จะได้ทั้งหมด**

| Schema | สร้างโดย | มีอะไร |
|---|---|---|
| `olist_raw` | `\copy` (Part 0.6) + `dbt seed` (Part 1.4) | 5 ตาราง Raw + `product_category_name_translation` |
| `olist_staging` | `dbt run` | 6 **View** — `stg_*` |
| `olist_marts` | `dbt run` | 5 **Table** — `dim_category`, `dim_products`, `dim_customers`, `dim_sellers`, `fct_daily_sales` |

---

### 1.4 โหลด Reference Data ด้วย `dbt seed`

```bash
docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt seed"
```

> ✅ **ผลที่ต้องได้:** `1 of 1 OK loaded seed file olist_raw.product_category_name_translation` — **71 แถว**

<details>
<summary><b>Show Output — <code>dbt seed</code></b></summary>

![dbt seed loading 71 rows into olist_raw.product_category_name_translation](./docs/screenshots/dbt-seed.png)

</details>

> 💡 **`dw_dbt` mount `./dbt` ไว้ที่ `/usr/app`** ดังนั้นโปรเจกต์อยู่ที่ `/usr/app/olist_dbt` พอดีตามใบ Lab
> และ `./dbt_root` mount ไว้ที่ `/root/.dbt` ซึ่งเป็นที่ที่ dbt มองหา `profiles.yml` โดยอัตโนมัติ
> จึงไม่ต้องใส่ `--profiles-dir`

---

### 1.5 ประกาศ Source

นี่คือจุดที่เชื่อม `{{ source('raw_olist', '...') }}` เข้ากับชื่อ Schema / Table จริงใน PostgreSQL:

`dbt/olist_dbt/models/staging/schema.yml`

```yaml
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

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/staging/schema.yml`](./script/dbt/olist_dbt/models/staging/schema.yml) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/staging/schema.yml dbt/olist_dbt/models/staging/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\staging\schema.yml dbt\olist_dbt\models\staging\
```

</details>

> 💡 **`source()` ต่างจาก `ref()`** — `ref()` ชี้ไปยัง Model / Seed ที่ dbt สร้างเอง ส่วน `source()`
> ชี้ไปยังตารางที่ **ระบบอื่นสร้าง** (ในที่นี้คือคำสั่ง `\copy` ของเรา) การประกาศ source ไว้ทำให้
> dbt วาด Lineage ได้ครบตั้งแต่ Raw — และทำให้ Cosmos ใน Part 3 วาด Graph ได้ถูก

---

### 1.6 สร้าง Staging Models

Staging Layer ทำหน้าที่ **แปลงชนิดข้อมูล**, **ตัดช่องว่าง** และ **เลือกเฉพาะคอลัมน์ที่ต้องใช้**
ทุกตัวเป็น **View** จึงไม่กินพื้นที่เพิ่ม

`dbt/olist_dbt/models/staging/stg_product_category_translation.sql`

```sql
{{ config(materialized='view') }}

SELECT
    TRIM(product_category_name) AS category_name_portuguese,
    TRIM(product_category_name_english) AS category_name_english
FROM {{ ref('product_category_name_translation') }}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/staging/stg_product_category_translation.sql`](./script/dbt/olist_dbt/models/staging/stg_product_category_translation.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/staging/stg_product_category_translation.sql dbt/olist_dbt/models/staging/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\staging\stg_product_category_translation.sql dbt\olist_dbt\models\staging\
```

</details>

> 💡 **ตัวนี้ใช้ `ref()` ไม่ใช่ `source()`** เพราะต้นทางคือ **seed** ที่ dbt เป็นคนโหลดเอง —
> นี่คือความต่างของสองขาใน Hybrid Ingestion: ขา Bulk Load ใช้ `source()`, ขา seed ใช้ `ref()`

---

`dbt/olist_dbt/models/staging/stg_olist_products.sql`

```sql
{{ config(materialized='view') }}

SELECT
    CAST(product_id AS VARCHAR) AS product_id,
    COALESCE(TRIM(product_category_name), 'unknown') AS product_category_name,
    CAST(product_weight_g AS INT) AS product_weight_g
FROM {{ source('raw_olist', 'olist_products') }}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/staging/stg_olist_products.sql`](./script/dbt/olist_dbt/models/staging/stg_olist_products.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/staging/stg_olist_products.sql dbt/olist_dbt/models/staging/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\staging\stg_olist_products.sql dbt\olist_dbt\models\staging\
```

</details>

> 📝 **`COALESCE(..., 'unknown')` ทำงานกับ 610 แถว** — สินค้า 610 จาก 32,951 ตัวไม่มีหมวดหมู่ในไฟล์
> ต้นฉบับ แถวพวกนี้จะได้ค่า `unknown` ซึ่ง **ไม่มีในตารางแปลภาษา** ผลคือใน `dim_products`
> จะได้ `category_key` เป็น `NULL` — ดูรายละเอียดใน Part 1.7

---

`dbt/olist_dbt/models/staging/stg_olist_orders.sql`

```sql
{{ config(materialized='view') }}

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_status AS VARCHAR) AS order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp
FROM {{ source('raw_olist', 'olist_orders') }}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/staging/stg_olist_orders.sql`](./script/dbt/olist_dbt/models/staging/stg_olist_orders.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/staging/stg_olist_orders.sql dbt/olist_dbt/models/staging/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\staging\stg_olist_orders.sql dbt\olist_dbt\models\staging\
```

</details>

> ⚠️ **Staging ตัวนี้ไม่ได้กรอง `order_status`** — ข้อมูลมี `canceled` **625** และ `unavailable` **609**
> คำสั่งซื้อ ซึ่งจะถูกนับรวมใน `fct_daily_sales` ด้วย ถ้ารายงานจริงต้องการเฉพาะยอดที่ปิดการขายแล้ว
> ต้องเติม `WHERE order_status = 'delivered'` เอง — Lab นี้จงใจไม่กรอง เพื่อให้ตัวเลขตรงกับ Raw

---

`dbt/olist_dbt/models/staging/stg_olist_order_items.sql`

```sql
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

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/staging/stg_olist_order_items.sql`](./script/dbt/olist_dbt/models/staging/stg_olist_order_items.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/staging/stg_olist_order_items.sql dbt/olist_dbt/models/staging/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\staging\stg_olist_order_items.sql dbt\olist_dbt\models\staging\
```

</details>

> 💡 **นี่คือตารางที่กำหนด Grain ของ Fact** — 1 แถว = **1 รายการสินค้าในคำสั่งซื้อ** ไม่ใช่ 1 คำสั่งซื้อ
> คำสั่งซื้อหนึ่งใบมีได้ถึง **21 รายการ** จึงมี 112,650 แถวจาก 98,666 คำสั่งซื้อ

---

`dbt/olist_dbt/models/staging/stg_olist_customers.sql`

```sql
{{ config(materialized='view') }}

SELECT
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(customer_unique_id AS VARCHAR) AS customer_unique_id,
    CAST(customer_city AS VARCHAR) AS customer_city,
    CAST(customer_state AS VARCHAR) AS customer_state
FROM {{ source('raw_olist', 'olist_customers') }}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/staging/stg_olist_customers.sql`](./script/dbt/olist_dbt/models/staging/stg_olist_customers.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/staging/stg_olist_customers.sql dbt/olist_dbt/models/staging/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\staging\stg_olist_customers.sql dbt\olist_dbt\models\staging\
```

</details>

---

`dbt/olist_dbt/models/staging/stg_olist_sellers.sql`

```sql
{{ config(materialized='view') }}

SELECT
    CAST(seller_id AS VARCHAR) AS seller_id,
    CAST(seller_city AS VARCHAR) AS seller_city,
    CAST(seller_state AS VARCHAR) AS seller_state
FROM {{ source('raw_olist', 'olist_sellers') }}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/staging/stg_olist_sellers.sql`](./script/dbt/olist_dbt/models/staging/stg_olist_sellers.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/staging/stg_olist_sellers.sql dbt/olist_dbt/models/staging/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\staging\stg_olist_sellers.sql dbt\olist_dbt\models\staging\
```

</details>

---

### 1.7 Snowflake Layer — Normalize หมวดหมู่สินค้า

> **แนวคิด Snowflake Schema:** แยกข้อมูลหมวดหมู่ออกมาเป็น `dim_category` แล้วให้ `dim_products`
> ถือแค่ Foreign Key — ลดความซ้ำซ้อนและรองรับลำดับชั้น (Hierarchy) ในอนาคต
> ต่างจาก Star Schema ที่จะยัดชื่อหมวดหมู่ลงไปใน `dim_products` ตรง ๆ

`dbt/olist_dbt/models/marts/dim_category.sql`

```sql
{{ config(materialized='table') }}

SELECT
    MD5(category_name_portuguese) AS category_key,
    category_name_portuguese,
    category_name_english
FROM {{ ref('stg_product_category_translation') }}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/marts/dim_category.sql`](./script/dbt/olist_dbt/models/marts/dim_category.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/marts/dim_category.sql dbt/olist_dbt/models/marts/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\marts\dim_category.sql dbt\olist_dbt\models\marts\
```

</details>

> 💡 **`MD5()` สร้าง Surrogate Key จาก Business Key** — ได้ค่าเดิมทุกครั้งที่ Build ใหม่
> ต่างจาก `row_number()` ที่เลขจะสลับกันทุกรอบ (**71 แถว**, ไม่มีชื่อซ้ำ)

---

`dbt/olist_dbt/models/marts/dim_products.sql`

```sql
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

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/marts/dim_products.sql`](./script/dbt/olist_dbt/models/marts/dim_products.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/marts/dim_products.sql dbt/olist_dbt/models/marts/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\marts\dim_products.sql dbt\olist_dbt\models\marts\
```

</details>

> ⚠️ **`LEFT JOIN` คือพระเอกของ Model นี้ — และคือเหตุผลที่ `dbt test` ผ่าน**
> มีหมวดหมู่ **3 ค่า** ที่หาคำแปลไม่เจอ ทำให้สินค้า **623 จาก 32,951 ตัว** ได้ `category_key` เป็น `NULL`:
>
> | หมวดหมู่ที่ไม่มีคำแปล | จำนวนสินค้า | ที่มา |
> |---|---:|---|
> | `unknown` | 610 | มาจาก `COALESCE()` ใน Part 1.6 (ต้นฉบับเป็นค่าว่าง) |
> | `portateis_cozinha_e_preparadores_de_alimentos` | 10 | ตกหล่นในไฟล์แปลภาษา |
> | `pc_gamer` | 3 | ตกหล่นในไฟล์แปลภาษา |
>
> ถ้าเปลี่ยนเป็น `INNER JOIN` สินค้า 623 ตัวจะ **หายไปเงียบ ๆ** แล้ว test `not_null` บน
> `product_id` ก็ยังผ่านอยู่ดี — เพราะแถวหายไปทั้งแถว ไม่ได้เป็น `NULL` **จำนวนแถวคือสิ่งเดียวที่จับได้**
> (ต้องได้ **32,951** เท่ากับ `olist_raw.olist_products` พอดี)

---

### 1.8 Star Layer — Dimensions & Fact

`dbt/olist_dbt/models/marts/dim_customers.sql`

```sql
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ ref('stg_olist_customers') }}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/marts/dim_customers.sql`](./script/dbt/olist_dbt/models/marts/dim_customers.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/marts/dim_customers.sql dbt/olist_dbt/models/marts/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\marts\dim_customers.sql dbt\olist_dbt\models\marts\
```

</details>

> ⚠️ **กับดักวิเคราะห์ที่ใหญ่ที่สุดของชุดข้อมูล Olist — `customer_id` ไม่ใช่ "ลูกค้า"**
> ในชุดนี้ `customer_id` ถูกสร้างใหม่ **ทุกครั้งที่สั่งซื้อ** ตัวเลขยืนยัน: มี 99,441 คำสั่งซื้อ,
> 99,441 `customer_id` และ **คำสั่งซื้อสูงสุดต่อหนึ่ง `customer_id` = 1**
> คนจริง ๆ คือ **`customer_unique_id`** ซึ่งมีแค่ **96,096** คน
> ผลคือใน `fct_daily_sales` คอลัมน์ `total_customers` (`COUNT(DISTINCT o.customer_id)`) จะ
> **เท่ากับ `total_orders` เป๊ะทุกแถวเสมอ** — ไม่ใช่บั๊ก แต่เป็นสิ่งที่ต้องรู้ก่อนเอาไปทำรายงาน
> ถ้าอยากนับ "ลูกค้า" จริง ๆ ต้อง `COUNT(DISTINCT c.customer_unique_id)` แทน

---

`dbt/olist_dbt/models/marts/dim_sellers.sql`

```sql
{{ config(materialized='table') }}

SELECT
    seller_id,
    seller_city,
    seller_state
FROM {{ ref('stg_olist_sellers') }}
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/marts/dim_sellers.sql`](./script/dbt/olist_dbt/models/marts/dim_sellers.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/marts/dim_sellers.sql dbt/olist_dbt/models/marts/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\marts\dim_sellers.sql dbt\olist_dbt\models\marts\
```

</details>

> 📝 **`dim_sellers` ถูกสร้างแต่ไม่มีใครเรียกใช้** — `fct_daily_sales` ไม่ได้ join ตัวนี้เลย
> เป็น Dimension ที่เตรียมไว้สำหรับการวิเคราะห์ฝั่งผู้ขายในอนาคต บน Graph ของ Cosmos (Part 3)
> จะเห็นมันเป็น Task ที่ไม่มีเส้นออกไปไหน — นั่นถูกแล้ว

---

`dbt/olist_dbt/models/marts/fct_daily_sales.sql`

```sql
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

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/marts/fct_daily_sales.sql`](./script/dbt/olist_dbt/models/marts/fct_daily_sales.sql) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/marts/fct_daily_sales.sql dbt/olist_dbt/models/marts/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\marts\fct_daily_sales.sql dbt\olist_dbt\models\marts\
```

</details>

**Grain ของ Fact ตัวนี้:** 1 แถว = **1 (วันที่ × รัฐของลูกค้า × หมวดหมู่สินค้า)** — เป็น
**Aggregated Fact** ไม่ใช่ Transaction Fact จึงได้ **56,859 แถว** จากรายการขาย 112,650 รายการ

> 📝 **`category_name_english` เป็น `NULL` ได้ และมีจริง 1,125 แถว** — มาจากสินค้า 623 ตัวที่ไม่มีคำแปล
> (Part 1.7) `GROUP BY` ของ PostgreSQL จัด `NULL` ทั้งหมดไว้กลุ่มเดียวกัน จึงได้ค่า
> `category_name_english` ที่ต่างกัน **72 ค่า** = 71 ชื่อภาษาอังกฤษ + `NULL` อีกหนึ่ง

> ⚠️ **สาม `JOIN` เป็น inner join ทั้งหมด** — ถ้า `dim_products` หรือ `dim_customers` ขาดแถวไป
> รายการขายนั้นจะ **หายเงียบ ๆ** จาก Fact ทางกันคือเช็กผลรวมใน Part 1.10: จำนวนรายการที่ถูก join
> ต้องเป็น **112,650 พอดี** เท่ากับ `olist_raw.olist_order_items`

---

### 1.9 กำหนด Data Quality Gate

ไฟล์นี้คือสิ่งที่ทำให้ `dbt test` มีอะไรให้ทดสอบ — และเป็น "ชนวน" ของ Failure Lab ใน Part 3:

`dbt/olist_dbt/models/marts/schema.yml`

```yaml
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

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dbt/olist_dbt/models/marts/schema.yml`](./script/dbt/olist_dbt/models/marts/schema.yml) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dbt/olist_dbt/models/marts/schema.yml dbt/olist_dbt/models/marts/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dbt\olist_dbt\models\marts\schema.yml dbt\olist_dbt\models\marts\
```

</details>

> 💡 **6 tests ทั้งหมด** — `unique` + `not_null` บน `dim_products.product_id`,
> `unique` + `not_null` บน `dim_customers.customer_id`, และ `not_null` บน `fct_daily_sales.sale_date`
> กับ `total_revenue` จำชื่อ `not_null_dim_customers_customer_id` ไว้ให้ดี — Part 3 จะทำให้มันแดง

---

### 1.10 Run และ Test

```bash
docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt run && dbt test"
```

<details>
<summary><b>Show Output — <code>dbt run</code> + <code>dbt test</code></b></summary>

![dbt run building 6 views and 5 tables, then dbt test passing all 6 tests](./docs/screenshots/dbt-run-test.png)

</details>

> ✅ **ผลที่ต้องได้:** `Completed successfully` ทั้งสองคำสั่ง — **11 models** (6 view + 5 table) และ **PASS=6**

**ผลตรวจสอบที่คาดหวัง**

| ตาราง / ค่า | ผลที่คาดหวัง |
|---|---:|
| `olist_raw.olist_customers` | 99,441 |
| `olist_raw.olist_orders` | 99,441 |
| `olist_raw.olist_order_items` | 112,650 |
| `olist_raw.olist_products` | 32,951 |
| `olist_raw.olist_sellers` | 3,095 |
| `olist_raw.product_category_name_translation` | 71 |
| `olist_marts.dim_category` | 71 |
| `olist_marts.dim_products` | 32,951 |
| `olist_marts.dim_customers` | 99,441 |
| `olist_marts.dim_sellers` | 3,095 |
| `olist_marts.fct_daily_sales` | **56,859** |
| `sum(total_revenue)` | **13,591,643.70** |
| `sum(total_freight_cost)` | **2,251,909.54** |
| `sum(total_orders)` | 100,152 |
| ช่วงวันที่ใน `sale_date` | `2016-09-04` → `2018-09-03` (616 วัน) |
| `dim_products` ที่ `category_key IS NULL` | 623 |
| `fct_daily_sales` ที่ `category_name_english IS NULL` | 1,125 |

> ✅ ตัวเลขทั้งหมดนี้ **ตรวจสอบกับไฟล์ CSV จริงแล้ว** — ถ้าได้ไม่ตรง ให้ย้อนดู Part 0.6
> (โหลดข้อมูลไม่ครบ) หรือ Part 1.7 (เผลอเปลี่ยน `LEFT JOIN` เป็น `INNER JOIN`)

> 📝 **`sum(total_orders)` = 100,152 ไม่ใช่ 99,441** — เพราะคำสั่งซื้อหนึ่งใบที่มีสินค้าหลายหมวดหมู่
> จะถูกนับซ้ำในทุกกลุ่มที่มันไปโผล่ นี่คือหน้าตาของ **non-additive measure** ที่เจอใน Lab 8:
> `COUNT(DISTINCT ...)` รวมข้ามแถวของ Fact ที่ Aggregate มาแล้วไม่ได้

**ตรวจด้วย SQL** (รันใน pgAdmin ➡️ Query Tool โดยเชื่อมต่อฐานข้อมูล `olist_db`):

```sql
select 'dim_category'    as object_name, count(*) as row_count from olist_marts.dim_category
union all
select 'dim_products',    count(*) from olist_marts.dim_products
union all
select 'dim_customers',   count(*) from olist_marts.dim_customers
union all
select 'dim_sellers',     count(*) from olist_marts.dim_sellers
union all
select 'fct_daily_sales', count(*) from olist_marts.fct_daily_sales;
```

<details>
<summary><b>Show Output — row counts</b></summary>

![pgAdmin query result listing the row count of every mart table in olist_db](./docs/screenshots/pgadmin-row-counts.png)

</details>

---

## 🌬️ Part 2: Advanced Airflow DAGs & Task Flow Patterns

DAG ตัวนี้ **ไม่ได้ย้ายข้อมูลจริง** — มันคือสนามซ้อมสำหรับ 5 Pattern ที่เจอในงานจริง:
Branching, Parallel Fan-out / Fan-in, TaskGroup, `trigger_rule` และ `on_failure_callback`

### 2.1 สร้าง DAG

`dags/lab_airflow_task_patterns.py`

```python
from airflow import DAG
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dags/lab_airflow_task_patterns.py`](./script/dags/lab_airflow_task_patterns.py) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dags/lab_airflow_task_patterns.py dags/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dags\lab_airflow_task_patterns.py dags\
```

</details>

> ⚠️ **บรรทัด `import` ต่างจากใบ Lab — ใบ Lab เขียนแบบ Airflow 2**
> ใบ Lab ใช้ `from airflow.operators.python import ...`, `from airflow.operators.empty import ...`
> และ `from airflow.utils.task_group import TaskGroup` ซึ่งเป็น path ของ **Airflow 2**
> แต่สแตกนี้เป็น **Airflow 3.2.2** ที่ย้าย Operator พื้นฐานออกไปอยู่ใน package
> `apache-airflow-providers-standard` แล้ว ถ้าใช้ path เดิม DAG จะขึ้น **Import Error**
> และ**ไม่โผล่บน UI เลย**
>
> | ใบ Lab (Airflow 2) | README นี้ (Airflow 3.2.2) |
> |---|---|
> | `airflow.operators.python` | `airflow.providers.standard.operators.python` |
> | `airflow.operators.empty` | `airflow.providers.standard.operators.empty` |
> | `airflow.utils.task_group` | `airflow.sdk` |
>
> `from airflow import DAG` และ `airflow.providers.common.sql.operators.sql` ใช้ได้เหมือนเดิมทั้งสองเวอร์ชัน
> `PythonOperator` ที่ใบ Lab import มาก็ไม่ได้ถูกใช้เลย จึงตัดออก
>
> **เช็กเองได้ตลอด:** `docker compose exec airflow-scheduler airflow dags list-import-errors`

#### คำอธิบายโดยละเอียดขององค์ประกอบใน DAG

| องค์ประกอบ (Component) | หน้าที่และหลักการทำงานเชิงลึก |
|---|---|
| **`start_date` & `catchup=False`** | `start_date` กำหนดจุดเริ่มต้นตรรกะเวลาของ DAG ส่วน `catchup=False` ป้องกันไม่ให้ Airflow สั่งรัน DAG ย้อนหลังรวบยอดนับร้อยรอบโดยไม่ตั้งใจเมื่อเพิ่งเปิดใช้งาน DAG |
| **`data_interval_start`** | สะท้อน Logical Date เริ่มต้นของรอบ Schedule จริง **ห้ามใช้ `CURRENT_DATE()`** เพราะจะทำให้การสั่ง Re-run หรือ Backfill ข้อมูลย้อนหลังผิดพลาด |
| **`BranchPythonOperator`** | ตัดสินใจเลือกเส้นทางประมวลผล ฟังก์ชัน Python ต้อง Return คืนค่า `task_id` (หรือ list ของ `task_id`) ที่ต้องการให้ทำงานต่อ Task ในสายที่ไม่ถูกเลือกจะกลายเป็นสถานะ `skipped` |
| **`TaskGroup`** | รวมกลุ่ม Tasks ที่เกี่ยวข้องกันให้แสดงผลเป็นกล่องเดียวที่ยุบ / ขยายได้บน Airflow UI ช่วยให้ Graph View สะอาดตา |
| **`trigger_rule`** | กฎการตัดสินใจรัน Task ถัดไป ปกติคือ `all_success` แต่เมื่อมี Branching ต้องเปลี่ยนเป็น `none_failed_min_one_success` เพื่อให้ Task ปลายทางทำงานได้แม้มีบางสายถูก Skip |
| **`on_failure_callback`** | Event Listener ที่ผูกฟังก์ชัน Python ไว้ เมื่อมี Task ใดใน DAG เกิด Failure Airflow จะส่ง Context Object เข้ามาในฟังก์ชันเพื่อแจ้งเตือนทันที |

> 💡 **ทำไม `check_data_freshness_branch` ต้อง return ชื่อเต็ม `ingestion_group.ingest_orders`**
> เพราะ Task ที่อยู่ใน `TaskGroup` จะถูกเติม prefix ด้วย `group_id` เสมอ ถ้า return แค่ `'ingest_orders'`
> Airflow จะหา Task ไม่เจอแล้ว Skip ทั้งสาย

---

### 2.2 สร้าง Connection `postgres_dwh` ใน Airflow

DAG เรียกใช้ `conn_id='postgres_dwh'` จึงต้องตั้งค่าใน Airflow Web UI ก่อน:

1. เปิดเบราว์เซอร์ไปที่ **Airflow UI**: [http://localhost:28080](http://localhost:28080)
   Login ด้วย **Username** `airflow` / **Password** `airflow`
2. ไปที่เมนู **Admin** ➡️ **Connections**
3. กดปุ่ม **+ (Add a new record)** แล้วกรอก:

| ช่อง | ค่า |
|---|---|
| **Connection Id** | `postgres_dwh` |
| **Connection Type** | `Postgres` |
| **Host** | `postgres` |
| **Database** | `olist_db` |
| **Login** | `dw_user` |
| **Password** | `dw_pass` |
| **Port** | `5432` |

4. กด **Save** แล้วกด **Test** เพื่อทดสอบการเชื่อมต่อ

<details>
<summary><b>📷 หน้าจอสร้าง Connection <code>postgres_dwh</code></b></summary>

![Airflow Admin Connections form filled in for postgres_dwh pointing at host postgres, database olist_db](./docs/screenshots/airflow-connection-postgres-dwh.png)

</details>

> 📝 **บัญชีนี้คนละชุดกับ pgAdmin** — Airflow UI ใช้ `airflow` / `airflow` ส่วน pgAdmin ใช้
> `dw_user@mail.com` / `dw_pass` และฐานข้อมูลใช้ `dw_user` / `dw_pass` อีกชุดหนึ่ง อย่าเอาไปสลับกัน

<details>
<summary><b>🔑 บัญชีทั้งหมดที่ใช้ใน Lab นี้</b></summary>

| ใช้ที่ไหน | URL / Host | Username | Password |
|---|---|---|---|
| **Airflow UI** | [localhost:28080](http://localhost:28080) | `airflow` | `airflow` |
| **pgAdmin** | [localhost:28880](http://localhost:28880) | `dw_user@mail.com` | `dw_pass` |
| **Metabase** | [localhost:23000](http://localhost:23000) | บัญชีที่ตั้งเองตอน setup ครั้งแรก | — |
| **PostgreSQL** (จาก pgAdmin / host) | `dw_postgres` : `5432` · host: `localhost:25432` | `dw_user` | `dw_pass` |
| **PostgreSQL** (จาก container อื่น เช่น dbt / DAG) | `postgres` : `5432` | `dw_user` | `dw_pass` |

</details>

> ⚠️ **ปุ่ม Test ไม่ขึ้น / กดไม่ได้** — ต้องมี `AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'` ใน
> `docker-compose.yaml` (Part 0.4) แล้วสั่ง `docker compose up -d --force-recreate` ใหม่

> ⚠️ **ต้องสร้าง Connection นี้ให้เสร็จก่อนเข้า Part 3** — DAG ของ Cosmos ใช้ `postgres_dwh`
> ตั้งแต่ตอน **Parse ไฟล์** ไม่ใช่ตอนรัน ถ้ายังไม่มี Connection DAG จะขึ้น Import Error ทันที

---

### 2.3 เปิดใช้งานและสั่งรัน DAG

1. กลับไปที่หน้า **DAGs**
2. ค้นหา DAG ชื่อ **`lab_airflow_task_patterns`**
3. สับสวิตช์หน้าชื่อ DAG จาก **Off** เป็น **On** (Unpause)
4. กดปุ่ม **Trigger DAG** (ไอคอน Play ด้านขวา)

> 💡 **DAG ไม่โผล่ใน 30 วินาทีแรกถือว่าปกติ** — scheduler สแกนโฟลเดอร์ `dags/` เป็นรอบ
> ถ้าเกิน 1–2 นาทีแล้วยังไม่เห็น ให้ดู Part 5 (`airflow dags list-import-errors`)

---

### 2.4 ตรวจสอบผลการประมวลผล

เปิด **Graph View** แล้วสังเกต 3 อย่าง:

```text
                                    ┌─ ingestion_group ────────────────┐
                                    │  ingest_orders                   │
start_pipeline → branch_check ──────┤  ingest_items      (ขนานกัน 3)   ├─→ audit_raw_data_quality ─┐
                          │         │  ingest_customers                │                          │
                          │         └──────────────────────────────────┘                          ├─→ end_pipeline
                          └─→ skip_pipeline_notice (skipped) ───────────────────────────────────────┘
```

| สิ่งที่ต้องสังเกต | ผลที่ควรเห็น |
|---|---|
| **Branching** | สาย `ingestion_group` **ทำงาน (เขียว)** ส่วน `skip_pipeline_notice` **ถูก Skip (ชมพู)** |
| **Parallel Execution** | Task ทั้ง 3 ใน `ingestion_group` เริ่มพร้อมกัน (ดูเวลา Start ใน Grid View) |
| **Trigger Rule** | `end_pipeline` **สำเร็จ (เขียว)** ทั้งที่มีสายหนึ่งถูก Skip ไป |

<details>
<summary><b>📷 Airflow Graph View — Branching + Parallel + Trigger Rule</b></summary>

![Airflow Graph View of lab_airflow_task_patterns: ingestion_group green, skip_pipeline_notice pink/skipped, end_pipeline green](./docs/screenshots/airflow-task-patterns-graph.png)

</details>

คลิกที่ Task ใดก็ได้ ➡️ เลือก **Logs** เพื่อดูรายละเอียดการพิมพ์ Log ของ SQL Query

> ⚠️ **`audit_raw_data_quality` จะพังถ้าข้าม Part 0.6 มา** — มันสั่ง
> `SELECT COUNT(*) FROM olist_raw.olist_orders;` ถ้ายังไม่ได้ Bulk Load จะขึ้น
> `relation "olist_raw.olist_orders" does not exist` — และนี่คือโอกาสดีที่จะไปดู Log ของ
> `on_failure_callback` ว่าพิมพ์ `[ALERT FAILURE]` ออกมาจริงไหม

---

## 🔗 Part 3: End-to-End Orchestration & Failure Lab

### 3.1 เขียน DAG เชื่อม dbt ด้วย Astronomer Cosmos

`dags/lab_olist_cosmos_pipeline.py`

```python
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
        project_config=ProjectConfig("/opt/airflow/dbt_projects/olist_dbt"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="dbt"),
    )
```

<details>
<summary><b>⚡ คำสั่งลัด — คัดลอกไฟล์นี้แทนการสร้างเอง</b></summary>

ต้นทาง: [`script/dags/lab_olist_cosmos_pipeline.py`](./script/dags/lab_olist_cosmos_pipeline.py) — วางทั้งสองบรรทัดได้เลย โดย `cd` นับจาก **root ของรีโป**

**Mac / Linux:**

```bash
cd week01-data-warehouse-setup/lab-week01
cp ../../week11-modern-data-stack-cosmos/script/dags/lab_olist_cosmos_pipeline.py dags/
```

**Windows (PowerShell):**

```powershell
cd week01-data-warehouse-setup\lab-week01
Copy-Item ..\..\week11-modern-data-stack-cosmos\script\dags\lab_olist_cosmos_pipeline.py dags\
```

</details>

> ⚠️ **`ProjectConfig` ต่างจากใบ Lab หนึ่งคำ** — ใบ Lab เขียน `"/opt/airflow/dbt/olist_dbt"`
> README นี้ใช้ **`"/opt/airflow/dbt_projects/olist_dbt"`** ตามเหตุผลใน Part 0.4
> (`/opt/airflow/dbt` ถูกจองให้ Lab 10 ไปแล้ว) ที่เหลือเหมือนใบ Lab ทุกตัวอักษร

> 💡 **Cosmos ไม่ได้อ่าน `dbt_root/profiles.yml` เลย** — มันสร้าง `profiles.yml` ชั่วคราวขึ้นมาเองจาก
> **Airflow Connection `postgres_dwh`** ผ่าน `PostgresUserPasswordProfileMapping`
> นี่คือข้อดีเชิง Production: credential อยู่ที่เดียวใน Airflow ไม่ต้องมี password ในไฟล์ที่ commit ลง Git
> — แต่ก็คือสาเหตุที่ `target.schema` กลายเป็น `olist_marts` และทำไมเราต้องมี Macro ใน Part 1.3

### 3.2 เปิดใช้งานและสั่งรัน

1. เปิด **Airflow UI** ([http://localhost:28080](http://localhost:28080))
2. ค้นหา DAG ชื่อ **`lab_olist_cosmos_integration`**
3. สับสวิตช์เปิดใช้งาน (Unpause) จาก **Off** เป็น **On**
4. กดปุ่ม **Trigger DAG**

### 3.3 สังเกตผลลัพธ์ dbt Task Group

1. คลิกเข้าไปที่ DAG แล้วเลือกมุมมอง **Graph**
2. คลี่กล่อง **`dbt_transformation_layer`** ออกมา
3. สังเกตโครงสร้างที่ Cosmos สร้างให้อัตโนมัติ:
   - **ทุก Model กลายเป็น Task ของตัวเอง** พร้อมเส้น Lineage เช่น
     `stg_olist_customers` ➡️ `dim_customers`
   - **ทุก Test ก็เป็น Task ด้วย** — จะเห็น `dim_customers_customer_id.<hash>` ต่อท้าย Model
   - `dbt seed` (`product_category_name_translation`) โผล่เป็น Task แรกสุดสายหนึ่ง
   - `dim_sellers` เป็น Task ที่ไม่มีเส้นออกไปไหน (ไม่มีใคร `ref()` ต่อ — ดู Part 1.8)

<details>
<summary><b>📷 Cosmos Graph — ทุก Model / Test เป็น Task แยกกัน</b></summary>

![Airflow Graph View with dbt_transformation_layer expanded showing one task per dbt model and test with lineage edges](./docs/screenshots/cosmos-graph-success.png)

</details>

> ✅ **วิธีตรวจสอบความถูกต้อง:** ทุก Task ภายใน TaskGroup เป็น **สีเขียว (Success)** และตารางใน
> `olist_marts` ครบทั้ง 5 ตัว ตามตัวเลขใน Part 1.10

> 💡 **นี่คือความต่างจาก Lab 10 ทั้งหมด** — Lab 10 มี Task ชื่อ `dbt_run` ตัวเดียวครอบทั้งโปรเจกต์
> พอพังก็รู้แค่ "dbt พัง" ต้องไปไล่อ่าน Log ยาว ๆ เอง แต่ Cosmos ทำให้ **Graph เป็นเอกสาร
> Lineage ที่รันได้** และ Recovery ได้ละเอียดระดับ Model เดียว

---

### 3.4 Failure Lab — จำลอง, สืบสวน, กู้คืน

#### ขั้นตอนที่ 1: จำลอง Data Failure

แทรกข้อมูลเน่า (`customer_id` เป็น `NULL`) ลงในตาราง Raw:

```bash
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "INSERT INTO olist_raw.olist_customers (customer_id, customer_unique_id, customer_city, customer_state) VALUES (NULL, 'invalid_id', 'Sao Paulo', 'SP');"
```

#### ขั้นตอนที่ 2: สืบสวนและตรวจสอบข้อผิดพลาด

1. กลับไปที่ **Airflow UI** แล้วสั่ง **Trigger** DAG `lab_olist_cosmos_integration` อีกรอบ
2. **สังเกตผล:** Task ชื่อ **`not_null_dim_customers_customer_id`** (ใน Cosmos จะแสดงเป็น
   `dim_customers_customer_id.<hash>`) เปลี่ยนเป็น **สีแดง (Failed)**
3. **ตรวจสอบ Log:** คลิก Task สีแดง ➡️ **Logs** จะพบข้อความจาก dbt:

```text
Failure in test not_null_dim_customers_customer_id (models/marts/schema.yml)
  Got 1 result, configured to fail if != 0
  compiled code at target/compiled/olist_dbt/models/marts/schema.yml/not_null_dim_customers_customer_id.sql
Done. PASS=0 WARN=0 ERROR=1 SKIP=0 TOTAL=1
```

<details>
<summary><b>📷 Cosmos Graph — Test สีแดงและ Task ปลายน้ำถูก Skip</b></summary>

![Airflow Graph View with the not_null dim_customers customer_id test task red and fct_daily_sales downstream showing upstream_failed](./docs/screenshots/cosmos-graph-failure.png)

</details>

**สามข้อสังเกตที่ทำให้เข้าใจ Data Quality Gate จริง ๆ**

| ข้อสังเกต | ทำไม |
|---|---|
| Test `unique_dim_customers_customer_id` **ไม่แดง** | PostgreSQL ถือว่า `NULL` แต่ละตัว **ไม่เท่ากัน** แถว `NULL` เดียวจึงไม่ทำให้ค่าซ้ำ |
| `fct_daily_sales` **ไม่มีแถวเสียเลย** | มันใช้ `JOIN ... ON o.customer_id = c.customer_id` ซึ่ง `NULL` ไม่ match อะไรทั้งสิ้น |
| แต่ Pipeline **หยุดทั้งสาย** | Cosmos วาง Test ไว้ **คั่นกลาง** ระหว่าง Model — `fct_daily_sales` จึงขึ้น `upstream_failed` |

> 💡 **นี่คือคุณค่าของ Data Quality Gate:** ข้อมูลเน่าตัวนี้ **ไม่ได้ทำให้ตัวเลขผิด** แต่มันคือสัญญาณว่า
> ระบบต้นทางเริ่มส่งข้อมูลผิดรูป ถ้าไม่มี Gate คอยจับ วันหนึ่งที่มันเน่าแบบทำให้ตัวเลขผิดจริง ๆ
> จะไม่มีใครรู้จนกว่าจะมีคนทักในที่ประชุม

#### ขั้นตอนที่ 3: แก้ไขข้อมูลและสั่ง Re-run

**1. ลบข้อมูลเน่าออกจากต้นทาง:**

```bash
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "DELETE FROM olist_raw.olist_customers WHERE customer_unique_id = 'invalid_id';"
```

**2. สั่ง Clear Task บน Airflow UI:**

- คลิก Task **`dim_customers`** (ตัว **Model** ไม่ใช่ตัว Test)
- เลือกเมนู **Clear Task** โดยเลือก **Downstream**
- กด **Clear**
- Task จะรันใหม่เฉพาะสายนั้นแล้วเปลี่ยนเป็น **สีเขียว (Success)** โดยไม่ต้องรันใหม่ทั้ง DAG

> ⚠️ **อย่า Clear แค่ Task ของ Test** — เป็นกับดักที่คนพลาดกันมากที่สุด
> การ Clear เฉพาะ Test จะรันแค่ `dbt test` ซ้ำ แต่ตาราง `dim_customers` **ยังมีแถว `NULL` ค้างอยู่**
> (เพราะเป็น `materialized='table'` ที่สร้างไว้รอบก่อน) Test ก็จะแดงซ้ำอีก
> ต้อง Clear ที่ **Model** เพื่อให้ dbt `CREATE TABLE ... AS` ใหม่จากข้อมูลที่แก้แล้ว แล้ว Test จึงจะผ่าน

<details>
<summary><b>📷 Clear Task (Downstream) — Pipeline กลับมาเขียว</b></summary>

![Airflow Clear Task dialog with Downstream selected, and the graph afterwards all green](./docs/screenshots/cosmos-clear-task-recovery.png)

</details>

---

## 📚 Part 4: สรุปภาพรวมและ Best Practices

| องค์ประกอบ | หน้าที่หลัก | แนวคิดสำคัญ |
|---|---|---|
| **Apache Airflow** | Workflow Orchestrator | ดูแลเรื่อง **WHEN & ORDER** — จังหวะเวลา, Branching, Parallel Pipeline และ Alerting |
| **Astronomer Cosmos** | Airflow ⇄ dbt Bridge | ดูแลเรื่อง **VISIBILITY** — แปลง Lineage ของ dbt ให้กลายเป็น Airflow Task ที่ Monitor และ Recover ได้รายตัว |
| **dbt (data build tool)** | Transformation Engine | ดูแลเรื่อง **HOW TO TRANSFORM** — Star / Snowflake Schema, Data Quality Gate (`dbt test`) และ Reference Data (`dbt seed`) |
| **PostgreSQL (`olist_db`)** | Compute & Storage Engine | ดูแลเรื่อง **COMPUTE POWER** — เก็บข้อมูลดิบและ Data Marts แยกจาก Airflow Metadata |

> **Production Checklist สำหรับนำไปใช้งานจริง**
> 1. **Hybrid Ingestion Strategy:** ใช้ Bulk Loading Engine สำหรับ Raw Transactions ขนาดใหญ่ และใช้ `dbt seed` สำหรับ Reference Lookups ไม่เกิน 10,000 แถว
> 2. **Isolated Data Warehouses:** แยก Database สำหรับสถิติธุรกิจ (`olist_db`) ออกจาก Database ระบบงานอื่นเสมอ
> 3. **Never Process Big Data in Airflow Python:** ให้ Airflow สั่ง Query หรือเรียก dbt ไปประมวลผลที่ Data Warehouse ห้ามดึงข้อมูลมาคำนวณใน Memory ของ Airflow
> 4. **Credentials อยู่ที่เดียว:** ใช้ Airflow Connection + Cosmos `ProfileMapping` แทนการเขียน password ลง `profiles.yml` ที่ commit ขึ้น Git
> 5. **Smart Recovery with Clear Task:** เมื่อเกิด Failure ให้สืบค้นสาเหตุจาก Task Logs แก้ที่ข้อมูลต้นทาง แล้ว `Clear Task` ที่ **Model** เพื่อ Re-run เฉพาะส่วนที่ล้มเหลว

**Lab 10 vs Lab 11 — ต่างกันตรงไหน**

| | **Lab 10** | **Lab 11** |
|---|---|---|
| dbt ถูกเรียกยังไง | `BashOperator` 1 ตัว (`dbt run`) | **Cosmos `DbtTaskGroup`** — 1 Task ต่อ 1 Model / Test |
| เห็นอะไรบน Graph | `dbt_run` กล่องเดียว | Lineage ทั้งโปรเจกต์ |
| Recover ได้ละเอียดแค่ไหน | ต้องรัน `dbt run` ใหม่ทั้งชุด | Clear เฉพาะ Model ที่พัง + downstream |
| Credential อยู่ที่ไหน | `profiles.yml` (มี password) | Airflow Connection |
| ชุดข้อมูล | 3,000 แถว | ~350,000 แถว |
| Schema | Star | **Star + Snowflake** |

---

## 🩺 Part 5: การแก้ปัญหาเบื้องต้น / Troubleshooting

| อาการ | จุดที่ควรตรวจสอบ |
|---|---|
| **DAG ไม่ปรากฏบน UI** | รัน `airflow dags list-import-errors` — สาเหตุอันดับ 1 คือ `import` แบบ Airflow 2 (Part 2.1) |
| **`ModuleNotFoundError: No module named 'cosmos'`** | ยังไม่ได้ `docker compose build` หลังเพิ่ม `astronomer-cosmos` ใน `dockerfile.airflow` (Part 0.4) |
| **`The conn_id 'postgres_dwh' isn't defined`** | ยังไม่ได้สร้าง Connection ใน Part 2.2 — Cosmos ต้องใช้ตั้งแต่ตอน Parse DAG |
| **`Runtime Error: fatal: Project not found`** | `ProjectConfig` ชี้ผิดชั้น — ต้องเป็น `/opt/airflow/dbt_projects/olist_dbt` (Part 0.4 / 3.1) |
| **`No such file or directory` ตอน `\copy`** | ยังไม่ได้ mount `raw_data` เข้า service **`postgres`** (Part 0.4) หรือยังไม่ได้แตก zip (Part 0.5) |
| **ตารางไปอยู่ schema `olist_marts_marts`** | ไม่มีไฟล์ `macros/generate_schema_name.sql` หรือเขียนผิด (Part 1.3) |
| **`relation "olist_raw.olist_orders" does not exist`** | ข้าม Part 0.6 มา — ต้อง Bulk Load ก่อนรัน DAG ของ Part 2 |
| **`dbt seed` หา CSV ไม่เจอ** | ไฟล์ต้องอยู่ `dbt/olist_dbt/seeds/` (Part 0.3) ไม่ใช่ `raw_data/` |
| **`Connection refused`** | ภายใน container ต้องใช้ `host: postgres` และ `port: 5432` **ไม่ใช่** `localhost:25432` |
| **`Permission denied` ตอนเขียน `target/`** | โฟลเดอร์ `dbt/olist_dbt/` ถูก Docker สร้างให้เองก่อนหน้า — ลบแล้ว `mkdir` ใหม่ตาม Part 0.2 แล้ว `--force-recreate` |
| **Test แดงซ้ำหลังแก้ข้อมูลแล้ว** | Clear ที่ Task ของ **Test** แทนที่จะเป็น **Model** — ดู ⚠️ ใน Part 3.4 |
| **`fct_daily_sales` ได้ไม่ถึง 56,859 แถว** | มีแถวหลุดจาก inner join (Part 1.8) หรือ Bulk Load ไม่ครบ — เช็กจำนวนแถว Raw ก่อน |
| **Lab 10 พังหลังทำ Lab 11** | เผลอ **แก้** บรรทัด `dbt/lab10:/opt/airflow/dbt` แทนที่จะ **เพิ่ม** บรรทัดใหม่ (Part 0.4) |

**คำสั่งช่วยตรวจสอบ** (เหมือนกันทุก OS):

```bash
# ดูสถานะ container
docker compose ps

# ดู DAG import errors (คำสั่งที่ใช้บ่อยที่สุดของสัปดาห์นี้)
docker compose exec airflow-scheduler airflow dags list-import-errors

# ดู Log ของ scheduler
docker compose logs airflow-scheduler

# ดูว่า Airflow เห็นโปรเจกต์ dbt ครบไหม
docker compose exec airflow-scheduler ls /opt/airflow/dbt_projects/olist_dbt

# ตรวจการเชื่อมต่อ dbt จากฝั่ง dw_dbt
docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt debug"

# ดู schema ทั้งหมดใน olist_db (เช็กว่าไม่มี olist_marts_marts โผล่มา)
docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\dn"
```

---

## 📤 Submission / สิ่งที่ต้องส่ง

ส่งคำตอบผ่าน **Google Form — Lab 11: Modern Data Stack Workshop** *(ลิงก์จากผู้สอน)*

| รายการ | รูปแบบ |
|---|---|
| **Checkpoint 1** | Screenshot **Graph View ของ DAG `lab_olist_cosmos_integration` ที่ Task ทั้งหมดสำเร็จ (สีเขียวครบ)** โดยคลี่กล่อง `dbt_transformation_layer` ออกให้เห็น Model และ Test รายตัว |

> 📝 Screenshot ควรเห็น **ชื่อ DAG `lab_olist_cosmos_integration`** และ **Task ภายใน
> `dbt_transformation_layer` เป็นสีเขียวครบ** ในภาพเดียว เพื่อพิสูจน์ว่า Cosmos รันทั้ง
> Model และ Test จบจริง ไม่ใช่รัน dbt แยกเองจาก `dw_dbt`

---

## 🛠️ Cheat Sheet

| Command | Description |
|---|---|
| `docker compose build` | Build image ใหม่หลังแก้ `dockerfile.airflow` |
| `docker compose up -d --force-recreate` | สร้าง container ใหม่ให้ Volume / env ที่เพิ่มมีผล |
| `docker compose ps` | ดูสถานะ container ทั้งหมด |
| `docker compose logs airflow-scheduler` | ดู Log ของ scheduler |
| `docker compose exec airflow-scheduler airflow dags list` | ลิสต์ DAG ทั้งหมดที่ Airflow มองเห็น |
| `docker compose exec airflow-scheduler airflow dags list-import-errors` | ดู Error ตอน import ไฟล์ DAG |
| `docker compose exec airflow-scheduler airflow connections list` | ตรวจว่า `postgres_dwh` ถูกสร้างแล้วหรือยัง |
| `docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt debug"` | ทดสอบการเชื่อมต่อของ dbt |
| `docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt seed"` | โหลด Reference Lookup Data |
| `docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt run"` | Build staging + marts |
| `docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt test"` | รัน generic tests ทั้ง 6 ตัว |
| `docker exec -it dw_dbt bash -c "cd /usr/app/olist_dbt && dbt ls"` | ลิสต์ node ทั้งหมด (สิ่งที่ Cosmos ใช้สร้าง Graph) |
| `docker exec -it dw_postgres psql -U dw_user -d olist_db` | เปิด psql ที่ฐานข้อมูล `olist_db` |
| `docker exec -it dw_postgres psql -U dw_user -d olist_db -c "\dn"` | ลิสต์ schema ทั้งหมดใน `olist_db` |

---

## 🧾 Quick Reference

| คำศัพท์ | ความหมายใน Lab นี้ |
|---|---|
| **Hybrid Ingestion** | ใช้ Bulk Load (`COPY`) กับข้อมูลใหญ่ และ `dbt seed` กับ Lookup เล็ก ๆ ในไปป์ไลน์เดียวกัน |
| **Snowflake Schema** | Dimension ที่ถูก Normalize ต่อไปอีกชั้น — ที่นี่คือ `dim_products` ➡️ `dim_category` |
| **Star Schema** | Fact ตรงกลาง ล้อมด้วย Dimension แบน ๆ ชั้นเดียว — ที่นี่คือ `fct_daily_sales` ➡️ `dim_customers` / `dim_products` |
| **Surrogate Key** | Key ที่ระบบสร้างเอง (`MD5(...)`) แทนการใช้ Business Key ตรง ๆ |
| **`TaskGroup`** | กล่องยุบ / ขยายได้บน Graph View ที่รวม Task ที่เกี่ยวข้องกัน |
| **`trigger_rule`** | กฎว่า Task จะรันเมื่อ upstream อยู่ในสถานะใด (`none_failed_min_one_success` ใช้คู่กับ Branching) |
| **`DbtTaskGroup`** | ของ Cosmos — อ่าน `dbt ls` แล้วสร้าง Airflow Task ให้ทุก Model / Seed / Test โดยอัตโนมัติ |
| **`ProfileMapping`** | ของ Cosmos — แปลง Airflow Connection ให้กลายเป็น dbt profile ชั่วคราว ไม่ต้องมี `profiles.yml` |
| **Clear Task** | สั่ง Airflow รัน Task ที่เลือก (และ downstream) ใหม่ โดยไม่แตะส่วนที่สำเร็จไปแล้ว |
| **Data Quality Gate** | `dbt test` ที่คั่นกลาง Pipeline — พังแล้วหยุดทั้งสาย ไม่ปล่อยข้อมูลเน่าไหลลง Mart |

---

*Data Warehouse — DSBA8 | Week 11*
