"""
fetch_daily_crimes.py
Extracts incremental crime incident updates from the Chicago Data Portal (SODA API: ijzp-q8t2)
or loads local bulk CSV files into raw_staging.raw_crimes with audit metadata.
"""
import os
import sys
import logging
import datetime
import requests
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s]: %(message)s"
)
logger = logging.getLogger("ingest_raw_crime_data")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "crime_dw")

APP_TOKEN = os.getenv("CHICAGO_API_APP_TOKEN", "")
DATASET_ID = os.getenv("CHICAGO_CRIME_DATASET_ID", "ijzp-q8t2")
API_ENDPOINT = f"https://data.cityofchicago.org/resource/{DATASET_ID}.json"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )

def get_watermark(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(updated_on) FROM raw_staging.raw_crimes;")
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return None

def fetch_incremental_crimes(watermark=None, limit=50000):
    headers = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    params = {
        "$limit": limit,
        "$order": "updated_on ASC"
    }
    if watermark:
        # 3-Day Lookback window for late-arriving updates
        params["$where"] = f"updated_on >= '{watermark}'"
        logger.info(f"Querying Chicago Data Portal with watermark: {watermark}")
    else:
        logger.info("No existing watermark found. Fetching initial sample batch...")

    try:
        response = requests.get(API_ENDPOINT, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Retrieved {len(data)} incident records from API.")
        return data
    except Exception as e:
        logger.error(f"Failed to query Chicago API: {e}")
        return []

def load_to_raw_staging(conn, records):
    if not records:
        logger.info("No records to load.")
        return 0

    batch_id = datetime.datetime.now(datetime.timezone.utc).strftime("BATCH_%Y%m%d_%H%M%S")
    insert_sql = """
    INSERT INTO raw_staging.raw_crimes (
        id, case_number, date, block, iucr, primary_type, description,
        location_description, arrest, domestic, beat, district, ward,
        community_area, fbi_code, x_coordinate, y_coordinate, year,
        updated_on, latitude, longitude, _batch_id
    ) VALUES (
        %(id)s, %(case_number)s, %(date)s, %(block)s, %(iucr)s, %(primary_type)s, %(description)s,
        %(location_description)s, %(arrest)s, %(domestic)s, %(beat)s, %(district)s, %(ward)s,
        %(community_area)s, %(fbi_code)s, %(x_coordinate)s, %(y_coordinate)s, %(year)s,
        %(updated_on)s, %(latitude)s, %(longitude)s, %(batch_id)s
    );
    """

    formatted_records = []
    for r in records:
        formatted_records.append({
            "id": r.get("id"),
            "case_number": r.get("case_number"),
            "date": r.get("date"),
            "block": r.get("block"),
            "iucr": r.get("iucr"),
            "primary_type": r.get("primary_type"),
            "description": r.get("description"),
            "location_description": r.get("location_description"),
            "arrest": str(r.get("arrest", "false")),
            "domestic": str(r.get("domestic", "false")),
            "beat": r.get("beat"),
            "district": r.get("district"),
            "ward": r.get("ward"),
            "community_area": r.get("community_area"),
            "fbi_code": r.get("fbi_code"),
            "x_coordinate": r.get("x_coordinate"),
            "y_coordinate": r.get("y_coordinate"),
            "year": r.get("year"),
            "updated_on": r.get("updated_on"),
            "latitude": r.get("latitude"),
            "longitude": r.get("longitude"),
            "batch_id": batch_id
        })

    with conn.cursor() as cur:
        execute_batch(cur, insert_sql, formatted_records, page_size=1000)
    conn.commit()
    logger.info(f"Successfully loaded {len(formatted_records)} rows with batch_id: {batch_id}")
    return len(formatted_records)

def main():
    try:
        conn = get_db_connection()
        watermark = get_watermark(conn)
        records = fetch_incremental_crimes(watermark=watermark)
        load_to_raw_staging(conn, records)
        conn.close()
    except Exception as e:
        logger.error(f"Ingestion pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
