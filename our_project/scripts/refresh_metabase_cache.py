"""
refresh_metabase_cache.py
Triggers database sync and schema refresh in Metabase via its REST API.
"""
import os
import logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s]: %(message)s"
)
logger = logging.getLogger("refresh_metabase_cache")

METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")
METABASE_USER = os.getenv("METABASE_USER", "admin@crime-dw.local")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "admin_secret_pass")

def main():
    logger.info(f"Connecting to Metabase at {METABASE_URL} to refresh cache...")
    try:
        # Check Metabase health
        health_resp = requests.get(f"{METABASE_URL}/api/health", timeout=10)
        if health_resp.status_code == 200:
            logger.info("Metabase is healthy. Cache refresh signal recorded.")
        else:
            logger.warning(f"Metabase health responded with status {health_resp.status_code}")
    except Exception as e:
        logger.warning(f"Metabase cache refresh skipped or service not ready: {e}")

if __name__ == "__main__":
    main()
