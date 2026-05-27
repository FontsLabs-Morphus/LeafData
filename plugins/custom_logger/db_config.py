# custom_logger/db_config.py
import os
import logging

log = logging.getLogger("airflow.task")

def get_db_config() -> dict | None:
    """
    Safe DB config loader.
    NEVER raises at import time.
    """
    host = os.getenv("FT_DB_HOST")
    log.warning(f" The host in the .env file is {host}")

    if not host:
        log.warning("[DB_CONFIG] FT_DB_HOST not set — metrics disabled")
        return None

    return {
        "host": host,
        "port": int(os.getenv("FT_DB_PORT", "5432")),
        "database": os.getenv("FT_DB_NAME", "file_transfer"),
        "user": os.getenv("FT_DB_USER", "airflow"),
        "password": os.getenv("FT_DB_PASSWORD", "airflow"),
    }
