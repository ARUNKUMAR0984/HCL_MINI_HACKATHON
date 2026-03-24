import psycopg2
import psycopg2.extras
import os
import time
import logging
from dotenv import load_dotenv
from typing import Optional

load_dotenv()
logger = logging.getLogger(__name__)

# ─── Schema Cache ────────────────────────────────────────────────────────────
_schema_cache: Optional[dict] = None
_schema_cache_ts: float = 0
SCHEMA_CACHE_TTL = 300  # seconds (5 min)


def get_db_connection():
    """Return a new psycopg2 connection. Raises on failure."""
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", 5432),
            connect_timeout=5,
        )
    except psycopg2.OperationalError as e:
        logger.error("DB connection failed: %s", e)
        raise RuntimeError("Database connection error") from e


def fetch_schema(force_refresh: bool = False) -> dict:
    """
    Return the public schema as:
        { "table_name": [{"column": ..., "type": ...}, ...] }

    Results are cached for SCHEMA_CACHE_TTL seconds to avoid repeated
    round-trips on every /query call.
    """
    global _schema_cache, _schema_cache_ts

    now = time.monotonic()
    if not force_refresh and _schema_cache and (now - _schema_cache_ts) < SCHEMA_CACHE_TTL:
        return _schema_cache

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    schema: dict = {}
    for table, column, datatype in rows:
        schema.setdefault(table, []).append({"column": column, "type": datatype})

    _schema_cache = schema
    _schema_cache_ts = now
    return schema
