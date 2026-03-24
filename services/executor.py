import logging
import psycopg2.extras
from database import get_db_connection

logger = logging.getLogger(__name__)


def execute_query(sql: str, params: tuple = ()) -> list[dict]:
    """
    Execute a read-only SQL statement with optional parameterized values.

    Always uses RealDictCursor so column names are preserved as dict keys.
    Raises on any DB error — caller is responsible for HTTP mapping.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            logger.debug("Executing SQL: %s | params: %s", sql, params)
            cur.execute(sql, params or None)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error("Query execution failed: %s", e)
        raise
    finally:
        conn.close()
