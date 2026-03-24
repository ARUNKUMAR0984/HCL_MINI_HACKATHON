import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    try:
        return psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
    except Exception as e:
        print("Database connection failed:", str(e))
        raise Exception("Database connection error")


# 🔥 NEW: Fetch schema dynamically
def fetch_schema():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
    """)

    rows = cursor.fetchall()

    schema = {}

    for table, column, datatype in rows:
        if table not in schema:
            schema[table] = []

        schema[table].append({
            "column": column,
            "type": datatype
        })

    cursor.close()
    conn.close()

    return schema
