from database import get_db_connection

def execute_query(sql: str):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(sql)

    # Get column names
    columns = [desc[0] for desc in cursor.description]

    # Fetch data
    rows = cursor.fetchall()

    result = []

    for row in rows:
        result.append(dict(zip(columns, row)))

    cursor.close()
    conn.close()

    return result