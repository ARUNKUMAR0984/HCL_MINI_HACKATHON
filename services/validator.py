def validate_sql(sql: str):
    forbidden_keywords = ["drop", "delete", "update", "insert", "alter"]

    sql_lower = sql.lower()

    for word in forbidden_keywords:
        if word in sql_lower:
            raise Exception(f"Forbidden operation detected: {word}")

    return True