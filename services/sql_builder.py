def build_sql(query: dict) -> str:
    table = query.get("table")
    columns = query.get("columns", [])
    filters = query.get("filters", [])
    sort = query.get("sort")
    limit = query.get("limit")

    # ✅ SELECT
    if columns:
        select_clause = ", ".join(columns)
    else:
        select_clause = "*"

    sql = f"SELECT {select_clause} FROM {table}"

    # ✅ WHERE
    if filters:
        conditions = []
        for f in filters:
            col = f["column"]
            op = f["operator"]
            val = f["value"]

            # Handle string values
            if isinstance(val, str):
                val = f"'{val}'"

            conditions.append(f"{col} {op} {val}")

        sql += " WHERE " + " AND ".join(conditions)

    # ✅ ORDER BY
    if sort:
        sql += f" ORDER BY {sort['column']} {sort['order'].upper()}"

    # ✅ LIMIT
    if limit:
        sql += f" LIMIT {limit}"

    return sql