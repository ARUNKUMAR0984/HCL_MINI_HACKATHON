def build_sql(query: dict) -> str:
    table = query.get("table")
    columns = query.get("columns", [])
    filters = query.get("filters", [])
    joins = query.get("joins", [])
    group_by = query.get("group_by", [])
    having = query.get("having", [])
    sort = query.get("sort")
    limit = query.get("limit")
    subquery = query.get("subquery")

    # ✅ SELECT
    select_clause = ", ".join(columns) if columns else "*"

    # ✅ FROM / SUBQUERY
    if subquery:
        sub_sql = build_sql(subquery)
        sql = f"SELECT {select_clause} FROM ({sub_sql}) AS sub"
    else:
        sql = f"SELECT {select_clause} FROM {table}"

    # ✅ JOINS
    for j in joins:
        join_type = j.get("type", "INNER")
        sql += f" {join_type} JOIN {j['table']} ON {j['on']}"

    # ✅ WHERE
    if filters:
        conditions = []
        for f in filters:
            col = f["column"]
            op = f["operator"]
            val = f["value"]

            if isinstance(val, dict) and "subquery" in val:
                sub_sql = build_sql(val["subquery"])
                conditions.append(f"{col} {op} ({sub_sql})")

            elif isinstance(val, str):
                conditions.append(f"{col} {op} '{val}'")

            else:
                conditions.append(f"{col} {op} {val}")

        sql += " WHERE " + " AND ".join(conditions)

    # ✅ GROUP BY
    if group_by:
        sql += " GROUP BY " + ", ".join(group_by)

    # ✅ HAVING
    if having:
        conditions = [
            f"{h['column']} {h['operator']} {h['value']}"
            for h in having
        ]
        sql += " HAVING " + " AND ".join(conditions)

    # ✅ ORDER BY
    if sort:
        sql += f" ORDER BY {sort['column']} {sort['order'].upper()}"

    # ✅ LIMIT
    if limit:
        sql += f" LIMIT {limit}"

    return sql
