def find_relevant_tables(query: str, schema: dict):
    query = query.lower()
    return [t for t in schema.keys() if t.lower() in query]


def detect_join(schema: dict, tables: list):
    joins = []

    if len(tables) < 2:
        return joins

    for t1 in tables:
        for t2 in tables:
            if t1 == t2:
                continue

            for col in schema[t2]:
                col_name = col["column"]

                if col_name.endswith("_id"):
                    ref_table = col_name.replace("_id", "")

                    if ref_table == t1:
                        joins.append({
                            "type": "INNER",
                            "table": t2,
                            "on": f"{t1}.id = {t2}.{col_name}"
                        })

    return joins


def fallback_parser(query: str, schema: dict):
    query_lower = query.lower()

    tables = find_relevant_tables(query_lower, schema)
    base_table = tables[0] if tables else list(schema.keys())[0]

    result = {
        "table": base_table,
        "columns": [f"{base_table}.*"],
        "filters": [],
        "joins": [],
        "group_by": [],
        "having": [],
        "sort": None,
        "limit": None
    }

    # 🔥 JOIN (dynamic)
    if len(tables) > 1:
        result["joins"] = detect_join(schema, tables)

        for t in tables:
            if t != base_table:
                result["columns"].append(f"{t}.*")

    # 🔥 LIMIT
    for word in query_lower.split():
        if word.isdigit():
            result["limit"] = int(word)

    # 🔥 SUBQUERY (average)
    if "average" in query_lower:
        result["filters"].append({
            "column": f"{base_table}.marks",
            "operator": ">",
            "value": {
                "subquery": {
                    "table": base_table,
                    "columns": ["AVG(marks)"]
                }
            }
        })

    return result


def text_to_json(user_query: str, schema: dict):
    try:
        return gemini_logic(user_query, schema)  # if available
    except:
        return fallback_parser(user_query, schema)
