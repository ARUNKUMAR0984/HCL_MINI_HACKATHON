import json

def fallback_parser(query: str):
    query = query.lower()

    result = {
        "table": "students",
        "columns": ["name", "marks"],
        "filters": [],
        "sort": None,
        "limit": None
    }

    if "top" in query:
        words = query.split()
        for w in words:
            if w.isdigit():
                result["limit"] = int(w)
                result["sort"] = {"column": "marks", "order": "desc"}

    if "marks" in query and "greater" in query:
        value = [int(s) for s in query.split() if s.isdigit()]
        if value:
            result["filters"].append({
                "column": "marks",
                "operator": ">",
                "value": value[0]
            })

    return result


def text_to_json(user_query: str, schema: dict):
    try:
        # 👉 TRY AI (if working)
        return gemini_logic(user_query, schema)

    except:
        # 🔥 FALLBACK (VERY IMPORTANT FOR YOUR PROJECT)
        return fallback_parser(user_query)