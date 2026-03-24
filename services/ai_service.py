"""
ai_service.py
-------------
Converts a natural-language query into a structured query dict.

Primary path  : Gemini (or any LLM you wire up).
Fallback path : rule-based NLP parser that handles the most common patterns.
"""

from __future__ import annotations
import re
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ─── Token helpers ────────────────────────────────────────────────────────────

_LIMIT_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "fifteen": 15, "twenty": 20, "fifty": 50, "hundred": 100,
}

_SORT_SIGNALS = {
    "highest": "DESC", "largest": "DESC", "most": "DESC",
    "maximum": "DESC", "max": "DESC", "best": "DESC", "top": "DESC",
    "lowest": "ASC",  "smallest": "ASC", "least": "ASC",
    "minimum": "ASC", "min": "ASC", "worst": "ASC", "bottom": "ASC",
    "latest": "DESC", "newest": "DESC", "recent": "DESC",
    "oldest": "ASC",  "earliest": "ASC",
}

_AGG_SIGNALS = {
    "average": "AVG", "avg": "AVG", "mean": "AVG",
    "total": "SUM",   "sum": "SUM",
    "count": "COUNT", "how many": "COUNT", "number of": "COUNT",
    "maximum": "MAX", "max": "MAX", "highest": "MAX",
    "minimum": "MIN", "min": "MIN", "lowest": "MIN",
}

# ─── Table matching ───────────────────────────────────────────────────────────

def _find_relevant_tables(query_lower: str, schema: dict) -> list[str]:
    """
    Match table names using whole-word regex so 'orders' doesn't match
    inside 'borders', and return them in the order they appear in the query.
    """
    matches: list[tuple[int, str]] = []
    for table in schema:
        pattern = rf"\b{re.escape(table.lower())}\b"
        m = re.search(pattern, query_lower)
        if m:
            matches.append((m.start(), table))
    matches.sort(key=lambda x: x[0])
    return [t for _, t in matches]


# ─── JOIN detection ───────────────────────────────────────────────────────────

def _detect_joins(schema: dict, tables: list[str]) -> list[dict]:
    joins: list[dict] = []
    seen: set[tuple] = set()

    for t1 in tables:
        for t2 in tables:
            if t1 == t2:
                continue
            for col in schema.get(t2, []):
                col_name = col["column"]
                if col_name.endswith("_id"):
                    ref_table = col_name[:-3]  # e.g. "student_id" → "student"
                    # ✅ Match singular against plural table name
                    if t1.rstrip("s") == ref_table or t1 == ref_table:
                        key = (t2, col_name)
                        if key not in seen:
                            seen.add(key)
                            joins.append({
                                "type": "INNER",
                                "table": t2,
                                "on": f"{t1}.id = {t2}.{col_name}",
                            })
    return joins


# ─── Limit extraction ─────────────────────────────────────────────────────────

def _extract_limit(tokens: list[str]) -> int | None:
    """
    Understand patterns like:
      "top 5", "first 10", "last 3", "show me 7", "give 20 results", "5"
    """
    trigger_words = {"top", "first", "last", "show", "give", "get", "fetch", "limit"}
    for i, tok in enumerate(tokens):
        # bare digit
        if tok.isdigit():
            return int(tok)
        # word number
        if tok in _LIMIT_WORDS:
            return _LIMIT_WORDS[tok]
        # trigger word followed by digit or word-number
        if tok in trigger_words and i + 1 < len(tokens):
            nxt = tokens[i + 1]
            if nxt.isdigit():
                return int(nxt)
            if nxt in _LIMIT_WORDS:
                return _LIMIT_WORDS[nxt]
    return None


# ─── Aggregation detection ────────────────────────────────────────────────────

def _detect_aggregation(query_lower: str, base_table: str, schema: dict) -> dict | None:
    """
    Return a structured aggregation spec, or None.

    Examples handled:
      "average marks"  → AVG(marks)
      "total salary"   → SUM(salary)
      "count students" → COUNT(*)
      "max score"      → MAX(score)
    """
    table_cols = {c["column"] for c in schema.get(base_table, [])}

    for signal, func in _AGG_SIGNALS.items():
        if signal in query_lower:
            # try to find the column immediately after the signal phrase
            rest = query_lower.split(signal, 1)[1].strip()
            first_word = rest.split()[0] if rest.split() else ""

            if first_word in table_cols:
                return {"function": func, "column": first_word}
            # COUNT without an obvious column → COUNT(*)
            if func == "COUNT":
                return {"function": "COUNT", "column": "*"}
            # generic fallback: pick numeric columns
            numeric_cols = [
                c["column"] for c in schema.get(base_table, [])
                if c["type"] in ("integer", "numeric", "real", "double precision", "bigint")
            ]
            if numeric_cols:
                return {"function": func, "column": numeric_cols[0]}
    return None


# ─── Filter extraction ────────────────────────────────────────────────────────

def _extract_filters(query_lower: str, base_table: str, schema: dict) -> list[dict]:
    """
    Detect simple equality / comparison filters from patterns like:
      "where age > 30", "marks above 80", "department = 'HR'"
      "students from class 10", "students in grade A"
    """
    filters: list[dict] = []
    table_cols = {c["column"] for c in schema.get(base_table, [])}

    # Pattern 1: explicit operators  col > val  /  col = 'val'
    op_map = {">": ">", "<": "<", ">=": ">=", "<=": "<=", "=": "=", "!=": "!="}
    for op_str, op_sym in op_map.items():
        pattern = rf"(\w+)\s*{re.escape(op_str)}\s*(['\"]?[\w\.]+['\"]?)"
        for m in re.finditer(pattern, query_lower):
            col, val = m.group(1), m.group(2).strip("'\"")
            if col in table_cols:
                try:
                    val = int(val)
                except ValueError:
                    try:
                        val = float(val)
                    except ValueError:
                        pass
                filters.append({"column": f"{base_table}.{col}", "operator": op_sym, "value": val})

    # Pattern 2: keyword-based  "marks above/below N"
    for keyword, op_sym in [("above", ">"), ("below", "<"), ("over", ">"), ("under", "<"),
                              ("greater than", ">"), ("less than", "<"), ("more than", ">")]:
        pattern = rf"(\w+)\s+{re.escape(keyword)}\s+([\d\.]+)"
        for m in re.finditer(pattern, query_lower):
            col, val_str = m.group(1), m.group(2)
            if col in table_cols:
                val: Any = float(val_str) if "." in val_str else int(val_str)
                filters.append({"column": f"{base_table}.{col}", "operator": op_sym, "value": val})

    # Pattern 3: "from/in <value>"  e.g. "from class 10", "in department HR"
    for col in table_cols:
        # singular and plural versions of column name near a value
        pattern = rf"\b{re.escape(col)}\s+(?:is\s+)?['\"]?([\w\s]+?)['\"]?(?:\b|$)"
        m = re.search(pattern, query_lower)
        if m:
            val_str = m.group(1).strip()
            if val_str and val_str not in {"the", "a", "an", "of", "for", "with"}:
                try:
                    val = int(val_str)
                except ValueError:
                    val = val_str
                filters.append({"column": f"{base_table}.{col}", "operator": "=", "value": val})

    # Deduplicate by column
    seen_cols: set[str] = set()
    deduped: list[dict] = []
    for f in filters:
        if f["column"] not in seen_cols:
            seen_cols.add(f["column"])
            deduped.append(f)

    return deduped


# ─── Sort detection ───────────────────────────────────────────────────────────

def _detect_sort(query_lower: str, base_table: str, schema: dict) -> dict | None:
    """
    Detect ORDER BY from patterns like:
      "order by salary desc", "sort by name", "highest marks", "latest created_at"
    """
    table_cols = {c["column"] for c in schema.get(base_table, [])}

    # Explicit ORDER BY / SORT BY
    m = re.search(r"(?:order|sort)\s+by\s+(\w+)(?:\s+(asc|desc))?", query_lower)
    if m:
        col = m.group(1)
        direction = (m.group(2) or "ASC").upper()
        if col in table_cols:
            return {"column": f"{base_table}.{col}", "order": direction}

    # Implicit from superlative adjectives
    tokens = query_lower.split()
    for i, tok in enumerate(tokens):
        if tok in _SORT_SIGNALS:
            direction = _SORT_SIGNALS[tok]
            # look for a column name near the signal word
            for offset in [1, -1, 2, -2]:
                idx = i + offset
                if 0 <= idx < len(tokens) and tokens[idx] in table_cols:
                    return {"column": f"{base_table}.{tokens[idx]}", "order": direction}
            # default to first numeric column
            for col_info in schema.get(base_table, []):
                if col_info["type"] in ("integer", "numeric", "real", "double precision", "bigint"):
                    return {"column": f"{base_table}.{col_info['column']}", "order": direction}

    return None


# ─── Group-by detection ───────────────────────────────────────────────────────

def _detect_group_by(query_lower: str, base_table: str, schema: dict) -> list[str]:
    """
    Detect GROUP BY from patterns like:
      "group by department", "per department", "by grade", "each class"
    """
    table_cols = {c["column"] for c in schema.get(base_table, [])}
    group_cols: list[str] = []

    # Explicit "group by X"
    m = re.search(r"group\s+by\s+(\w+)", query_lower)
    if m and m.group(1) in table_cols:
        group_cols.append(f"{base_table}.{m.group(1)}")
        return group_cols

    # "per X" / "each X" / "by X"
    m = re.search(r"\b(?:per|each|by)\s+(\w+)", query_lower)
    if m and m.group(1) in table_cols:
        group_cols.append(f"{base_table}.{m.group(1)}")

    return group_cols


# ─── Core fallback parser ─────────────────────────────────────────────────────

def fallback_parser(query: str, schema: dict) -> dict:
    """
    Rule-based parser. Handles:
      - Multi-table JOINs
      - Aggregations (AVG, SUM, COUNT, MAX, MIN)
      - Filters with operators and keyword synonyms
      - GROUP BY / HAVING
      - ORDER BY (explicit and via superlatives)
      - LIMIT (numeric and word-form)
      - Subquery for "above average"
    """
    query_lower = query.lower().strip()
    tokens = re.findall(r"\w+", query_lower)

    # ── tables ────────────────────────────────────────────────────────────────
    tables = _find_relevant_tables(query_lower, schema)
    if not tables:
        tables = [next(iter(schema))]
    base_table = tables[0]

    # ── aggregation ───────────────────────────────────────────────────────────
    # ── aggregation ───────────────────────────────────────────────────────────────
    agg = _detect_aggregation(query_lower, base_table, schema)

    # ✅ Detect joins FIRST before building columns
    joins = _detect_joins(schema, tables) if len(tables) > 1 else []

    if agg:
        func, col = agg["function"], agg["column"]
        expr = f"{func}({col})" if col == "*" else f"{func}({base_table}.{col})"
        columns = [f"{expr} AS result"]
    else:
        columns = [f"{base_table}.*"]
        # ✅ Only add secondary table columns if a join was actually found
        for t in tables[1:]:
            if any(j["table"] == t for j in joins):
                columns.append(f"{t}.*")

    # ── filters ───────────────────────────────────────────────────────────────
    filters = _extract_filters(query_lower, base_table, schema)

    # Special case: "above average" → subquery filter
    if re.search(r"\babove\s+average\b|\bgreater\s+than\s+average\b", query_lower):
        # find a numeric column if none found yet
        num_cols = [
            c["column"] for c in schema.get(base_table, [])
            if c["type"] in ("integer", "numeric", "real", "double precision", "bigint")
        ]
        target_col = num_cols[0] if num_cols else "value"
        filters.append({
            "column": f"{base_table}.{target_col}",
            "operator": ">",
            "value": {
                "subquery": {
                    "table": base_table,
                    "columns": [f"AVG({target_col})"],
                    "filters": [], "joins": [], "group_by": [],
                    "having": [], "sort": None, "limit": None,
                }
            },
        })

    # ── group by ──────────────────────────────────────────────────────────────
    group_by = _detect_group_by(query_lower, base_table, schema)

    # ── having ────────────────────────────────────────────────────────────────
    # If aggregating + group_by + a comparison found in the query, move it to HAVING
    having: list[dict] = []
    if agg and group_by:
        for f in list(filters):
            if f["column"].endswith(agg.get("column", "")):
                having.append({
                    "column": f"{agg['function']}({f['column']})",
                    "operator": f["operator"],
                    "value": f["value"],
                })
                filters.remove(f)

    # ── joins ─────────────────────────────────────────────────────────────────
   
    # ── sort ──────────────────────────────────────────────────────────────────
    sort = _detect_sort(query_lower, base_table, schema)

    # ── limit ────────────────────────────────────────────────────────────────
    limit = _extract_limit(tokens)

    return {
        "table":    base_table,
        "columns":  columns,
        "filters":  filters,
        "joins":    joins,
        "group_by": group_by,
        "having":   having,
        "sort":     sort,
        "limit":    limit,
        "subquery": None,
    }


# ─── LLM prompt for Gemini / any OpenAI-compatible model ─────────────────────

SYSTEM_PROMPT = """
You are a SQL query builder. Given a user question and a database schema,
return ONLY a valid JSON object (no markdown, no explanation) matching this shape:

{
  "table":    "<primary table name>",
  "columns":  ["<col or expression>", ...],
  "filters":  [{"column": "t.col", "operator": "=", "value": <scalar or subquery>}],
  "joins":    [{"type": "INNER|LEFT|RIGHT", "table": "<t>", "on": "<t1.col = t2.col>"}],
  "group_by": ["<col>"],
  "having":   [{"column": "AGG(col)", "operator": ">", "value": <num>}],
  "sort":     {"column": "<t.col>", "order": "ASC|DESC"} or null,
  "limit":    <int> or null,
  "subquery": null
}

Rules:
- Always qualify column names with the table name (students.marks, not just marks).
- Use parameterized-style values (no string interpolation — just put the real value).
- For aggregations include a GROUP BY when selecting non-aggregated columns.
- Never use DROP / DELETE / UPDATE / INSERT / ALTER.
- Return ONLY the JSON object, nothing else.
""".strip()


def gemini_logic(user_query: str, schema: dict) -> dict:
    """
    Call Gemini (or swap in any LLM) with a schema-aware system prompt.
    Raises on failure so the caller can fall back to fallback_parser.
    """
    import json
    import google.generativeai as genai  # type: ignore
    import os

    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel("gemini-1.5-flash")

    schema_str = json.dumps(schema, indent=2)
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"Schema:\n{schema_str}\n\n"
        f"Question: {user_query}\n\n"
        f"JSON:"
    )

    response = model.generate_content(prompt)
    raw = response.text.strip()

    # Strip accidental code fences
    raw = re.sub(r"^```(?:json)?", "", raw).strip()
    raw = re.sub(r"```$", "", raw).strip()

    return json.loads(raw)


# ─── Public entry point ───────────────────────────────────────────────────────

def text_to_json(user_query: str, schema: dict) -> dict:
    """
    Try LLM first; fall back to the rule-based parser on any error.
    """
    try:
        result = gemini_logic(user_query, schema)
        logger.debug("LLM parse succeeded for: %s", user_query)
        return result
    except Exception as e:
        logger.warning("LLM parse failed (%s); using fallback parser.", e)
        return fallback_parser(user_query, schema)
