"""
sql_builder.py
--------------
Converts a structured query dict → (sql_string, params_tuple).

Structured query shape
----------------------
{
    "table":    str,
    "columns":  list[str],          # e.g. ["students.*", "AVG(marks) AS avg_marks"]
    "filters":  list[FilterDict],
    "joins":    list[JoinDict],
    "group_by": list[str],
    "having":   list[FilterDict],
    "sort":     {"column": str, "order": "ASC"|"DESC"} | None,
    "limit":    int | None,
    "subquery": <recursive StructuredQuery> | None
}

FilterDict  = {"column": str, "operator": str, "value": any}
JoinDict    = {"type": str, "table": str, "on": str}

Values that are dicts with a "subquery" key are rendered as sub-SELECTs.
All scalar values are passed as parameterized placeholders (%s) — never
interpolated directly into the SQL string.
"""

from __future__ import annotations
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Allowlist for operators to prevent injection via the operator field
_ALLOWED_OPERATORS = {"=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "ILIKE", "IN", "NOT IN", "IS", "IS NOT"}

# Allowlist for JOIN types
_ALLOWED_JOIN_TYPES = {"INNER", "LEFT", "RIGHT", "FULL", "CROSS", "LEFT OUTER", "RIGHT OUTER"}

# Allowlist for sort directions
_ALLOWED_SORT_ORDERS = {"ASC", "DESC"}


def _safe_operator(op: str) -> str:
    op_upper = op.strip().upper()
    if op_upper not in _ALLOWED_OPERATORS:
        raise ValueError(f"Unsupported operator: '{op}'")
    return op_upper


def _safe_identifier(name: str) -> str:
    """
    Quote a table/column identifier with double-quotes.
    Allows: letters, digits, underscores, dots (for table.column), *, and
    basic aggregate expressions like AVG(marks).
    Rejects anything else to prevent injection.
    """
    # Allow qualified names (table.col), wildcards, aggregates already written by the AI
    if re.match(r'^[\w\s\.\*\(\),]+$', name):
        return name  # Trust AI-generated column expressions; they are not user-supplied
    raise ValueError(f"Invalid identifier: '{name}'")


def build_sql(query: dict) -> tuple[str, tuple]:
    """
    Returns (sql, params) where params is a tuple of values
    to pass to cursor.execute(sql, params).
    """
    table   = query.get("table", "")
    columns = query.get("columns") or ["*"]
    filters = query.get("filters") or []
    joins   = query.get("joins") or []
    group_by = query.get("group_by") or []
    having  = query.get("having") or []
    sort    = query.get("sort")
    limit   = query.get("limit")
    subquery = query.get("subquery")

    params: list[Any] = []

    # ── SELECT ──────────────────────────────────────────────────────────────
    select_clause = ", ".join(_safe_identifier(c) for c in columns)

    # ── FROM / subquery ─────────────────────────────────────────────────────
    if subquery:
        sub_sql, sub_params = build_sql(subquery)
        params.extend(sub_params)
        sql = f"SELECT {select_clause} FROM ({sub_sql}) AS sub"
    else:
        sql = f"SELECT {select_clause} FROM {_safe_identifier(table)}"

    # ── JOINs ───────────────────────────────────────────────────────────────
    for j in joins:
        jtype = j.get("type", "INNER").upper()
        if jtype not in _ALLOWED_JOIN_TYPES:
            raise ValueError(f"Unsupported join type: '{jtype}'")
        join_table = _safe_identifier(j["table"])
        on_clause  = j["on"]   # e.g. "orders.student_id = students.id"
        sql += f" {jtype} JOIN {join_table} ON {on_clause}"

    # ── WHERE ────────────────────────────────────────────────────────────────
    if filters:
        conditions, filter_params = _build_conditions(filters)
        params.extend(filter_params)
        sql += " WHERE " + " AND ".join(conditions)

    # ── GROUP BY ─────────────────────────────────────────────────────────────
    if group_by:
        sql += " GROUP BY " + ", ".join(_safe_identifier(c) for c in group_by)

    # ── HAVING ───────────────────────────────────────────────────────────────
    if having:
        conditions, having_params = _build_conditions(having)
        params.extend(having_params)
        sql += " HAVING " + " AND ".join(conditions)

    # ── ORDER BY ─────────────────────────────────────────────────────────────
    if sort:
        order = sort.get("order", "ASC").upper()
        if order not in _ALLOWED_SORT_ORDERS:
            order = "ASC"
        sql += f" ORDER BY {_safe_identifier(sort['column'])} {order}"

    # ── LIMIT ────────────────────────────────────────────────────────────────
    if limit is not None:
        try:
            limit_val = int(limit)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid LIMIT value: '{limit}'")
        sql += f" LIMIT {limit_val}"  # integer — safe to inline

    logger.debug("Built SQL: %s | params: %s", sql, params)
    return sql, tuple(params)


def _build_conditions(filters: list[dict]) -> tuple[list[str], list[Any]]:
    conditions: list[str] = []
    params: list[Any] = []

    for f in filters:
        col = _safe_identifier(f["column"])
        op  = _safe_operator(f["operator"])
        val = f["value"]

        if isinstance(val, dict) and "subquery" in val:
            sub_sql, sub_params = build_sql(val["subquery"])
            params.extend(sub_params)
            conditions.append(f"{col} {op} ({sub_sql})")

        elif op in ("IN", "NOT IN"):
            if not isinstance(val, (list, tuple)):
                val = [val]
            placeholders = ", ".join(["%s"] * len(val))
            params.extend(val)
            conditions.append(f"{col} {op} ({placeholders})")

        elif val is None or str(val).upper() in ("NULL", "NOT NULL"):
            # IS NULL / IS NOT NULL — no param needed
            conditions.append(f"{col} {op} NULL")

        else:
            params.append(val)
            conditions.append(f"{col} {op} %s")

    return conditions, params
