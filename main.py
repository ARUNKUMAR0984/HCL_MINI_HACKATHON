"""
main.py  –  FastAPI entry point
"""

import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import get_db_connection, fetch_schema
from services.sql_builder import build_sql
from services.validator import validate_sql
from services.executor import execute_query
from services.ai_service import text_to_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Text-to-SQL API", version="2.0")

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request models ────────────────────────────────────────────────────────────
class TextQuery(BaseModel):
    query: str


# ── Helpers ───────────────────────────────────────────────────────────────────
def _run_structured_query(structured: dict) -> tuple[str, list[dict]]:
    """Build → validate → execute. Returns (sql, rows)."""
    sql, params = build_sql(structured)
    validate_sql(sql)
    data = execute_query(sql, params)
    return sql, data


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"message": "Text-to-SQL backend is running"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/schema")
def get_schema(refresh: bool = False):
    """
    Return the live database schema.
    Pass ?refresh=true to bust the cache.
    """
    try:
        schema = fetch_schema(force_refresh=refresh)
        return {"success": True, "schema": schema}
    except Exception as e:
        logger.error("Schema fetch error: %s", e)
        raise HTTPException(status_code=500, detail="Error fetching schema")


@app.post("/build-sql")
def generate_sql(query: dict):
    """Convert a structured query dict to SQL (no execution)."""
    try:
        sql, params = build_sql(query)
        return {"success": True, "sql": sql, "params": list(params)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/execute")
def execute(query: dict):
    """Build, validate, and execute a structured query dict."""
    try:
        sql, data = _run_structured_query(query)
        return {"success": True, "sql": sql, "data": data}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Execute error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/text-to-json")
def convert_text(payload: TextQuery):
    """Parse natural language into a structured query dict (no SQL execution)."""
    try:
        schema = fetch_schema()
        result = text_to_json(payload.query, schema)
        return {"success": True, "json": result}
    except Exception as e:
        logger.exception("text-to-json error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query")
def full_query(payload: TextQuery):
    """
    Full pipeline: natural language → structured JSON → SQL → results.

    Returns every intermediate artifact so the frontend can show
    the generated SQL alongside the data.
    """
    try:
        schema = fetch_schema()                          # cached
        structured = text_to_json(payload.query, schema)  # LLM / fallback
        sql, data = _run_structured_query(structured)

        return {
            "success":    True,
            "input":      payload.query,
            "json":       structured,
            "sql":        sql,
            "row_count":  len(data),
            "data":       data,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Full query error for: %s", payload.query)
        raise HTTPException(status_code=500, detail=str(e))
