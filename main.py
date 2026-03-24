from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from database import get_db_connection
from services.sql_builder import build_sql
from services.sql_builder import build_sql
from services.validator import validate_sql
from services.executor import execute_query
from fastapi import HTTPException
from services.ai_service import text_to_json
from database import fetch_schema

app = FastAPI()

# ✅ CORS (for React frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change later in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Root API
@app.get("/")
def root():
    return {"message": "🚀 Backend is running successfully"}

# ✅ Health Check
@app.get("/health")
def health():
    return {"status": "ok"}

# ✅ Schema API
@app.get("/schema")
def get_schema():
    try:
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

        return {
            "success": True,
            "schema": schema
        }

    except Exception as e:
        print("Schema fetch error:", str(e))
        raise HTTPException(
            status_code=500,
            detail="Error fetching schema"
        )
    
@app.post("/build-sql")
def generate_sql(query: dict):
    try:
        sql = build_sql(query)

        return {
            "success": True,
            "sql": sql
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/execute")
def execute(query: dict):
    try:
        # Step 1: Build SQL
        sql = build_sql(query)

        # Step 2: Validate
        validate_sql(sql)

        # Step 3: Execute
        result = execute_query(sql)

        return {
            "success": True,
            "sql": sql,
            "data": result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/text-to-json")
def convert_text(query: dict):
    try:
        user_query = query.get("query")

        schema = get_schema()["schema"]

        result = text_to_json(user_query, schema)

        return {
            "success": True,
            "json": result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
@app.post("/query")
def full_query(query: dict):
    try:
        user_text = query.get("query")

        # ✅ REAL schema
        schema = fetch_schema()

        # ✅ TEXT → JSON
        structured_query = text_to_json(user_text, schema)

        # ✅ JSON → SQL
        sql = build_sql(structured_query)

        # ✅ VALIDATE
        validate_sql(sql)

        # ✅ EXECUTE
        data = execute_query(sql)

        return {
            "success": True,
            "input": user_text,
            "json": structured_query,
            "sql": sql,
            "data": data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
