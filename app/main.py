from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import date, datetime
import json
import pandas as pd
import numpy as np

from app.routers.analytics import analytics_router
from app.routers.covid import covid_router
from app.routers.dashboard import dashboard_router

import os
import logging
from dotenv import load_dotenv
import snowflake.connector
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

APP_NAME = "COVID Analytics Suite - Multi-Source Data Platform"

class CacheMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
            
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = dict(message.get("headers", []))
                if scope["method"] == "GET":
                    message["headers"] = [
                        *message.get("headers", []),
                        (b"Cache-Control", b"max-age=300"),
                    ]
            await send(message)
            
        return await self.app(scope, receive, send_wrapper)

class SafeJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        def clean_item(item):
            if isinstance(item, dict):
                return {k: clean_item(v) for k, v in item.items()}
            elif isinstance(item, list):
                return [clean_item(i) for i in item]
            elif pd.isna(item) or item is None:
                return None
            elif isinstance(item, (int, float)):
                if np.isnan(item) or np.isinf(item):
                    return None
                return float(item) if isinstance(item, (np.float64, np.float32)) else int(item)
            elif isinstance(item, np.integer):
                return int(item)
            elif isinstance(item, np.floating):
                if np.isnan(item) or np.isinf(item):
                    return None
                return float(item)
            elif hasattr(item, 'isoformat'):
                return item.isoformat()
            else:
                return item
        
        cleaned_obj = clean_item(obj)
        return super().encode(cleaned_obj)

app = FastAPI(title=APP_NAME, version="2.0.0")
app.add_middleware(CacheMiddleware)

@app.middleware("http")
async def catch_json_errors(request, call_next):
    try:
        response = await call_next(request)
        return response
    except ValueError as e:
        if "Out of range float values are not JSON compliant" in str(e):
            logger.error(f"JSON serialization error: {e}")
            return JSONResponse(
                status_code=500,
                content={"detail": "Data processing error - invalid numeric values detected"}
            )
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise e

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    if os.path.exists("static"):
        app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception as e:
    logger.warning(f"Could not mount static files: {e}")

app.include_router(analytics_router)
app.include_router(covid_router)
app.include_router(dashboard_router)

SF_CFG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DB", "covid_meta")
MONGO_COUNTRY_COL = os.getenv("MONGODB_COUNTRY_COL", "country_stats")

REQUIRED_SF = ["account", "user", "password", "warehouse", "database", "schema"]

def _sf_conn():
    missing = [k for k in REQUIRED_SF if not SF_CFG.get(k)]
    if missing:
        raise RuntimeError(f"Snowflake ENV is incomplete: missing {missing}")
    return snowflake.connector.connect(
        account=SF_CFG["account"],
        user=SF_CFG["user"],
        password=SF_CFG["password"],
        warehouse=SF_CFG["warehouse"],
        database=SF_CFG["database"],
        schema=SF_CFG["schema"],
        role=SF_CFG.get("role"),
        autocommit=True,
    )

def clean_for_json(obj):
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif pd.isna(obj) or obj is None:
        return None
    elif isinstance(obj, (int, float)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj) if isinstance(obj, (np.float64, np.float32)) else int(obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        return obj

def sf_query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        with _sf_conn() as con:
            cur = con.cursor()
            try:
                cur.execute(sql, params)
                try:
                    df = cur.fetch_pandas_all()
                except Exception as e:
                    logger.warning(f"fetch_pandas_all failed: {e}. Falling back to manual DataFrame creation.")
                    rows = cur.fetchall()
                    cols = [desc[0] for desc in cur.description]
                    df = pd.DataFrame(rows, columns=cols)
            finally:
                cur.close()
        
        return clean_dataframe(df)
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        raise

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace([np.inf, -np.inf], np.nan)
    return df

def _mongo():
    if not MONGO_URI:
        raise RuntimeError("MONGODB_URI not set")
    try:
        cli = MongoClient(MONGO_URI)
        return cli[MONGO_DB]
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise

class CountryStat(BaseModel):
    country: str
    gdp_per_capita: float
    population: int

class Annotation(BaseModel):
    dashboard_id: str = Field(default="covid_dashboard")
    author: str
    text: str
    tags: Optional[List[str]] = None
    created_at: Optional[datetime] = None

@app.get("/")
def root():
    return clean_for_json({
        "message": "COVID Analytics Suite - Multi-Source Data Platform",
        "version": "2.0.0",
        "features": [
            "Multi-source COVID-19 data integration",
            "Advanced analytics and correlations",
            "Real-time dashboard visualization",
            "Predictive modeling",
            "Cross-validation between data sources"
        ],
        "data_sources": [
            "Johns Hopkins University (JHU)",
            "Robert Koch Institute (RKI) - Germany",
            "World Health Organization (WHO)",
            "European Centre for Disease Prevention and Control (ECDC)",
            "Our World in Data (OWID) - Vaccinations",
            "Travel restrictions database"
        ],
        "available_endpoints": {
            "/health": "System health check",
            "/covid/*": "COVID-19 data endpoints",
            "/analytics/*": "Advanced analytics endpoints", 
            "/dashboard/*": "Dashboard and visualization endpoints"
        }
    })

@app.get("/health")
def health(verbose: int = Query(0, ge=0, le=1)):
    info: Dict[str, Any] = {
        "app": APP_NAME,
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        test_query = "SELECT 1 as health_check, CURRENT_TIMESTAMP() as timestamp"
        result = sf_query_df(test_query)
        info["snowflake"] = {
            "status": True,
            "timestamp": result.iloc[0]['TIMESTAMP'].isoformat() if not result.empty else None
        }
    except Exception as e:
        info["snowflake"] = {
            "status": False,
            "error": repr(e) if verbose else "Connection failed"
        }
        logger.warning(f"Snowflake health check failed: {e}")
    
    try:
        db = _mongo()
        collections = db.list_collection_names()
        info["mongodb"] = {
            "status": True,
            "collections_count": len(collections),
            "available_collections": collections if verbose else None
        }
    except Exception as e:
        info["mongodb"] = {
            "status": False,
            "error": repr(e) if verbose else "Connection failed"
        }
        logger.warning(f"MongoDB health check failed: {e}")
    
    if verbose and info.get("snowflake", {}).get("status"):
        data_sources_health = {}
        tables_to_check = [
            "OPTIMIZED_JHU_COVID_19_TIMESERIES",
            "RKI_GER_COVID19_DASHBOARD", 
            "WHO_SITUATION_REPORTS",
            "HUM_RESTRICTIONS_AIRLINE",
            "ECDC_GLOBAL",
            "OWID_VACCINATIONS"
        ]
        
        for table in tables_to_check:
            try:
                check_sql = f"SELECT COUNT(*) as row_count FROM WORK_DB.PUBLIC.{table} LIMIT 1"
                result = sf_query_df(check_sql)
                row_count = clean_for_json(result.iloc[0]['ROW_COUNT']) if not result.empty else 0
                data_sources_health[table] = {
                    "available": True,
                    "row_count": int(row_count) if row_count is not None else 0
                }
            except Exception as e:
                data_sources_health[table] = {
                    "available": False,
                    "error": str(e)
                }
        
        info["data_sources"] = data_sources_health
    
    overall_status = (
        info.get("snowflake", {}).get("status", False) and 
        info.get("mongodb", {}).get("status", False)
    )
    
    info["overall_status"] = "healthy" if overall_status else "degraded"
    
    return clean_for_json(info)

@app.post("/metadata/country")
def upsert_country_meta(item: CountryStat):
    try:
        db = _mongo()
        col = db[MONGO_COUNTRY_COL]
        
        clean_item = clean_for_json(item.dict())
        
        res = col.update_one({"country": item.country}, {"$set": clean_item}, upsert=True)
        out = {"matched": res.matched_count, "modified": res.modified_count}
        if res.upserted_id:
            out["upserted_id"] = str(res.upserted_id)
        return clean_for_json(out)
    except Exception as e:
        logger.error(f"Error upserting country meta {e}")
        raise HTTPException(status_code=500, detail="Failed to update country metadata")

@app.post("/annotations")
def add_annotation(item: Annotation):
    try:
        item.created_at = item.created_at or datetime.utcnow()
        db = _mongo()
        col = db["annotations"]
        
        clean_item = clean_for_json(item.dict())
        
        res = col.insert_one(clean_item)
        return clean_for_json({"inserted_id": str(res.inserted_id)})
    except Exception as e:
        logger.error(f"Error adding annotation: {e}")
        raise HTTPException(status_code=500, detail="Failed to add annotation")

@app.get("/annotations")
def list_annotations(dashboard_id: str = "covid_dashboard", limit: int = Query(100, ge=1, le=1000)):
    try:
        db = _mongo()
        col = db["annotations"]
        docs_cursor = col.find({"dashboard_id": dashboard_id}).sort("created_at", -1).limit(limit)
        docs = list(docs_cursor)
        for d in docs:
            d["_id"] = str(d["_id"])
        return clean_for_json({"items": docs})
    except Exception as e:
        logger.error(f"Error listing annotations: {e}")
        raise HTTPException(status_code=500, detail="Failed to list annotations")

@app.get("/info/data-sources")
def get_data_sources_info():
    return clean_for_json({
        "data_sources": {
            "jhu_timeseries": {
                "name": "Johns Hopkins University COVID-19 Timeseries",
                "table": "OPTIMIZED_JHU_COVID_19_TIMESERIES",
                "description": "Global COVID-19 case and death counts by country and date",
                "coverage": "Global",
                "update_frequency": "Daily",
                "metrics": ["confirmed_cases", "deaths", "recovered"]
            },
            "rki_germany": {
                "name": "Robert Koch Institute Germany Dashboard",
                "table": "RKI_GER_COVID19_DASHBOARD",
                "description": "Detailed COVID-19 data for German regions",
                "coverage": "Germany (regional level)",
                "update_frequency": "Daily",
                "metrics": ["cases", "deaths", "cases_per_100k", "death_rate"]
            },
            "who_reports": {
                "name": "WHO Situation Reports",
                "table": "WHO_SITUATION_REPORTS",
                "description": "Official WHO COVID-19 situation reports by country",
                "coverage": "Global",
                "update_frequency": "Daily",
                "metrics": ["total_cases", "new_cases", "total_deaths", "new_deaths"]
            },
            "ecdc_global": {
                "name": "European Centre for Disease Prevention and Control",
                "table": "ECDC_GLOBAL",
                "description": "European and global COVID-19 surveillance data",
                "coverage": "Global (EU focus)",
                "update_frequency": "Daily",
                "metrics": ["cases", "deaths", "population_data"]
            },
            "owid_vaccinations": {
                "name": "Our World in Data Vaccinations",
                "table": "OWID_VACCINATIONS",
                "description": "Global COVID-19 vaccination statistics",
                "coverage": "Global",
                "update_frequency": "Daily",
                "metrics": ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "vaccination_rates"]
            },
            "travel_restrictions": {
                "name": "Travel and Airline Restrictions",
                "table": "HUM_RESTRICTIONS_AIRLINE",
                "description": "COVID-19 related travel and airline restrictions",
                "coverage": "Global",
                "update_frequency": "As needed",
                "metrics": ["restriction_text", "country", "airline", "publication_date"]
            }
        },
        "analytics_capabilities": {
            "correlation_analysis": "Cross-source correlation analysis between different metrics",
            "time_series_forecasting": "ARIMA-based prediction models",
            "comparative_analysis": "Multi-country comparisons across data sources",
            "quality_validation": "Data quality checks and cross-validation",
            "visualization": "Interactive dashboard with real-time charts"
        }
    })

@app.get("/dashboard/", response_class=HTMLResponse)
def get_dashboard():
    try:
        dashboard_path = "templates/dashboard.html"
        if os.path.exists(dashboard_path):
            with open(dashboard_path, "r", encoding="utf-8") as file:
                html_content = file.read()
            return html_content
        else:
            logger.error(f"Dashboard template not found at {dashboard_path}")
            raise HTTPException(status_code=500, detail="Dashboard template not found.")
    except FileNotFoundError:
        logger.error("Dashboard template file not found.")
        raise HTTPException(status_code=500, detail="Dashboard template not found.")
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        raise HTTPException(status_code=500, detail="Error loading dashboard.")