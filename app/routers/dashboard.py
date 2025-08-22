from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
from app.models.schemas import CountryStat, Annotation
from app.database.mongodb import _mongo
from datetime import datetime
from typing import Dict, Any, List, Optional

dashboard_router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

@dashboard_router.get("/", response_class=HTMLResponse)
def get_dashboard():
    with open("templates/dashboard.html", "r", encoding="utf-8") as file:
        html_content = file.read()
    return html_content

@dashboard_router.post("/metadata/country")
def upsert_country_meta(item: CountryStat):
    db = _mongo()
    col = db["country_stats"]
    try:
        res = col.update_one({"country": item.country}, {"$set": item.dict()}, upsert=True)
        out = {"matched": res.matched_count, "modified": res.modified_count}
        if res.upserted_id:
            out["upserted_id"] = str(res.upserted_id)
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to update country metadata")

@dashboard_router.post("/annotations")
def add_annotation(item: Annotation):
    item.created_at = item.created_at or datetime.utcnow()
    db = _mongo()
    col = db["annotations"]
    try:
        res = col.insert_one(item.dict())
        return {"inserted_id": str(res.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to add annotation")

@dashboard_router.get("/annotations")
def list_annotations(dashboard_id: str = "covid_dashboard", limit: int = Query(100, ge=1, le=1000)):
    db = _mongo()
    col = db["annotations"]
    try:
        docs_cursor = col.find({"dashboard_id": dashboard_id}).sort("created_at", -1).limit(limit)
        docs = list(docs_cursor)
        for d in docs:
            d["_id"] = str(d["_id"])
        return {"items": docs}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to list annotations")