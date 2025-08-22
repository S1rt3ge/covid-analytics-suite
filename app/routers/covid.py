from fastapi import APIRouter, Query, HTTPException
from datetime import date
from typing import List, Optional
import pandas as pd
import numpy as np
import logging

from app.services.covid_service import (
    get_daily_deaths, 
    get_covid_summary,
    get_german_covid_data,
    get_who_situation_reports,
    get_travel_restrictions,
    get_ecdc_global_data,
    get_vaccination_data,
    get_comprehensive_covid_report
)

logger = logging.getLogger(__name__)

covid_router = APIRouter(prefix="/covid", tags=["COVID"])

def clean_response_data(obj):
    if isinstance(obj, dict):
        return {k: clean_response_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_response_data(item) for item in obj]
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

@covid_router.get("/daily_deaths")
def daily_deaths_route(country: str, year: int):
    try:
        result = get_daily_deaths(country, year)
        return clean_response_data(result)
    except Exception as e:
        logger.error(f"Error in daily_deaths_route: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching daily deaths data: {e}")

@covid_router.get("/summary")
def covid_summary_route(
    country: str,
    date_from: date,
    date_to: date,
    case_type: str = Query("deaths", pattern="^(cases|confirmed|deaths|recovered)$")
):
    try:
        result = get_covid_summary(country, date_from, date_to, case_type)
        return clean_response_data(result)
    except Exception as e:
        logger.error(f"Error in covid_summary_route: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching summary data: {e}")

@covid_router.get("/germany/regional")
def german_covid_regional_data(
    date_from: date,
    date_to: date
):
    try:
        result = get_german_covid_data(date_from, date_to)
        return clean_response_data(result)
    except Exception as e:
        logger.error(f"Error in german_covid_regional_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching German regional data: {e}")

@covid_router.get("/who/reports")
def who_situation_reports(
    date_from: date,
    date_to: date,
    limit: int = Query(50, ge=1, le=200)
):
    try:
        result = get_who_situation_reports(date_from, date_to, limit)
        return clean_response_data(result)
    except Exception as e:
        logger.error(f"Error in who_situation_reports: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching WHO reports: {e}")

@covid_router.get("/travel/restrictions")
def travel_restrictions(
    date_from: date,
    date_to: date
):
    try:
        result = get_travel_restrictions(date_from, date_to)
        return clean_response_data(result)
    except Exception as e:
        logger.error(f"Error in travel_restrictions: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching travel restrictions: {e}")

@covid_router.get("/ecdc/global")
def ecdc_global_data(
    date_from: date,
    date_to: date,
    countries: Optional[str] = Query(
        None, 
        description="Comma-separated country names, e.g. 'Germany,France,Italy'"
    )
):
    try:
        country_list = None
        if countries:
            country_list = [c.strip() for c in countries.split(",") if c.strip()]
        
        result = get_ecdc_global_data(date_from, date_to, country_list)
        return clean_response_data(result)
    except Exception as e:
        logger.error(f"Error in ecdc_global_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching ECDC global data: {e}")

@covid_router.get("/vaccination")
def vaccination_data(
    countries: Optional[str] = Query(
        None,
        description="Comma-separated country names, e.g. 'Germany,France,Italy'"
    ),
    date_from: Optional[date] = Query(None, description="Start date for vaccination data"),
    date_to: Optional[date] = Query(None, description="End date for vaccination data")
):
    try:
        country_list = None
        if countries:
            country_list = [c.strip() for c in countries.split(",") if c.strip()]
        
        result = get_vaccination_data(country_list, date_from, date_to)
        return clean_response_data(result)
    except Exception as e:
        logger.error(f"Error in vaccination_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching vaccination data: {e}")

@covid_router.get("/comprehensive-report")
def comprehensive_covid_report(
    date_from: date = Query(..., description="Start date for analysis"),
    date_to: date = Query(..., description="End date for analysis"),
    countries: str = Query(..., description="Comma-separated country names, e.g. 'Germany,France,Italy'")
):
    try:
        country_list = [c.strip() for c in countries.split(",") if c.strip()]
        if not country_list:
            raise HTTPException(status_code=400, detail="At least one country must be specified")
        
        result = get_comprehensive_covid_report(country_list, date_from, date_to)
        return clean_response_data(result)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error in comprehensive_covid_report: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating comprehensive report: {e}")

@covid_router.get("/vaccination/top-countries")
def top_vaccinated_countries(limit: int = Query(20, ge=1, le=50)):
    try:
        data = get_vaccination_data()
        
        if not data.get("latest_by_country"):
            raise HTTPException(status_code=404, detail="No vaccination data available")
        
        valid_countries = []
        for country in data["latest_by_country"]:
            vaccination_rate = country.get("PEOPLE_FULLY_VACCINATED_PER_HUNDRED")
            if vaccination_rate is not None and not (pd.isna(vaccination_rate) or np.isinf(vaccination_rate)):
                valid_countries.append(country)
        
        sorted_countries = sorted(
            valid_countries,
            key=lambda x: x.get("PEOPLE_FULLY_VACCINATED_PER_HUNDRED", 0),
            reverse=True
        )
        
        result = {
            "top_countries": sorted_countries[:limit],
            "data_source": "OWID_VACCINATIONS",
            "metric": "people_fully_vaccinated_per_hundred"
        }
        
        return clean_response_data(result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in top_vaccinated_countries: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching top vaccinated countries: {e}")

@covid_router.get("/germany/counties-summary")
def german_counties_summary(
    date_from: date,
    date_to: date,
    top_n: int = Query(10, ge=1, le=50)
):
    try:
        data = get_german_covid_data(date_from, date_to)
        
        if not data.get("top_regions"):
            raise HTTPException(status_code=404, detail="No German regional data available for the specified period")
        
        result = {
            "period": data["date_range"],
            "summary": data["summary"],
            f"top_{top_n}_counties": data["top_regions"][:top_n],
            "total_counties_with_data": data["total_records"]
        }
        
        return clean_response_data(result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in german_counties_summary: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching German counties summary: {e}")

@covid_router.get("/travel/airlines-affected")
def most_affected_airlines(
    date_from: date,
    date_to: date,
    top_n: int = Query(10, ge=1, le=30)
):
    try:
        data = get_travel_restrictions(date_from, date_to)
        
        if not data.get("airlines_most_affected"):
            raise HTTPException(status_code=404, detail="No travel restriction data available for the specified period")
        
        airlines_list = []
        for airline, count in list(data["airlines_most_affected"].items())[:top_n]:
            clean_count = count
            if pd.isna(clean_count) or np.isinf(clean_count):
                clean_count = 0
            
            airlines_list.append({
                "airline": airline,
                "restrictions_count": int(clean_count)
            })
        
        result = {
            "period": data["date_range"],
            "most_affected_airlines": airlines_list,
            "total_restrictions": data["total_restrictions"],
            "countries_with_restrictions": len(data.get("countries_most_restricted", {}))
        }
        
        return clean_response_data(result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in most_affected_airlines: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching airline restriction data: {e}")