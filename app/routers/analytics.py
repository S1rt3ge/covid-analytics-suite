from fastapi import APIRouter, Query, HTTPException
from app.services.analytics_service import mortality_vs_gdp
from app.services.covid_service import predict_future_infections
from app.services.multi_source_analytics_service import (
    vaccination_vs_mortality_analysis,
    travel_restrictions_impact_analysis,
    multi_source_country_comparison,
    pandemic_timeline_analysis
)
from app.models.schemas import MortalityGDPResponse
from typing import List, Optional
from datetime import date
import pandas as pd
import numpy as np
import json

analytics_router = APIRouter(prefix="/analytics", tags=["Analytics"])

def clean_data_for_json(obj):
    if isinstance(obj, dict):
        return {k: clean_data_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_data_for_json(item) for item in obj]
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

@analytics_router.get("/mortality-vs-gdp", response_model=MortalityGDPResponse)
def mortality_vs_gdp_endpoint(
    year: int = Query(..., ge=2020, le=2100),
    countries: str = Query(
        None,
        description="Comma-separated country names, e.g. 'Japan,China,India'"
    )
):
    try:
        result = mortality_vs_gdp(year, countries)
        return clean_data_for_json(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in mortality vs GDP analysis: {e}")

@analytics_router.get("/predict-infections")
def predict_infections_endpoint(
    country: str,
    days_ahead: int = Query(7, ge=1, le=30)
):
    try:
        result = predict_future_infections(country, days_ahead)
        return clean_data_for_json(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

@analytics_router.get("/vaccination-vs-mortality")
def vaccination_mortality_correlation(
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
        
        result = vaccination_vs_mortality_analysis(country_list, date_from, date_to)
        return clean_data_for_json(result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in vaccination vs mortality analysis: {e}")

@analytics_router.get("/travel-restrictions-impact")
def travel_restrictions_impact(
    countries: Optional[str] = Query(
        None,
        description="Comma-separated country names, e.g. 'Germany,France,Italy'"
    ),
    date_from: date = Query(..., description="Start date for analysis"),
    date_to: date = Query(..., description="End date for analysis")
):
    try:
        country_list = None
        if countries:
            country_list = [c.strip() for c in countries.split(",") if c.strip()]
        
        result = travel_restrictions_impact_analysis(date_from, date_to, country_list)
        return clean_data_for_json(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in travel restrictions impact analysis: {e}")

@analytics_router.get("/multi-source-comparison")
def multi_source_comparison(
    date_from: date = Query(..., description="Start date for analysis"),
    date_to: date = Query(..., description="End date for analysis"),
    countries: str = Query(..., description="Comma-separated country names, e.g. 'Germany,France,Italy'")
):
    try:
        country_list = [c.strip() for c in countries.split(",") if c.strip()]
        if not country_list:
            raise HTTPException(status_code=400, detail="At least one country must be specified")
        
        result = multi_source_country_comparison(country_list, date_from, date_to)
        return clean_data_for_json(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in multi-source comparison: {e}")

@analytics_router.get("/pandemic-timeline")
def pandemic_timeline(
    start_date: date = Query(..., description="Start date for analysis"),
    end_date: date = Query(..., description="End date for analysis"),
    countries: str = Query(..., description="Comma-separated country names, e.g. 'Germany,France,Italy'"),
    include_milestones: bool = Query(False, description="Include predefined milestone events")
):
    try:
        country_list = [c.strip() for c in countries.split(",") if c.strip()]
        if not country_list:
            raise HTTPException(status_code=400, detail="At least one country must be specified")
        
        milestones = None
        if include_milestones:
            milestones = [
                {"date": "2020-03-11", "event": "WHO declares COVID-19 a pandemic", "type": "global"},
                {"date": "2020-12-08", "event": "First COVID-19 vaccination (UK)", "type": "vaccination"},
                {"date": "2021-01-06", "event": "Alpha variant becomes dominant", "type": "variant"},
                {"date": "2021-12-01", "event": "Omicron variant detected", "type": "variant"},
                {"date": "2022-05-05", "event": "WHO declares end of Public Health Emergency", "type": "global"}
            ]
        
        result = pandemic_timeline_analysis(country_list, start_date, end_date, milestones)
        return clean_data_for_json(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in pandemic timeline analysis: {e}")

@analytics_router.get("/data-source-quality")
def data_source_quality_check(
    date_from: date = Query(..., description="Start date for analysis"),
    date_to: date = Query(..., description="End date for analysis"),
    countries: str = Query(..., description="Comma-separated country names")
):
    try:
        country_list = [c.strip() for c in countries.split(",") if c.strip()]
        
        comparison_data = multi_source_country_comparison(country_list, date_from, date_to)
        
        quality_report = {
            "countries_analyzed": country_list,
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "data_source_coverage": comparison_data["data_source_availability"],
            "quality_metrics": {},
            "recommendations": []
        }
        
        total_countries = len(country_list)
        
        for source, count in comparison_data["data_source_availability"].items():
            coverage_pct = (count / total_countries) * 100 if total_countries > 0 else 0
            quality_report["quality_metrics"][source] = {
                "countries_with_data": count,
                "coverage_percentage": round(coverage_pct, 2),
                "quality_rating": get_quality_rating(coverage_pct)
            }
            
            if coverage_pct < 50:
                quality_report["recommendations"].append(f"Low coverage for {source.upper()} data source ({coverage_pct:.1f}%). Consider alternative sources or different time period.")
            elif coverage_pct < 80:
                quality_report["recommendations"].append(f"Moderate coverage for {source.upper()} data source ({coverage_pct:.1f}%). Results may be incomplete for some countries.")
        
        if quality_report["quality_metrics"]:
            avg_coverage = sum(quality_report["quality_metrics"][source]["coverage_percentage"] 
                              for source in quality_report["quality_metrics"]) / len(quality_report["quality_metrics"])
        else:
            avg_coverage = 0
        
        quality_report["overall_quality"] = {
            "average_coverage": round(avg_coverage, 2),
            "rating": get_quality_rating(avg_coverage),
            "data_completeness": "high" if avg_coverage > 80 else "moderate" if avg_coverage > 60 else "low"
        }
        
        return clean_data_for_json(quality_report)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in data quality analysis: {e}")

@analytics_router.get("/cross-validation")
def cross_validate_data_sources(
    date_from: date = Query(..., description="Start date for analysis"),
    date_to: date = Query(..., description="End date for analysis"),
    countries: str = Query(..., description="Comma-separated country names"),
    metric: str = Query("cases", description="Metric to cross-validate: cases, deaths")
):
    try:
        country_list = [c.strip() for c in countries.split(",") if c.strip()]
        
        comparison_data = multi_source_country_comparison(country_list, date_from, date_to)
        
        cross_validation_results = {
            "metric": metric,
            "countries": country_list,
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "source_comparisons": [],
            "discrepancies": [],
            "reliability_scores": {}
        }
        
        for country in country_list:
            if country in comparison_data["detailed_data"]:
                country_data = comparison_data["detailed_data"][country]
                
                values = {}
                if metric == "cases":
                    jhu_val = country_data["jhu_data"].get("MAX_CASES")
                    if jhu_val is not None and not (pd.isna(jhu_val) or np.isinf(jhu_val)):
                        values["jhu"] = jhu_val
                    
                    ecdc_val = country_data["ecdc_data"].get("TOTAL_CASES")
                    if ecdc_val is not None and not (pd.isna(ecdc_val) or np.isinf(ecdc_val)):
                        values["ecdc"] = ecdc_val
                    
                    who_val = country_data["who_data"].get("TOTAL_CASES")
                    if who_val is not None and not (pd.isna(who_val) or np.isinf(who_val)):
                        values["who"] = who_val
                        
                elif metric == "deaths":
                    ecdc_val = country_data["ecdc_data"].get("TOTAL_DEATHS")
                    if ecdc_val is not None and not (pd.isna(ecdc_val) or np.isinf(ecdc_val)):
                        values["ecdc"] = ecdc_val
                    
                    who_val = country_data["who_data"].get("TOTAL_DEATHS")
                    if who_val is not None and not (pd.isna(who_val) or np.isinf(who_val)):
                        values["who"] = who_val
                
                available_values = {k: v for k, v in values.items() if v is not None and v > 0}
                
                if len(available_values) > 1:
                    values_list = list(available_values.values())
                    mean_value = np.mean(values_list)
                    std_value = np.std(values_list)
                    coefficient_of_variation = (std_value / mean_value) * 100 if mean_value > 0 else 0
                    
                    comparison = {
                        "country": country,
                        "sources_available": list(available_values.keys()),
                        "values": available_values,
                        "mean": round(float(mean_value), 2),
                        "std_deviation": round(float(std_value), 2),
                        "coefficient_of_variation": round(float(coefficient_of_variation), 2),
                        "consistency_rating": get_consistency_rating(coefficient_of_variation)
                    }
                    
                    cross_validation_results["source_comparisons"].append(comparison)
                    
                    if coefficient_of_variation > 20:
                        cross_validation_results["discrepancies"].append({
                            "country": country,
                            "issue": f"High variability in {metric} data between sources",
                            "coefficient_of_variation": round(float(coefficient_of_variation), 2),
                            "sources": available_values
                        })
        
        for source in ["jhu", "ecdc", "who"]:
            source_deviations = []
            for comparison in cross_validation_results["source_comparisons"]:
                if source in comparison["sources_available"]:
                    source_value = comparison["values"][source]
                    mean_value = comparison["mean"]
                    if mean_value > 0:
                        deviation = abs((source_value - mean_value) / mean_value) * 100
                        source_deviations.append(deviation)
            
            if source_deviations:
                avg_deviation = np.mean(source_deviations)
                cross_validation_results["reliability_scores"][source] = {
                    "average_deviation_percent": round(float(avg_deviation), 2),
                    "reliability_rating": get_reliability_rating(avg_deviation),
                    "samples_count": len(source_deviations)
                }
        
        return clean_data_for_json(cross_validation_results)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in cross-validation: {e}")

@analytics_router.get("/advanced-correlation-matrix")
def advanced_correlation_analysis(
    date_from: date = Query(..., description="Start date for analysis"),
    date_to: date = Query(..., description="End date for analysis"),
    countries: str = Query(..., description="Comma-separated country names")
):
    try:
        from app.database.snowflake import sf_query_df
        
        country_list = [c.strip() for c in countries.split(",") if c.strip()]
        
        metrics_sql = """
        WITH ecdc_data AS (
            SELECT 
                COUNTRY_REGION as country,
                MAX(CASES) as total_cases,
                MAX(DEATHS) as total_deaths,
                MAX(POPULATION) as population,
                SUM(CASES_SINCE_PREV_DAY) as new_cases_period,
                SUM(DEATHS_SINCE_PREV_DAY) as new_deaths_period
            FROM WORK_DB.PUBLIC.ECDC_GLOBAL
            WHERE DATE BETWEEN %s AND %s
              AND UPPER(COUNTRY_REGION) IN ({placeholders})
            GROUP BY COUNTRY_REGION
        ),
        vaccination_data AS (
            SELECT 
                COUNTRY_REGION as country,
                MAX(PEOPLE_FULLY_VACCINATED_PER_HUNDRED) as vaccination_rate,
                MAX(TOTAL_VACCINATIONS) as total_vaccinations
            FROM WORK_DB.PUBLIC.OWID_VACCINATIONS
            WHERE DATE BETWEEN %s AND %s
              AND UPPER(COUNTRY_REGION) IN ({placeholders})
            GROUP BY COUNTRY_REGION
        ),
        restrictions_data AS (
            SELECT 
                COUNTRY as country,
                COUNT(*) as restrictions_count
            FROM WORK_DB.PUBLIC.HUM_RESTRICTIONS_AIRLINE
            WHERE PUBLISHED BETWEEN %s AND %s
              AND UPPER(COUNTRY) IN ({placeholders})
            GROUP BY COUNTRY
        )
        SELECT 
            e.country,
            e.total_cases,
            e.total_deaths,
            e.population,
            e.new_cases_period,
            e.new_deaths_period,
            v.vaccination_rate,
            v.total_vaccinations,
            COALESCE(r.restrictions_count, 0) as restrictions_count,
            CASE WHEN e.population > 0 THEN (e.total_cases / e.population * 100000) ELSE 0 END as cases_per_100k,
            CASE WHEN e.population > 0 THEN (e.total_deaths / e.population * 100000) ELSE 0 END as deaths_per_100k,
            CASE WHEN e.total_cases > 0 THEN (e.total_deaths::float / e.total_cases * 100) ELSE 0 END as case_fatality_rate
        FROM ecdc_data e
        LEFT JOIN vaccination_data v ON UPPER(e.country) = UPPER(v.country)
        LEFT JOIN restrictions_data r ON UPPER(e.country) = UPPER(r.country)
        """
        
        placeholders = ','.join(['UPPER(%s)'] * len(country_list))
        metrics_sql = metrics_sql.format(placeholders=placeholders)
        
        params = [date_from, date_to] + country_list + [date_from, date_to] + country_list + [date_from, date_to] + country_list
        
        df = sf_query_df(metrics_sql, tuple(params))
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found for correlation analysis")
        
        numeric_columns = [
            'total_cases', 'total_deaths', 'population', 'new_cases_period', 
            'new_deaths_period', 'vaccination_rate', 'total_vaccinations',
            'restrictions_count', 'cases_per_100k', 'deaths_per_100k', 'case_fatality_rate'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        correlation_df = df[numeric_columns].corr()
        
        correlation_matrix = {}
        for i, metric1 in enumerate(numeric_columns):
            correlation_matrix[metric1] = {}
            for j, metric2 in enumerate(numeric_columns):
                correlation_value = correlation_df.iloc[i, j]
                if pd.isna(correlation_value) or np.isinf(correlation_value):
                    correlation_matrix[metric1][metric2] = {
                        "correlation": None,
                        "strength": "undefined"
                    }
                else:
                    correlation_matrix[metric1][metric2] = {
                        "correlation": round(float(correlation_value), 4),
                        "strength": get_correlation_strength(correlation_value)
                    }
        
        strong_correlations = []
        for i, metric1 in enumerate(numeric_columns):
            for j, metric2 in enumerate(numeric_columns):
                if i < j:
                    corr_value = correlation_df.iloc[i, j]
                    if not pd.isna(corr_value) and not np.isinf(corr_value) and abs(corr_value) > 0.5:
                        strong_correlations.append({
                            "metric1": metric1,
                            "metric2": metric2,
                            "correlation": round(float(corr_value), 4),
                            "strength": get_correlation_strength(corr_value),
                            "interpretation": interpret_correlation(metric1, metric2, corr_value)
                        })
        
        strong_correlations.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        
        summary_data = df[numeric_columns].describe().round(2)
        summary_dict = {}
        for col in summary_data.columns:
            summary_dict[col] = {}
            for idx in summary_data.index:
                val = summary_data.loc[idx, col]
                if pd.isna(val) or np.isinf(val):
                    summary_dict[col][idx] = None
                else:
                    summary_dict[col][idx] = float(val)
        
        result = {
            "analysis_type": "advanced_correlation_matrix",
            "countries": country_list,
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "correlation_matrix": correlation_matrix,
            "strong_correlations": strong_correlations[:10],
            "metrics_analyzed": numeric_columns,
            "sample_size": len(df),
            "data_summary": summary_dict
        }
        
        return clean_data_for_json(result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in correlation analysis: {e}")

def get_quality_rating(coverage_percentage: float) -> str:
    if coverage_percentage >= 90:
        return "excellent"
    elif coverage_percentage >= 80:
        return "good"
    elif coverage_percentage >= 60:
        return "fair"
    elif coverage_percentage >= 40:
        return "poor"
    else:
        return "very_poor"

def get_consistency_rating(coefficient_of_variation: float) -> str:
    if coefficient_of_variation <= 5:
        return "very_consistent"
    elif coefficient_of_variation <= 15:
        return "consistent"
    elif coefficient_of_variation <= 30:
        return "moderately_consistent"
    else:
        return "inconsistent"

def get_reliability_rating(average_deviation: float) -> str:
    if average_deviation <= 10:
        return "highly_reliable"
    elif average_deviation <= 25:
        return "reliable"
    elif average_deviation <= 50:
        return "moderately_reliable"
    else:
        return "unreliable"

def get_correlation_strength(correlation: float) -> str:
    if pd.isna(correlation) or np.isinf(correlation):
        return "undefined"
    
    abs_corr = abs(correlation)
    
    if abs_corr < 0.1:
        return "negligible"
    elif abs_corr < 0.3:
        return "weak"
    elif abs_corr < 0.5:
        return "moderate"
    elif abs_corr < 0.7:
        return "strong"
    else:
        return "very strong"

def interpret_correlation(metric1: str, metric2: str, correlation: float) -> str:
    if pd.isna(correlation) or np.isinf(correlation):
        return "Cannot interpret undefined correlation"
        
    direction = "positive" if correlation > 0 else "negative"
    strength = get_correlation_strength(correlation)
    
    interpretations = {
        ("total_cases", "total_deaths"): f"{strength.title()} {direction} correlation between total cases and deaths is expected",
        ("vaccination_rate", "deaths_per_100k"): f"{strength.title()} {direction} correlation suggests vaccination {'effectiveness' if direction == 'negative' else 'may not be effective'}",
        ("restrictions_count", "cases_per_100k"): f"{strength.title()} {direction} correlation between restrictions and case rates",
        ("population", "total_cases"): f"{strength.title()} {direction} correlation between population size and total cases"
    }
    
    key = tuple(sorted([metric1, metric2]))
    return interpretations.get(key, f"{strength.title()} {direction} correlation between {metric1} and {metric2}")