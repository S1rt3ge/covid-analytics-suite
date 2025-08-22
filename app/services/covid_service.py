from app.database.snowflake import sf_query_df
from datetime import date, timedelta
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from typing import Dict, Any, List, Optional
import logging
from app.utils.cache import simple_cache

logger = logging.getLogger(__name__)

def clean_numeric_value(value):
    if pd.isna(value) or np.isinf(value):
        return None
    try:
        return float(value) if isinstance(value, (int, float, np.number)) else None
    except (ValueError, TypeError):
        return None

@simple_cache(timeout_minutes=30)
def get_daily_deaths(country: str, year: int) -> Dict[str, Any]:
    sql = """
    WITH d AS (
        SELECT COUNTRY_REGION AS country, DATE, MAX(CASES) AS cum
        FROM WORK_DB.PUBLIC.OPTIMIZED_JHU_COVID_19_TIMESERIES
        WHERE UPPER(CASE_TYPE) = 'DEATHS'
          AND UPPER(COUNTRY_REGION) = UPPER(%s)
          AND DATE >= TO_DATE(%s||'-01-01') AND DATE < TO_DATE((%s+1)||'-01-01')
          AND CASES IS NOT NULL
        GROUP BY COUNTRY_REGION, DATE
    ),
    dd AS (
        SELECT country, DATE, (cum - LAG(cum) OVER (PARTITION BY country ORDER BY DATE)) AS daily_raw
        FROM d
    )
    SELECT DATE, GREATEST(COALESCE(daily_raw, 0), 0) AS DAILY_DEATHS
    FROM dd
    ORDER BY DATE
    """
    
    try:
        df = sf_query_df(sql, (country, year, year))
        
        if df.empty:
            return {"country": country, "year": year, "days": 0, "total_deaths": 0, "series": []}
        
        df['DAILY_DEATHS'] = df['DAILY_DEATHS'].apply(clean_numeric_value)
        df = df.dropna(subset=['DAILY_DEATHS'])
        
        total = sum(d for d in df["DAILY_DEATHS"] if d is not None) if not df.empty else 0
        
        series = []
        for _, row in df.iterrows():
            deaths_val = clean_numeric_value(row["DAILY_DEATHS"])
            if deaths_val is not None:
                series.append({
                    "date": pd.to_datetime(row["DATE"]).strftime("%Y-%m-%d"), 
                    "deaths": int(deaths_val)
                })
        
        return {
            "country": country, 
            "year": year, 
            "days": len(series), 
            "total_deaths": int(total), 
            "series": series
        }
    except Exception as e:
        logger.error(f"Error in get_daily_deaths: {e}")
        return {"country": country, "year": year, "days": 0, "total_deaths": 0, "series": [], "error": str(e)}

def get_covid_summary(
    country: str,
    date_from: date,
    date_to: date,
    case_type: str = "deaths"
) -> Dict[str, Any]:
    sql = """
    WITH d AS (
        SELECT DATE, MAX(CASES) AS cum
        FROM WORK_DB.PUBLIC.OPTIMIZED_JHU_COVID_19_TIMESERIES
        WHERE UPPER(CASE_TYPE) = UPPER(%s)
          AND UPPER(COUNTRY_REGION) = UPPER(%s)
          AND DATE BETWEEN %s AND %s
          AND CASES IS NOT NULL
        GROUP BY DATE
    ),
    dd AS (
        SELECT DATE, (cum - LAG(cum) OVER (ORDER BY DATE)) AS daily_raw
        FROM d
    )
    SELECT SUM(GREATEST(COALESCE(daily_raw, 0), 0)) AS total
    FROM dd
    """
    
    try:
        df = sf_query_df(sql, (case_type, country, date_from, date_to))
        
        if df.empty or df.iloc[0, 0] is None or pd.isna(df.iloc[0, 0]):
            total = 0
        else:
            total_val = clean_numeric_value(df.iloc[0, 0])
            total = int(total_val) if total_val is not None else 0
            
        return {
            "country": country, 
            "case_type": case_type, 
            "from": str(date_from), 
            "to": str(date_to), 
            "value": total
        }
    except Exception as e:
        logger.error(f"Error in get_covid_summary: {e}")
        return {
            "country": country, 
            "case_type": case_type, 
            "from": str(date_from), 
            "to": str(date_to), 
            "value": 0,
            "error": str(e)
        }

def predict_future_infections(country: str, days_ahead: int = 7) -> Dict[str, Any]:
    sql = """
    SELECT DATE, CASES
    FROM WORK_DB.PUBLIC.OPTIMIZED_JHU_COVID_19_TIMESERIES
    WHERE UPPER(CASE_TYPE) = 'CONFIRMED'
      AND UPPER(COUNTRY_REGION) = UPPER(%s)
      AND DATE >= TO_DATE('2020-03-01')
      AND CASES IS NOT NULL
    ORDER BY DATE
    """
    
    try:
        df = sf_query_df(sql, (country,))
        
        if df.empty or len(df) < 10:
            raise ValueError(f"Insufficient data for prediction for country {country}")
        
        df['CASES'] = df['CASES'].apply(clean_numeric_value)
        df = df.dropna(subset=['CASES'])
        df = df[df['CASES'] >= 0]
        
        if len(df) < 10:
            raise ValueError(f"Insufficient valid data for prediction for country {country}")
        
        series = df['CASES'].values
        
        try:
            model = ARIMA(series, order=(2, 1, 2))
            fitted_model = model.fit()
            forecast_result = fitted_model.get_forecast(steps=days_ahead)
            forecast = forecast_result.predicted_mean
            conf_int = forecast_result.conf_int()
            
            last_date = df['DATE'].max()
            dates = [last_date + timedelta(days=i+1) for i in range(days_ahead)]
            predictions = []
            
            for i, (d, p) in enumerate(zip(dates, forecast)):
                pred_val = clean_numeric_value(p)
                
                lower_val = clean_numeric_value(conf_int[i, 0]) if len(conf_int) > i else pred_val
                upper_val = clean_numeric_value(conf_int[i, 1]) if len(conf_int) > i else pred_val
                
                predictions.append({
                    "date": d.strftime("%Y-%m-%d"), 
                    "predicted_cases": max(0, int(pred_val)) if pred_val is not None else 0,
                    "confidence_lower": max(0, int(lower_val)) if lower_val is not None else 0,
                    "confidence_upper": int(upper_val) if upper_val is not None else 0
                })
            
            last_observed_val = clean_numeric_value(df[df['DATE'] == last_date]['CASES'].iloc[0])
            
            return {
                "country": country,
                "forecast_days": days_ahead,
                "predictions": predictions,
                "model": "ARIMA(2,1,2)",
                "last_observed_date": last_date.strftime("%Y-%m-%d"),
                "last_observed_value": int(last_observed_val) if last_observed_val is not None else 0
            }
        except Exception as e:
            raise RuntimeError(f"Prediction error for country {country}: {e}")
            
    except Exception as e:
        if isinstance(e, (ValueError, RuntimeError)):
            raise
        else:
            raise RuntimeError(f"Data retrieval error for prediction: {e}")

def get_german_covid_data(date_from: date, date_to: date) -> Dict[str, Any]:
    sql = """
    SELECT 
        COUNTY,
        CASES,
        DEATHS,
        CASES_PER_100K,
        DEATH_RATE,
        POPULATION,
        LAST_UPDATE_DATE
    FROM WORK_DB.PUBLIC.OPTIMIZED_RKI_GER_COVID19_DASHBOARD
    WHERE LAST_UPDATE_DATE BETWEEN %s AND %s
      AND CASES IS NOT NULL
    ORDER BY CASES DESC
    LIMIT 100
    """
    
    try:
        df = sf_query_df(sql, (date_from, date_to))
        
        if df.empty:
            return {
                "country": "Germany",
                "date_range": {"from": str(date_from), "to": str(date_to)},
                "total_records": 0,
                "data": []
            }
        
        numeric_cols = ['CASES', 'DEATHS', 'CASES_PER_100K', 'DEATH_RATE', 'POPULATION']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_numeric_value)
        
        total_cases = sum(c for c in df['CASES'] if c is not None) if 'CASES' in df.columns else 0
        total_deaths = sum(d for d in df['DEATHS'] if d is not None) if 'DEATHS' in df.columns else 0
        
        death_rates = [dr for dr in df['DEATH_RATE'] if dr is not None] if 'DEATH_RATE' in df.columns else []
        avg_death_rate = sum(death_rates) / len(death_rates) if death_rates else 0
        
        top_regions = []
        for _, row in df.head(10).iterrows():
            region = {}
            for col, val in row.items():
                if col in numeric_cols:
                    region[col] = clean_numeric_value(val)
                elif col == 'LAST_UPDATE_DATE' and hasattr(val, 'isoformat'):
                    region[col] = val.isoformat()
                else:
                    region[col] = val
            top_regions.append(region)
        
        return {
            "country": "Germany",
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "total_records": len(df),
            "summary": {
                "total_cases": int(total_cases),
                "total_deaths": int(total_deaths),
                "average_death_rate": round(avg_death_rate, 2)
            },
            "top_regions": top_regions
        }
        
    except Exception as e:
        logger.error(f"Error in get_german_covid_data: {e}")
        return {
            "country": "Germany",
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "total_records": 0,
            "error": str(e),
            "data": []
        }

def get_who_situation_reports(date_from: date, date_to: date, limit: int = 50) -> Dict[str, Any]:
    sql = """
    SELECT 
        COUNTRY,
        DATE,
        TOTAL_CASES,
        CASES_NEW,
        DEATHS,
        DEATHS_NEW,
        TRANSMISSION_CLASSIFICATION,
        DAYS_SINCE_LAST_REPORTED_CASE
    FROM WORK_DB.PUBLIC.OPTIMIZED_WHO_SITUATION_REPORTS
    WHERE DATE BETWEEN %s AND %s
    ORDER BY DATE DESC, TOTAL_CASES DESC
    LIMIT %s
    """
    
    try:
        df = sf_query_df(sql, (date_from, date_to, limit))
        
        if df.empty:
            return {
                "date_range": {"from": str(date_from), "to": str(date_to)},
                "total_records": 0,
                "data": []
            }
        
        numeric_cols = ['TOTAL_CASES', 'CASES_NEW', 'DEATHS', 'DEATHS_NEW', 'DAYS_SINCE_LAST_REPORTED_CASE']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_numeric_value)
        
        country_stats = df.groupby('COUNTRY').agg({
            'TOTAL_CASES': lambda x: max([v for v in x if v is not None] or [0]),
            'DEATHS': lambda x: max([v for v in x if v is not None] or [0]),
            'CASES_NEW': lambda x: sum([v for v in x if v is not None]),
            'DEATHS_NEW': lambda x: sum([v for v in x if v is not None])
        }).reset_index()
        
        country_summary = []
        for _, row in country_stats.head(10).iterrows():
            summary = {}
            for col, val in row.items():
                summary[col] = clean_numeric_value(val) if col != 'COUNTRY' else val
            country_summary.append(summary)
        
        detailed_reports = []
        for _, row in df.head(20).iterrows():
            report = {}
            for col, val in row.items():
                if col in numeric_cols:
                    report[col] = clean_numeric_value(val)
                elif col == 'DATE' and hasattr(val, 'isoformat'):
                    report[col] = val.isoformat()
                else:
                    report[col] = val
            detailed_reports.append(report)
        
        transmission_stats = df['TRANSMISSION_CLASSIFICATION'].value_counts().to_dict() if 'TRANSMISSION_CLASSIFICATION' in df.columns else {}
        
        return {
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "total_records": len(df),
            "country_summary": country_summary,
            "detailed_reports": detailed_reports,
            "transmission_stats": transmission_stats
        }
        
    except Exception as e:
        logger.error(f"Error in get_who_situation_reports: {e}")
        return {
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "total_records": 0,
            "error": str(e),
            "data": []
        }

def get_travel_restrictions(date_from: date, date_to: date) -> Dict[str, Any]:
    sql = """
    SELECT 
        COUNTRY,
        AIRLINE,
        RESTRICTION_TEXT,
        PUBLISHED,
        SOURCES,
        LAT,
        LONG
    FROM WORK_DB.PUBLIC.OPTIMIZED_HUM_RESTRICTIONS_AIRLINE
    WHERE PUBLISHED BETWEEN %s AND %s
    ORDER BY PUBLISHED DESC
    LIMIT 100
    """
    
    try:
        df = sf_query_df(sql, (date_from, date_to))
        
        if df.empty:
            return {
                "date_range": {"from": str(date_from), "to": str(date_to)},
                "total_restrictions": 0,
                "data": []
            }
        
        if 'LAT' in df.columns:
            df['LAT'] = df['LAT'].apply(clean_numeric_value)
        if 'LONG' in df.columns:
            df['LONG'] = df['LONG'].apply(clean_numeric_value)
        
        country_counts = df['COUNTRY'].value_counts().head(10).to_dict() if 'COUNTRY' in df.columns else {}
        airline_counts = df['AIRLINE'].value_counts().head(10).to_dict() if 'AIRLINE' in df.columns else {}
        
        recent_restrictions = []
        for _, row in df.head(15).iterrows():
            restriction = {}
            for col, val in row.items():
                if col in ['LAT', 'LONG']:
                    restriction[col] = clean_numeric_value(val)
                elif col == 'PUBLISHED' and hasattr(val, 'isoformat'):
                    restriction[col] = val.isoformat()
                else:
                    restriction[col] = val
            recent_restrictions.append(restriction)
        
        return {
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "total_restrictions": len(df),
            "countries_most_restricted": country_counts,
            "airlines_most_affected": airline_counts,
            "recent_restrictions": recent_restrictions
        }
        
    except Exception as e:
        logger.error(f"Error in get_travel_restrictions: {e}")
        return {
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "total_restrictions": 0,
            "error": str(e),
            "data": []
        }

def get_ecdc_global_data(date_from: date, date_to: date, countries: Optional[List[str]] = None) -> Dict[str, Any]:
    base_sql = """
    SELECT 
        COUNTRY_REGION,
        DATE,
        CASES,
        DEATHS,
        CASES_SINCE_PREV_DAY,
        DEATHS_SINCE_PREV_DAY,
        POPULATION
    FROM WORK_DB.PUBLIC.OPTIMIZED_ECDC_GLOBAL
    WHERE DATE BETWEEN %s AND %s
    """
    
    params = [date_from, date_to]
    
    if countries and len(countries) > 0:
        placeholders = ','.join(['UPPER(%s)'] * len(countries))
        base_sql += f" AND UPPER(COUNTRY_REGION) IN ({placeholders})"
        params.extend(countries)
    
    base_sql += " ORDER BY DATE DESC, CASES DESC LIMIT 200"
    
    try:
        df = sf_query_df(base_sql, tuple(params))
        
        if df.empty:
            return {
                "date_range": {"from": str(date_from), "to": str(date_to)},
                "countries_requested": countries or [],
                "total_records": 0,
                "data": []
            }
        
        numeric_cols = ['CASES', 'DEATHS', 'CASES_SINCE_PREV_DAY', 'DEATHS_SINCE_PREV_DAY', 'POPULATION']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_numeric_value)
        
        country_totals = df.groupby('COUNTRY_REGION').agg({
            'CASES': lambda x: max([v for v in x if v is not None] or [0]),
            'DEATHS': lambda x: max([v for v in x if v is not None] or [0]),
            'CASES_SINCE_PREV_DAY': lambda x: sum([v for v in x if v is not None]),
            'DEATHS_SINCE_PREV_DAY': lambda x: sum([v for v in x if v is not None]),
            'POPULATION': lambda x: next((v for v in x if v is not None), 0)
        }).reset_index()
        
        country_summaries = []
        for _, row in country_totals.iterrows():
            summary = {
                'COUNTRY_REGION': row['COUNTRY_REGION'],
                'CASES': clean_numeric_value(row['CASES']),
                'DEATHS': clean_numeric_value(row['DEATHS']),
                'CASES_SINCE_PREV_DAY': clean_numeric_value(row['CASES_SINCE_PREV_DAY']),
                'DEATHS_SINCE_PREV_DAY': clean_numeric_value(row['DEATHS_SINCE_PREV_DAY']),
                'POPULATION': clean_numeric_value(row['POPULATION'])
            }
            
            if summary['POPULATION'] and summary['POPULATION'] > 0:
                if summary['CASES']:
                    summary['CASES_PER_100K'] = round((summary['CASES'] / summary['POPULATION']) * 100000, 2)
                if summary['DEATHS']:
                    summary['DEATHS_PER_100K'] = round((summary['DEATHS'] / summary['POPULATION']) * 100000, 2)
            
            country_summaries.append(summary)
        
        country_summaries.sort(key=lambda x: x.get('CASES', 0) or 0, reverse=True)
        
        daily_data = []
        for _, row in df.head(50).iterrows():
            daily_record = {}
            for col, val in row.items():
                if col in numeric_cols:
                    daily_record[col] = clean_numeric_value(val)
                elif col == 'DATE' and hasattr(val, 'isoformat'):
                    daily_record[col] = val.isoformat()
                else:
                    daily_record[col] = val
            daily_data.append(daily_record)
        
        return {
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "countries_requested": countries or [],
            "total_records": len(df),
            "country_summaries": country_summaries,
            "daily_data": daily_data
        }
        
    except Exception as e:
        logger.error(f"Error in get_ecdc_global_data: {e}")
        return {
            "date_range": {"from": str(date_from), "to": str(date_to)},
            "countries_requested": countries or [],
            "total_records": 0,
            "error": str(e),
            "data": []
        }

def get_vaccination_data(countries: Optional[List[str]] = None, date_from: Optional[date] = None, date_to: Optional[date] = None) -> Dict[str, Any]:
    base_sql = """
    SELECT 
        COUNTRY_REGION,
        DATE,
        TOTAL_VACCINATIONS,
        PEOPLE_VACCINATED,
        PEOPLE_FULLY_VACCINATED,
        DAILY_VACCINATIONS,
        TOTAL_VACCINATIONS_PER_HUNDRED,
        PEOPLE_VACCINATED_PER_HUNDRED,
        PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
        DAILY_VACCINATIONS_PER_MILLION,
        VACCINES
    FROM WORK_DB.PUBLIC.OPTIMIZED_OWID_VACCINATIONS
    WHERE 1=1
    """
    
    params = []
    
    if date_from and date_to:
        base_sql += " AND DATE BETWEEN %s AND %s"
        params.extend([date_from, date_to])
    
    if countries and len(countries) > 0:
        base_sql += f" AND UPPER(COUNTRY_REGION) IN ({','.join(['UPPER(%s)'] * len(countries))})"
        params.extend(countries)
    
    base_sql += " ORDER BY DATE DESC, TOTAL_VACCINATIONS DESC LIMIT 300"
    
    try:
        df = sf_query_df(base_sql, tuple(params))
        
        if df.empty:
            return {
                "date_range": {"from": str(date_from) if date_from else None, "to": str(date_to) if date_to else None},
                "countries_requested": countries or [],
                "total_records": 0,
                "data": []
            }
        
        numeric_cols = ['TOTAL_VACCINATIONS', 'PEOPLE_VACCINATED', 'PEOPLE_FULLY_VACCINATED', 
                       'DAILY_VACCINATIONS', 'TOTAL_VACCINATIONS_PER_HUNDRED', 
                       'PEOPLE_VACCINATED_PER_HUNDRED', 'PEOPLE_FULLY_VACCINATED_PER_HUNDRED',
                       'DAILY_VACCINATIONS_PER_MILLION']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_numeric_value)
        
        latest_by_country = []
        for country in df['COUNTRY_REGION'].unique():
            country_data = df[df['COUNTRY_REGION'] == country].sort_values('DATE').tail(1)
            if not country_data.empty:
                record = {}
                for col, val in country_data.iloc[0].items():
                    if col in numeric_cols:
                        record[col] = clean_numeric_value(val)
                    elif col == 'DATE' and hasattr(val, 'isoformat'):
                        record[col] = val.isoformat()
                    else:
                        record[col] = val
                latest_by_country.append(record)
        
        top_vaccinated = []
        if latest_by_country:
            valid_vaccination_data = [
                country for country in latest_by_country 
                if country.get('PEOPLE_FULLY_VACCINATED_PER_HUNDRED') is not None
            ]
            valid_vaccination_data.sort(key=lambda x: x.get('PEOPLE_FULLY_VACCINATED_PER_HUNDRED', 0), reverse=True)
            top_vaccinated = valid_vaccination_data[:15]
        
        vaccine_usage = {}
        if 'VACCINES' in df.columns:
            vaccine_series = df['VACCINES'].dropna()
            all_vaccines = []
            for vaccines_str in vaccine_series:
                if vaccines_str and isinstance(vaccines_str, str):
                    all_vaccines.extend([v.strip() for v in vaccines_str.split(',')])
            
            from collections import Counter
            vaccine_counts = Counter(all_vaccines)
            vaccine_usage = dict(vaccine_counts.most_common(10))
        
        max_total_vaccinations = 0
        if latest_by_country:
            total_vaccinations = [
                country.get('TOTAL_VACCINATIONS', 0) or 0 
                for country in latest_by_country
            ]
            max_total_vaccinations = max(total_vaccinations) if total_vaccinations else 0
        
        return {
            "date_range": {"from": str(date_from) if date_from else None, "to": str(date_to) if date_to else None},
            "countries_requested": countries or [],
            "total_records": len(df),
            "latest_by_country": latest_by_country,
            "top_vaccinated_countries": top_vaccinated,
            "vaccine_types_usage": vaccine_usage,
            "global_vaccination_progress": {
                "total_vaccinations_worldwide": int(max_total_vaccinations),
                "countries_with_data": df['COUNTRY_REGION'].nunique()
            }
        }
        
    except Exception as e:
        logger.error(f"Error in get_vaccination_data: {e}")
        return {
            "date_range": {"from": str(date_from) if date_from else None, "to": str(date_to) if date_to else None},
            "countries_requested": countries or [],
            "total_records": 0,
            "error": str(e),
            "data": []
        }

def get_comprehensive_covid_report(countries: List[str], date_from: date, date_to: date) -> Dict[str, Any]:
    report = {
        "request_params": {
            "countries": countries,
            "date_from": str(date_from),
            "date_to": str(date_to)
        },
        "data_sources": {}
    }
    
    try:
        jhu_data = []
        for country in countries:
            try:
                country_data = get_covid_summary(country, date_from, date_to, "confirmed")
                deaths_data = get_covid_summary(country, date_from, date_to, "deaths")
                
                if not country_data.get("error") and not deaths_data.get("error"):
                    jhu_data.append({
                        "country": country,
                        "confirmed_cases": country_data["value"],
                        "deaths": deaths_data["value"]
                    })
            except Exception:
                pass
        report["data_sources"]["jhu_timeseries"] = jhu_data
    except Exception as e:
        report["data_sources"]["jhu_timeseries"] = {"error": str(e)}
    
    try:
        report["data_sources"]["who_reports"] = get_who_situation_reports(date_from, date_to, 100)
    except Exception as e:
        report["data_sources"]["who_reports"] = {"error": str(e)}
    
    try:
        report["data_sources"]["ecdc_global"] = get_ecdc_global_data(date_from, date_to, countries)
    except Exception as e:
        report["data_sources"]["ecdc_global"] = {"error": str(e)}
    
    try:
        report["data_sources"]["vaccination"] = get_vaccination_data(countries, date_from, date_to)
    except Exception as e:
        report["data_sources"]["vaccination"] = {"error": str(e)}
    
    try:
        report["data_sources"]["travel_restrictions"] = get_travel_restrictions(date_from, date_to)
    except Exception as e:
        report["data_sources"]["travel_restrictions"] = {"error": str(e)}
    
    if any(c.upper() == "GERMANY" for c in countries):
        try:
            report["data_sources"]["germany_detailed"] = get_german_covid_data(date_from, date_to)
        except Exception as e:
            report["data_sources"]["germany_detailed"] = {"error": str(e)}
    
    return report