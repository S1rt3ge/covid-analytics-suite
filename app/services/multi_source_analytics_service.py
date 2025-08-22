import pandas as pd
import numpy as np
from fastapi import HTTPException
from app.database.snowflake import sf_query_df
from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
from app.utils.cache import simple_cache

def clean_numeric_value(value):
    if pd.isna(value) or np.isinf(value):
        return None
    return float(value)

def safe_division(numerator, denominator):
    if denominator == 0 or pd.isna(numerator) or pd.isna(denominator):
        return None
    return numerator / denominator

@simple_cache(timeout_minutes=45)
def vaccination_vs_mortality_analysis(
    countries: Optional[List[str]] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None
) -> Dict[str, Any]:
    
    vaccination_sql = """
    SELECT 
        COUNTRY_REGION as country,
        MAX(PEOPLE_FULLY_VACCINATED_PER_HUNDRED) as vaccination_rate,
        MAX(TOTAL_VACCINATIONS) as total_vaccinations,
        MAX(DATE) as last_vaccination_date
    FROM WORK_DB.PUBLIC.OWID_VACCINATIONS
    WHERE PEOPLE_FULLY_VACCINATED_PER_HUNDRED IS NOT NULL
    """
    
    vaccination_params = []
    
    if date_from and date_to:
        vaccination_sql += " AND DATE BETWEEN %s AND %s"
        vaccination_params.extend([date_from, date_to])
    
    if countries:
        vaccination_sql += f" AND UPPER(COUNTRY_REGION) IN ({','.join(['UPPER(%s)'] * len(countries))})"
        vaccination_params.extend(countries)
    
    vaccination_sql += " GROUP BY COUNTRY_REGION"
    
    try:
        vaccination_df = sf_query_df(vaccination_sql, tuple(vaccination_params))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching vaccination data: {e}")
    
    if vaccination_df.empty:
        raise HTTPException(status_code=404, detail="No vaccination data found")
    
    mortality_sql = """
    SELECT 
        COUNTRY_REGION as country,
        MAX(DEATHS) as total_deaths,
        MAX(POPULATION) as population,
        SUM(DEATHS_SINCE_PREV_DAY) as new_deaths_period
    FROM WORK_DB.PUBLIC.ECDC_GLOBAL
    WHERE DEATHS IS NOT NULL
    """
    
    mortality_params = []
    
    if date_from and date_to:
        mortality_sql += " AND DATE BETWEEN %s AND %s"
        mortality_params.extend([date_from, date_to])
    
    if countries:
        mortality_sql += f" AND UPPER(COUNTRY_REGION) IN ({','.join(['UPPER(%s)'] * len(countries))})"
        mortality_params.extend(countries)
    
    mortality_sql += " GROUP BY COUNTRY_REGION"
    
    try:
        mortality_df = sf_query_df(mortality_sql, tuple(mortality_params))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching mortality data: {e}")
    
    if mortality_df.empty:
        raise HTTPException(status_code=404, detail="No mortality data found")
    
    merged_df = vaccination_df.merge(mortality_df, on='country', how='inner')
    
    if merged_df.empty:
        raise HTTPException(status_code=404, detail="No matching data between vaccination and mortality datasets")
    
    merged_df['vaccination_rate'] = merged_df['vaccination_rate'].apply(clean_numeric_value)
    merged_df['total_deaths'] = merged_df['total_deaths'].apply(clean_numeric_value)
    merged_df['population'] = merged_df['population'].apply(clean_numeric_value)
    
    merged_df['deaths_per_100k'] = merged_df.apply(
        lambda row: clean_numeric_value(safe_division(row['total_deaths'] * 100000, row['population'])), axis=1
    )
    
    merged_df = merged_df.dropna(subset=['vaccination_rate', 'deaths_per_100k'])
    merged_df = merged_df[
        (merged_df['vaccination_rate'].notna()) & 
        (merged_df['deaths_per_100k'].notna()) &
        (~merged_df['vaccination_rate'].isin([np.inf, -np.inf])) &
        (~merged_df['deaths_per_100k'].isin([np.inf, -np.inf]))
    ]
    
    if len(merged_df) < 3:
        raise HTTPException(status_code=404, detail="Insufficient data for correlation analysis")
    
    correlation = merged_df['vaccination_rate'].corr(merged_df['deaths_per_100k'])
    correlation = clean_numeric_value(correlation)
    
    slope = None
    try:
        if correlation is not None:
            slope_result = np.polyfit(merged_df['vaccination_rate'], merged_df['deaths_per_100k'], 1)
            slope = clean_numeric_value(slope_result[0]) if len(slope_result) > 0 else None
    except:
        slope = None
    
    country_data = []
    for _, row in merged_df.iterrows():
        country_data.append({
            'country': row['country'],
            'vaccination_rate': clean_numeric_value(row['vaccination_rate']),
            'deaths_per_100k': clean_numeric_value(row['deaths_per_100k']),
            'population': clean_numeric_value(row['population'])
        })
    
    return {
        "analysis_type": "vaccination_vs_mortality",
        "date_range": {
            "from": str(date_from) if date_from else None,
            "to": str(date_to) if date_to else None
        },
        "countries_analyzed": len(merged_df),
        "correlation_coefficient": correlation,
        "slope": slope,
        "interpretation": {
            "correlation_strength": get_correlation_strength(correlation) if correlation is not None else "undefined",
            "relationship": "negative" if correlation and correlation < 0 else "positive" if correlation and correlation > 0 else "no correlation"
        },
        "country_data": country_data
    }

def travel_restrictions_impact_analysis(
    date_from: date,
    date_to: date,
    countries: Optional[List[str]] = None
) -> Dict[str, Any]:
    
    restrictions_sql = """
    SELECT 
        COUNTRY,
        COUNT(*) as restrictions_count,
        MIN(PUBLISHED) as first_restriction_date,
        MAX(PUBLISHED) as last_restriction_date
    FROM WORK_DB.PUBLIC.HUM_RESTRICTIONS_AIRLINE
    WHERE PUBLISHED BETWEEN %s AND %s
    """
    
    restrictions_params = [date_from, date_to]
    
    if countries:
        restrictions_sql += f" AND UPPER(COUNTRY) IN ({','.join(['UPPER(%s)'] * len(countries))})"
        restrictions_params.extend(countries)
    
    restrictions_sql += " GROUP BY COUNTRY"
    
    try:
        restrictions_df = sf_query_df(restrictions_sql, tuple(restrictions_params))
    except Exception as e:
        restrictions_df = pd.DataFrame()
    
    cases_sql = """
    SELECT 
        COUNTRY,
        SUM(COALESCE(CASES_NEW, 0)) as total_new_cases,
        AVG(COALESCE(CASES_NEW, 0)) as avg_daily_cases,
        MAX(DATE) as last_report_date
    FROM WORK_DB.PUBLIC.WHO_SITUATION_REPORTS
    WHERE DATE BETWEEN %s AND %s
      AND CASES_NEW IS NOT NULL
    """
    
    cases_params = [date_from, date_to]
    
    if countries:
        cases_sql += f" AND UPPER(COUNTRY) IN ({','.join(['UPPER(%s)'] * len(countries))})"
        cases_params.extend(countries)
    
    cases_sql += " GROUP BY COUNTRY"
    
    try:
        cases_df = sf_query_df(cases_sql, tuple(cases_params))
    except Exception as e:
        cases_df = pd.DataFrame()
    
    if not restrictions_df.empty and not cases_df.empty:
        merged_df = restrictions_df.merge(cases_df, left_on='COUNTRY', right_on='COUNTRY', how='inner')
        
        if not merged_df.empty:
            merged_df['restrictions_count'] = merged_df['restrictions_count'].apply(clean_numeric_value)
            merged_df['avg_daily_cases'] = merged_df['avg_daily_cases'].apply(clean_numeric_value)
            merged_df['total_new_cases'] = merged_df['total_new_cases'].apply(clean_numeric_value)
            
            merged_df = merged_df.dropna(subset=['restrictions_count', 'avg_daily_cases'])
            
            correlation = None
            if len(merged_df) > 1:
                correlation = merged_df['restrictions_count'].corr(merged_df['avg_daily_cases'])
                correlation = clean_numeric_value(correlation)
            
            most_restrictions = []
            highest_cases = []
            
            for _, row in merged_df.nlargest(5, 'restrictions_count').iterrows():
                most_restrictions.append({
                    'COUNTRY': row['COUNTRY'],
                    'restrictions_count': clean_numeric_value(row['restrictions_count'])
                })
            
            for _, row in merged_df.nlargest(5, 'total_new_cases').iterrows():
                highest_cases.append({
                    'COUNTRY': row['COUNTRY'],
                    'total_new_cases': clean_numeric_value(row['total_new_cases']),
                    'restrictions_count': clean_numeric_value(row['restrictions_count'])
                })
            
            return {
                "analysis_type": "travel_restrictions_impact",
                "date_range": {"from": str(date_from), "to": str(date_to)},
                "countries_analyzed": len(merged_df),
                "correlation_restrictions_vs_cases": correlation,
                "countries_with_most_restrictions": most_restrictions,
                "countries_with_highest_cases": highest_cases,
                "summary_stats": {
                    "avg_restrictions_per_country": clean_numeric_value(merged_df['restrictions_count'].mean()),
                    "total_restrictions": clean_numeric_value(merged_df['restrictions_count'].sum()),
                    "total_new_cases": clean_numeric_value(merged_df['total_new_cases'].sum())
                }
            }
    
    return {
        "analysis_type": "travel_restrictions_impact",
        "date_range": {"from": str(date_from), "to": str(date_to)},
        "error": "Insufficient data for analysis",
        "restrictions_data_available": not restrictions_df.empty,
        "cases_data_available": not cases_df.empty
    }

def multi_source_country_comparison(
    countries: List[str],
    date_from: date,
    date_to: date
) -> Dict[str, Any]:
    
    comparison_data = {}
    
    for country in countries:
        country_data = {
            "country": country,
            "jhu_data": {},
            "ecdc_data": {},
            "who_data": {},
            "vaccination_data": {},
            "restrictions_data": {}
        }
        
        try:
            jhu_sql = """
            SELECT 
                MAX(CASES) as max_cases
            FROM WORK_DB.PUBLIC.OPTIMIZED_JHU_COVID_19_TIMESERIES
            WHERE UPPER(COUNTRY_REGION) = UPPER(%s)
              AND UPPER(CASE_TYPE) = 'CONFIRMED'
              AND DATE BETWEEN %s AND %s
              AND CASES IS NOT NULL
            """
            jhu_df = sf_query_df(jhu_sql, (country, date_from, date_to))
            if not jhu_df.empty and not jhu_df.iloc[0].isna().all():
                row_dict = jhu_df.iloc[0].to_dict()
                cleaned_dict = {k: clean_numeric_value(v) for k, v in row_dict.items()}
                country_data["jhu_data"] = cleaned_dict
        except Exception:
            pass
        
        try:
            ecdc_sql = """
            SELECT 
                MAX(CASES) as total_cases,
                MAX(DEATHS) as total_deaths,
                MAX(POPULATION) as population,
                SUM(COALESCE(CASES_SINCE_PREV_DAY, 0)) as new_cases_period
            FROM WORK_DB.PUBLIC.ECDC_GLOBAL
            WHERE UPPER(COUNTRY_REGION) = UPPER(%s)
              AND DATE BETWEEN %s AND %s
            """
            ecdc_df = sf_query_df(ecdc_sql, (country, date_from, date_to))
            if not ecdc_df.empty and not ecdc_df.iloc[0].isna().all():
                row_dict = ecdc_df.iloc[0].to_dict()
                cleaned_dict = {k: clean_numeric_value(v) for k, v in row_dict.items()}
                country_data["ecdc_data"] = cleaned_dict
        except Exception:
            pass
        
        try:
            who_sql = """
            SELECT 
                MAX(TOTAL_CASES) as total_cases,
                MAX(DEATHS) as total_deaths,
                SUM(COALESCE(CASES_NEW, 0)) as new_cases_period,
                COUNT(*) as reports_count
            FROM WORK_DB.PUBLIC.WHO_SITUATION_REPORTS
            WHERE UPPER(COUNTRY) = UPPER(%s)
              AND DATE BETWEEN %s AND %s
            """
            who_df = sf_query_df(who_sql, (country, date_from, date_to))
            if not who_df.empty and not who_df.iloc[0].isna().all():
                row_dict = who_df.iloc[0].to_dict()
                cleaned_dict = {k: clean_numeric_value(v) for k, v in row_dict.items()}
                country_data["who_data"] = cleaned_dict
        except Exception:
            pass
        
        try:
            vaccination_sql = """
            SELECT 
                MAX(TOTAL_VACCINATIONS) as total_vaccinations,
                MAX(PEOPLE_FULLY_VACCINATED_PER_HUNDRED) as fully_vaccinated_rate,
                MAX(DATE) as last_vaccination_report
            FROM WORK_DB.PUBLIC.OWID_VACCINATIONS
            WHERE UPPER(COUNTRY_REGION) = UPPER(%s)
            """
            vaccination_params = [country]
            
            if date_from and date_to:
                vaccination_sql += " AND DATE BETWEEN %s AND %s"
                vaccination_params.extend([date_from, date_to])
            
            vaccination_df = sf_query_df(vaccination_sql, tuple(vaccination_params))
            if not vaccination_df.empty and not vaccination_df.iloc[0].isna().all():
                row_dict = vaccination_df.iloc[0].to_dict()
                cleaned_dict = {}
                for k, v in row_dict.items():
                    if k == 'LAST_VACCINATION_REPORT' and hasattr(v, 'isoformat'):
                        cleaned_dict[k] = v.isoformat()
                    else:
                        cleaned_dict[k] = clean_numeric_value(v)
                country_data["vaccination_data"] = cleaned_dict
        except Exception:
            pass
        
        try:
            restrictions_sql = """
            SELECT 
                COUNT(*) as total_restrictions,
                MIN(PUBLISHED) as first_restriction,
                MAX(PUBLISHED) as last_restriction
            FROM WORK_DB.PUBLIC.HUM_RESTRICTIONS_AIRLINE
            WHERE UPPER(COUNTRY) = UPPER(%s)
              AND PUBLISHED BETWEEN %s AND %s
            """
            restrictions_df = sf_query_df(restrictions_sql, (country, date_from, date_to))
            if not restrictions_df.empty and not restrictions_df.iloc[0].isna().all():
                row_dict = restrictions_df.iloc[0].to_dict()
                cleaned_dict = {}
                for k, v in row_dict.items():
                    if k in ['FIRST_RESTRICTION', 'LAST_RESTRICTION'] and hasattr(v, 'isoformat'):
                        cleaned_dict[k] = v.isoformat()
                    else:
                        cleaned_dict[k] = clean_numeric_value(v)
                country_data["restrictions_data"] = cleaned_dict
        except Exception:
            pass
        
        comparison_data[country] = country_data
    
    summary_table = []
    for country, data in comparison_data.items():
        row = {
            "country": country,
            "jhu_max_cases": data["jhu_data"].get("MAX_CASES"),
            "ecdc_total_cases": data["ecdc_data"].get("TOTAL_CASES"),
            "who_total_cases": data["who_data"].get("TOTAL_CASES"),
            "vaccination_rate": data["vaccination_data"].get("FULLY_VACCINATED_RATE"),
            "total_restrictions": data["restrictions_data"].get("TOTAL_RESTRICTIONS", 0),
            "population": data["ecdc_data"].get("POPULATION")
        }
        
        if row["population"] and row["ecdc_total_cases"]:
            cases_per_100k = safe_division(row["ecdc_total_cases"] * 100000, row["population"])
            row["cases_per_100k"] = clean_numeric_value(cases_per_100k)
        else:
            row["cases_per_100k"] = None
        
        summary_table.append(row)
    
    return {
        "analysis_type": "multi_source_country_comparison",
        "countries": countries,
        "date_range": {"from": str(date_from), "to": str(date_to)},
        "detailed_data": comparison_data,
        "summary_table": summary_table,
        "data_source_availability": {
            "jhu": sum(1 for c in comparison_data.values() if c["jhu_data"]),
            "ecdc": sum(1 for c in comparison_data.values() if c["ecdc_data"]),
            "who": sum(1 for c in comparison_data.values() if c["who_data"]),
            "vaccination": sum(1 for c in comparison_data.values() if c["vaccination_data"]),
            "restrictions": sum(1 for c in comparison_data.values() if c["restrictions_data"])
        }
    }

def pandemic_timeline_analysis(
    countries: List[str],
    start_date: date,
    end_date: date,
    milestone_events: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    
    timeline_sql = """
    SELECT 
        COUNTRY_REGION as country,
        DATE,
        CASES,
        DEATHS,
        COALESCE(CASES_SINCE_PREV_DAY, 0) as new_cases,
        COALESCE(DEATHS_SINCE_PREV_DAY, 0) as new_deaths
    FROM WORK_DB.PUBLIC.ECDC_GLOBAL
    WHERE DATE BETWEEN %s AND %s
    """
    
    timeline_params = [start_date, end_date]
    
    if countries:
        timeline_sql += f" AND UPPER(COUNTRY_REGION) IN ({','.join(['UPPER(%s)'] * len(countries))})"
        timeline_params.extend(countries)
    
    timeline_sql += " ORDER BY DATE, COUNTRY_REGION"
    
    try:
        timeline_df = sf_query_df(timeline_sql, tuple(timeline_params))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching timeline data: {e}")
    
    if timeline_df.empty:
        raise HTTPException(status_code=404, detail="No timeline data found")
    
    vaccination_timeline_sql = """
    SELECT 
        COUNTRY_REGION as country,
        DATE,
        TOTAL_VACCINATIONS,
        PEOPLE_FULLY_VACCINATED,
        DAILY_VACCINATIONS
    FROM WORK_DB.PUBLIC.OWID_VACCINATIONS
    WHERE DATE BETWEEN %s AND %s
    """
    
    vaccination_params = [start_date, end_date]
    
    if countries:
        vaccination_timeline_sql += f" AND UPPER(COUNTRY_REGION) IN ({','.join(['UPPER(%s)'] * len(countries))})"
        vaccination_params.extend(countries)
    
    vaccination_timeline_sql += " ORDER BY DATE, COUNTRY_REGION"
    
    try:
        vaccination_df = sf_query_df(vaccination_timeline_sql, tuple(vaccination_params))
    except Exception:
        vaccination_df = pd.DataFrame()
    
    if not vaccination_df.empty:
        combined_df = timeline_df.merge(
            vaccination_df, 
            on=['country', 'DATE'], 
            how='left'
        )
    else:
        combined_df = timeline_df
    
    numeric_columns = ['CASES', 'DEATHS', 'new_cases', 'new_deaths', 'TOTAL_VACCINATIONS', 
                      'PEOPLE_FULLY_VACCINATED', 'DAILY_VACCINATIONS']
    
    for col in numeric_columns:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].apply(clean_numeric_value)
    
    key_moments = {}
    
    for country in countries:
        country_data = combined_df[combined_df['country'].str.upper() == country.upper()]
        
        if country_data.empty:
            continue
        
        moments = {
            "first_case_date": None,
            "peak_daily_cases": None,
            "peak_daily_cases_date": None,
            "first_death_date": None,
            "peak_daily_deaths": None,
            "peak_daily_deaths_date": None,
            "first_vaccination_date": None
        }
        
        first_case = country_data[(country_data['CASES'].notna()) & (country_data['CASES'] > 0)].head(1)
        if not first_case.empty:
            moments["first_case_date"] = first_case.iloc[0]['DATE'].strftime('%Y-%m-%d')
        
        if 'new_cases' in country_data.columns and country_data['new_cases'].notna().any():
            valid_cases = country_data[country_data['new_cases'].notna() & (country_data['new_cases'] > 0)]
            if not valid_cases.empty:
                peak_cases_row = valid_cases.loc[valid_cases['new_cases'].idxmax()]
                moments["peak_daily_cases"] = clean_numeric_value(peak_cases_row['new_cases'])
                moments["peak_daily_cases_date"] = peak_cases_row['DATE'].strftime('%Y-%m-%d')
        
        first_death = country_data[(country_data['DEATHS'].notna()) & (country_data['DEATHS'] > 0)].head(1)
        if not first_death.empty:
            moments["first_death_date"] = first_death.iloc[0]['DATE'].strftime('%Y-%m-%d')
        
        if 'new_deaths' in country_data.columns and country_data['new_deaths'].notna().any():
            valid_deaths = country_data[country_data['new_deaths'].notna() & (country_data['new_deaths'] > 0)]
            if not valid_deaths.empty:
                peak_deaths_row = valid_deaths.loc[valid_deaths['new_deaths'].idxmax()]
                moments["peak_daily_deaths"] = clean_numeric_value(peak_deaths_row['new_deaths'])
                moments["peak_daily_deaths_date"] = peak_deaths_row['DATE'].strftime('%Y-%m-%d')
        
        if 'TOTAL_VACCINATIONS' in country_data.columns:
            first_vaccination = country_data[(country_data['TOTAL_VACCINATIONS'].notna()) & 
                                           (country_data['TOTAL_VACCINATIONS'] > 0)].head(1)
            if not first_vaccination.empty:
                moments["first_vaccination_date"] = first_vaccination.iloc[0]['DATE'].strftime('%Y-%m-%d')
        
        key_moments[country] = moments
    
    timeline_records = []
    for _, row in combined_df.iterrows():
        record = {}
        for col, val in row.items():
            if col == 'DATE' and hasattr(val, 'isoformat'):
                record[col] = val.isoformat()
            elif col in numeric_columns:
                record[col] = clean_numeric_value(val)
            else:
                record[col] = val
        timeline_records.append(record)
    
    return {
        "analysis_type": "pandemic_timeline",
        "countries": countries,
        "date_range": {"from": str(start_date), "to": str(end_date)},
        "key_moments": key_moments,
        "milestone_events": milestone_events or [],
        "timeline_data": timeline_records,
        "summary_statistics": {
            "total_days_analyzed": (end_date - start_date).days,
            "countries_with_data": len(key_moments),
            "data_completeness": {
                "cases_data": len([r for r in timeline_records if r.get('CASES') is not None]),
                "deaths_data": len([r for r in timeline_records if r.get('DEATHS') is not None]),
                "vaccination_data": len([r for r in timeline_records if r.get('TOTAL_VACCINATIONS') is not None])
            }
        }
    }

def get_correlation_strength(correlation: float) -> str:
    if correlation is None or pd.isna(correlation):
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