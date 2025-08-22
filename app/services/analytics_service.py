import pandas as pd
import numpy as np
from fastapi import HTTPException
from app.database.snowflake import sf_query_df
from app.database.mongodb import get_country_stats_collection
from app.models.schemas import MortalityGDPResponse
from typing import Optional, List, Dict, Any
from datetime import date
from app.utils.cache import simple_cache

@simple_cache(timeout_minutes=60)
def mortality_vs_gdp(
    year: int,
    countries: Optional[str] = None
) -> MortalityGDPResponse:
    sql = """
    WITH d AS (
        SELECT COUNTRY_REGION AS country, DATE, MAX(CASES) AS cum
        FROM WORK_DB.PUBLIC.OPTIMIZED_JHU_COVID_19_TIMESERIES
        WHERE UPPER(CASE_TYPE) = 'DEATHS'
          AND DATE >= TO_DATE(%s||'-01-01') AND DATE < TO_DATE((%s+1)||'-01-01')
        GROUP BY COUNTRY_REGION, DATE
    ),
    dd AS (
        SELECT country, DATE, (cum - LAG(cum) OVER (PARTITION BY country ORDER BY DATE)) AS daily_raw
        FROM d
    )
    SELECT country, SUM(GREATEST(NVL(daily_raw, 0), 0)) AS deaths
    FROM dd
    GROUP BY country
    """
    deaths_df = sf_query_df(sql, (year, year))
    if deaths_df.empty:
        raise HTTPException(404, "No data from Snowflake for selected year")

    deaths_df.rename(columns={"DEATHS": "deaths", "COUNTRY": "country"}, inplace=True)
    deaths_df["country"] = deaths_df["country"].astype(str)
    deaths_df["deaths"] = pd.to_numeric(deaths_df["deaths"], errors="coerce")

    col = get_country_stats_collection()
    meta_cursor = col.find({}, {"_id": 0, "country": 1, "gdp_per_capita": 1, "population": 1})
    meta_df = pd.DataFrame(list(meta_cursor))
    if meta_df.empty:
        raise HTTPException(500, "Mongo collection is empty; POST /metadata/country first")

    meta_df["country"] = meta_df["country"].astype(str)
    meta_df["gdp_per_capita"] = pd.to_numeric(meta_df["gdp_per_capita"], errors="coerce")
    meta_df["population"] = pd.to_numeric(meta_df["population"], errors="coerce")

    df = deaths_df.merge(meta_df, on="country", how="inner")
    df = df.dropna(subset=["deaths", "gdp_per_capita", "population"])
    df = df[(df["deaths"] >= 0) & (df["gdp_per_capita"] > 0) & (df["population"] > 0)]

    if df.empty:
        raise HTTPException(404, "No valid country intersection between Snowflake and Mongo.")

    selected_countries_lower = set()
    if countries:
        selected_list = [c.strip() for c in countries.split(",") if c.strip()]
        if selected_list:
            selected_countries_lower = {c.lower() for c in selected_list}
            df = df[df["country"].str.lower().isin(selected_countries_lower)]
            if df.empty:
                raise HTTPException(404, "No analytics data for selected countries in this year.")

    df["deaths_per_100k"] = df["deaths"] / df["population"] * 100_000

    corr = float(df["deaths_per_100k"].corr(df["gdp_per_capita"])) if len(df) >= 3 else None
    slope = None
    if len(df) >= 3:
        X = df["gdp_per_capita"].to_numpy()
        y = df["deaths_per_100k"].to_numpy()
        try:
            slope, _ = np.polyfit(X, y, 1)
        except np.linalg.LinAlgError:
            slope = None

    if selected_countries_lower:
        order_map = {name.lower(): i for i, name in enumerate([c.strip() for c in countries.split(",") if c.strip()])}
        df["__order"] = df["country"].str.lower().map(order_map)
        df["__order"].fillna(len(order_map), inplace=True)
        sample_df = df.sort_values(["__order"]).drop(columns="__order")[
            ["country", "deaths", "population", "gdp_per_capita", "deaths_per_100k"]
        ]
    else:
        sample_df = df.sort_values("deaths_per_100k", ascending=False).head(10)[
            ["country", "deaths", "population", "gdp_per_capita", "deaths_per_100k"]
        ]

    return MortalityGDPResponse(
        year=year,
        n_countries=int(len(df)),
        pearson_corr=corr,
        slope_per_1k_gdp=(slope * 1000 if slope is not None else None),
        sample=sample_df.round(3).replace([np.inf, -np.inf], np.nan).fillna(0).to_dict(orient="records"),
    )