from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class MortalityGDPResponse(BaseModel):
    year: int
    n_countries: int
    pearson_corr: Optional[float]
    slope_per_1k_gdp: Optional[float]
    sample: List[Dict[str, Any]]

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