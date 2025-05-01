from typing import Dict, List, Optional, Any, Tuple
import os, json, re
from pathlib import Path
from datetime import datetime
import pandas as pd

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

from realestate_analytics.data.caching import FileBasedCache

ETL_CACHE_KEYS = {
    "nearby_comparable_solds": [
        "NearbyComparableSoldsProcessor/one_year_sold_listing",
        "NearbyComparableSoldsProcessor/on_listing",
        "NearbyComparableSoldsProcessor/comparable_sold_listings_result",
    ],
    "historic_metrics": [
        "SoldMedianMetricsProcessor/five_years_sold_listing",
        "SoldMedianMetricsProcessor/five_years_price_series",
        "SoldMedianMetricsProcessor/five_years_dom_series",
        "SoldMedianMetricsProcessor/five_years_over_ask_series",
        "SoldMedianMetricsProcessor/five_years_below_ask_series",
        "SoldMedianMetricsProcessor/five_years_sold_listing_count_series",
    ],
    "last_mth_metrics": [
        "LastMthMetricsProcessor/on_current_listing",
    ],
    "absorption_rate": [
        "NearbyComparableSoldsProcessor/one_year_sold_listing",
        "NearbyComparableSoldsProcessor/on_listing",
    ],
    "current_mth_metrics": [
        "LastMthMetricsProcessor/on_current_listing",
    ]
}


class CacheKeyInfo(BaseModel):
    key: str
    exists: bool
    size: Optional[int] = None  # number of records if known
    earliest_date: Optional[str] = None  # YYYY-MM-DD or YYYY-MM
    latest_date: Optional[str] = None
    date_column: Optional[str] = None  # which column or source used

def get_size(value: Any) -> Optional[int]:
    if isinstance(value, pd.DataFrame):
        return len(value)
    elif isinstance(value, dict):
        return len(value)
    elif isinstance(value, list):
        return len(value)
    elif isinstance(value, str):
        # Try to parse as JSON
        try:
            parsed = json.loads(value.replace("'", '"'))  # Handles single quotes case too
            if isinstance(parsed, dict) or isinstance(parsed, list):
                return len(parsed)
        except Exception:
            pass  # Not JSON parsable, treat as plain string
    return None

def get_min_max_date(key: str, value: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    date_column_mapping = {
        "NearbyComparableSoldsProcessor/one_year_sold_listing": "lastTransition",
        "NearbyComparableSoldsProcessor/on_listing": "lastUpdate",
        "SoldMedianMetricsProcessor/five_years_sold_listing": "lastTransition",
        "LastMthMetricsProcessor/on_current_listing": "lastUpdate",
    }

    if isinstance(value, pd.DataFrame):
        if key in date_column_mapping:
            column = date_column_mapping[key]
            if column in value.columns:
                try:
                    dates = pd.to_datetime(value[column], errors='coerce')
                    min_date = dates.min()
                    max_date = dates.max()
                    if pd.notna(min_date) and pd.notna(max_date):
                        return min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d'), column
                except Exception:
                    pass

        if key.startswith("SoldMedianMetricsProcessor/five_years_") and key.endswith("_series"):
            date_pattern = re.compile(r"^20\d{2}-\d{2}$")
            date_cols = sorted([col for col in value.columns if date_pattern.match(col)])
            if date_cols:
                return date_cols[0], date_cols[-1], "column names (YYYY-MM)"

    return None, None, None

def get_etl_cache_diagnostics(etl_type: str, cache: FileBasedCache) -> List[CacheKeyInfo]:
    if etl_type not in ETL_CACHE_KEYS:
        raise ValueError(f"Invalid ETL type: {etl_type}")

    result = []
    for key in ETL_CACHE_KEYS[etl_type]:
        try:
            value = cache.get(key)            
            if value is None:
                result.append(CacheKeyInfo(key=key, exists=False))
            else:
                size = get_size(value)
                earliest_date, latest_date, date_column = get_min_max_date(key, value)
                result.append(CacheKeyInfo(key=key, 
                                           exists=True, 
                                           size=size, 
                                           earliest_date=earliest_date, latest_date=latest_date, 
                                           date_column=date_column))
                
        except Exception as e:
            result.append(CacheKeyInfo(key=key, exists=False))

    return result

