from typing import Optional, List, Literal
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from realestate_analytics.api.dependencies import get_cache
from realestate_analytics.api.utils import get_time_series_data

router = APIRouter()


@router.get("/price")
async def get_sold_median_price(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined.")
):
    cache = get_cache()
    price_series = cache.get('five_years_price_series')
    if price_series is None:
        raise HTTPException(status_code=404, detail="Price series data not found")
    
    data = get_time_series_data(price_series, geog_id, property_type)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
        
    return {"geog_id": geog_id, "property_type": property_type or "ALL", "data": data}
    

@router.get("/dom")
async def get_sold_median_dom(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined.")
):
    cache = get_cache()
    dom_series = cache.get('five_years_dom_series')
    if dom_series is None:
        raise HTTPException(status_code=404, detail="Days on market series data not found")
    
    data = get_time_series_data(dom_series, geog_id, property_type)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")    
    
    return {"geog_id": geog_id, "property_type": property_type or "ALL", "data": data}

@router.get("/over-ask")
async def get_sold_over_ask_percentage(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined.")
):
    cache = get_cache()
    over_ask_series = cache.get('five_years_over_ask_series')
    if over_ask_series is None:
        raise HTTPException(status_code=404, detail="Over ask percentage series data not found")
    
    data = get_time_series_data(over_ask_series, geog_id, property_type)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
    
    return {"geog_id": geog_id, "property_type": property_type or "ALL", "data": data}

@router.get("/under-ask")
async def get_sold_under_ask_percentage(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined.")
):
    cache = get_cache()
    under_ask_series = cache.get('five_years_below_ask_series')
    if under_ask_series is None:
        raise HTTPException(status_code=404, detail="Under ask percentage series data not found")
    
    data = get_time_series_data(under_ask_series, geog_id, property_type)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
    
    return {"geog_id": geog_id, "property_type": property_type or "ALL", "data": data}