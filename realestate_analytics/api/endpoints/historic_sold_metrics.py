from typing import Optional, List, Literal, Union
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from realestate_analytics.api.dependencies import get_cache, get_geo_collection
from realestate_analytics.api.utils import get_time_series_data
from realestate_analytics.data.geo import GeoCollection

router = APIRouter()

class GeoProperties(BaseModel):
    name: str
    geog_id: str
    property_type: str
    level: int
    parent_geog_id: Optional[str] = None
    has_children: bool = False
    metric_type: str
    metric_data: List[dict]


class Geometry(BaseModel):
    type: Literal["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]
    coordinates: Union[List[float], List[List[float]], List[List[List[float]]], List[List[List[List[float]]]]]

class GeoJSONFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Optional[Geometry] = None
    properties: GeoProperties


@router.get("/price")
async def get_sold_median_price(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined."),
    return_geojson: bool = Query(False, description="Return data in GeoJSON format if true")
):
    cache = get_cache()
    price_series = cache.get('five_years_price_series')
    if price_series is None:
        raise HTTPException(status_code=404, detail="Price series data not found")
    
    data = get_time_series_data(price_series, geog_id, property_type)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
        
    if not return_geojson:
        return {"geog_id": geog_id, "property_type": property_type or "ALL", "data": data}
    
    # GeoJSON response
    geo_collection: GeoCollection = get_geo_collection()
    geo = geo_collection.get_by_id(geog_id)
    if not geo:
        raise HTTPException(status_code=404, detail=f"{geog_id} not found")
    
    properties = GeoProperties(
        name=geo.name,
        geog_id=geo.geog_id,
        property_type=property_type or "ALL",
        level=geo.level,
        parent_geog_id=geo.parent_id,
        has_children=len(geo.children) > 0,
        metric_type="median_price",
        metric_data=data
    )

    feature = GeoJSONFeature(
        geometry=None,  # We're not including geometry in this response
        properties=properties
    )

    return feature.model_dump()
    

@router.get("/dom")
async def get_sold_median_dom(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined."),
    return_geojson: bool = Query(False, description="Return data in GeoJSON format if true")
):
    cache = get_cache()
    dom_series = cache.get('five_years_dom_series')
    if dom_series is None:
        raise HTTPException(status_code=404, detail="Days on market series data not found")
    
    data = get_time_series_data(dom_series, geog_id, property_type)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")    
    
    if not return_geojson:
        return {"geog_id": geog_id, "property_type": property_type or "ALL", "data": data}

    # GeoJSON response
    geo_collection: GeoCollection = get_geo_collection()
    geo = geo_collection.get_by_id(geog_id)
    if not geo:
        raise HTTPException(status_code=404, detail=f"{geog_id} not found")
    
    properties = GeoProperties(
        name=geo.name,
        geog_id=geo.geog_id,
        property_type=property_type or "ALL",
        level=geo.level,
        parent_geog_id=geo.parent_id,
        has_children=len(geo.children) > 0,
        metric_type="median_dom",
        metric_data=data
    )

    feature = GeoJSONFeature(
        geometry=None,  # We're not including geometry in this response
        properties=properties
    )
    
    return feature.model_dump()


@router.get("/over-ask")
async def get_sold_over_ask_percentage(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined."),
    return_geojson: bool = Query(False, description="Return data in GeoJSON format if true")
):
    cache = get_cache()
    over_ask_series = cache.get('five_years_over_ask_series')
    if over_ask_series is None:
        raise HTTPException(status_code=404, detail="Over ask percentage series data not found")
    
    data = get_time_series_data(over_ask_series, geog_id, property_type)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
    
    if not return_geojson:
        return {"geog_id": geog_id, "property_type": property_type or "ALL", "data": data}
    
    # GeoJSON response
    geo_collection: GeoCollection = get_geo_collection()
    geo = geo_collection.get_by_id(geog_id)
    if not geo:
        raise HTTPException(status_code=404, detail=f"{geog_id} not found")
    
    properties = GeoProperties(
        name=geo.name,
        geog_id=geo.geog_id,
        property_type=property_type or "ALL",
        level=geo.level,
        parent_geog_id=geo.parent_id,
        has_children=len(geo.children) > 0,
        metric_type="over_ask_percentage",
        metric_data=data
    )

    feature = GeoJSONFeature(
        geometry=None,  # We're not including geometry in this response
        properties=properties
    )

    return feature.model_dump()


@router.get("/under-ask")
async def get_sold_under_ask_percentage(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined."),
    return_geojson: bool = Query(False, description="Return data in GeoJSON format if true")
):
    cache = get_cache()
    under_ask_series = cache.get('five_years_below_ask_series')
    if under_ask_series is None:
        raise HTTPException(status_code=404, detail="Under ask percentage series data not found")
    
    data = get_time_series_data(under_ask_series, geog_id, property_type)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
    
    if not return_geojson:
        return {"geog_id": geog_id, "property_type": property_type or "ALL", "data": data}
    
    # GeoJSON response
    geo_collection: GeoCollection = get_geo_collection()
    geo = geo_collection.get_by_id(geog_id)
    if not geo:
        raise HTTPException(status_code=404, detail=f"{geog_id} not found")
    
    properties = GeoProperties(
        name=geo.name,
        geog_id=geo.geog_id,
        property_type=property_type or "ALL",
        level=geo.level,
        parent_geog_id=geo.parent_id,
        has_children=len(geo.children) > 0,
        metric_type="under_ask_percentage",
        metric_data=data
    )

    feature = GeoJSONFeature(
        geometry=None,  # We're not including geometry in this response
        properties=properties
    )

    return feature.model_dump()