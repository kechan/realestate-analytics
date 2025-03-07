from typing import Optional, List, Literal, Union
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

import pandas as pd

from realestate_analytics.api.dependencies import get_cache, get_geo_collection
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
    metric_data: dict

class Geometry(BaseModel):
    type: Literal["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]
    coordinates: Union[List[float], List[List[float]], List[List[List[float]]], List[List[List[List[float]]]]]

class GeoJSONFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Optional[Geometry] = None
    properties: GeoProperties

@router.get("/current-metrics")
async def get_current_month_metrics(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined."),
    return_geojson: bool = Query(False, description="Return data in GeoJSON format if true")
):
    """
    Get current month metrics (median asking price and new listings count) for a specific geographic area and property type.
    """
    cache = get_cache()
    current_metrics = cache.get('CurrentMthMetricsProcessor/current_mth_metrics_results')
    
    if current_metrics is None:
        raise HTTPException(status_code=404, detail="Current month metrics data not found")
    
    # Filter based on geog_id
    metrics_df = current_metrics[current_metrics['geog_id'] == geog_id]
    
    # Handle property_type filtering
    if property_type is None or property_type.upper() == 'ALL':
        metrics_df = metrics_df[metrics_df['propertyType'] == 'ALL']
    else:
        metrics_df = metrics_df[metrics_df['propertyType'] == property_type]
    
    if metrics_df.empty:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
    
    # Get the metrics
    row = metrics_df.iloc[0]
    metric_data = {
        "median_asking_price": float(row['median_asking_price']) if pd.notna(row['median_asking_price']) else None,
        "new_listings": int(row['new_listings'])
    }
    
    if not return_geojson:
        return {
            "geog_id": geog_id,
            "property_type": property_type or "ALL",
            "metrics": metric_data
        }
    
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
        metric_type="current_month_metrics",
        metric_data=metric_data
    )

    feature = GeoJSONFeature(
        geometry=None,  # We're not including geometry in this response
        properties=properties
    )

    return feature.model_dump()    