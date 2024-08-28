from typing import Optional, List, Dict, Any, Literal, Union
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

import pandas as pd
from realestate_analytics.api.dependencies import get_archiver, get_geo_collection
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

@router.get("/last-month")
async def get_last_month_metrics(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined."),
    return_geojson: bool = Query(False, description="Return data in GeoJSON format if true")
):
    archiver = get_archiver()
    last_mth_metrics = archiver.retrieve('last_mth_metrics_results')
    
    if last_mth_metrics is None:
        raise HTTPException(status_code=404, detail="Last month metrics not found")
    
    # Convert to DataFrame
    df = pd.DataFrame(last_mth_metrics)
    
    # Filter based on geog_id
    df = df[df['geog_id'] == geog_id]

    # rename propertyType to property_type
    df.rename(columns={'propertyType': 'property_type'}, inplace=True)

    # Handle property_type filtering
    if property_type is None or property_type.upper() == 'ALL':
        # When property_type is None or 'ALL', we want the row where property_type is 'ALL'
        df = df[df['property_type'] == 'ALL']
    else:
        df = df[df['property_type'] == property_type]
    
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
    
    # Determine the month from the latest timestamp
    month = archiver.get_latest_timestamp_in_YYYYMM('last_mth_metrics_results')
    result = {'month': month}
    result.update(df.to_dict('records')[0])
    
    if not return_geojson:
        return result
    
    # GeoJSON response
    geo_collection: GeoCollection = get_geo_collection()
    geo = geo_collection.get_by_id(geog_id)
    if not geo:
        raise HTTPException(status_code=404, detail=f"{geog_id} not found")
    
    properties = GeoProperties(
        name=geo.name,
        geog_id=geo.geog_id,
        property_type=result['property_type'],
        level=geo.level,
        parent_geog_id=geo.parent_id,
        has_children=len(geo.children) > 0,
        metric_type="last_month_metrics",
        metric_data={
            "month": result['month'],
            "median_price": result['median_price'],
            "new_listings_count": result['new_listings_count'],
        }
    )

    feature = GeoJSONFeature(
        geometry=None,  # We're not including geometry in this response
        properties=properties
    )

    return feature.model_dump()