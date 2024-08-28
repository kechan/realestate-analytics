from typing import Optional, List, Literal, Union

import pandas as pd

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

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


@router.get("/absorption-rate")
async def get_absorption_rate(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined."),
    return_geojson: bool = Query(False, description="Return data in GeoJSON format if true")
):
    archiver = get_archiver()
    absorption_rates = archiver.retrieve('absorption_rates')
    
    if absorption_rates is None:
        raise HTTPException(status_code=404, detail="Absorption rates not found")
    
    # Convert to DataFrame
    df = pd.DataFrame(absorption_rates)
    
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
    
    record = df.to_dict('records')[0]
    
    if pd.isna(record['absorption_rate']):
        record['absorption_rate'] = None
    
    # Convert to list of dictionaries for JSON serialization
    month = archiver.get_latest_timestamp_in_YYYYMM('absorption_rates')
    result = {'month': month}
    result.update(record)

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
        metric_type="absorption_rate",
        metric_data={
            "month": result['month'],
            "sold_count": result['sold_count'],
            "current_count": result['current_count'],
            "absorption_rate": result['absorption_rate']
        }
    )

    feature = GeoJSONFeature(
        geometry=None,  # We're not including geometry in this response
        properties=properties
    )

    return feature.model_dump()
    
