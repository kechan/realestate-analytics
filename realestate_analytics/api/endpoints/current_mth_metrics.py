from typing import Optional, List, Literal, Union
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

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

    