import logging
from typing import List, Optional, Union, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel


from realestate_analytics.api.dependencies import get_geo_collection
from realestate_analytics.data.geo import GeoCollection

router = APIRouter()
logger = logging.getLogger(__name__)

class GeoInfo(BaseModel):
    name: str
    geog_id: str
    level: int
    parent_geog_id: Optional[str] = None
    has_children: bool = False

class GeoInfoWithGeometry(GeoInfo):
    geometry: Optional[List] = None
    def geometry_lng_first(self) -> Optional[List[List[List[float]]]]:
      """
      Returns the geometry with coordinates swapped from [lat, lng] to [lng, lat].
      
      Returns:
          Optional[List[List[List[float]]]]: Swapped geometry or None if no geometry exists.
      """
      if self.geometry is None:
          return None
      
      return [
          [
              [coord[1], coord[0]] for coord in polygon
          ]
          for polygon in self.geometry
      ]

# GeoJSON Pydantic Models
class Geometry(BaseModel):
    type: Literal["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]
    coordinates: Union[List[float], List[List[float]], List[List[List[float]]], List[List[List[List[float]]]]]

class GeoProperties(BaseModel):
    name: str
    geog_id: str
    level: int
    parent_geog_id: Optional[str] = None
    has_children: bool = False

class GeoJSONFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Optional[Geometry] = None
    properties: GeoProperties

class GeoJSONFeatureCollection(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[GeoJSONFeature]


# Helper functions
def geo_to_geojson_feature(geo: Union[GeoInfo, GeoInfoWithGeometry]) -> GeoJSONFeature:
    feature = GeoJSONFeature(
        geometry=None,
        properties=GeoProperties(
            name=geo.name,
            geog_id=geo.geog_id,
            level=geo.level,
            parent_geog_id=geo.parent_geog_id,
            has_children=geo.has_children
        )
    )
    if isinstance(geo, GeoInfoWithGeometry) and geo.geometry:
        # Swap lat and lng for each coordinate
        swapped_geometry = geo.geometry_lng_first()   # GeoJSON coordinate is like (lng, lat)
        if swapped_geometry:
          feature.geometry = Geometry(
              type="MultiPolygon" if len(geo.geometry) > 1 else "Polygon",
              coordinates=swapped_geometry
          )
    return feature

def geos_to_geojson_feature_collection(geos: List[Union[GeoInfo, GeoInfoWithGeometry]]) -> GeoJSONFeatureCollection:
    return GeoJSONFeatureCollection(features=[geo_to_geojson_feature(geo) for geo in geos])



# @router.get("/", response_model=List[GeoInfo])
@router.get("/", response_model=GeoJSONFeatureCollection)
async def get_geos(
    level: Optional[int] = Query(None, description="Filter by geographic level"),
    parent_id: Optional[str] = Query(None, description="Filter by parent geographic ID"),
    geometry: bool = Query(False, description="Include geometry data if true"),
    geo_collection: GeoCollection = Depends(get_geo_collection)
):
    """
    Retrieve a list of geographic entities, optionally filtered by level and/or parent.
    """

    if parent_id is not None:
        parent_geo = geo_collection.get_by_id(parent_id)
        if not parent_geo:
            raise HTTPException(status_code=404, detail="Parent geo not found")
        
        if level is not None:
            geos = [geo for geo in parent_geo.children if geo.level == level]
            if not geos:
                # Instead of returning an empty list, we provide a more informative response
                raise HTTPException(
                    status_code=404, 
                    detail=f"No child geos found at level {level} for parent {parent_id}"
                )
        else:
            geos = parent_geo.children
    elif level is not None:
        geos = geo_collection.get_by_level(level)
    else:
        # Default to returning top-level geos (e.g., level 40)
        geos = geo_collection.get_by_level(40)

    if geometry:
        geos_info = [
            GeoInfoWithGeometry(
                name=geo.name,
                geog_id=geo.geog_id,
                level=geo.level,
                parent_geog_id=geo.parent_id,
                has_children=len(geo.children) > 0,
                geometry=geo.simplified_geometry
            ) for geo in geos
        ]
    else:
      geos_info = [
          GeoInfo(
              name=geo.name,
              geog_id=geo.geog_id,
              level=geo.level,
              parent_geog_id=geo.parent_id,
              has_children=len(geo.children) > 0
          ) for geo in geos
      ]

    geos_info.sort(key=lambda x: x.name)
    return geos_to_geojson_feature_collection(geos_info)

# note search has to precede other routes that come after, do not mix up the order
# @router.get("/search", response_model=List[GeoInfo])
@router.get("/search", response_model=GeoJSONFeatureCollection)
async def search_geos(
    query: str = Query(..., min_length=1, description="Search query string"),
    level: Optional[int] = Query(None, description="Filter by geographic level"),
    geometry: bool = Query(False, description="Include simplified geometry data if true"),
    geo_collection: GeoCollection = Depends(get_geo_collection)
):
    matching_collection = geo_collection.search(query, level)
    
    if len(matching_collection) == 0:
        return GeoJSONFeatureCollection(features=[])
    
    if geometry:
        geos = [
            GeoInfoWithGeometry(
                name=geo.name,
                geog_id=geo.geog_id,
                level=geo.level,
                parent_geog_id=geo.parent_id,
                has_children=len(geo.children) > 0,
                geometry=geo.simplified_geometry
            ) for geo in matching_collection
        ]
    else:    
        geos = [
            GeoInfo(
                name=geo.name,
                geog_id=geo.geog_id,
                level=geo.level,
                parent_geog_id=geo.parent_id,
                has_children=len(geo.children) > 0
            ) for geo in matching_collection
        ]

    geos.sort(key=lambda x: x.name)
    return geos_to_geojson_feature_collection(geos)


@router.get("/gta-cities", response_model=GeoJSONFeatureCollection)
async def get_gta_cities(
    geometry: bool = Query(False, description="Include geometry data if true"),
    geo_collection: GeoCollection = Depends(get_geo_collection)
):
    """
    Retrieve all cities (level 30) under the Greater Toronto Area (GTA).
    """
    # Reuse the get_geos function with hardcoded parameters for GTA
    return await get_geos(
        level=30,  # City level
        parent_id="g40_dpz3tpuh",  # GTA's geog_id
        geometry=geometry,
        geo_collection=geo_collection
    )


@router.get("/{geog_id}", response_model=GeoJSONFeature)
async def get_geo_by_id(
    geog_id: str,
    geometry: bool = Query(False, description="Include geometry data if true"),
    geo_collection: GeoCollection = Depends(get_geo_collection)
):
    """
    Retrieve a specific geographic entity by its ID.
    """
    geo = geo_collection.get_by_id(geog_id)
    if not geo:
        raise HTTPException(status_code=404, detail=f"Geographic entity with id {geog_id} not found")
    
    if geometry:
        # Use the subclass model that includes geometry
        geo_info = GeoInfoWithGeometry(
            name=geo.name,
            geog_id=geo.geog_id,
            level=geo.level,
            parent_geog_id=geo.parent_id,
            has_children=len(geo.children) > 0,
            geometry=geo.geometry
        )
    else:
        # Use the original model without geometry
        geo_info = GeoInfo(
            name=geo.name,
            geog_id=geo.geog_id,
            level=geo.level,
            parent_geog_id=geo.parent_id,
            has_children=len(geo.children) > 0
        )
      
    return geo_to_geojson_feature(geo_info)


# @router.get("/hierarchy/{geog_id}", response_model=List[GeoInfo])
@router.get("/hierarchy/{geog_id}", response_model=GeoJSONFeatureCollection)
async def get_geo_hierarchy(
    geog_id: str,
    geometry: bool = Query(False, description="Include simplified geometry data if true"),
    geo_collection: GeoCollection = Depends(get_geo_collection)
):
    """
    Retrieve the full hierarchy (ancestors) of a geographic entity.
    """
    geo = geo_collection.get_by_id(geog_id)
    if not geo:
        raise HTTPException(status_code=404, detail=f"Geographic entity with id {geog_id} not found")
    
    hierarchy = []
    current = geo
    while current:
        if geometry:
            hierarchy.append(GeoInfoWithGeometry(
                name=current.name,
                geog_id=current.geog_id,
                level=current.level,
                parent_geog_id=current.parent_id,
                has_children=len(current.children) > 0,
                geometry=current.simplified_geometry
            ))
        else:
            hierarchy.append(GeoInfo(
                name=current.name,
                geog_id=current.geog_id,
                level=current.level,
                parent_geog_id=current.parent_id,
                has_children=len(current.children) > 0
            ))
        current = geo_collection.get_by_id(current.parent_id) if current.parent_id else None
    
    # return list(reversed(hierarchy))
    return geos_to_geojson_feature_collection(list(reversed(hierarchy)))




