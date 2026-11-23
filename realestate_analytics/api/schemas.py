"""Public response models for the standalone analytics API."""
from typing import Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, constr


class MetricDefinition(BaseModel):
    name: str
    unit: str
    description: str
    value_type: Literal["number", "integer", "object"]


class MetricPoint(BaseModel):
    month: str
    value: Union[int, float]


class MetricSeries(BaseModel):
    available: bool
    latest_available_month: Optional[str]
    data: List[MetricPoint]


class CurrentMetrics(BaseModel):
    median_asking_price: Optional[float] = None
    new_listings: Optional[int] = None


class MetricsResponse(BaseModel):
    geog_id: str
    property_type: str
    source_index: str
    document_updated_at: Optional[str] = None
    series: Dict[str, MetricSeries]
    current_metrics: Optional[CurrentMetrics] = None


class GeoProvenance(BaseModel):
    source: Literal["elasticsearch", "mixed", "geo_collection"] = "elasticsearch"
    fallback_fields: List[str] = Field(default_factory=list)


class GeoProperties(BaseModel):
    geog_id: str
    longId: Optional[str] = None
    name: str
    level: int
    province: Optional[str] = None
    parent_geog_id: Optional[str] = None
    parent_status: Literal["source", "inferred", "unresolved", "root", "cache"]
    geometry_status: Literal["available", "missing", "not_requested"]


class GeoFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Optional[dict] = None
    properties: GeoProperties
    provenance: GeoProvenance = Field(default_factory=GeoProvenance)


class GeoDetailProperties(BaseModel):
    geog_id: str
    name: Optional[str] = None
    level: Optional[int] = None
    province: Optional[str] = None
    geometry_status: Literal["available", "missing", "not_requested"]


class GeoDetailFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Optional[dict] = None
    properties: GeoDetailProperties
    details: dict
    provenance: GeoProvenance = Field(default_factory=GeoProvenance)


class GeoCollectionResponse(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[GeoFeature]
    total: int
    limit: Optional[int]
    offset: int
    provenance: GeoProvenance = Field(default_factory=GeoProvenance)


class GeoHierarchyResponse(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[GeoFeature]
    complete: bool
    provenance: GeoProvenance = Field(default_factory=GeoProvenance)


class ProvinceChildrenResponse(GeoCollectionResponse):
    geometry_tolerance: Optional[float] = None


class BoundaryRequest(BaseModel):
    geog_ids: List[constr(pattern=r"^g\d+_[A-Za-z0-9_-]+$", max_length=128)] = Field(min_length=1, max_length=2000)


class BoundaryProperties(BaseModel):
    geog_id: str
    geometry_status: Literal["available", "missing"]


class BoundaryFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Optional[dict] = None
    properties: BoundaryProperties
    provenance: GeoProvenance


class BoundaryCollection(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[BoundaryFeature]
    geometry_tolerance: float = 0.0025
