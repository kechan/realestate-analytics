import type { Feature, MultiPolygon, Polygon } from 'geojson';

export type GeoGeometry = Polygon | MultiPolygon;
export interface Provenance {
  source: 'elasticsearch' | 'mixed' | 'geo_collection';
  fallback_fields: string[];
}
export interface GeoIdentity {
  geog_id: string;
  name: string | null;
  level: number | null;
  province: string | null;
  geometry_status: 'available' | 'missing' | 'not_requested';
}
export interface GeoProperties extends GeoIdentity {
  longId?: string | null;
  parent_geog_id: string | null;
  parent_status: 'source' | 'inferred' | 'unresolved' | 'root' | 'cache';
}
export interface GeoFeature extends Feature<GeoGeometry | null, GeoProperties> {
  provenance: Provenance;
}
export interface GeoDetail extends Feature<GeoGeometry | null, GeoIdentity> {
  details: {
    data: {
      id?: string;
      intro?: string | null;
      profiles?: Record<string, string | null> | null;
      centroid?: { lat: number; lng: number } | null;
      bounding_box?: { west: number; east: number; south: number; north: number } | null;
      [key: string]: unknown;
    };
    meta?: { sections?: Record<string, string>; [key: string]: unknown };
  };
  provenance: Provenance;
}
export type MapFeature = GeoFeature | GeoDetail;
export interface GeoCollection {
  type: 'FeatureCollection';
  features: GeoFeature[];
  total: number;
  limit: number | null;
  offset: number;
  provenance: Provenance;
}
export interface NavigationBoundary {
  type: 'Feature';
  geometry: GeoGeometry | null;
  properties: Pick<GeoIdentity, 'geog_id' | 'geometry_status'>;
  provenance: Provenance;
}
export interface NavigationBoundaries {
  type: 'FeatureCollection';
  features: NavigationBoundary[];
  geometry_tolerance: number;
}
export interface Hierarchy {
  type: 'FeatureCollection';
  features: GeoFeature[];
  complete: boolean;
  provenance: Provenance;
}
export type PropertyType = 'ALL' | 'DETACHED' | 'SEMI-DETACHED' | 'TOWNHOUSE' | 'CONDO' | 'OTHER';
export type MetricName =
  | 'median_sold_price' | 'median_dom' | 'over_ask_percentage' | 'below_ask_percentage'
  | 'sold_listing_count' | 'absorption_rate' | 'months_of_inventory'
  | 'mth_end_snapshot_listing_count' | 'last_mth_new_listings'
  | 'last_mth_median_asking_price' | 'last_mth_min_asking_price' | 'last_mth_max_asking_price';
export type MetricUnit = 'currency' | 'days' | 'percent' | 'listings' | 'ratio' | 'months' | 'mixed';
export interface MetricDefinition {
  name: MetricName | 'current_metrics';
  unit: MetricUnit;
  description: string;
  value_type: 'number' | 'integer' | 'object';
}
export interface MetricPoint { month: string; value: number }
export interface ChartPoint { month: string; value: number | null }
export interface MetricSeries {
  available: boolean;
  latest_available_month: string | null;
  data: MetricPoint[];
}
export interface MarketMetrics {
  geog_id: string;
  property_type: PropertyType;
  source_index: string;
  document_updated_at: string | null;
  series: Partial<Record<MetricName, MetricSeries>>;
  current_metrics?: { median_asking_price: number | null; new_listings: number | null } | null;
}
export type HistoryRange = 'all' | '1y' | '3y';
export interface ViewState {
  geo: string;
  province?: string;
  propertyType: PropertyType;
  metric: MetricName;
  range: HistoryRange;
}
