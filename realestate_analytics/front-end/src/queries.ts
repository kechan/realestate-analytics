import { QueryClient, useQuery } from '@tanstack/react-query';
import { api, ApiError } from './api';
import { GTA_ID } from './domain/geography';
import type { GeoCollection, GeoFeature, NavigationBoundaries, PropertyType } from './types';

export const queryClient = new QueryClient({ defaultOptions: { queries: {
  staleTime: 5 * 60_000,
  gcTime: 5 * 60_000,
  refetchOnWindowFocus: false,
  refetchOnReconnect: false,
  retry: (count, error) => count < 1 && error instanceof ApiError && (error.status === 0 || error.status >= 500),
} } });
export const useDetail = (id: string) => useQuery({ queryKey: ['geography', 'detail', id], queryFn: ({ signal }) => api.detail(id, signal), enabled: Boolean(id) });
export const useHierarchy = (id: string, enabled = true) => useQuery({ queryKey: ['geography', 'hierarchy', id], queryFn: ({ signal }) => api.hierarchy(id, signal), enabled });
export const useChildren = (id: string, geometry = false, enabled = true) => useQuery({
  queryKey: ['geography', 'children', id, geometry],
  queryFn: ({ signal }) => api.children(id, geometry, signal, id === GTA_ID ? 30 : undefined), enabled,
});
export const useMetrics = (id: string, propertyType: PropertyType, enabled = true) => useQuery({
  queryKey: ['metrics', id, propertyType], queryFn: ({ signal }) => api.metrics(id, propertyType, signal),
  staleTime: 60_000, enabled,
});
export const useCatalog = () => useQuery({ queryKey: ['catalog'], queryFn: ({ signal }) => api.catalog(signal), staleTime: Infinity });
export const useProvinceChildren = (province?: string, geometry = false, enabled = true) => useQuery({
  queryKey: ['geography', 'province-children', province, geometry],
  queryFn: ({ signal }) => api.provinceChildren(province!, geometry, signal), enabled: Boolean(province) && enabled,
});
export const useProvinceBoundaries = (province?: string, children?: GeoCollection) => {
  const ids = children?.features.map(f => f.properties.geog_id) || [];
  return useQuery({ queryKey: ['geography', 'province-boundaries', province, ids],
    queryFn: ({ signal }) => api.navigationBoundaries(ids, signal), enabled: Boolean(province && ids.length),
  });
};
export function joinNavigationBoundaries(children: GeoFeature[], boundaries?: NavigationBoundaries): GeoFeature[] {
  const byId = new Map(boundaries?.features.map(f => [f.properties.geog_id, f]));
  return children.map(child => {
    const boundary = byId.get(child.properties.geog_id);
    if (!boundary) return child;
    const fields = [...new Set([...(child.provenance?.fallback_fields || []), ...boundary.provenance.fallback_fields])].sort();
    const source = child.provenance?.source === 'geo_collection' && boundary.provenance.source === 'geo_collection'
      ? 'geo_collection' : fields.length ? 'mixed' : 'elasticsearch';
    return { ...child, geometry: boundary.geometry,
      properties: { ...child.properties, geometry_status: boundary.properties.geometry_status },
      provenance: { source, fallback_fields: fields } };
  });
}
