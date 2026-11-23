import type { GeoCollection, GeoDetail, Hierarchy, MarketMetrics, MetricDefinition, PropertyType, NavigationBoundaries } from './types';
import type { MappingReport } from './domain/imsMappings';

const BASE = (import.meta.env.VITE_API_BASE_URL || '/api/v1').replace(/\/$/, '');
export class ApiError extends Error {
  constructor(public status: number, message: string) { super(message); this.name = 'ApiError'; }
}
function object(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
async function request<T>(path: string, signal: AbortSignal, valid: (data: unknown) => boolean, body?: unknown, uploadHeaders?: Record<string, string>): Promise<T> {
  let response: Response;
  const rawFile = body instanceof Blob;
  try { response = await fetch(`${BASE}${path}`, { signal, method: body === undefined ? 'GET' : 'POST',
    headers: { accept: 'application/json', ...(body === undefined ? {} : { 'content-type': rawFile ? 'text/csv' : 'application/json' }), ...uploadHeaders },
    ...(body === undefined ? {} : { body: rawFile ? body as Blob : JSON.stringify(body) }) }); }
  catch (error) {
    if (signal.aborted) throw error;
    throw new ApiError(0, 'Could not reach the API. Check that FastAPI is running.');
  }
  if (!response.ok) {
    if (rawFile) {
      let detail = '';
      try { const problem: unknown = await response.json(); if (object(problem) && typeof problem.detail === 'string') detail = problem.detail; } catch { /* use the status fallback */ }
      throw new ApiError(response.status, detail || 'The CSV preview could not be processed. Check the files and that FastAPI has been restarted.');
    }
    throw new ApiError(response.status, response.status === 404 ? 'No published data for this selection.'
      : response.status === 409 ? 'An IMS scan is already running. Wait for it to finish before trying again.'
      : response.status === 503 ? 'The data service is temporarily unavailable.'
      : response.status === 422 ? 'The API could not accept this selection.' : 'The API could not load this data.');
  }
  let data: unknown;
  try { data = await response.json(); } catch { throw new ApiError(502, 'The API returned an unreadable response.'); }
  if (!valid(data)) throw new ApiError(502, 'The API response has an unexpected format.');
  return data as T;
}
const isFeature = (data: unknown) => object(data) && data.type === 'Feature' && object(data.properties)
  && typeof data.properties.geog_id === 'string';
const isCollection = (data: unknown) => object(data) && data.type === 'FeatureCollection'
  && Array.isArray(data.features) && data.features.every(isFeature);
const isMappingReport = (d: unknown) => object(d) && d.complete === true
  && d.historical_provenance_available === false && typeof d.document_count === 'number'
  && typeof d.tuple_count === 'number' && object(d.sources) && Array.isArray(d.rows)
  && d.rows.every(r => object(r) && typeof r.count === 'number' && typeof r.conflict === 'boolean'
    && typeof r.mapping_source === 'string' && typeof r.lineage_status === 'string');

export const api = {
  imsMappings(signal: AbortSignal) {
    return request<MappingReport>('/ims-mappings', signal, isMappingReport);
  },
  previewImsCSV(files: File[], signal: AbortSignal) {
    const isCSVReport = (d: unknown) => isMappingReport(d) && object(d) && d.report_mode === 'csv';
    if (files.length === 1) return request<MappingReport>('/ims-mappings/preview', signal, isCSVReport, files[0]);
    // Blob parts retain file boundaries via their byte lengths, including CSVs
    // without trailing newlines and files with their own UTF-8 BOM.
    return request<MappingReport>('/ims-mappings/preview', signal, isCSVReport, new Blob(files), {
      'content-type': 'application/vnd.ims.csv-batch',
      'x-ims-csv-sizes': files.map(file => file.size).join(','),
    });
  },
  detail(id: string, signal: AbortSignal) {
    return request<GeoDetail>(`/geos/${encodeURIComponent(id)}?include_geometry=true`, signal,
      d => isFeature(d) && object(d) && object(d.details) && object(d.details.data));
  },
  hierarchy(id: string, signal: AbortSignal) {
    return request<Hierarchy>(`/geos/${encodeURIComponent(id)}/hierarchy`, signal, d => isCollection(d) && object(d) && typeof d.complete === 'boolean');
  },
  children(id: string, geometry: boolean, signal: AbortSignal, level?: number) {
    const params = new URLSearchParams({ parent_id: id, include_geometry: String(geometry) });
    if (level != null) params.set('level', String(level));
    return request<GeoCollection>(`/geos?${params}`, signal, d => isCollection(d) && object(d) && typeof d.total === 'number');
  },
  search(query: string, offset: number, signal: AbortSignal) {
    const params = new URLSearchParams({ query, include_geometry: 'false', limit: '20', offset: String(offset) });
    return request<GeoCollection>(`/geos?${params}`, signal, d => isCollection(d) && object(d) && typeof d.total === 'number');
  },
  provinceMarkets(province: string, query: string, offset: number, signal: AbortSignal) {
    const params = new URLSearchParams({ province, include_geometry: 'false', limit: '30', offset: String(offset) });
    const term = query.trim();
    if (term) params.set('query', term);
    else params.set('level', '30');
    return request<GeoCollection>(`/geos?${params}`, signal, d => isCollection(d) && object(d) && typeof d.total === 'number');
  },
  provinceChildren(province: string, geometry: boolean, signal: AbortSignal) {
    return request<GeoCollection>(`/provinces/${encodeURIComponent(province)}/children?include_geometry=${geometry}`, signal,
      d => isCollection(d) && object(d) && typeof d.total === 'number');
  },
  async navigationBoundaries(ids: string[], signal: AbortSignal): Promise<NavigationBoundaries> {
    const features: NavigationBoundaries['features'] = [];
    // Preserve all children even if a future province exceeds the bulk endpoint's
    // request cap. Never silently truncate, and avoid unbounded client fan-out.
    for (let offset = 0; offset < ids.length; offset += 2000) {
      const batch = ids.slice(offset, offset + 2000);
      const result = await request<NavigationBoundaries>('/geos/boundaries', signal,
        d => isCollection(d) && object(d) && d.geometry_tolerance === 0.0025,
        { geog_ids: batch });
      const requested = new Set(batch);
      if (result.features.length !== batch.length || new Set(result.features.map(f => f.properties.geog_id)).size !== batch.length
        || result.features.some(f => !requested.has(f.properties.geog_id))) throw new ApiError(502, 'The boundary response is incomplete.');
      features.push(...result.features);
    }
    return { type: 'FeatureCollection', features, geometry_tolerance: 0.0025 };
  },
  metrics(id: string, propertyType: PropertyType, signal: AbortSignal) {
    return request<MarketMetrics>(`/metrics/${encodeURIComponent(id)}?property_type=${encodeURIComponent(propertyType)}`, signal,
      d => object(d) && d.geog_id === id && d.property_type === propertyType && object(d.series)
        && Object.values(d.series).every(s => object(s) && typeof s.available === 'boolean' && Array.isArray(s.data)));
  },
  catalog(signal: AbortSignal) {
    return request<MetricDefinition[]>('/metrics', signal, d => Array.isArray(d)
      && d.every(m => object(m) && typeof m.name === 'string' && typeof m.unit === 'string'));
  },
};
