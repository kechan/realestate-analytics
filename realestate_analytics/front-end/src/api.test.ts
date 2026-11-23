import { describe, expect, it, vi } from 'vitest';
import { api, ApiError } from './api';
import { detail, market, response } from './test/fixtures';

describe('versioned API contract', () => {
  const signal = () => new AbortController().signal;
  it('requests detail geometry with the new parameter and no browser credentials', async () => {
    const fetch = vi.fn().mockResolvedValue(response(detail())); vi.stubGlobal('fetch', fetch);
    await api.detail('g30_city', signal());
    expect(fetch.mock.calls[0][0]).toBe('/api/v1/geos/g30_city?include_geometry=true');
    expect(fetch.mock.calls[0][1].headers).toEqual({ accept: 'application/json' });
  });
  it('uses one document request for all monthly and current metrics', async () => {
    const fetch = vi.fn().mockResolvedValue(response(market())); vi.stubGlobal('fetch', fetch);
    const data = await api.metrics('g30_city', 'ALL', signal());
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(data.current_metrics?.new_listings).toBe(0);
    expect(fetch.mock.calls[0][0]).toBe('/api/v1/metrics/g30_city?property_type=ALL');
  });
  it('always scopes child requests to their parent', async () => {
    const fetch = vi.fn().mockResolvedValue(response({ type: 'FeatureCollection', features: [], total: 0 })); vi.stubGlobal('fetch', fetch);
    await api.children('g40_root', false, signal(), 30);
    const params = new URL(String(fetch.mock.calls[0][0]), 'http://test').searchParams;
    expect(params.get('parent_id')).toBe('g40_root');
    expect(params.get('include_geometry')).toBe('false');
    expect(params.get('limit')).toBeNull();
  });
  it('bounds lightweight search and encodes literal user input', async () => {
    const fetch = vi.fn().mockResolvedValue(response({ type: 'FeatureCollection', features: [], total: 0 })); vi.stubGlobal('fetch', fetch);
    await api.search('Toronto & York', 20, signal());
    const params = new URL(String(fetch.mock.calls[0][0]), 'http://test').searchParams;
    expect(params.get('query')).toBe('Toronto & York'); expect(params.get('offset')).toBe('20');
    expect(params.get('limit')).toBe('20'); expect(params.get('include_geometry')).toBe('false');
  });
  it('requests only paginated metadata when browsing a province', async () => {
    const fetch = vi.fn().mockResolvedValue(response({ type: 'FeatureCollection', features: [], total: 0 })); vi.stubGlobal('fetch', fetch);
    await api.provinceMarkets('BC', 'North & West', 30, signal());
    const params = new URL(String(fetch.mock.calls[0][0]), 'http://test').searchParams;
    expect(Object.fromEntries(params)).toEqual({ province: 'BC', include_geometry: 'false', limit: '30', offset: '30', query: 'North & West' });
  });
  it('limits the unsearched province directory to cities while searching other area levels', async () => {
    const fetch = vi.fn().mockImplementation(async () => response({ type: 'FeatureCollection', features: [], total: 0 })); vi.stubGlobal('fetch', fetch);
    await api.provinceMarkets('QC', '  ', 0, signal());
    await api.provinceMarkets('QC', ' Estrie ', 0, signal());
    const browse = new URL(String(fetch.mock.calls[0][0]), 'http://test').searchParams;
    const search = new URL(String(fetch.mock.calls[1][0]), 'http://test').searchParams;
    expect(browse.get('level')).toBe('30'); expect(browse.has('query')).toBe(false);
    expect(search.get('province')).toBe('QC'); expect(search.get('level')).toBeNull();
    expect(search.get('query')).toBe('Estrie'); expect(search.get('limit')).toBe('30');
  });
  it('distinguishes missing documents from upstream failures', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({}, 404)));
    await expect(api.metrics('g30_city', 'ALL', signal())).rejects.toMatchObject({ status: 404 });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({}, 503)));
    await expect(api.metrics('g30_city', 'ALL', signal())).rejects.toMatchObject({ status: 503 });
  });
  it('rejects a metrics response for a different selection', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(market('CONDO'))));
    await expect(api.metrics('g30_city', 'ALL', signal())).rejects.toMatchObject({ status: 502 });
  });
  it('does not swallow cancellation as a network error', async () => {
    const controller = new AbortController(); controller.abort();
    const aborted = new DOMException('Aborted', 'AbortError');
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(aborted));
    await expect(api.detail('g30_city', controller.signal)).rejects.toBe(aborted);
  });
  it('turns actual connection errors into an actionable API error', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('Failed to fetch')));
    await expect(api.detail('g30_city', signal())).rejects.toBeInstanceOf(ApiError);
    await expect(api.detail('g30_city', signal())).rejects.toMatchObject({ status: 0 });
  });
});

describe('explicit navigation boundary batches', () => {
  it('posts only selected IDs and does not repeat province hierarchy discovery', async () => {
    const fetch = vi.fn(async (_url: string, options: RequestInit) => {
      const ids = JSON.parse(String(options.body)).geog_ids;
      return response({ type: 'FeatureCollection', geometry_tolerance: 0.0025,
        features: ids.map((id: string) => ({ type: 'Feature', geometry: null, properties: { geog_id: id, geometry_status: 'missing' },
          provenance: { source: 'elasticsearch', fallback_fields: [] } })) });
    });
    vi.stubGlobal('fetch', fetch);
    const ids = Array.from({ length: 2001 }, (_, i) => `g30_${i}`);
    const result = await api.navigationBoundaries(ids, new AbortController().signal);
    expect(result.features.map(f => f.properties.geog_id)).toEqual(ids);
    expect(fetch.mock.calls.map(([url]) => url)).toEqual(['/api/v1/geos/boundaries', '/api/v1/geos/boundaries']);
    expect(fetch.mock.calls.every(([, options]) => options.method === 'POST')).toBe(true);
    expect(JSON.parse(String(fetch.mock.calls[0][1].body)).geog_ids).toHaveLength(2000);
  });
  it('rejects duplicate, missing or unrelated boundaries instead of silently rendering a partial map', async () => {
    for (const ids of [['g30_a'], ['g30_a','g30_a'], ['g30_a','g30_other']]) {
      vi.stubGlobal('fetch', vi.fn(async () => response({ type: 'FeatureCollection', geometry_tolerance: 0.0025,
        features: ids.map(id => ({ type: 'Feature', properties: { geog_id: id } })) })));
      await expect(api.navigationBoundaries(['g30_a','g30_b'], new AbortController().signal)).rejects.toMatchObject({ status: 502 });
    }
  });
});
