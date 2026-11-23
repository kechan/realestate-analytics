import { describe, expect, it } from 'vitest';
import { joinNavigationBoundaries } from './queries';
import { geo } from './test/fixtures';

describe('navigation boundary merge', () => {
  it('joins by ID despite reordered responses, preserving current metadata and whole polygons', () => {
    const parent = geo('g35_parent', 'Fresh region', 35);
    const city = geo('g30_city', 'Fresh city', 30, 'g35_parent');
    const before = structuredClone([parent, city]);
    const polygon = geo().geometry;
    const result = joinNavigationBoundaries([parent, city], { type: 'FeatureCollection', geometry_tolerance: .0025, features: [
      { type: 'Feature', geometry: null, properties: { geog_id: 'g30_city', geometry_status: 'missing' }, provenance: { source: 'elasticsearch', fallback_fields: [] } },
      { type: 'Feature', geometry: polygon, properties: { geog_id: 'g35_parent', geometry_status: 'available' }, provenance: { source: 'geo_collection', fallback_fields: ['geometry'] } },
    ] });
    expect(result[0].geometry).toBe(polygon);
    expect(result[0].properties.name).toBe('Fresh region');
    expect(result[0].provenance).toEqual({ source: 'mixed', fallback_fields: ['geometry', 'parent_geog_id'] });
    expect(result[1].properties.parent_geog_id).toBe('g35_parent');
    expect(result[1].geometry).toBeNull();
    expect([parent, city]).toEqual(before);
  });
});
