import { geoJSON, Polygon } from 'leaflet';
import type { FeatureCollection, Geometry } from 'geojson';
import { describe, expect, it } from 'vitest';
import { hasGeometry, levelLabel } from './geography';
import { geo } from '../test/fixtures';

describe('GeoJSON map semantics', () => {
  it('keeps longitude-first API coordinates and creates correct Leaflet bounds', () => {
    const feature = geo(); const before = JSON.stringify(feature);
    const bounds = geoJSON(feature).getBounds();
    expect(bounds.getWest()).toBe(-79.6); expect(bounds.getEast()).toBe(-79.1);
    expect(bounds.getSouth()).toBe(43.6); expect(bounds.getNorth()).toBe(43.9);
    expect(JSON.stringify(feature)).toBe(before);
  });
  it('retains polygon holes rather than drawing them as separate filled regions', () => {
    const feature = geo();
    feature.geometry = { type: 'Polygon', coordinates: [
      [[-80, 43], [-79, 43], [-79, 44], [-80, 44], [-80, 43]],
      [[-79.8, 43.2], [-79.8, 43.8], [-79.2, 43.8], [-79.2, 43.2], [-79.8, 43.2]],
    ] };
    const layer = geoJSON(feature).getLayers()[0] as Polygon;
    expect(layer.getLatLngs()).toHaveLength(2);
    expect((layer.getLatLngs()[0] as { lat: number; lng: number }[])[0]).toMatchObject({ lat: 43, lng: -80 });
  });
  it('supports disconnected MultiPolygon parts and their combined bounds', () => {
    const feature = geo();
    feature.geometry = { type: 'MultiPolygon', coordinates: [
      [[[-80, 43], [-79, 43], [-79, 44], [-80, 43]]],
      [[[-78, 45], [-77, 45], [-77, 46], [-78, 45]]],
    ] };
    const collection: FeatureCollection<Geometry | null> = { type: 'FeatureCollection', features: [feature] };
    const bounds = geoJSON(collection).getBounds();
    expect(bounds.getWest()).toBe(-80); expect(bounds.getEast()).toBe(-77); expect(bounds.getNorth()).toBe(46);
    expect((geoJSON(feature).getLayers()[0] as Polygon).getLatLngs()).toHaveLength(2);
  });
  it('does not claim a missing boundary exists', () => {
    const feature = geo(); feature.geometry = null; expect(hasGeometry(feature)).toBe(false);
    feature.geometry = { type: 'Polygon', coordinates: [] }; expect(hasGeometry(feature)).toBe(false);
  });
  it('labels actual levels without assuming every child is a district', () => {
    expect(levelLabel(10)).toBe('Neighbourhood'); expect(levelLabel(20)).toBe('District');
    expect(levelLabel(35)).toBe('Area'); expect(levelLabel(null)).toBe('Area');
  });
});
