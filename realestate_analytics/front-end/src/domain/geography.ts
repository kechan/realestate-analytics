import type { GeoIdentity, GeoGeometry } from '../types';

export const GTA_ID = 'g40_dpz3tpuh';
export const GTA_NAME = 'Greater Toronto Area';
export const PROVINCES: Record<string, string> = {
  ON: 'Ontario', QC: 'Quebec', BC: 'British Columbia', AB: 'Alberta', MB: 'Manitoba',
  SK: 'Saskatchewan', NS: 'Nova Scotia', NB: 'New Brunswick', NL: 'Newfoundland and Labrador',
  PE: 'Prince Edward Island', YT: 'Yukon', NT: 'Northwest Territories', NU: 'Nunavut',
};
export function levelLabel(level: number | null): string {
  return ({ 10: 'Neighbourhood', 20: 'District', 30: 'City', 35: 'Area', 40: 'Region' } as Record<number, string>)[level ?? 0] || 'Area';
}
export const nameOf = (geo: { properties: GeoIdentity } | null | undefined) => geo?.properties.name || 'Selected location';
export function hasGeometry(feature: { geometry: GeoGeometry | null }): boolean {
  return Boolean(feature.geometry && ['Polygon', 'MultiPolygon'].includes(feature.geometry.type)
    && Array.isArray(feature.geometry.coordinates) && feature.geometry.coordinates.length);
}
