import type { GeoDetail, GeoFeature, MarketMetrics, MetricDefinition, MetricSeries, PropertyType } from '../types';
import { METRICS } from '../domain/metrics';
import { GTA_ID } from '../domain/geography';

export const series = (values = [10, 0, 20]): MetricSeries => ({ available: true, latest_available_month: '2026-09',
  data: values.map((value, index) => ({ month: `2026-${String(7 + index).padStart(2, '0')}`, value })) });
export const market = (type: PropertyType = 'ALL', gid = 'g30_city'): MarketMetrics => ({
  geog_id: gid, property_type: type, source_index: 'rlp_mkt_trends_current', document_updated_at: '2026-11-01T05:00:00',
  series: {
    median_sold_price: series(type === 'ALL' ? [700000, 0, 815000] : [800000, 900000, 950000]),
    last_mth_median_asking_price: { available: true, latest_available_month: '2026-10', data: [{ month: '2026-10', value: 799000 }] },
    absorption_rate: series([0.1, 0, 0.255]), months_of_inventory: series([8, 4, 3.92]),
  },
  current_metrics: { median_asking_price: 799000, new_listings: 0 },
});
export const catalog: MetricDefinition[] = Object.entries(METRICS).map(([name, meta]) => ({
  name: name as MetricDefinition['name'], unit: meta.unit, description: meta.label, value_type: meta.bar ? 'integer' : 'number',
}));
export function geo(id = 'g30_city', name = 'Toronto', level = 30, parent: string | null = GTA_ID): GeoFeature {
  return { type: 'Feature', properties: { geog_id: id, name, level, province: 'ON', parent_geog_id: parent,
    parent_status: parent ? 'cache' : 'root', geometry_status: 'available' },
    geometry: { type: 'Polygon', coordinates: [[[-79.6, 43.6], [-79.1, 43.6], [-79.1, 43.9], [-79.6, 43.6]]] },
    provenance: { source: 'mixed', fallback_fields: ['parent_geog_id'] },
  };
}
export function detail(g = geo()): GeoDetail {
  const { parent_geog_id: _parent, parent_status: _status, ...properties } = g.properties;
  return { ...g, properties, details: { data: { id: g.properties.geog_id, intro: 'A city in Canada.',
    profiles: { transport: 'A transportation profile.', housing: 'A housing profile.' } }, meta: {} } };
}
export const response = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } });
