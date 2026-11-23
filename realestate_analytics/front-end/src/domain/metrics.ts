import type { ChartPoint, HistoryRange, MetricName, MetricPoint, MetricSeries, MetricUnit, PropertyType } from '../types';

export const PROPERTY_TYPES: { value: PropertyType; label: string }[] = [
  { value: 'ALL', label: 'All homes' }, { value: 'DETACHED', label: 'Detached' },
  { value: 'SEMI-DETACHED', label: 'Semi-detached' }, { value: 'TOWNHOUSE', label: 'Townhouse' },
  { value: 'CONDO', label: 'Condo' }, { value: 'OTHER', label: 'Other' },
];

export const METRIC_GROUPS = ['Sales', 'Asking prices', 'Inventory'] as const;

export const METRICS: Record<MetricName, { label: string; unit: MetricUnit; group: typeof METRIC_GROUPS[number]; bar?: boolean }> = {
  median_sold_price: { label: 'Median sold price', unit: 'currency', group: 'Sales' },
  median_dom: { label: 'Days on market', unit: 'days', group: 'Sales' },
  over_ask_percentage: { label: 'Sold above asking', unit: 'percent', group: 'Sales' },
  below_ask_percentage: { label: 'Sold below asking', unit: 'percent', group: 'Sales' },
  sold_listing_count: { label: 'Sold listings', unit: 'listings', group: 'Sales', bar: true },
  last_mth_median_asking_price: { label: 'Median asking price', unit: 'currency', group: 'Asking prices' },
  last_mth_min_asking_price: { label: 'Filtered minimum asking price', unit: 'currency', group: 'Asking prices' },
  last_mth_max_asking_price: { label: 'Filtered maximum asking price', unit: 'currency', group: 'Asking prices' },
  last_mth_new_listings: { label: 'New listings', unit: 'listings', group: 'Inventory', bar: true },
  mth_end_snapshot_listing_count: { label: 'Month-end active listings', unit: 'listings', group: 'Inventory', bar: true },
  absorption_rate: { label: 'Absorption rate', unit: 'ratio', group: 'Inventory' },
  months_of_inventory: { label: 'Months of inventory', unit: 'months', group: 'Inventory' },
};

export const METRIC_ENTRIES = Object.entries(METRICS) as [MetricName, typeof METRICS[MetricName]][];

export function monthIndex(month: string): number {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(month)) return NaN;
  return Number(month.slice(0, 4)) * 12 + Number(month.slice(5)) - 1;
}

export function monthLabel(month: string | null | undefined, long = false): string {
  const index = month ? monthIndex(month) : NaN;
  if (!Number.isFinite(index)) return 'Not available';
  return new Intl.DateTimeFormat('en-CA', { month: long ? 'long' : 'short', year: 'numeric', timeZone: 'UTC' })
    .format(new Date(Date.UTC(Math.floor(index / 12), index % 12, 1)));
}

export function latestPoint(series?: MetricSeries): MetricPoint | undefined {
  if (!series?.available) return undefined;
  return series.data.filter(p => Number.isFinite(monthIndex(p.month)) && Number.isFinite(p.value))
    .reduce<MetricPoint | undefined>((last, p) => !last || p.month > last.month ? p : last, undefined);
}

export function chartPoints(series: MetricSeries | undefined, range: HistoryRange = 'all'): ChartPoint[] {
  if (!series?.available) return [];
  const valid = series.data.filter(p => Number.isFinite(monthIndex(p.month)) && Number.isFinite(p.value));
  if (!valid.length) return [];
  const values = new Map(valid.map(p => [monthIndex(p.month), p.value]));
  const end = Math.max(...values.keys());
  const start = Math.max(Math.min(...values.keys()), range === 'all' ? -Infinity : end - (range === '1y' ? 11 : 35));
  return Array.from({ length: end - start + 1 }, (_, offset) => {
    const index = start + offset;
    return { month: `${Math.floor(index / 12)}-${String(index % 12 + 1).padStart(2, '0')}`, value: values.get(index) ?? null };
  });
}

export function formatValue(value: number | null | undefined, unit: MetricUnit, compact = false): string {
  if (value == null || !Number.isFinite(value)) return '—';
  if (unit === 'currency') return new Intl.NumberFormat('en-CA', {
    style: 'currency', currency: 'CAD', maximumFractionDigits: compact ? 1 : 0,
    ...(compact ? { notation: 'compact' as const } : {}),
  }).format(value);
  const formatted = new Intl.NumberFormat('en-CA', {
    maximumFractionDigits: unit === 'listings' ? 0 : 2,
    ...(compact && unit === 'listings' ? { notation: 'compact' as const } : {}),
  }).format(unit === 'ratio' ? value * 100 : value);
  return `${formatted}${unit === 'ratio' || unit === 'percent' ? '%' : unit === 'days' ? ' days' : ''}`;
}

export function seriesCsv(points: ChartPoint[], metric: MetricName): string {
  // Values remain in API units: absorption is a ratio, not a pre-multiplied percentage.
  return `month,${metric}\r\n${points.map(p => `${p.month},${p.value ?? ''}`).join('\r\n')}\r\n`;
}

export function updatedLabel(value?: string | null): string | null {
  if (!value) return null;
  // ES timestamps sometimes lack a timezone. Keep the source's calendar date;
  // don't claim a local instant or infer that all series share its month.
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(value);
  if (!match || !Number.isFinite(monthIndex(`${match[1]}-${match[2]}`))) return null;
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  if (date.getUTCDate() !== Number(match[3])) return null;
  return new Intl.DateTimeFormat('en-CA', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }).format(date);
}
