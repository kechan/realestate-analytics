import { useCallback, useSyncExternalStore } from 'react';
import { PROVINCES } from './domain/geography';
import { METRICS, PROPERTY_TYPES } from './domain/metrics';
import type { ViewState } from './types';

const CHANGE = 'market-explorer:navigate';
export const DEFAULT_VIEW: ViewState = { geo: '', propertyType: 'ALL', metric: 'median_sold_price', range: 'all' };
export function readView(search: string): ViewState {
  const params = new URLSearchParams(search);
  const propertyType = params.get('property_type');
  const metric = params.get('metric');
  const range = params.get('range');
  const geo = params.get('geo')?.trim() || '';
  const province = params.get('province')?.toUpperCase();
  return {
    geo,
    ...(!geo && province && Object.hasOwn(PROVINCES, province) ? { province } : {}),
    propertyType: PROPERTY_TYPES.some(p => p.value === propertyType) ? propertyType as ViewState['propertyType'] : 'ALL',
    metric: metric && Object.hasOwn(METRICS, metric) ? metric as ViewState['metric'] : 'median_sold_price',
    range: range === '1y' || range === '3y' ? range : 'all',
  };
}
export function viewSearch(view: ViewState): string {
  const params = new URLSearchParams();
  if (view.geo) params.set('geo', view.geo);
  else if (view.province && Object.hasOwn(PROVINCES, view.province)) params.set('province', view.province);
  if (view.propertyType !== 'ALL') params.set('property_type', view.propertyType);
  if (view.metric !== 'median_sold_price') params.set('metric', view.metric);
  if (view.range !== 'all') params.set('range', view.range);
  return params.size ? `?${params}` : '';
}
const subscribe = (callback: () => void) => {
  window.addEventListener('popstate', callback); window.addEventListener(CHANGE, callback);
  return () => { window.removeEventListener('popstate', callback); window.removeEventListener(CHANGE, callback); };
};
export function useViewState() {
  const search = useSyncExternalStore(subscribe, () => window.location.search);
  const state = readView(search);
  const update = useCallback((patch: Partial<ViewState>) => {
    const next = { ...readView(window.location.search), ...patch };
    const nextSearch = viewSearch(next);
    if (nextSearch === window.location.search) return;
    window.history.pushState(null, '', `${window.location.pathname}${nextSearch}`);
    window.dispatchEvent(new Event(CHANGE));
  }, []);
  return [state, update] as const;
}
