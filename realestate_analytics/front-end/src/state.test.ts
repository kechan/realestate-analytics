import { act, renderHook } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { DEFAULT_VIEW, readView, useViewState, viewSearch } from './state';
import { GTA_ID } from './domain/geography';

describe('shareable view state', () => {
  it('defaults to Canada, ALL homes, sold price and all history', () => expect(readView('')).toEqual(DEFAULT_VIEW));
  it('preserves the GTA deep link and province navigation without inventing geography IDs', () => {
    expect(readView(`?geo=${GTA_ID}`).geo).toBe(GTA_ID);
    expect(viewSearch({ ...DEFAULT_VIEW, geo: GTA_ID })).toBe(`?geo=${GTA_ID}`);
    expect(readView('?province=bc')).toEqual({ ...DEFAULT_VIEW, province: 'BC' });
    expect(readView('?province=unknown')).toEqual(DEFAULT_VIEW);
    expect(readView(`?geo=${GTA_ID}&province=BC`)).toEqual({ ...DEFAULT_VIEW, geo: GTA_ID });
    expect(viewSearch({ ...DEFAULT_VIEW, province: 'BC' })).toBe('?province=BC');
  });
  it('rejects unsupported object metrics, property types and ranges', () => {
    expect(readView('?metric=current_metrics&property_type=HOUSE&range=bad')).toEqual(DEFAULT_VIEW);
    expect(readView('?metric=__proto__')).toEqual(DEFAULT_VIEW);
  });
  it('round-trips a non-GTA location without inventing GTA ancestry', () => {
    const view = { ...DEFAULT_VIEW, geo: 'g30_vancouver', propertyType: 'CONDO' as const, metric: 'months_of_inventory' as const, range: '3y' as const };
    expect(readView(viewSearch(view))).toEqual(view);
    expect(viewSearch(DEFAULT_VIEW)).toBe('');
  });
  it('URL-encodes selected IDs rather than interpolating them', () => {
    const view = { ...DEFAULT_VIEW, geo: 'g30_unsafe/?&x=1' };
    expect(readView(viewSearch(view)).geo).toBe(view.geo);
  });
  it('updates the URL and restores selections on popstate', () => {
    const { result } = renderHook(() => useViewState());
    act(() => result.current[1]({ geo: 'g30_city', propertyType: 'DETACHED' }));
    expect(result.current[0].geo).toBe('g30_city');
    expect(window.location.search).toContain('property_type=DETACHED');
    act(() => { window.history.replaceState(null, '', '/'); window.dispatchEvent(new PopStateEvent('popstate')); });
    expect(result.current[0].geo).toBe('');
    expect(result.current[0].propertyType).toBe('ALL');
  });
});
