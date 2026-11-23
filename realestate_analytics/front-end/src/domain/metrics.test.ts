import { describe, expect, it } from 'vitest';
import { chartPoints, formatValue, latestPoint, METRICS, monthIndex, monthLabel, PROPERTY_TYPES, seriesCsv } from './metrics';
import { series } from '../test/fixtures';

describe('published metric semantics', () => {
  it('keeps all twelve monthly metrics separate from the current object', () => {
    expect(Object.keys(METRICS)).toHaveLength(12);
    expect(Object.hasOwn(METRICS, 'current_metrics')).toBe(false);
    expect(PROPERTY_TYPES.map(p => p.value)).toContain('OTHER');
    expect(PROPERTY_TYPES).toHaveLength(6);
  });
  it('preserves zero and distinguishes unavailable values', () => {
    expect(formatValue(0, 'currency')).toBe('$0');
    expect(formatValue(0, 'listings')).toBe('0');
    expect(formatValue(null, 'listings')).toBe('—');
    expect(formatValue(undefined, 'ratio')).toBe('—');
    expect(formatValue(NaN, 'currency')).toBe('—');
  });
  it('converts absorption ratios once and leaves percentages alone', () => {
    expect(formatValue(.255, 'ratio')).toBe('25.5%');
    expect(formatValue(25.5, 'percent')).toBe('25.5%');
    expect(formatValue(1.25, 'ratio')).toBe('125%');
  });
  it('uses readable currency, days, and months without implied units', () => {
    expect(formatValue(815000, 'currency')).toBe('$815,000');
    expect(formatValue(15.4, 'days')).toBe('15.4 days');
    expect(formatValue(3.92, 'months')).toBe('3.92');
  });
  it('inserts nulls for missing months without turning zero into missing', () => {
    const input = { ...series(), data: [{ month: '2026-09', value: 0 }, { month: '2026-07', value: 12 }] };
    const before = JSON.stringify(input);
    expect(chartPoints(input)).toEqual([{ month: '2026-07', value: 12 }, { month: '2026-08', value: null }, { month: '2026-09', value: 0 }]);
    expect(JSON.stringify(input)).toBe(before);
  });
  it('anchors one-year range to the metric’s last published month', () => {
    const input = { ...series(), data: [{ month: '2023-01', value: 10 }, { month: '2026-09', value: 20 }] };
    const points = chartPoints(input, '1y');
    expect(points).toHaveLength(12);
    expect(points[0]).toEqual({ month: '2025-10', value: null });
    expect(points.at(-1)).toEqual({ month: '2026-09', value: 20 });
    expect(chartPoints(input, '3y')).toHaveLength(36);
  });
  it('does not invent history for unavailable or empty series', () => {
    expect(chartPoints(undefined)).toEqual([]);
    expect(chartPoints({ ...series(), available: false })).toEqual([]);
    expect(chartPoints({ ...series(), data: [] })).toEqual([]);
  });
  it('finds the actual latest point without mutating source order', () => {
    const input = { ...series(), data: [{ month: '2026-10', value: 7 }, { month: '2026-08', value: 9 }] };
    expect(latestPoint(input)).toEqual({ month: '2026-10', value: 7 });
    expect(input.data[0].month).toBe('2026-10');
    expect(latestPoint({ ...input, available: false })).toBeUndefined();
  });
  it('handles year boundaries without local-timezone month shifts', () => {
    expect(monthIndex('2026-01') - monthIndex('2025-12')).toBe(1);
    expect(monthLabel('2026-01')).toBe('Jan 2026');
    expect(monthIndex('2026-13')).toBeNaN();
    expect(monthLabel(null)).toBe('Not available');
    expect(chartPoints({ ...series(), data: [{ month: '2025-12', value: 1 }, { month: '2026-01', value: 2 }] })).toHaveLength(2);
  });
  it('exports exact API ratios, zeroes, and empty missing cells', () => {
    expect(seriesCsv([{ month: '2026-07', value: .255 }, { month: '2026-08', value: null }, { month: '2026-09', value: 0 }], 'absorption_rate'))
      .toBe('month,absorption_rate\r\n2026-07,0.255\r\n2026-08,\r\n2026-09,0\r\n');
  });
});
