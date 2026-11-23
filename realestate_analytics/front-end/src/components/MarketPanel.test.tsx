import { useState } from 'react';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import MarketPanel, { Summary } from './MarketPanel';
import { useMetrics } from '../queries';
import type { HistoryRange, MetricName, PropertyType } from '../types';
import { renderClient } from '../test/helpers';
import { catalog, market, response } from '../test/fixtures';

vi.mock('./ChartView', () => ({ default: ({ metric }: { metric: string }) => <div data-testid="chart">Chart: {metric}</div> }));

function Screen({ propertyType = 'ALL' }: { propertyType?: PropertyType }) {
  const [metric, setMetric] = useState<MetricName>('median_sold_price');
  const [range, setRange] = useState<HistoryRange>('all');
  const data = useMetrics('g30_city', propertyType);
  return <><Summary data={data.data} loading={data.isPending} /><MarketPanel id="g30_city" name="Toronto" propertyType={propertyType}
    metric={metric} range={range} onMetric={setMetric} onRange={setRange} /></>;
}
function installFetch() {
  const fetch = vi.fn(async (url: string) => {
    const u = new URL(url, 'http://test');
    return response(u.pathname.endsWith('/metrics') ? catalog : market((u.searchParams.get('property_type') || 'ALL') as PropertyType));
  });
  vi.stubGlobal('fetch', fetch); return fetch;
}

describe('shared charts and summary', () => {
  it('deduplicates consumers and does not refetch on metric/range changes', async () => {
    const fetch = installFetch(); const user = userEvent.setup(); renderClient(<Screen />);
    await screen.findByText('$815,000');
    const calls = () => fetch.mock.calls.filter(([url]) => String(url).includes('/metrics/g30_city'));
    expect(calls()).toHaveLength(1);
    await user.selectOptions(screen.getByRole('combobox', { name: 'History metric' }), 'months_of_inventory');
    await user.click(screen.getByRole('button', { name: '1Y' }));
    expect(screen.getByTestId('chart').textContent).toContain('months_of_inventory');
    expect(calls()).toHaveLength(1);
  });
  it('makes one new request for a property-type change', async () => {
    const fetch = installFetch(); const view = renderClient(<Screen />);
    await screen.findByText('$815,000');
    view.rerender(<Screen propertyType="DETACHED" />);
    await screen.findByText('$950,000');
    expect(fetch.mock.calls.filter(([url]) => String(url).includes('/metrics/g30_city'))).toHaveLength(2);
  });
  it('shows distinct reporting periods and the zero current-month count', async () => {
    installFetch(); renderClient(<Screen />);
    await screen.findByText('$815,000');
    expect(screen.getByText('Oct 2026')).toBeTruthy();
    expect(screen.getAllByText('Sep 2026').length).toBe(3);
    expect(screen.getByText('25.5%')).toBeTruthy();
    expect(screen.getByText('3.92')).toBeTruthy();
    expect(screen.getByText('0')).toBeTruthy();
  });
  it('opens the data table and restores focus when Escape closes it', async () => {
    installFetch(); const user = userEvent.setup(); renderClient(<Screen />);
    await screen.findByText('$815,000');
    const button = screen.getByRole('button', { name: 'View data' });
    await user.click(button);
    expect(screen.getByRole('dialog')).toBeTruthy();
    expect(screen.getByRole('table').textContent).toContain('$0');
    expect(screen.getByRole('button', { name: 'Download CSV' })).toBeTruthy();
    await user.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    expect(document.activeElement).toBe(button);
  });
  it('handles missing metrics as a local empty state', async () => {
    vi.stubGlobal('fetch', vi.fn(async (url: string) => String(url).endsWith('/metrics') ? response(catalog) : response({}, 404)));
    renderClient(<Screen />);
    await screen.findByText('No published market data');
    expect(screen.getByRole('combobox', { name: 'History metric' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'View data' }).hasAttribute('disabled')).toBe(true);
  });
});
