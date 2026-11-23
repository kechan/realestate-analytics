import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import App from './App';
import { GTA_ID } from './domain/geography';
import { renderClient } from './test/helpers';
import { catalog, detail, geo, market, response } from './test/fixtures';

vi.mock('./components/AreaMap', () => ({ default: ({ children, onPreview, onNavigate }: {
  children: ReturnType<typeof geo>[]; onPreview: (area: ReturnType<typeof geo>) => void; onNavigate: (id: string) => void;
}) => <div data-testid="map" onMouseEnter={() => onPreview(geo())}>{children.length} area outlines
  {children.map(child => <button key={child.properties.geog_id} onClick={() => onNavigate(child.properties.geog_id)}>
    Explore {child.properties.name} on map
  </button>)}
</div> }));
vi.mock('./components/ChartView', () => ({ default: () => <div>History chart</div> }));

function installFetch({ missingMetrics = false, incomplete = false } = {}) {
  const root = geo(GTA_ID, 'Greater Toronto Area', 40, null);
  const city = geo();
  const fetch = vi.fn(async (url: string) => {
    const u = new URL(url, 'http://test');
    if (u.pathname === '/api/v1/metrics') return response(catalog);
    if (u.pathname === '/api/v1/geos/boundaries') return response({ type: 'FeatureCollection', features: [root], geometry_tolerance: 0.0025 });
    if (u.pathname.startsWith('/api/v1/provinces/')) return response({ type: 'FeatureCollection', features: [root], total: 1, offset: 0, limit: null });
    if (u.pathname.startsWith('/api/v1/metrics/')) return missingMetrics ? response({}, 404)
      : response(market((u.searchParams.get('property_type') || 'ALL') as 'ALL', u.pathname.split('/').at(-1)));
    if (u.pathname.endsWith('/hierarchy')) return response({ type: 'FeatureCollection',
      features: u.pathname.includes(`/${GTA_ID}/`) ? [root] : incomplete ? [city] : [root, city], complete: !incomplete });
    if (u.pathname === '/api/v1/geos') {
      const features = u.searchParams.get('parent_id') === GTA_ID || u.searchParams.get('province') === 'ON' ? [city] : [];
      return response({ type: 'FeatureCollection', features, total: features.length, offset: 0, limit: null });
    }
    return response(detail(u.pathname.endsWith(GTA_ID) ? root : city));
  });
  vi.stubGlobal('fetch', fetch); return fetch;
}

describe('explorer navigation', () => {
  beforeEach(() => { window.history.replaceState(null, '', `/?geo=${GTA_ID}`); });
  it('does not reopen a map hover when closing a moved preview exposes the polygon underneath', async () => {
    installFetch(); renderClient(<App />);
    const map = await screen.findByTestId('map');
    vi.useFakeTimers(); fireEvent.mouseEnter(map);
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    fireEvent.click(screen.getByRole('button', { name: 'Close market preview' }));
    fireEvent.mouseEnter(map);
    await act(async () => { await vi.advanceTimersByTimeAsync(500); });
    expect(screen.queryByRole('region', { name: 'Toronto market preview' })).toBeNull();
    fireEvent.pointerMove(map); fireEvent.mouseEnter(map);
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(screen.getByRole('region', { name: 'Toronto market preview' })).toBeTruthy();
  });
  it.each(['pointer', 'keyboard'])('retains an interacting hover preview through native dropdown leave/blur (%s)', async input => {
    const fetch = installFetch(); renderClient(<App />);
    const city = await screen.findByRole('button', { name: /^Toronto/ });
    vi.useFakeTimers();
    fireEvent.mouseEnter(city);
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    const preview = screen.getByRole('region', { name: 'Toronto market preview' });
    const property = within(preview).getByRole('combobox', { name: 'Preview property type' });
    // Opening a browser-owned menu can move the pointer/focus outside the DOM.
    if (input === 'pointer') fireEvent.pointerDown(property);
    else fireEvent.focus(property);
    fireEvent.mouseLeave(preview);
    fireEvent.blur(property, { relatedTarget: null });
    await act(async () => { await vi.advanceTimersByTimeAsync(500); });
    expect(screen.getByRole('region', { name: 'Toronto market preview' })).toBe(preview);
    fireEvent.change(property, { target: { value: 'DETACHED' } });
    await act(async () => { await vi.advanceTimersByTimeAsync(100); });
    expect(within(preview).getByText('$950,000')).toBeTruthy();
    expect(fetch.mock.calls.some(([url]) => String(url).includes('/metrics/g30_city?property_type=DETACHED'))).toBe(true);
    fireEvent.click(within(preview).getByRole('button', { name: 'Close market preview' }));
    expect(screen.queryByRole('region', { name: 'Toronto market preview' })).toBeNull();
  });
  it('still dismisses an untouched hover preview after leaving', async () => {
    installFetch(); renderClient(<App />);
    const city = await screen.findByRole('button', { name: /^Toronto/ });
    vi.useFakeTimers(); fireEvent.mouseEnter(city);
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    fireEvent.mouseLeave(screen.getByRole('region', { name: 'Toronto market preview' }));
    await act(async () => { await vi.advanceTimersByTimeAsync(500); });
    expect(screen.queryByRole('region', { name: 'Toronto market preview' })).toBeNull();
  });
  it('does not close an open drawer when delayed location details arrive', async () => {
    const normal = installFetch();
    let finish: (value: Response) => void = () => {};
    vi.stubGlobal('fetch', vi.fn((url: string) => {
      if (String(url).includes('/geos/g30_city?')) return new Promise<Response>(resolve => { finish = resolve; });
      if (String(url).endsWith('/hierarchy')) return Promise.resolve(response({ type: 'FeatureCollection', features: [], complete: false }));
      return normal(url);
    }));
    window.history.replaceState(null, '', '/?geo=g30_city');
    const user = userEvent.setup(); renderClient(<App />);
    await user.click(screen.getByRole('button', { name: 'Areas' }));
    expect(screen.getByRole('dialog', { name: 'Areas' })).toBeTruthy();
    await act(async () => finish(response(detail())));
    await screen.findByRole('heading', { level: 1, name: 'Toronto' });
    expect(screen.getByRole('dialog', { name: 'Areas' })).toBeTruthy();
  });
  it('moves focus into the Areas drawer and restores it on Escape', async () => {
    installFetch(); const user = userEvent.setup(); renderClient(<App />);
    await screen.findByRole('button', { name: /^Toronto/ });
    const toggle = screen.getByRole('button', { name: 'Areas' });
    await user.click(toggle);
    expect(screen.getByRole('dialog', { name: 'Areas' }).contains(document.activeElement)).toBe(true);
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog', { name: 'Areas' })).toBeNull();
    expect(document.activeElement).toBe(toggle);
  });
  it('shows GTA metrics and history, then opens a city without duplicate metric requests', async () => {
    const fetch = installFetch(); const user = userEvent.setup(); renderClient(<App />);
    await screen.findByRole('button', { name: /^Toronto/ });
    await screen.findByRole('heading', { name: 'Market history' });
    await waitFor(() => expect(within(screen.getByLabelText('Market summary')).getByText('$815,000')).toBeTruthy());
    expect(screen.getByRole('combobox', { name: 'Property type' })).toBeTruthy();
    expect(fetch.mock.calls.filter(([url]) => String(url).includes(`/metrics/${GTA_ID}`))).toHaveLength(1);
    await user.click(screen.getByRole('button', { name: /^Toronto/ }));
    await screen.findByRole('heading', { level: 1, name: 'Toronto' });
    await waitFor(() => expect(within(screen.getByLabelText('Market summary')).getByText('$815,000')).toBeTruthy());
    expect(screen.getByTestId('map')).toBeTruthy();
    expect(window.location.search).toContain('geo=g30_city');
    expect(fetch.mock.calls.filter(([url]) => String(url).includes('/metrics/g30_city'))).toHaveLength(1);
  });
  it('updates GTA metrics by property type and includes Canada and Ontario in its path', async () => {
    const fetch = installFetch(); const user = userEvent.setup(); renderClient(<App />);
    await user.selectOptions(screen.getByRole('combobox', { name: 'Property type' }), 'DETACHED');
    await waitFor(() => expect(within(screen.getByLabelText('Market summary')).getByText('$950,000')).toBeTruthy());
    expect(fetch.mock.calls.filter(([url]) => String(url).includes(`/metrics/${GTA_ID}?property_type=DETACHED`))).toHaveLength(1);
    const path = screen.getByRole('navigation', { name: 'Location path' });
    expect(within(path).getByRole('button', { name: 'Canada' })).toBeTruthy();
    expect(within(path).getByRole('button', { name: 'Ontario' })).toBeTruthy();
    expect(within(path).getByText('Greater Toronto Area')).toBeTruthy();
    expect(screen.queryByText('Choose a city from the map or the list to view market history.')).toBeNull();
  });
  it.each([GTA_ID, 'g30_city'])('keeps the map and profile usable when metrics are missing for %s', async id => {
    installFetch({ missingMetrics: true }); window.history.replaceState(null, '', `/?geo=${id}`); renderClient(<App />);
    await screen.findByText('No published market data');
    expect(screen.getByTestId('map')).toBeTruthy();
    expect(screen.getByRole('combobox', { name: 'Property type' })).toBeTruthy();
    const user = userEvent.setup(); await user.click(screen.getByText('Area profile'));
    expect(screen.getByText('A housing profile.')).toBeTruthy();
  });
  it('does not fabricate GTA ancestors for an incomplete source path', async () => {
    installFetch({ incomplete: true }); window.history.replaceState(null, '', '/?geo=g30_city'); renderClient(<App />);
    await screen.findByText('The published parent path is incomplete.');
    const path = screen.getByRole('navigation', { name: 'Location path' });
    expect(within(path).getByText('Toronto')).toBeTruthy();
    expect(path.textContent).not.toContain('Greater Toronto Area');
  });
  it('returns to Canada from the brand button', async () => {
    installFetch(); window.history.replaceState(null, '', '/?geo=g30_city'); renderClient(<App />);
    await screen.findByRole('heading', { level: 1, name: 'Toronto' });
    const user = userEvent.setup(); await user.click(screen.getByRole('button', { name: 'Market Explorer' }));
    await waitFor(() => expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('Explore Canada'));
    expect(window.location.search).toBe('');
  });
});

describe('Canada landing navigation', () => {
  it('renders Canada without downloading geographies or requesting national metrics', async () => {
    const fetch = installFetch(); renderClient(<App />);
    expect(screen.getByRole('heading', { name: 'Explore Canada', level: 1 })).toBeTruthy();
    await screen.findByRole('button', { name: 'Browse British Columbia' });
    expect(fetch.mock.calls.some(([url]) => String(url).includes('/geos'))).toBe(false);
    expect(fetch.mock.calls.some(([url]) => String(url).includes('/metrics/'))).toBe(false);
  });
  it('keeps GTA one click away and restores Canada through browser history', async () => {
    installFetch(); const user = userEvent.setup(); renderClient(<App />);
    await user.click(screen.getByRole('button', { name: 'Explore GTA' }));
    await screen.findByRole('heading', { level: 1, name: 'Greater Toronto Area' });
    expect(window.location.search).toBe(`?geo=${GTA_ID}`);
    expect(screen.getByRole('button', { name: /^Toronto/ })).toBeTruthy();
    act(() => { window.history.replaceState(null, '', '/'); window.dispatchEvent(new PopStateEvent('popstate')); });
    expect(screen.getByRole('heading', { level: 1, name: 'Explore Canada' })).toBeTruthy();
  });
  it('drills from province to region to city and back using immediate children', async () => {
    const fetch = installFetch(); const user = userEvent.setup(); renderClient(<App />);
    await user.click(screen.getByRole('button', { name: 'Ontario' }));
    await screen.findByRole('heading', { level: 1, name: 'Ontario' });
    expect(window.location.search).toBe('?province=ON');
    await user.click(await screen.findByRole('button', { name: /^Greater Toronto Area\b/ }));
    await screen.findByRole('heading', { level: 1, name: 'Greater Toronto Area' });
    await user.click(await screen.findByRole('button', { name: /^Toronto\b/ }));
    await screen.findByRole('heading', { level: 1, name: 'Toronto' });
    expect(fetch.mock.calls.some(([url]) => String(url).includes('/provinces/ON/children?include_geometry=false'))).toBe(true);
    const path = screen.getByRole('navigation', { name: 'Location path' });
    expect(within(path).getByRole('button', { name: 'Canada' })).toBeTruthy();
    expect(within(path).getByRole('button', { name: 'Ontario' })).toBeTruthy();
    await user.click(within(screen.getByRole('complementary')).getByRole('button', { name: /^Greater Toronto Area\b/ }));
    await screen.findByRole('heading', { level: 1, name: 'Greater Toronto Area' });
    await user.click(within(screen.getByRole('complementary')).getByRole('button', { name: 'Ontario' }));
    await screen.findByRole('heading', { level: 1, name: 'Ontario' });
    expect(fetch.mock.calls.some(([url]) => /\/geos\/(?:Canada|ON|undefined)/.test(String(url)))).toBe(false);
  });
  it('uses the same drill-down for province polygons and GTA city polygons as the sidebar', async () => {
    installFetch(); window.history.replaceState(null, '', '/?province=ON');
    const user = userEvent.setup(); renderClient(<App />);
    await user.click(await screen.findByRole('button', { name: 'Explore Greater Toronto Area on map' }));
    await screen.findByRole('heading', { level: 1, name: 'Greater Toronto Area' });
    expect(window.location.search).toBe(`?geo=${GTA_ID}`);
    expect(await screen.findByRole('navigation', { name: 'Child areas' })).toBeTruthy();
    await user.click(await screen.findByRole('button', { name: 'Explore Toronto on map' }));
    await screen.findByRole('heading', { level: 1, name: 'Toronto' });
    expect(window.location.search).toBe('?geo=g30_city');
    expect(screen.queryByRole('region', { name: /market preview$/ })).toBeNull();
  });
});
