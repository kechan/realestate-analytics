import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { CountryNavigation, CountryOverview } from './CountryBrowser';
import CanadaMap from './CanadaMap';
import { CANADA_SHAPES } from '../domain/canadaMap';
import { PROVINCES } from '../domain/geography';
import { geo, response } from '../test/fixtures';
import { renderClient } from '../test/helpers';
import type { GeoFeature } from '../types';

vi.mock('./ProvinceMap', () => ({ default: ({ children, onPreview, onNavigate }: {
  children: GeoFeature[]; onPreview: (g: GeoFeature, pin?: boolean) => void; onNavigate: (id: string) => void;
}) => <div data-testid="province-map">{children.map(g => <button key={g.properties.geog_id}
  onMouseEnter={() => onPreview(g)} onClick={() => onNavigate(g.properties.geog_id)}>Explore {g.properties.name} on map</button>)}</div> }));
const collection = (features: GeoFeature[] = [], total = features.length, offset = 0) => response({ type: 'FeatureCollection', features, total, limit: 30, offset });

describe('Canada browsing', () => {
  it('includes all provinces and territories and supports keyboard selection', () => {
    expect(CANADA_SHAPES.map(shape => shape.code).sort()).toEqual(Object.keys(PROVINCES).sort());
    expect(CANADA_SHAPES.every(shape => shape.path.startsWith('M') && !shape.path.includes('NaN'))).toBe(true);
    const onSelect = vi.fn(); renderClient(<CanadaMap onSelect={onSelect} />);
    fireEvent.keyDown(screen.getByRole('button', { name: 'Browse Nunavut' }), { key: 'Enter' });
    expect(onSelect).toHaveBeenCalledWith('NU');
  });
  it('does not fetch a directory or show search results before someone searches', async () => {
    const fetch = vi.fn(async (_url: string) => collection()); vi.stubGlobal('fetch', fetch);
    renderClient(<CountryOverview province="ON" onProvince={vi.fn()} onNavigate={vi.fn()} />);
    await screen.findByTestId('province-map');
    await waitFor(() => expect(fetch).toHaveBeenCalled());
    expect(fetch.mock.calls.every(([url]) => String(url).includes('/provinces/ON/children'))).toBe(true);
    expect(screen.queryByRole('button', { name: 'Next' })).toBeNull();
    expect(screen.queryByText('No matching locations')).toBeNull();
  });
  it('paginates search results and resets pagination for a new term', async () => {
    const fetch = vi.fn(async (url: string) => {
      const u = new URL(url, 'http://test');
      if (u.pathname.includes('/provinces/')) return collection();
      const offset = Number(u.searchParams.get('offset'));
      const term = u.searchParams.get('query');
      const features = term === 'Selected' ? [geo('g30_found', 'Selected town')]
        : Array.from({ length: offset ? 2 : 30 }, (_, i) => geo(`g30_${offset + i}`, `Town ${offset + i}`));
      return collection(features, term === 'Selected' ? 1 : 32, offset);
    });
    vi.stubGlobal('fetch', fetch); const onNavigate = vi.fn(); const user = userEvent.setup();
    renderClient(<CountryOverview province="ON" onProvince={vi.fn()} onNavigate={onNavigate} />);
    const input = screen.getByRole('searchbox', { name: 'Search Ontario locations' });
    await user.type(input, 'Town');
    await screen.findByText('1–30 of 32 locations');
    await user.click(screen.getByRole('button', { name: 'Next' }));
    await screen.findByText('31–32 of 32 locations');
    await user.clear(input); await user.type(input, 'Selected');
    const town = await screen.findByRole('button', { name: /^Selected town\b/ });
    expect(screen.getByText('1–1 of 1 location')).toBeTruthy();
    await user.click(town); expect(onNavigate).toHaveBeenCalledWith('g30_found');
    expect(fetch.mock.calls.some(([url]) => String(url).includes('offset=0&query=Selected'))).toBe(true);
  });
  it('finds Estrie at any level and hides results immediately when search is cleared', async () => {
    const fetch = vi.fn(async (url: string) => {
      const u = new URL(url, 'http://test');
      if (u.pathname.includes('/provinces/')) return collection();
      const features = u.searchParams.get('query') === 'Estrie' && !u.searchParams.has('level') ? [geo('g35_d19ad471', 'Estrie', 35, null)] : [];
      return collection(features);
    });
    vi.stubGlobal('fetch', fetch); const user = userEvent.setup(); const onNavigate = vi.fn();
    renderClient(<CountryOverview province="QC" onProvince={vi.fn()} onNavigate={onNavigate} />);
    const input = screen.getByRole('searchbox', { name: 'Search Quebec locations' });
    await user.type(input, 'Estrie');
    const result = await screen.findByRole('button', { name: /^Estrie\b/ });
    expect(within(result).getByText('Area')).toBeTruthy();
    await user.click(result); expect(onNavigate).toHaveBeenCalledWith('g35_d19ad471');
    const calls = fetch.mock.calls.length;
    await user.clear(input);
    expect(screen.queryByRole('button', { name: /^Estrie\b/ })).toBeNull();
    await new Promise(resolve => setTimeout(resolve, 400));
    expect(fetch.mock.calls.length).toBe(calls);
  });
  it('shares province children between the sliding sidebar and map, including mixed geography levels', async () => {
    const estrie = geo('g35_d19ad471', 'Estrie', 35, null);
    const fetch = vi.fn(async (url: string) => url.includes('/geos/boundaries')
      ? response({ type: 'FeatureCollection', features: [estrie], geometry_tolerance: 0.0025 }) : collection([estrie])); vi.stubGlobal('fetch', fetch);
    const onNavigate = vi.fn(), onProvince = vi.fn(), onPreview = vi.fn(); const user = userEvent.setup();
    renderClient(<><CountryNavigation province="QC" onProvince={onProvince} onNavigate={onNavigate} onClose={vi.fn()} />
      <CountryOverview province="QC" onProvince={onProvince} onNavigate={onNavigate} onPreview={onPreview} /></>);
    const row = await screen.findByRole('button', { name: /^Estrie\b/ });
    expect(screen.queryByRole('navigation', { name: 'Provinces and territories' })).toBeNull();
    const boundary = await screen.findByRole('button', { name: 'Explore Estrie on map' });
    await user.hover(boundary);
    expect(onPreview).toHaveBeenCalledWith(estrie);
    await user.click(boundary); expect(onNavigate).toHaveBeenCalledWith(estrie.properties.geog_id);
    onNavigate.mockClear();
    await user.click(row); expect(onNavigate).toHaveBeenCalledWith(estrie.properties.geog_id);
    await user.click(screen.getAllByRole('button', { name: 'Canada' })[0]); expect(onProvince).toHaveBeenCalledWith();
    expect(fetch.mock.calls.filter(([url]) => String(url).includes('include_geometry=false'))).toHaveLength(1);
    expect(fetch.mock.calls.filter(([url]) => String(url).includes('include_geometry=true'))).toHaveLength(0);
    await waitFor(() => expect(fetch.mock.calls.filter(([url]) => String(url).includes('/geos/boundaries'))).toHaveLength(1));
  });
  it('keeps navigation and all-level search usable when child boundaries fail', async () => {
    vi.stubGlobal('fetch', vi.fn(async (url: string) => String(url).includes('/provinces/') ? response({}, 503) : collection()));
    const onProvince = vi.fn(); const user = userEvent.setup();
    renderClient(<CountryOverview province="BC" onProvince={onProvince} onNavigate={vi.fn()} />);
    await screen.findByText('Child areas could not be loaded.');
    await user.type(screen.getByRole('searchbox'), 'Vancouver');
    await screen.findByText('No matching locations');
    await user.click(within(screen.getByRole('navigation', { name: 'Browse path' })).getByRole('button', { name: 'Canada' }));
    expect(onProvince).toHaveBeenCalledWith();
  });
});
