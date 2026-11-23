import { act, fireEvent, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { geo } from '../test/fixtures';
import { BoundaryLayer, BoundaryLoadingIndicator, Viewport } from './AreaMap';

const map = vi.hoisted(() => ({ fitBounds: vi.fn(), invalidateSize: vi.fn(), getContainer: vi.fn() }));
vi.mock('react-leaflet', () => ({ useMap: () => map, AttributionControl: () => null,
  Marker: ({ icon, position }: { icon: { options: { html: string } }; position: { lat: number; lng: number } }) =>
    <div data-testid="loading-marker" data-lng={position.lng} dangerouslySetInnerHTML={{ __html: icon.options.html }} />,
  GeoJSON: ({ children, eventHandlers }: { children: ReactNode; eventHandlers: Record<string, () => void> }) =>
    <button onMouseOver={eventHandlers.mouseover} onMouseOut={eventHandlers.mouseout} onClick={eventHandlers.click}>{children}</button>,
  Tooltip: ({ children }: { children: ReactNode }) => <>{children}</>,
  MapContainer: () => null, ScaleControl: () => null, TileLayer: () => null }));

describe('child outline loading', () => {
  it('stays centered on the parent until the new children have painted', () => {
    let frames: FrameRequestCallback[] = [];
    vi.stubGlobal('requestAnimationFrame', (callback: FrameRequestCallback) => frames.push(callback));
    vi.stubGlobal('cancelAnimationFrame', vi.fn());
    const parent = geo();
    const empty: ReturnType<typeof geo>[] = [];
    const view = render(<BoundaryLoadingIndicator parent={parent} children={empty} loading />);
    expect(screen.getByRole('status').getAttribute('aria-label')).toBe('Loading child area outlines');
    expect(screen.getByTestId('loading-marker').getAttribute('data-lng')).toBe('-79.35');
    view.rerender(<BoundaryLoadingIndicator parent={parent} children={[geo('g20_child')]} loading={false} />);
    const paint = () => act(() => { const current = frames; frames = []; current.forEach(callback => callback(0)); });
    paint();
    expect(screen.queryByRole('status')).not.toBeNull();
    paint();
    expect(screen.queryByRole('status')).toBeNull();
  });

  it('does not spin for cached or empty completed results', () => {
    const children: ReturnType<typeof geo>[] = [];
    render(<BoundaryLoadingIndicator parent={geo()} children={children} loading={false} />);
    expect(screen.queryByRole('status')).toBeNull();
  });
});

describe('boundary interactions', () => {
  it('previews on hover but navigates to the exact geography on click', () => {
    const child = geo('g40_gta', 'Greater Toronto Area', 40);
    const onPreview = vi.fn(), onLeave = vi.fn(), onNavigate = vi.fn();
    render(<BoundaryLayer child={child} active={false} onPreview={onPreview} onLeave={onLeave} onNavigate={onNavigate} />);
    const boundary = screen.getByRole('button', { name: 'Greater Toronto Area' });
    fireEvent.mouseOver(boundary);
    expect(onPreview).toHaveBeenCalledExactlyOnceWith(child);
    fireEvent.mouseOut(boundary);
    expect(onLeave).toHaveBeenCalledTimes(1);
    fireEvent.click(boundary);
    expect(onNavigate).toHaveBeenCalledExactlyOnceWith('g40_gta');
    expect(onPreview).toHaveBeenCalledTimes(1);
  });
});

describe('map camera lifecycle', () => {
  beforeEach(() => {
    const container = document.createElement('div');
    Object.defineProperties(container, { clientWidth: { value: 900 }, clientHeight: { value: 320 } });
    map.getContainer.mockReturnValue(container);
  });

  it('uses current dimensions before fitting a newly selected location', () => {
    let height = 470;
    map.invalidateSize.mockImplementation(() => { height = 320; });
    map.fitBounds.mockImplementation(() => expect(height).toBe(320));
    const view = render(<Viewport id="g30_city" parent={geo()} children={[]} fit={0} />);
    expect(map.fitBounds).toHaveBeenCalledTimes(1);
    expect(map.fitBounds.mock.calls[0][0].getWest()).toBe(-79.6);
    // Background child data must not snap a user's camera back to the boundary.
    view.rerender(<Viewport id="g30_city" parent={geo()} children={[geo('g20_child')]} fit={0} />);
    expect(map.fitBounds).toHaveBeenCalledTimes(1);
  });
  it('refits for an explicit Fit action or a different location', () => {
    const view = render(<Viewport id="g30_city" parent={geo()} children={[]} fit={0} />);
    view.rerender(<Viewport id="g30_city" parent={geo()} children={[]} fit={1} />);
    expect(map.fitBounds).toHaveBeenCalledTimes(2);
    view.rerender(<Viewport id="g20_next" parent={geo('g20_next')} children={[]} fit={1} />);
    expect(map.fitBounds).toHaveBeenCalledTimes(3);
  });
});
