import { act, fireEvent, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import Preview from './Preview';
import { renderClient } from '../test/helpers';
import { geo, market, response } from '../test/fixtures';

vi.mock('./ChartView', () => ({ default: () => <div>History chart</div> }));

async function setup() {
  const fetch = vi.fn(async () => response(market()));
  vi.stubGlobal('fetch', fetch);
  let resize: ResizeObserverCallback = () => {};
  const disconnect = vi.fn();
  vi.stubGlobal('ResizeObserver', class {
    constructor(callback: ResizeObserverCallback) { resize = callback; }
    observe() {} unobserve() {} disconnect = disconnect;
  });
  const onPropertyType = vi.fn(); const onClose = vi.fn(); const onInteract = vi.fn();
  const rendered = renderClient(<div data-testid="map-frame"><Preview geo={geo()} propertyType="ALL" initialMetric="median_sold_price"
    onPropertyType={onPropertyType} onClose={onClose} onInteract={onInteract} onNavigate={vi.fn()} onKeep={vi.fn()} onLeave={vi.fn()} /></div>);
  await screen.findByText('$815,000');
  const panel = screen.getByRole('region', { name: 'Toronto market preview' });
  const handle = screen.getByRole('button', { name: 'Move market preview' });
  const size = { width: 800, height: 600, panelHeight: 240 };
  Object.defineProperties(screen.getByTestId('map-frame'), {
    clientWidth: { get: () => size.width }, clientHeight: { get: () => size.height },
  });
  Object.defineProperties(panel, {
    offsetLeft: { get: () => panel.style.left ? parseFloat(panel.style.left) : 480 },
    offsetTop: { get: () => panel.style.top ? parseFloat(panel.style.top) : 320 },
    offsetWidth: { get: () => 300 }, offsetHeight: { get: () => size.panelHeight },
  });
  const captured = new Set<number>();
  Object.assign(handle, {
    setPointerCapture: vi.fn((id: number) => captured.add(id)),
    hasPointerCapture: (id: number) => captured.has(id),
    releasePointerCapture: vi.fn((id: number) => captured.delete(id)),
  });
  const pointer = (clientX: number, clientY: number) => ({ pointerId: 7, button: 0, isPrimary: true, clientX, clientY });
  return { ...rendered, panel, handle, pointer, size, fetch, onClose, onInteract, onPropertyType, disconnect,
    resize: () => act(() => resize([], {} as ResizeObserver)) };
}

describe('draggable market preview', () => {
  it.each(['mouse', 'touch'])('moves by the title, captures the %s pointer and stops on release without fetching again', async pointerType => {
    const { panel, handle, pointer, fetch, onInteract } = await setup();
    fireEvent.pointerDown(handle, { ...pointer(500, 350), pointerType });
    fireEvent.pointerMove(handle, { ...pointer(200, 200), pointerType });
    expect(panel.style.left).toBe('180px'); expect(panel.style.top).toBe('170px');
    expect(handle.setPointerCapture).toHaveBeenCalledWith(7);
    expect(document.activeElement).toBe(handle); expect(onInteract).toHaveBeenCalled();
    fireEvent.pointerUp(handle, pointer(200, 200));
    fireEvent.pointerMove(handle, pointer(300, 300));
    expect(panel.style.left).toBe('180px');
    expect(handle.releasePointerCapture).toHaveBeenCalledWith(7);
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('keeps the whole preview inside the map during drag, resize and content growth', async () => {
    const { panel, handle, pointer, size, resize, unmount, disconnect } = await setup();
    fireEvent.pointerDown(handle, pointer(500, 350));
    fireEvent.pointerMove(handle, pointer(-1000, -1000));
    expect(panel.style.left).toBe('8px'); expect(panel.style.top).toBe('8px');
    fireEvent.pointerMove(handle, pointer(2000, 2000));
    expect(panel.style.left).toBe('492px'); expect(panel.style.top).toBe('352px');
    fireEvent.pointerCancel(handle, pointer(2000, 2000));
    size.width = 400; size.height = 300; resize();
    expect(panel.style.left).toBe('92px'); expect(panel.style.top).toBe('52px');
    size.panelHeight = 290; resize();
    expect(panel.style.top).toBe('5px');
    fireEvent.pointerMove(handle, pointer(0, 0));
    expect(panel.style.top).toBe('5px');
    unmount(); expect(disconnect).toHaveBeenCalledOnce();
  });
  it('supports keyboard movement while dropdowns and close remain independent', async () => {
    const { panel, handle, pointer, onPropertyType, onClose } = await setup();
    fireEvent.keyDown(handle, { key: 'ArrowLeft' });
    fireEvent.keyDown(handle, { key: 'ArrowUp', shiftKey: true });
    expect(panel.style.left).toBe('470px'); expect(panel.style.top).toBe('280px');
    const select = screen.getByRole('combobox', { name: 'Preview property type' });
    fireEvent.pointerDown(select, pointer(0, 0)); fireEvent.pointerMove(select, pointer(100, 100));
    fireEvent.change(select, { target: { value: 'DETACHED' } });
    expect(onPropertyType).toHaveBeenCalledWith('DETACHED');
    expect(panel.style.left).toBe('470px');
    fireEvent.click(screen.getByRole('button', { name: 'Close market preview' }));
    expect(onClose).toHaveBeenCalledOnce();
  });
});
