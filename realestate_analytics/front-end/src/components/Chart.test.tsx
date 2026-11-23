import { act, render, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import Chart from './Chart';

const engine = vi.hoisted(() => ({ setOption: vi.fn(), resize: vi.fn(), dispose: vi.fn(), init: vi.fn() }));
vi.mock('echarts/core', () => ({ init: engine.init, use: vi.fn() }));
vi.mock('echarts/charts', () => ({ BarChart: {}, LineChart: {} }));
vi.mock('echarts/components', () => ({ AriaComponent: {}, GridComponent: {}, TooltipComponent: {} }));
vi.mock('echarts/renderers', () => ({ SVGRenderer: {} }));

describe('chart lifecycle', () => {
  it('waits for a real layout size and disposes its instance and observer', async () => {
    let notify = () => {};
    const disconnect = vi.fn();
    vi.stubGlobal('ResizeObserver', class {
      constructor(callback: () => void) { notify = callback; }
      observe() {} disconnect = disconnect;
    });
    engine.init.mockReturnValue(engine);
    const width = vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(0);
    const height = vi.spyOn(HTMLElement.prototype, 'clientHeight', 'get').mockReturnValue(0);
    const view = render(<Chart metric="median_sold_price" points={[{ month: '2026-09', value: 815000 }]} />);
    expect(engine.init).not.toHaveBeenCalled();
    width.mockReturnValue(600); height.mockReturnValue(155);
    act(() => notify());
    await waitFor(() => expect(engine.setOption).toHaveBeenCalled());
    expect(engine.init).toHaveBeenCalledTimes(1);
    act(() => notify()); expect(engine.resize).toHaveBeenCalledTimes(1);
    view.unmount();
    expect(disconnect).toHaveBeenCalledTimes(1); expect(engine.dispose).toHaveBeenCalledTimes(1);
  });
  it('updates the existing chart when a metric changes', async () => {
    engine.init.mockReturnValue(engine);
    vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(600);
    vi.spyOn(HTMLElement.prototype, 'clientHeight', 'get').mockReturnValue(155);
    const view = render(<Chart metric="median_sold_price" points={[{ month: '2026-09', value: 815000 }]} />);
    await waitFor(() => expect(engine.setOption).toHaveBeenCalled());
    view.rerender(<Chart metric="sold_listing_count" points={[{ month: '2026-09', value: 0 }]} />);
    expect(engine.init).toHaveBeenCalledTimes(1);
    expect(engine.setOption.mock.lastCall?.[0].series[0]).toMatchObject({ type: 'bar', data: [0] });
  });
});
