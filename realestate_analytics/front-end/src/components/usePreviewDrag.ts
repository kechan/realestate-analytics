import { useCallback, useEffect, useRef } from 'react';
import type { KeyboardEvent, PointerEvent } from 'react';

export default function usePreviewDrag() {
  const panel = useRef<HTMLElement>(null);
  const position = useRef<{ x: number; y: number } | null>(null);
  const drag = useRef<{ id: number; x: number; y: number; left: number; top: number } | null>(null);
  const place = useCallback((x: number, y: number) => {
    const node = panel.current;
    const map = node?.parentElement;
    if (!node || !map) return;
    const maxX = Math.max(0, map.clientWidth - node.offsetWidth);
    const maxY = Math.max(0, map.clientHeight - node.offsetHeight);
    const insetX = Math.min(8, maxX / 2);
    const insetY = Math.min(8, maxY / 2);
    const left = Math.min(Math.max(x, insetX), maxX - insetX);
    const top = Math.min(Math.max(y, insetY), maxY - insetY);
    position.current = { x: left, y: top };
    // Pointer movement must not rerender the chart or refetch any market data.
    Object.assign(node.style, { left: `${left}px`, top: `${top}px`, right: 'auto', bottom: 'auto' });
  }, []);

  useEffect(() => {
    const node = panel.current;
    if (!node?.parentElement) return;
    const observer = new ResizeObserver(() => {
      if (position.current) place(position.current.x, position.current.y);
    });
    observer.observe(node);
    observer.observe(node.parentElement);
    return () => observer.disconnect();
  }, [place]);

  const onPointerDown = (event: PointerEvent<HTMLButtonElement>) => {
    const node = panel.current;
    if (!node || event.button !== 0 || event.isPrimary === false) return;
    event.preventDefault(); event.stopPropagation();
    event.currentTarget.focus({ preventScroll: true });
    event.currentTarget.setPointerCapture(event.pointerId);
    drag.current = { id: event.pointerId, x: event.clientX, y: event.clientY, left: node.offsetLeft, top: node.offsetTop };
  };
  const onPointerMove = (event: PointerEvent<HTMLButtonElement>) => {
    const start = drag.current;
    if (!start || start.id !== event.pointerId) return;
    event.stopPropagation();
    place(start.left + event.clientX - start.x, start.top + event.clientY - start.y);
  };
  const stop = (event: PointerEvent<HTMLButtonElement>) => {
    if (drag.current?.id !== event.pointerId) return;
    drag.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
  };
  const onKeyDown = (event: KeyboardEvent<HTMLButtonElement>) => {
    const delta: Record<string, [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    const direction = delta[event.key];
    const node = panel.current;
    if (!direction || !node) return;
    event.preventDefault(); event.stopPropagation();
    const step = event.shiftKey ? 40 : 10;
    place(node.offsetLeft + direction[0] * step, node.offsetTop + direction[1] * step);
  };
  return { panel, handle: { onPointerDown, onPointerMove, onPointerUp: stop, onPointerCancel: stop,
    onLostPointerCapture: () => { drag.current = null; }, onKeyDown } };
}
