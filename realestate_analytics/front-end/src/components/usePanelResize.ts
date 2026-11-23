import { useEffect, useRef, useState } from 'react';
import type { KeyboardEvent, PointerEvent } from 'react';

function clampHeight(height: number, min: number, max: number) {
  return Math.min(max, Math.max(min, Math.round(height)));
}

export default function usePanelResize<T extends HTMLElement>({ id, storageKey, min, max }: {
  id: string; storageKey: string; min: number; max: number;
}) {
  const panel = useRef<T>(null);
  const drag = useRef<{ id: number; y: number; initial: number; height: number } | null>(null);
  const [height, setHeight] = useState<number | null>(() => {
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored === null || !stored.trim()) return null;
      const value = Number(stored);
      return Number.isFinite(value) && value >= 0 ? clampHeight(value, min, max) : null;
    } catch {
      return null;
    }
  });
  const [dragHeight, setDragHeight] = useState<number | null>(null);
  const [defaultHeight, setDefaultHeight] = useState(min);
  const usingDefault = height === null;

  useEffect(() => {
    const node = panel.current;
    if (!usingDefault || !node) return;
    const measure = () => setDefaultHeight(clampHeight(node.offsetHeight, min, max));
    const observer = new ResizeObserver(measure);
    measure();
    observer.observe(node);
    return () => observer.disconnect();
  }, [usingDefault, min, max]);

  function saveHeight(value: number) {
    setHeight(value);
    try { localStorage.setItem(storageKey, String(value)); }
    catch { /* Resizing still works when browser storage is unavailable. */ }
  }

  function startResize(event: PointerEvent<HTMLDivElement>) {
    if (!panel.current || drag.current || event.button !== 0 || !event.isPrimary) return;
    event.preventDefault();
    event.currentTarget.focus({ preventScroll: true });
    event.currentTarget.setPointerCapture(event.pointerId);
    const initial = panel.current.offsetHeight;
    drag.current = { id: event.pointerId, y: event.clientY, initial, height: initial };
  }

  function moveResize(event: PointerEvent<HTMLDivElement>) {
    const start = drag.current;
    if (!start || start.id !== event.pointerId) return;
    start.height = clampHeight(start.initial + event.clientY - start.y, min, max);
    setDragHeight(start.height);
  }

  function finishResize(event: PointerEvent<HTMLDivElement>) {
    const start = drag.current;
    if (!start || start.id !== event.pointerId) return;
    drag.current = null;
    setDragHeight(null);
    if (start.height !== start.initial) saveHeight(start.height);
  }

  function resizeWithKeyboard(event: KeyboardEvent<HTMLDivElement>) {
    if (drag.current) return;
    const current = panel.current?.offsetHeight ?? height ?? defaultHeight;
    const step = event.shiftKey ? 80 : 24;
    const sizes: Record<string, number> = {
      ArrowUp: current - step, ArrowDown: current + step, Home: min, End: max,
    };
    if (!Object.hasOwn(sizes, event.key)) return;
    event.preventDefault();
    saveHeight(clampHeight(sizes[event.key], min, max));
  }

  const resizedHeight = dragHeight ?? height ?? undefined;
  return {
    panel, height: resizedHeight,
    handle: {
      role: 'separator', tabIndex: 0, 'aria-orientation': 'horizontal' as const,
      'aria-controls': id, 'aria-valuemin': min, 'aria-valuemax': max,
      'aria-valuenow': resizedHeight ?? defaultHeight,
      onPointerDown: startResize, onPointerMove: moveResize, onPointerUp: finishResize,
      onPointerCancel: finishResize, onLostPointerCapture: finishResize, onKeyDown: resizeWithKeyboard,
    },
  };
}
