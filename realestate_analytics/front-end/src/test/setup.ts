import { afterEach, vi } from 'vitest';
import { cleanup } from '@testing-library/react';

Object.defineProperty(window, 'matchMedia', { writable: true, value: (query: string) => ({
  matches: query.includes('min-width') || query.includes('hover: hover'), media: query, onchange: null,
  addListener: () => {}, removeListener: () => {}, addEventListener: () => {}, removeEventListener: () => {}, dispatchEvent: () => true,
}) });
globalThis.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
window.scrollTo = () => {};
HTMLElement.prototype.scrollTo = () => {};
HTMLElement.prototype.scrollIntoView = () => {};

afterEach(() => {
  cleanup(); vi.unstubAllGlobals(); vi.useRealTimers();
  window.history.replaceState(null, '', '/');
});
