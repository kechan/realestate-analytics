import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render } from '@testing-library/react';
import type { ReactElement, ReactNode } from 'react';
import { afterEach } from 'vitest';

const clients: QueryClient[] = [];
afterEach(() => { clients.splice(0).forEach(client => client.clear()); });
export function renderClient(element: ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false, staleTime: 60_000, gcTime: 0 } } });
  clients.push(client);
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  return { ...render(element, { wrapper }), client };
}
