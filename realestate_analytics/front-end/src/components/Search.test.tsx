import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import Search from './Search';
import { renderClient } from '../test/helpers';
import { geo, response } from '../test/fixtures';

describe('Canadian location search', () => {
  it('does not run a blind or one-character search', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); renderClient(<Search onSelect={() => {}} />);
    await user.type(screen.getByRole('combobox'), 'T');
    await new Promise(resolve => setTimeout(resolve, 330));
    expect(fetch).not.toHaveBeenCalled();
  });
  it('shows province and level and permits keyboard selection', async () => {
    const choose = vi.fn(); const vancouver = geo('g30_vancouver', 'Vancouver'); vancouver.properties.province = 'BC';
    vi.stubGlobal('fetch', vi.fn(async () => response({ type: 'FeatureCollection', features: [vancouver], total: 1, offset: 0, limit: 20 })));
    const user = userEvent.setup(); renderClient(<Search onSelect={choose} />);
    const input = screen.getByRole('combobox'); await user.type(input, 'Vancouver');
    await screen.findByText('Vancouver'); expect(screen.getByText('BC · City')).toBeTruthy();
    await user.keyboard('{Enter}'); expect(choose).toHaveBeenCalledWith('g30_vancouver');
    expect(screen.queryByRole('listbox')).toBeNull();
  });
  it('does not let a slow previous search overwrite newer results', async () => {
    let finish: (value: Response) => void = () => {};
    const fetch = vi.fn((url: string) => new URL(url, 'http://test').searchParams.get('query') === 'Tor'
      ? new Promise<Response>(resolve => { finish = resolve; })
      : Promise.resolve(response({ type: 'FeatureCollection', features: [geo('g30_vancouver', 'Vancouver')], total: 1, offset: 0, limit: 20 })));
    vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); renderClient(<Search onSelect={() => {}} />);
    const input = screen.getByRole('combobox'); await user.type(input, 'Tor');
    await waitFor(() => expect(fetch).toHaveBeenCalled());
    await user.clear(input); await user.type(input, 'Vancouver');
    await screen.findByText('Vancouver');
    await act(async () => finish(response({ type: 'FeatureCollection', features: [geo()], total: 1, offset: 0, limit: 20 })));
    expect(screen.queryByText('Toronto')).toBeNull(); expect(screen.getByText('Vancouver')).toBeTruthy();
  });
  it('requests another explicit page only when More results is used', async () => {
    const fetch = vi.fn(async (url: string) => {
      const offset = Number(new URL(url, 'http://test').searchParams.get('offset'));
      return response({ type: 'FeatureCollection', features: [geo(`g30_${offset}`, offset ? 'Second place' : 'First place')], total: 2, offset, limit: 20 });
    });
    vi.stubGlobal('fetch', fetch); const user = userEvent.setup(); renderClient(<Search onSelect={() => {}} />);
    await user.type(screen.getByRole('combobox'), 'Place'); await screen.findByText('First place');
    await user.click(screen.getByRole('button', { name: 'More results' })); await screen.findByText('Second place');
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(new URL(fetch.mock.calls[1][0], 'http://test').searchParams.get('offset')).toBe('1');
  });
});
