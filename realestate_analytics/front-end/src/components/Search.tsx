import { useInfiniteQuery } from '@tanstack/react-query';
import { Search as SearchIcon, X } from 'lucide-react';
import { useEffect, useId, useRef, useState } from 'react';
import { api } from '../api';
import { levelLabel, nameOf } from '../domain/geography';
import { IconButton } from './Controls';
import styles from '../styles.module.css';

export default function Search({ onSelect }: { onSelect: (id: string) => void }) {
  const [input, setInput] = useState('');
  const [term, setTerm] = useState('');
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const root = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const listId = useId();
  useEffect(() => { const timer = setTimeout(() => { setTerm(input.trim()); setActive(0); }, 300); return () => clearTimeout(timer); }, [input]);
  useEffect(() => {
    const outside = (e: PointerEvent) => { if (!root.current?.contains(e.target as Node)) setOpen(false); };
    document.addEventListener('pointerdown', outside);
    return () => document.removeEventListener('pointerdown', outside);
  }, []);
  const search = useInfiniteQuery({
    queryKey: ['search', term], initialPageParam: 0,
    queryFn: ({ signal, pageParam }) => api.search(term, pageParam, signal),
    getNextPageParam: last => last.features.length && last.offset + last.features.length < last.total ? last.offset + last.features.length : undefined,
    enabled: term.length >= 2 && open,
    staleTime: 60_000,
  });
  const settling = input.trim() !== term;
  const results = settling ? [] : search.data?.pages.flatMap(p => p.features) || [];
  const choose = (id: string) => { onSelect(id); setInput(''); setTerm(''); setOpen(false); inputRef.current?.blur(); };
  const showResults = open && input.trim().length >= 2;
  return <div className={styles.search} ref={root} onBlur={event => {
    if (!event.currentTarget.contains(event.relatedTarget as Node)) setOpen(false);
  }}>
    <SearchIcon className={styles.searchIcon} size={19} aria-hidden="true" />
    <input ref={inputRef} aria-label="Search Canadian locations" placeholder="Search Canadian locations" value={input}
      role="combobox" aria-autocomplete="list" aria-expanded={showResults} aria-controls={showResults ? listId : undefined}
      aria-activedescendant={showResults && results[active] ? `${listId}-${active}` : undefined}
      onFocus={() => setOpen(true)} onChange={e => { setInput(e.target.value); setOpen(true); }}
      onKeyDown={e => {
        if (e.key === 'Escape') { setOpen(false); e.stopPropagation(); }
        if (e.key === 'ArrowDown') { e.preventDefault(); setOpen(true); setActive(i => Math.min(i + 1, Math.max(0, results.length - 1))); }
        if (e.key === 'ArrowUp') { e.preventDefault(); setActive(i => Math.max(i - 1, 0)); }
        if (e.key === 'Enter' && open && results[active]) { e.preventDefault(); choose(results[active].properties.geog_id); }
      }} />
    {input && <IconButton className={styles.clearSearch} label="Clear search" onClick={() => { setInput(''); inputRef.current?.focus(); }}><X size={15} /></IconButton>}
    {showResults && <div className={styles.searchDropdown}>
      <div id={listId} role="listbox" aria-label="Locations">
        {results.map((geo, i) => <button type="button" role="option" tabIndex={-1} aria-selected={i === active} id={`${listId}-${i}`}
          className={`${styles.searchResult} ${i === active ? styles.searchActive : ''}`} key={geo.properties.geog_id}
          onPointerMove={() => setActive(i)} onClick={() => choose(geo.properties.geog_id)}>
          <strong>{nameOf(geo)}</strong><span>{[geo.properties.province, levelLabel(geo.properties.level)].filter(Boolean).join(' · ')}</span>
        </button>)}
      </div>
      {settling || search.isPending ? <p role="status" className={styles.searchNote}>Searching…</p>
        : search.isError ? <div className={styles.searchNote} role="alert">{search.error.message} <button className={styles.textButton} onClick={() => void search.refetch()}>Try again</button></div>
        : !results.length ? <p role="status" className={styles.searchNote}>No locations found. Try another name.</p> : null}
      {!settling && search.hasNextPage && <button className={styles.moreResults} type="button" disabled={search.isFetchingNextPage}
        onClick={() => void search.fetchNextPage()}>{search.isFetchingNextPage ? 'Loading…' : 'More results'}</button>}
    </div>}
  </div>;
}
