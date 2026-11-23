import { lazy, Suspense, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ArrowRight, ChevronLeft, ChevronRight, Compass, X } from 'lucide-react';
import { api, ApiError } from '../api';
import { joinNavigationBoundaries, useProvinceBoundaries, useProvinceChildren } from '../queries';
import { GTA_ID, PROVINCES, hasGeometry, levelLabel, nameOf } from '../domain/geography';
import type { GeoFeature } from '../types';
import { IconButton, Loading, StateMessage } from './Controls';
import Navigation from './Navigation';
import VisualBoundary from './VisualBoundary';
import shared from '../styles.module.css';
import styles from './CountryBrowser.module.css';

const CanadaMap = lazy(() => import('./CanadaMap'));
const ProvinceMap = lazy(() => import('./ProvinceMap'));
const provinces = Object.entries(PROVINCES).sort((a, b) => a[1].localeCompare(b[1]));
const noop = () => {};
interface BrowseProps { province?: string; onProvince: (province?: string) => void; onNavigate: (id: string) => void }
interface PreviewProps {
  previewId?: string; previewPanel?: ReactNode;
  onPreview?: (geo: GeoFeature, pin?: boolean) => void; onLeave?: () => void;
  onKeep?: () => void; onMapMove?: () => void;
}

export function CountryNavigation({ province, onProvince, onNavigate, onClose, previewId, onPreview = noop, onLeave = noop }:
  BrowseProps & PreviewProps & { onClose: () => void }) {
  const children = useProvinceChildren(province);
  if (province) return <Navigation home={false} name={PROVINCES[province]} children={children.data?.features || []}
    ancestors={[]} onProvince={() => onProvince()} loading={children.isPending} error={children.error}
    previewId={previewId} onNavigate={onNavigate} onPreview={onPreview} onLeave={onLeave}
    onRetry={() => void children.refetch()} onClose={onClose} />;
  return <>
    <div className={shared.navTop}>
      <button className={shared.parentLink} type="button" disabled><ChevronLeft size={17} />Canada</button>
      <IconButton label="Close areas" className={shared.mobileOnly} onClick={onClose}><X size={20} /></IconButton>
    </div>
    <div className={shared.navHeading}><h2>Explore Canada</h2><p>Provinces & territories</p></div>
    <nav className={shared.areaList} aria-label="Provinces and territories">{provinces.map(([code, name]) =>
      <button key={code} type="button" className={shared.areaRow} onClick={() => onProvince(code)}>
        <span>{name}</span><ChevronRight size={16} aria-hidden="true" />
      </button>)}</nav>
    <div className={shared.navFooter}><Compass size={15} /><span>Find a market anywhere in Canada</span></div>
  </>;
}

function ProvinceSearch({ province, onNavigate }: { province: string; onNavigate: (id: string) => void }) {
  const [input, setInput] = useState(''); const [term, setTerm] = useState(''); const [offset, setOffset] = useState(0);
  useEffect(() => { const timer = setTimeout(() => { setTerm(input.trim()); setOffset(0); }, 300); return () => clearTimeout(timer); }, [input]);
  const result = useQuery({ queryKey: ['geography', 'markets', province, 'all-areas', term, offset],
    queryFn: ({ signal }) => api.provinceMarkets(province, term, offset, signal), enabled: Boolean(term) });
  const settling = input.trim() !== term;
  const data = settling ? undefined : result.data;
  return <section className={styles.markets} aria-label={`Search ${PROVINCES[province]}`}>
    <div className={styles.marketHeading}><div><h2>Find a location in {PROVINCES[province]}</h2><p>Search every level: regions, cities, districts and neighbourhoods.</p></div>
      <input type="search" aria-label={`Search ${PROVINCES[province]} locations`} placeholder="Search by location name…" value={input} onChange={event => setInput(event.target.value)} />
    </div>
    {!input.trim() ? null : settling || result.isPending ? <Loading label="Searching locations…" /> : result.isError
      ? <StateMessage error title="Could not search locations" description={result.error.message} onRetry={() => void result.refetch()} />
      : !data?.features.length ? <StateMessage title="No matching locations" description="Try a different name, or use the Canada-wide search above." />
        : <><div className={styles.marketGrid}>{data.features.map(feature => <button type="button" key={feature.properties.geog_id} onClick={() => onNavigate(feature.properties.geog_id)}>
          <span className={styles.marketName}><span>{nameOf(feature)}</span><small>{levelLabel(feature.properties.level)}</small></span><ArrowRight size={16} aria-hidden="true" />
        </button>)}</div>
          <div className={styles.pagination}><p>{offset + 1}–{offset + data.features.length} of {data.total} {data.total === 1 ? 'location' : 'locations'}</p>
            <div><button type="button" disabled={offset === 0} onClick={() => setOffset(value => Math.max(0, value - 30))}><ChevronLeft size={15} />Previous</button>
              <button type="button" disabled={offset + data.features.length >= data.total} onClick={() => setOffset(value => value + 30)}>Next<ChevronRight size={15} /></button></div>
          </div></>}
  </section>;
}

export function CountryOverview({ province, onProvince, onNavigate, previewId, previewPanel, onPreview = noop, onLeave = noop, onKeep = noop, onMapMove = noop }:
  BrowseProps & PreviewProps) {
  const children = useProvinceChildren(province);
  const outlines = useProvinceBoundaries(province, children.data);
  const areas = useMemo(() => joinNavigationBoundaries(children.data?.features || [], outlines.data), [children.data, outlines.data]);
  const missing = outlines.isSuccess ? areas.filter(geo => !hasGeometry(geo)).length : 0;
  const error = children.error || outlines.error;
  return <>
    <section className={`${shared.locationHeading} ${styles.heading}`} aria-labelledby="location-title">
      {province && <nav className={shared.breadcrumbs} aria-label="Browse path"><button type="button" onClick={() => onProvince()}>Canada</button><ChevronRight size={13} /><span aria-current="location">{PROVINCES[province]}</span></nav>}
      <div className={styles.eyebrow}>MARKET EXPLORER</div><h1 id="location-title">{province ? PROVINCES[province] : 'Explore Canada'}</h1>
      <p>{province ? 'Explore an area on the map or in the Areas list. Search below for any location.' : 'Choose a province or territory, or search for a location above.'}</p>
    </section>
    <section className={`${styles.mapPanel} ${province ? styles.provinceMap : ''}`} aria-label={province ? `${PROVINCES[province]} overview` : 'Canada overview'}
      onMouseEnter={onKeep} onMouseLeave={onLeave} onPointerMove={onMapMove}>
      <VisualBoundary name="map" resetKey={province || 'Canada'}><Suspense fallback={<Loading label="Loading map…" />}>
        {province ? <ProvinceMap province={province} children={areas} previewId={previewId}
          loading={!error && (children.isPending || outlines.isFetching || Boolean(children.data?.features.length && outlines.isPending))}
          revision={String(outlines.dataUpdatedAt)} onPreview={onPreview} onLeave={onLeave} onNavigate={onNavigate} /> : <CanadaMap onSelect={onProvince} />}
      </Suspense></VisualBoundary>
      {province && error && <div className={shared.mapProgress} role="status">
        {error instanceof ApiError && error.status === 404 ? 'Restart FastAPI to enable province drill-down.' : 'Child areas could not be loaded.'}
        <button type="button" className={shared.textButton} onClick={() => { void children.refetch(); if (children.data?.features.length) void outlines.refetch(); }}>Retry</button>
      </div>}
      {province && missing > 0 && <div className={shared.mapProgress}>{missing} {missing === 1 ? 'area has' : 'areas have'} no published boundary; use the Areas list.</div>}
      {province && children.isSuccess && !children.data.features.length && <div className={shared.mapProgress}>No child areas are published. Search for a location below.</div>}
      {previewPanel}
    </section>
    {province ? <ProvinceSearch key={province} province={province} onNavigate={onNavigate} />
      : <section className={styles.quickStart} aria-label="Quick access"><div><h2>Looking for the GTA?</h2><p>Go straight to the regional map and city drill-down.</p></div>
        <button className={shared.primaryButton} type="button" onClick={() => onNavigate(GTA_ID)}>Explore Greater Toronto<ArrowRight size={16} /></button></section>}
  </>;
}
