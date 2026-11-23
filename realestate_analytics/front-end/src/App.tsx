import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react';
import { useIsFetching, useQueryClient } from '@tanstack/react-query';
import { BookOpen, Check, ChevronRight, GripVertical, Link, Menu, RefreshCw } from 'lucide-react';
import { GTA_ID, GTA_NAME, hasGeometry, levelLabel, nameOf, PROVINCES } from './domain/geography';
import { useCatalog, useChildren, useDetail, useHierarchy, useMetrics } from './queries';
import { useViewState } from './state';
import type { GeoFeature, MetricName, PropertyType } from './types';
import { IconButton, Loading, PropertySelect, StateMessage } from './components/Controls';
import Navigation from './components/Navigation';
import Search from './components/Search';
import MarketPanel, { Summary } from './components/MarketPanel';
import MetricsGlossary from './components/MetricsGlossary';
import Preview from './components/Preview';
import Profile from './components/Profile';
import ResizableMapPanel from './components/ResizableMapPanel';
import useDialogFocus from './components/useDialogFocus';
import VisualBoundary from './components/VisualBoundary';
import { CountryNavigation, CountryOverview } from './components/CountryBrowser';
import styles from './styles.module.css';

const AreaMap = lazy(() => import('./components/AreaMap'));

export default function App() {
  const [view, update] = useViewState();
  const isGta = view.geo === GTA_ID;
  const browsing = !view.geo;
  const [drawer, setDrawer] = useState(false);
  const [sidebarWidth, setSidebarWidth] = useState(256);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const sidebarDrag = useRef<{ id: number; x: number; width: number } | null>(null);
  const [glossaryOpen, setGlossaryOpen] = useState(false);
  const [navDirection, setNavDirection] = useState<'forward' | 'back'>('forward');
  const [notice, setNotice] = useState('');
  const [copied, setCopied] = useState(false);
  const [preview, setPreview] = useState<{ geo: GeoFeature; pinned: boolean } | null>(null);
  const hoverTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const leaveTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const suppressMapHover = useRef(false);
  const sidebar = useRef<HTMLElement>(null);
  const content = useRef<HTMLElement>(null);
  const client = useQueryClient();
  const busy = useIsFetching({ predicate: q => q.queryKey[0] !== 'search' }) > 0;
  const detail = useDetail(view.geo);
  const hierarchy = useHierarchy(view.geo, !browsing);
  const children = useChildren(view.geo, false, !browsing);
  const polygons = useChildren(view.geo, true, !browsing && children.isSuccess && children.data.features.length > 0);
  const market = useMetrics(view.geo, view.propertyType, !browsing);
  useCatalog();
  const ancestors = hierarchy.data?.features || [];
  const hierarchyFeature = ancestors.find(g => g.properties.geog_id === view.geo);
  const feature = detail.data || hierarchyFeature;
  const name = browsing ? view.province ? PROVINCES[view.province] : 'Canada' : isGta ? GTA_NAME : feature ? nameOf(feature) : detail.isPending ? 'Loading location…' : 'Selected location';
  const province = feature?.properties.province || (isGta ? 'ON' : undefined);
  const regionChildren = polygons.data?.features || [];

  const closeDrawer = useCallback(() => setDrawer(false), []);
  const closeGlossary = useCallback(() => setGlossaryOpen(false), []);
  useDialogFocus(sidebar, drawer, closeDrawer);
  const clearTimers = useCallback(() => { clearTimeout(hoverTimer.current); clearTimeout(leaveTimer.current); }, []);
  const closePreview = useCallback(() => { clearTimers(); setPreview(null); }, [clearTimers]);
  const dismissPreview = useCallback(() => {
    // Removing a moved panel can expose a polygon under a stationary pointer.
    // Do not immediately reopen it from that synthetic hover transition.
    suppressMapHover.current = true;
    closePreview();
  }, [closePreview]);
  useEffect(() => () => clearTimers(), [clearTimers]);
  useEffect(() => {
    closePreview(); closeDrawer();
  }, [view.geo, view.province, closePreview, closeDrawer]);
  useEffect(() => {
    document.title = `${name} · Market Explorer`;
  }, [name]);
  useEffect(() => {
    const wide = window.matchMedia('(min-width: 1024px)');
    const change = () => { if (wide.matches) closeDrawer(); closePreview(); };
    wide.addEventListener('change', change);
    return () => wide.removeEventListener('change', change);
  }, [closeDrawer, closePreview]);
  useEffect(() => {
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') dismissPreview(); };
    document.addEventListener('keydown', escape);
    return () => document.removeEventListener('keydown', escape);
  }, [dismissPreview]);
  useEffect(() => {
    if (!notice) return;
    const timer = setTimeout(() => { setNotice(''); setCopied(false); }, 3500);
    return () => clearTimeout(timer);
  }, [notice]);

  const navigate = useCallback((id: string) => {
    const level = (gid: string) => Number(/^g(\d+)_/.exec(gid)?.[1] || 0);
    setNavDirection(!id || (view.geo && level(id) > level(view.geo)) ? 'back' : 'forward');
    closePreview(); closeDrawer(); update({ geo: id, province: undefined });
    content.current?.scrollTo({ top: 0 }); window.scrollTo({ top: 0 });
  }, [update, closePreview, closeDrawer, view.geo]);
  const browseProvince = useCallback((province?: string) => {
    setNavDirection(!province || view.geo ? 'back' : 'forward');
    closePreview(); closeDrawer(); update({ geo: '', province });
    content.current?.scrollTo({ top: 0 }); window.scrollTo({ top: 0 });
  }, [update, closePreview, closeDrawer, view.geo]);
  const showPreview = useCallback((geo: GeoFeature, pinned = false) => {
    clearTimers();
    if (!pinned && !window.matchMedia('(hover: hover) and (min-width: 1024px)').matches) return;
    if (pinned) { setPreview({ geo, pinned: true }); return; }
    hoverTimer.current = setTimeout(() => setPreview(current => current?.pinned ? current : { geo, pinned: false }), 250);
  }, [clearTimers]);
  const showMapPreview = useCallback((geo: GeoFeature, pinned = false) => {
    if (!pinned && suppressMapHover.current) return;
    showPreview(geo, pinned);
  }, [showPreview]);
  const leavePreview = useCallback(() => {
    clearTimers();
    leaveTimer.current = setTimeout(() => setPreview(current => current?.pinned ? current : null), 350);
  }, [clearTimers]);
  const keepPreview = useCallback(() => clearTimers(), [clearTimers]);
  const pinPreview = useCallback(() => {
    clearTimers();
    setPreview(current => current ? { ...current, pinned: true } : current);
  }, [clearTimers]);
  const setPropertyType = useCallback((propertyType: PropertyType) => update({ propertyType }), [update]);
  const setMetric = useCallback((metric: MetricName) => update({ metric }), [update]);
  const refresh = () => { void client.invalidateQueries({ predicate: query => query.queryKey[0] !== 'search', refetchType: 'active' }); };
  const copyLink = async () => {
    try { await navigator.clipboard.writeText(window.location.href); setCopied(true); setNotice('Link copied.'); }
    catch { setNotice('Could not copy the link. You can copy the address from your browser.'); }
  };
  const mapAvailable = Boolean((feature && hasGeometry(feature)) || regionChildren.some(hasGeometry));
  const boundaryLoading = detail.isPending || children.isPending || Boolean(children.data?.features.length && polygons.isPending);
  const noGeometryCount = polygons.isSuccess ? regionChildren.filter(g => !hasGeometry(g)).length : 0;
  const previewPanel = preview && <Preview key={preview.geo.properties.geog_id} geo={preview.geo} propertyType={view.propertyType} initialMetric={view.metric}
    onPropertyType={setPropertyType} onNavigate={navigate} onClose={dismissPreview} onKeep={keepPreview} onInteract={pinPreview} onLeave={leavePreview} />;

  return <div className={styles.app} style={{ gridTemplateColumns: `${sidebarCollapsed ? 0 : sidebarWidth}px 12px minmax(0, 1fr)` }}>
    <a className={styles.skipLink} href="#market-content">Skip to content</a>
    <header className={styles.header}>
      <button type="button" className={styles.areasButton} aria-controls="areas-navigation" aria-expanded={drawer} onClick={() => setDrawer(true)}><Menu size={20} />Areas</button>
      <button type="button" className={styles.brand} onClick={() => navigate('')}>Market Explorer</button>
      <nav className={styles.appNav} aria-label="Application"><a href="/" aria-current="page">Markets</a><a href="/ims-property-type-mapping">IMS property type mapping</a></nav>
      <Search onSelect={navigate} />
      <div className={styles.headerActions}>
        <button type="button" className={`${styles.gtaLink} ${isGta ? styles.gtaLinkActive : ''}`} aria-label="Explore GTA" aria-current={isGta ? 'location' : undefined} onClick={() => navigate(GTA_ID)}>GTA</button>
        <IconButton label="Refresh data" disabled={busy} onClick={refresh}><RefreshCw size={18} className={busy ? styles.spinning : undefined} /></IconButton>
        <IconButton label="Copy link to this view" onClick={() => void copyLink()}>{copied ? <Check size={18} /> : <Link size={18} />}</IconButton>
      </div>
    </header>
    {drawer && <div className={styles.drawerBackdrop} onClick={closeDrawer} aria-hidden="true" />}
    <aside ref={sidebar} id="areas-navigation" className={`${styles.sidebar} ${drawer ? styles.drawerOpen : ''} ${sidebarCollapsed ? styles.sidebarCollapsed : ''}`}
      role={drawer ? 'dialog' : undefined} aria-modal={drawer || undefined} aria-label="Areas">
      <div key={view.geo || view.province || 'Canada'} className={`${styles.navSlide} ${navDirection === 'back' ? styles.navSlideBack : ''}`}>
      {browsing ? <CountryNavigation province={view.province} onProvince={browseProvince} onNavigate={navigate} onClose={closeDrawer}
        previewId={preview?.geo.properties.geog_id} onPreview={showPreview} onLeave={leavePreview} /> : <Navigation home={false} name={name} children={children.data?.features || []} ancestors={ancestors}
        province={province} onProvince={browseProvince}
        loading={children.isPending} error={children.error} previewId={preview?.geo.properties.geog_id}
        onNavigate={navigate} onPreview={showPreview} onLeave={leavePreview} onRetry={() => void children.refetch()} onClose={closeDrawer} />}
      </div>
      <button type="button" className={styles.glossaryButton} aria-haspopup="dialog" onClick={() => setGlossaryOpen(true)}>
        <BookOpen size={16} aria-hidden="true" /><span>Metrics glossary</span><ChevronRight size={15} aria-hidden="true" />
      </button>
    </aside>
    <div className={styles.sidebarDivider}>
      <div className={styles.sidebarResizeHandle} role="separator" tabIndex={0} aria-label="Resize areas pane" aria-orientation="vertical"
        aria-controls="areas-navigation" aria-valuemin={0} aria-valuemax={480} aria-valuenow={sidebarCollapsed ? 0 : sidebarWidth}
        aria-valuetext={sidebarCollapsed ? 'Areas pane collapsed' : `${sidebarWidth} pixels`}
        title="Drag to resize; drag left to collapse"
        onPointerDown={event => {
          if (event.button !== 0 || !event.isPrimary) return;
          event.preventDefault(); event.currentTarget.focus(); event.currentTarget.setPointerCapture(event.pointerId);
          sidebarDrag.current = { id: event.pointerId, x: event.clientX, width: sidebarCollapsed ? 0 : sidebarWidth };
        }}
        onPointerMove={event => {
          const start = sidebarDrag.current;
          if (!start || start.id !== event.pointerId) return;
          const width = start.width + event.clientX - start.x;
          if (width < 120) setSidebarCollapsed(true);
          else { setSidebarCollapsed(false); setSidebarWidth(Math.min(480, Math.max(180, width))); }
        }}
        onPointerUp={event => { if (sidebarDrag.current?.id === event.pointerId) sidebarDrag.current = null; }}
        onPointerCancel={() => { sidebarDrag.current = null; }}
        onLostPointerCapture={() => { sidebarDrag.current = null; }}
        onKeyDown={event => {
          if (event.key === 'Home') { event.preventDefault(); setSidebarCollapsed(true); }
          else if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
            event.preventDefault();
            if (sidebarCollapsed) { if (event.key === 'ArrowRight') setSidebarCollapsed(false); }
            else { const width = sidebarWidth + (event.key === 'ArrowRight' ? 24 : -24); if (width < 180) setSidebarCollapsed(true); else setSidebarWidth(Math.min(480, width)); }
          }
        }}><GripVertical size={16} aria-hidden="true" /></div>
      {sidebarCollapsed && <button type="button" className={styles.sidebarRestore} aria-label="Show areas pane" onClick={() => setSidebarCollapsed(false)}><ChevronRight size={16} /></button>}
    </div>
    <main ref={content} id="market-content" className={styles.main} tabIndex={-1}>
      {browsing ? <CountryOverview province={view.province} onProvince={browseProvince} onNavigate={navigate}
        previewId={preview?.geo.properties.geog_id} previewPanel={previewPanel} onPreview={showMapPreview} onLeave={leavePreview} onKeep={keepPreview}
        onMapMove={() => { suppressMapHover.current = false; }} /> : <>
      <section className={styles.locationHeading} aria-labelledby="location-title">
        <nav className={styles.breadcrumbs} aria-label="Location path">
          <button type="button" onClick={() => browseProvince()}>Canada</button><ChevronRight size={13} aria-hidden="true" />
          {province && <><button type="button" onClick={() => browseProvince(province)}>{PROVINCES[province] || province}</button><ChevronRight size={13} aria-hidden="true" /></>}
          {ancestors.length ? ancestors.map((geo, index) => <span key={geo.properties.geog_id}>
            {index > 0 && <ChevronRight size={13} aria-hidden="true" />}
            {geo.properties.geog_id === view.geo ? <span aria-current="location">{nameOf(geo)}</span>
              : <button type="button" onClick={() => navigate(geo.properties.geog_id)}>{nameOf(geo)}</button>}
          </span>) : <span>{hierarchy.isPending ? 'Loading location path…' : name}</span>}
        </nav>
        <div className={styles.locationTitle}><div>
          <div className={styles.locationIdTrigger}>
            <h1 id="location-title" tabIndex={0} aria-describedby="location-ids">{name}</h1>
            <div id="location-ids" className={styles.locationIdTooltip} role="tooltip">
              <div>geog_id <code>{view.geo}</code></div>
              <div>longId <code>{hierarchyFeature?.properties.longId || (hierarchy.isPending ? 'Loading…' : 'Unavailable')}</code></div>
            </div>
          </div>
          <p>{[province ? PROVINCES[province] || province : null, feature ? levelLabel(feature.properties.level) : null].filter(Boolean).join(' · ')}</p></div>
          <PropertySelect value={view.propertyType} onChange={setPropertyType} />
        </div>
        <Summary data={market.data} loading={market.isPending} />
        {hierarchy.isError && <div className={styles.pathNote} role="status">Parent path unavailable. <button className={styles.textButton} onClick={() => void hierarchy.refetch()}>Retry</button></div>}
        {hierarchy.data && !hierarchy.data.complete && <div className={styles.pathNote}>The published parent path is incomplete.</div>}
      </section>
      <ResizableMapPanel aria-label={`${name} map`} onMouseEnter={keepPreview} onMouseLeave={leavePreview}
        onPointerMove={() => { suppressMapHover.current = false; }}>
        {!mapAvailable && boundaryLoading ? <Loading label="Loading area boundary…" />
          : !mapAvailable ? <StateMessage title="No boundary available" description="You can still explore areas and market history." onRetry={() => { void detail.refetch(); if (children.data?.features.length) void polygons.refetch(); }} />
            : <VisualBoundary name="map" resetKey={view.geo}><Suspense fallback={<Loading label="Loading map…" />}><AreaMap id={view.geo} home={false} parent={feature} children={regionChildren}
              loading={!children.isError && !polygons.isError && (children.isPending || polygons.isFetching || Boolean(children.data?.features.length && polygons.isPending))}
              previewId={preview?.geo.properties.geog_id} revision={`${detail.dataUpdatedAt}:${polygons.dataUpdatedAt}`} onPreview={showMapPreview} onLeave={leavePreview} onNavigate={navigate} /></Suspense></VisualBoundary>}
        {mapAvailable && (polygons.isError || detail.isError) && <div className={styles.mapProgress} role="status">
          {detail.isError ? 'Main boundary unavailable.' : 'Some outlines could not be loaded.'} <button className={styles.textButton} onClick={() => { void detail.refetch(); if (children.data?.features.length) void polygons.refetch(); }}>Retry</button>
        </div>}
        {noGeometryCount > 0 && <div className={styles.mapProgress}>{noGeometryCount} {noGeometryCount === 1 ? 'area has' : 'areas have'} no published boundary.</div>}
        {previewPanel}
      </ResizableMapPanel>
      <MarketPanel key={view.geo} id={view.geo} name={name} propertyType={view.propertyType} metric={view.metric} range={view.range} onMetric={setMetric} onRange={range => update({ range })} />
      <Profile geo={detail.data} listProvenance={children.data?.provenance} />
      </>}
    </main>
    {glossaryOpen && <MetricsGlossary onClose={closeGlossary} />}
    {notice && <div className={styles.toast} role="status">{notice}</div>}
  </div>;
}
