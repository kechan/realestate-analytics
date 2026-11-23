import { memo, useEffect, useMemo, useRef, useState } from 'react';
import { AttributionControl, GeoJSON, MapContainer, Marker, ScaleControl, TileLayer, Tooltip, useMap } from 'react-leaflet';
import { divIcon, geoJSON } from 'leaflet';
import type { Feature, FeatureCollection } from 'geojson';
import { Expand, RefreshCw } from 'lucide-react';
import { hasGeometry, nameOf } from '../domain/geography';
import type { GeoFeature, GeoGeometry } from '../types';
import 'leaflet/dist/leaflet.css';
import styles from '../styles.module.css';

const BOUNDARY_BLUE = '#214a9c';

export const BoundaryLayer = memo(function BoundaryLayer({ child, active, onPreview, onLeave, onNavigate }: {
  child: GeoFeature; active: boolean;
  onPreview: (geo: GeoFeature, pin?: boolean) => void; onLeave: () => void;
  onNavigate: (id: string) => void;
}) {
  return <GeoJSON data={child}
    style={{ color: BOUNDARY_BLUE, weight: active ? 2.5 : 1,
      fillColor: BOUNDARY_BLUE, fillOpacity: active ? 0.2 : 0.035 }}
    eventHandlers={{ mouseover: () => onPreview(child), mouseout: onLeave, click: () => onNavigate(child.properties.geog_id) }}>
    <Tooltip sticky>{nameOf(child)}</Tooltip>
  </GeoJSON>;
});

export function Viewport({ id, parent, children, fit }: { id: string; parent?: Feature<GeoGeometry | null>; children: GeoFeature[]; fit: number }) {
  const map = useMap();
  const lastFit = useRef('');
  useEffect(() => {
    const key = `${id}:${fit}`;
    if (lastFit.current === key) return;
    const features = parent && hasGeometry(parent) ? [parent] : children.filter(hasGeometry);
    if (!features.length) return;
    const collection: FeatureCollection<GeoGeometry | null> = { type: 'FeatureCollection', features };
    const bounds = geoJSON(collection).getBounds();
    if (!bounds.isValid()) return;
    const container = map.getContainer();
    const fitSelection = () => {
      const shortestSide = Math.min(container.clientWidth, container.clientHeight);
      if (!shortestSide) return false;
      // Home/detail and responsive layouts have different map heights. Refresh
      // Leaflet's cached size before computing this selection's bounds/center;
      // ResizeObserver may not have delivered the new dimensions yet.
      map.invalidateSize({ pan: false });
      const padding = Math.min(28, Math.floor((shortestSide - 1) / 2));
      map.fitBounds(bounds, { padding: [padding, padding], maxZoom: 15, animate: false });
      lastFit.current = key;
      return true;
    };
    if (fitSelection()) return;

    // A map restored in its collapsed state gets its first fit once reopened.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const observer = new ResizeObserver(() => {
      clearTimeout(timer);
      if (container.clientHeight) {
        timer = setTimeout(() => { if (fitSelection()) observer.disconnect(); }, 120);
      }
    });
    observer.observe(container);
    return () => { clearTimeout(timer); observer.disconnect(); };
  }, [id, parent, children, fit, map]);
  useEffect(() => {
    // Leaflet's default pan adjustment preserves the current geographic center
    // as the container resizes; it does not refit or reset the user's zoom.
    const observer = new ResizeObserver(() => map.invalidateSize());
    observer.observe(map.getContainer());
    return () => observer.disconnect();
  }, [map]);
  return null;
}

export function BoundaryLoadingIndicator({ parent, children, loading }: {
  parent?: Feature<GeoGeometry | null>; children: GeoFeature[]; loading: boolean;
}) {
  const [painted, setPainted] = useState(children);
  useEffect(() => {
    if (loading) return;
    // Leaflet attaches SVG paths during commit. Allow a browser paint before
    // removing the indicator, including when a large response arrives at once.
    let second = 0;
    const first = requestAnimationFrame(() => {
      second = requestAnimationFrame(() => setPainted(children));
    });
    return () => { cancelAnimationFrame(first); cancelAnimationFrame(second); };
  }, [children, loading]);
  const center = useMemo(() => {
    const features = parent && hasGeometry(parent) ? [parent] : children.filter(hasGeometry);
    if (!features.length) return undefined;
    const bounds = geoJSON({ type: 'FeatureCollection', features } as FeatureCollection).getBounds();
    return bounds.isValid() ? bounds.getCenter() : undefined;
  }, [parent, children]);
  const icon = useMemo(() => divIcon({ className: styles.boundaryLoadingMarker,
    iconSize: [44, 44], iconAnchor: [22, 22],
    html: `<div class="${styles.boundaryLoading}" role="status" aria-label="Loading child area outlines"><span class="${styles.spinner}" aria-hidden="true"></span></div>`,
  }), []);
  return center && (loading || painted !== children)
    ? <Marker position={center} icon={icon} interactive={false} keyboard={false} /> : null;
}

export default function AreaMap({ id, home, parent, children, loading = false, previewId, revision, onPreview, onLeave, onNavigate, parentAttribution }: {
  id: string; home: boolean; parent?: Feature<GeoGeometry | null>; children: GeoFeature[]; previewId?: string;
  parentAttribution?: string;
  loading?: boolean;
  revision: string; onPreview: (geo: GeoFeature, pin?: boolean) => void; onLeave: () => void;
  onNavigate: (id: string) => void;
}) {
  const [fit, setFit] = useState(0);
  const [tileError, setTileError] = useState(false);
  const [tileRevision, setTileRevision] = useState(0);
  const tileUrl = import.meta.env.VITE_MAP_TILE_URL || 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';
  const attribution = import.meta.env.VITE_MAP_ATTRIBUTION || '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';
  const visible = useMemo(() => children.filter(hasGeometry), [children]);
  return <>
    <MapContainer className={styles.leafletMap} center={[56, -96]} zoom={3} zoomSnap={0.25} zoomDelta={0.5} scrollWheelZoom={false} attributionControl={false}>
      <TileLayer key={tileRevision} url={tileUrl} attribution={attribution} eventHandlers={{ tileerror: () => setTileError(true) }} />
      {parent && hasGeometry(parent) && <GeoJSON key={`parent-${id}-${revision}`} data={parent} attribution={parentAttribution}
        style={{ color: BOUNDARY_BLUE, weight: home ? 2 : 2.4, fillColor: BOUNDARY_BLUE, fillOpacity: 0.035 }} interactive={false} />}
      {visible.map(child => <BoundaryLayer key={`${child.properties.geog_id}-${revision}`} child={child}
        active={previewId === child.properties.geog_id} onPreview={onPreview} onLeave={onLeave} onNavigate={onNavigate} />)}
      <Viewport id={id} parent={parent} children={children} fit={fit} />
      <BoundaryLoadingIndicator key={id} parent={parent} children={visible} loading={loading} />
      <ScaleControl position="bottomright" />
      <AttributionControl position="bottomright" prefix={false} />
    </MapContainer>
    <button type="button" className={styles.fitButton} onClick={() => setFit(value => value + 1)}><Expand size={16} aria-hidden="true" />Fit {home ? 'region' : 'area'}</button>
    {tileError && <div className={styles.tileWarning} role="status">Basemap unavailable. Area boundaries remain usable.
      <button type="button" aria-label="Retry basemap tiles" onClick={() => { setTileError(false); setTileRevision(value => value + 1); }}><RefreshCw size={14} /></button>
    </div>}
    <p className={styles.srOnly}>Use the Areas list to explore locations with the keyboard. Use the zoom buttons to zoom the map.</p>
  </>;
}
