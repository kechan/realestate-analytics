import { ChevronLeft, ChevronRight, Info, X } from 'lucide-react';
import { PROVINCES, levelLabel, nameOf } from '../domain/geography';
import type { GeoFeature } from '../types';
import { IconButton, Loading, StateMessage } from './Controls';
import styles from '../styles.module.css';

export interface NavigationProps {
  home: boolean;
  province?: string | null;
  onProvince: (province?: string) => void;
  name: string;
  children: GeoFeature[];
  ancestors: GeoFeature[];
  loading: boolean;
  error: Error | null;
  previewId?: string;
  onNavigate: (id: string) => void;
  onPreview: (geo: GeoFeature) => void;
  onLeave: () => void;
  onRetry: () => void;
  onClose: () => void;
}
export default function Navigation({ home, name, province, onProvince, children, ancestors, loading, error, previewId, onNavigate, onPreview, onLeave, onRetry, onClose }: NavigationProps) {
  const parent = ancestors.length > 1 ? ancestors[ancestors.length - 2] : undefined;
  return <>
    <div className={styles.navTop}>
      <button type="button" className={styles.parentLink} onClick={() => parent ? onNavigate(parent.properties.geog_id) : onProvince(home ? 'ON' : province || undefined)}>
        <ChevronLeft size={17} aria-hidden="true" /><span>{parent ? nameOf(parent) : home ? 'Ontario' : province ? PROVINCES[province] || province : 'Canada'}</span>
      </button>
      <IconButton label="Close areas" className={styles.mobileOnly} onClick={onClose}><X size={20} /></IconButton>
    </div>
    <div className={styles.navHeading}>
      {!home && <h2>{name}</h2>}
      <p>{loading ? 'Loading areas…' : `${children.length} ${home ? 'cities' : 'areas'}`}</p>
    </div>
    <nav className={styles.areaList} aria-label={home ? 'GTA cities' : 'Child areas'} onMouseLeave={onLeave}>
      {loading ? <Loading compact label="Loading areas…" /> : error ? <StateMessage compact error title="Could not load areas" description={error.message} onRetry={onRetry} />
        : !children.length ? <StateMessage compact title="No smaller areas" description="You can still explore this location’s market history." />
          : children.map(geo => <button type="button" className={`${styles.areaRow} ${previewId === geo.properties.geog_id ? styles.areaActive : ''}`}
            key={geo.properties.geog_id} onClick={() => onNavigate(geo.properties.geog_id)}
            onMouseEnter={() => onPreview(geo)} onFocus={() => onPreview(geo)} onBlur={onLeave}>
            <span><span className={styles.areaName}>{nameOf(geo)}</span>{!home && <span className={styles.areaLevel}>{levelLabel(geo.properties.level)}</span>}</span>
            <ChevronRight size={16} aria-hidden="true" />
          </button>)}
    </nav>
    <div className={styles.navFooter}><Info size={15} aria-hidden="true" /><span>Select an area to explore</span></div>
  </>;
}
