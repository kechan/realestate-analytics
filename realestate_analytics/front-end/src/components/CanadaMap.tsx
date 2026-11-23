import { useState } from 'react';
import { CANADA_SHAPES } from '../domain/canadaMap';
import { PROVINCES } from '../domain/geography';
import styles from './CountryBrowser.module.css';

export default function CanadaMap({ province, onSelect }: { province?: string; onSelect: (code: string) => void }) {
  const [hover, setHover] = useState<string>();
  const selected = CANADA_SHAPES.find(shape => shape.code === province);
  const box = selected?.bounds;
  // Pad small provinces generously; the overview is navigation, not cadastral data.
  const padding = box ? Math.max(12, Math.max(box[2], box[3]) * 0.12) : 0;
  const viewBox = box ? [box[0] - padding, box[1] - padding, box[2] + padding * 2, box[3] + padding * 2].join(' ') : '0 0 1000 710';
  const active = hover || province;
  return <div className={styles.map}>
    <svg role="group" viewBox={viewBox} aria-label={province ? `${PROVINCES[province]} navigation map` : 'Canada provinces and territories'}
      className={styles.mapSvg} preserveAspectRatio="xMidYMid meet">
      <title>{province ? PROVINCES[province] : 'Explore Canada by province or territory'}</title>
      {CANADA_SHAPES.map(shape => <g key={shape.code} role="button" tabIndex={!province || province === shape.code ? 0 : -1} aria-label={`Browse ${PROVINCES[shape.code]}`}
        aria-hidden={province && province !== shape.code ? true : undefined}
        aria-pressed={province === shape.code} className={`${styles.region} ${active === shape.code ? styles.activeRegion : ''} ${province && province !== shape.code ? styles.quietRegion : ''}`}
        onMouseEnter={() => setHover(shape.code)} onMouseLeave={() => setHover(undefined)} onFocus={() => setHover(shape.code)} onBlur={() => setHover(undefined)}
        onClick={() => onSelect(shape.code)} onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onSelect(shape.code); } }}>
        <path d={shape.path} fillRule="evenodd" vectorEffect="non-scaling-stroke" />
        {!province && <text x={shape.label[0]} y={shape.label[1]} aria-hidden="true">{shape.code}</text>}
      </g>)}
    </svg>
    {!province && <div className={styles.mapCaption} aria-live="polite">{active ? PROVINCES[active] : 'Choose a province or territory'}</div>}
    <a className={styles.attribution} href="https://www.naturalearthdata.com/" target="_blank" rel="noreferrer">Navigation outlines: Natural Earth</a>
  </div>;
}
