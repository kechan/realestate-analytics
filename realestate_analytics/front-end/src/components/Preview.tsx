import { ArrowUpRight, GripVertical, X } from 'lucide-react';
import { useId, useState } from 'react';
import { ApiError } from '../api';
import { nameOf } from '../domain/geography';
import { formatValue, latestPoint, METRICS, monthLabel } from '../domain/metrics';
import { useMetrics } from '../queries';
import type { GeoFeature, MetricName, PropertyType } from '../types';
import ChartView from './ChartView';
import { IconButton, Loading, MetricSelect, PropertySelect, StateMessage } from './Controls';
import styles from '../styles.module.css';
import usePreviewDrag from './usePreviewDrag';

export default function Preview({ geo, propertyType, initialMetric, onPropertyType, onNavigate, onClose, onKeep, onInteract, onLeave }: {
  geo: GeoFeature; propertyType: PropertyType; initialMetric: MetricName;
  onPropertyType: (value: PropertyType) => void; onNavigate: (id: string) => void; onClose: () => void;
  onKeep: () => void; onInteract: () => void; onLeave: () => void;
}) {
  const [metric, setMetric] = useState(initialMetric);
  const { panel, handle } = usePreviewDrag();
  const moveHelp = useId();
  const market = useMetrics(geo.properties.geog_id, propertyType);
  const point = latestPoint(market.data?.series[metric]);
  const absent = market.error instanceof ApiError && market.error.status === 404;
  return <section ref={panel} className={styles.preview} aria-label={`${nameOf(geo)} market preview`} onMouseEnter={onKeep} onMouseLeave={onLeave}
    // Native select menus can send mouse-leave/blur while their options are open.
    // Once someone uses the preview, retain it until an explicit close/navigation.
    onPointerDownCapture={onInteract} onFocus={onInteract}
    onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget as Node)) onLeave(); }}>
    <div className={styles.previewHeading}><h3 aria-label={nameOf(geo)}><button type="button" className={styles.previewDragHandle}
      aria-label="Move market preview" aria-describedby={moveHelp} title="Drag to move; arrow keys also move the preview" {...handle}>
      <GripVertical size={15} aria-hidden="true" /><span>{nameOf(geo)}</span>
    </button></h3><IconButton label="Close market preview" onClick={onClose}><X size={16} /></IconButton></div>
    <p id={moveHelp} className={styles.srOnly}>Drag the title to move this preview within the map, or use arrow keys. Hold Shift for larger steps.</p>
    <div className={styles.previewControls}><MetricSelect value={metric} onChange={setMetric} compact /><PropertySelect value={propertyType} onChange={onPropertyType} compact /></div>
    {market.isPending ? <Loading compact label="Loading market preview…" />
      : market.isError && !market.data ? <StateMessage compact error={!absent} title={absent ? 'No published market data' : 'Could not load preview'} description={absent ? undefined : market.error.message} onRetry={absent ? undefined : () => void market.refetch()} />
        : <>
          <div className={styles.previewValue}>{formatValue(point?.value, METRICS[metric].unit)}<span>{point ? monthLabel(point.month) : 'Not available'}</span></div>
          <ChartView series={market.data?.series[metric]} metric={metric} small />
          {market.isError && <p className={styles.inlineNote} role="status">Refresh failed; showing the previous response.</p>}
        </>}
    <button type="button" className={styles.exploreButton} onClick={() => onNavigate(geo.properties.geog_id)}>Explore {geo.properties.level === 30 ? 'city' : 'area'}<ArrowUpRight size={17} /></button>
  </section>;
}
