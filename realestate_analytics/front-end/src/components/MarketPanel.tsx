import { GripHorizontal, Info } from 'lucide-react';
import { useCallback, useMemo, useState } from 'react';
import { ApiError } from '../api';
import { chartPoints, formatValue, latestPoint, METRICS, monthLabel, updatedLabel } from '../domain/metrics';
import { useCatalog, useMetrics } from '../queries';
import type { HistoryRange, MarketMetrics, MetricName, PropertyType } from '../types';
import ChartView from './ChartView';
import { Loading, MetricSelect, StateMessage } from './Controls';
import DataDialog from './DataDialog';
import usePanelResize from './usePanelResize';
import styles from '../styles.module.css';

const SUMMARY: MetricName[] = ['median_sold_price', 'last_mth_median_asking_price', 'absorption_rate', 'months_of_inventory'];
export function Summary({ data, loading = false }: { data?: MarketMetrics; loading?: boolean }) {
  return <dl className={styles.summary} aria-label="Market summary" aria-busy={loading}>
    {SUMMARY.map(metric => {
      const point = latestPoint(data?.series[metric]);
      return <div key={metric}><dt>{METRICS[metric].label}</dt><dd className={loading ? styles.skeletonValue : ''}>{loading ? '\u00a0' : formatValue(point?.value, METRICS[metric].unit)}</dd>
        <dd className={styles.summaryMonth}>{loading ? 'Loading…' : point ? monthLabel(point.month) : 'Not available'}</dd></div>;
    })}
  </dl>;
}

export default function MarketPanel({ id, name, propertyType, metric, range, onMetric, onRange }: {
  id: string; name: string; propertyType: PropertyType; metric: MetricName; range: HistoryRange;
  onMetric: (metric: MetricName) => void; onRange: (range: HistoryRange) => void;
}) {
  // This shares the exact query used by the summary and any matching popup.
  const market = useMetrics(id, propertyType);
  const catalog = useCatalog();
  const [table, setTable] = useState(false);
  const plot = usePanelResize<HTMLDivElement>({
    id: 'market-history-plot', storageKey: 'market-explorer:plot-height:v1', min: 155, max: 1000,
  });
  const closeTable = useCallback(() => setTable(false), []);
  const series = market.data?.series[metric];
  const points = useMemo(() => chartPoints(series, range), [series, range]);
  const absent = market.error instanceof ApiError && market.error.status === 404;
  const description = catalog.data?.find(m => m.name === metric)?.description;
  const updated = updatedLabel(market.data?.document_updated_at);
  const current = market.data?.current_metrics;
  return <div className={styles.resizablePanel}><section className={styles.historyPanel} aria-labelledby="history-title">
    <div className={styles.historyToolbar}>
      <div className={styles.historyTitle}><h2 id="history-title">Market history</h2><MetricSelect value={metric} onChange={onMetric} /></div>
      <div className={styles.historyActions}><div className={styles.ranges} role="group" aria-label="History range">
        {(['1y', '3y', 'all'] as const).map(value => <button type="button" key={value} aria-pressed={range === value}
          className={range === value ? styles.rangeActive : ''} onClick={() => onRange(value)}>{value === 'all' ? 'All' : value.toUpperCase()}</button>)}
      </div><button type="button" className={styles.secondaryButton} disabled={!points.length} onClick={() => setTable(true)}>View data</button></div>
    </div>
    <div id="market-history-plot" ref={plot.panel} className={styles.historyBody} style={{ height: plot.height }}>
      {market.isPending ? <Loading label="Loading market history…" />
        : market.isError && !market.data ? <StateMessage error={!absent} title={absent ? 'No published market data' : 'Could not load market history'}
          description={absent ? 'Try a different property type or location. The map and navigation are still available.' : market.error.message} onRetry={absent ? undefined : () => void market.refetch()} />
          : <ChartView series={series} metric={metric} range={range} />}
    </div>
    <div className={styles.historyNotes}>
      <details><summary><Info size={14} aria-hidden="true" />About this metric</summary>
        <p>{description || METRICS[metric].label}{METRICS[metric].unit === 'ratio' ? '. Displayed as a percentage: sales divided by month-end active inventory.' : ''}
          {' '}Reporting months can differ between metrics. Gaps are unpublished months, not zeroes.{updated ? ` Document updated ${updated}; this does not mean every metric covers that month.` : ''}</p>
        {catalog.isError && <button className={styles.textButton} onClick={() => void catalog.refetch()}>Retry metric descriptions</button>}
      </details>
      {market.isFetching && !market.isPending && <span role="status">Updating…</span>}
      {market.isError && market.data && <span role="status">Refresh failed; showing the previous response. <button className={styles.textButton} onClick={() => void market.refetch()}>Retry</button></span>}
    </div>
    <dl className={styles.currentSnapshot} aria-label="Current metrics">
      <div><dt>Current snapshot</dt><dd>{updated ? `Document updated ${updated}` : 'Current-month metrics'}</dd></div>
      <div><dt>Median asking price</dt><dd>{formatValue(current?.median_asking_price, 'currency')}</dd></div>
      <div><dt>New listings</dt><dd>{formatValue(current?.new_listings, 'listings')}</dd></div>
    </dl>
    {table && <DataDialog points={points} metric={metric} name={name} onClose={closeTable} />}
  </section>
    <div {...plot.handle} className={styles.panelResizeHandle} aria-label="Resize plot height"
      title="Drag up or down to resize the plot"><GripHorizontal size={19} aria-hidden="true" /></div>
  </div>;
}
