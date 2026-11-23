import { lazy, Suspense, useMemo } from 'react';
import { chartPoints } from '../domain/metrics';
import type { HistoryRange, MetricName, MetricSeries } from '../types';
import { Loading, StateMessage } from './Controls';
import VisualBoundary from './VisualBoundary';

const Chart = lazy(() => import('./Chart'));
export default function ChartView({ series, metric, range = 'all', small = false }: {
  series?: MetricSeries; metric: MetricName; range?: HistoryRange; small?: boolean;
}) {
  const points = useMemo(() => chartPoints(series, range), [series, range]);
  if (!points.length) return <StateMessage compact={small} title="No published history" description={small ? undefined : 'This metric is not available for this location and property type.'} />;
  return <VisualBoundary name="chart" resetKey={metric}><Suspense fallback={<Loading compact={small} label="Loading chart…" />}><Chart points={points} metric={metric} small={small} /></Suspense></VisualBoundary>;
}
