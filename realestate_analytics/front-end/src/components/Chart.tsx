import { useEffect, useRef, useState } from 'react';
import { init, use as register } from 'echarts/core';
import type { ECharts } from 'echarts/core';
import { BarChart, LineChart } from 'echarts/charts';
import { AriaComponent, GridComponent, TooltipComponent } from 'echarts/components';
import { SVGRenderer } from 'echarts/renderers';
import { formatValue, METRICS, monthLabel } from '../domain/metrics';
import type { ChartPoint, MetricName } from '../types';
import styles from '../styles.module.css';

register([LineChart, BarChart, GridComponent, TooltipComponent, AriaComponent, SVGRenderer]);

export default function Chart({ points, metric, small = false }: { points: ChartPoint[]; metric: MetricName; small?: boolean }) {
  const host = useRef<HTMLDivElement>(null);
  const instance = useRef<ECharts | null>(null);
  const [ready, setReady] = useState(false);
  const meta = METRICS[metric];
  const last = points.at(-1);
  useEffect(() => {
    if (!host.current) return;
    const resize = () => {
      const element = host.current;
      if (!element?.isConnected || !element.clientWidth || !element.clientHeight) return;
      if (instance.current) instance.current.resize();
      else {
        instance.current = init(element, undefined, { renderer: 'svg' });
        setReady(true);
      }
    };
    const observer = new ResizeObserver(resize);
    observer.observe(host.current);
    resize();
    return () => { observer.disconnect(); instance.current?.dispose(); instance.current = null; };
  }, []);
  useEffect(() => {
    instance.current?.setOption({
      animation: !window.matchMedia('(prefers-reduced-motion: reduce)').matches,
      animationDuration: 180,
      textStyle: { fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif', fontSize: small ? 10 : 11 },
      aria: { enabled: true, description: `${meta.label}. ${points.filter(p => p.value !== null).length} monthly observations. ${small ? 'Explore this area for the full data table.' : 'Use View data for exact values.'}` },
      grid: { left: small ? 52 : 76, right: small ? 15 : 30, top: small ? 14 : 26, bottom: small ? 24 : 32 },
      tooltip: { trigger: 'axis', confine: true, borderColor: '#e4e8ee', padding: [10, 14],
        formatter: (raw: unknown) => {
          const item = (Array.isArray(raw) ? raw[0] : raw) as { dataIndex?: number } | undefined;
          const point = item?.dataIndex != null ? points[item.dataIndex] : undefined;
          return point ? `${monthLabel(point.month)}<br/><strong>${formatValue(point.value, meta.unit)}</strong>` : '';
        },
      },
      xAxis: { type: 'category', data: points.map(p => p.month), boundaryGap: Boolean(meta.bar),
        axisLine: { lineStyle: { color: '#d9e0e9' } }, axisTick: { show: false },
        axisLabel: { color: '#64748b', hideOverlap: true, interval: points.length > 18 ? 0 : 'auto',
          formatter: (month: string, index: number) => points.length > 18 ? (index === 0 || month.endsWith('-01') ? month.slice(0, 4) : '') : monthLabel(month),
        },
      },
      yAxis: { type: 'value', scale: !meta.bar, min: meta.bar ? 0 : undefined, splitNumber: small ? 2 : 4,
        axisLabel: { color: '#64748b', formatter: (value: number) => formatValue(value, meta.unit, true) },
        splitLine: { lineStyle: { color: '#e9edf3' } },
      },
      series: [{ type: meta.bar ? 'bar' : 'line', data: points.map(p => p.value), connectNulls: false,
        smooth: false, showSymbol: !small && points.length <= 60, symbol: 'circle', symbolSize: 5,
        barMaxWidth: 20, itemStyle: { color: '#b4232c', borderRadius: meta.bar ? [3, 3, 0, 0] : undefined },
        lineStyle: { color: '#b4232c', width: 2.3 }, emphasis: { scale: 1.5 },
      }],
    }, { notMerge: true });
  }, [points, metric, meta, small, ready]);
  return <div className={small ? styles.smallChart : styles.chart}>
    {!small && last && <span className={styles.chartEnd}>{formatValue(last.value, meta.unit)}<span> · {monthLabel(last.month)}</span></span>}
    <div ref={host} className={styles.chartCanvas} role="img" aria-label={`${meta.label}, monthly history${last ? ` through ${monthLabel(last.month)}` : ''}`} />
  </div>;
}
