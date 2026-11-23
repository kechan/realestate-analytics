import { Download, X } from 'lucide-react';
import { useRef } from 'react';
import { formatValue, METRICS, monthLabel, seriesCsv } from '../domain/metrics';
import type { ChartPoint, MetricName } from '../types';
import { IconButton } from './Controls';
import useDialogFocus from './useDialogFocus';
import styles from '../styles.module.css';

export default function DataDialog({ points, metric, name, onClose }: { points: ChartPoint[]; metric: MetricName; name: string; onClose: () => void }) {
  const ref = useRef<HTMLDivElement>(null);
  useDialogFocus(ref, true, onClose);
  const meta = METRICS[metric];
  const download = () => {
    const blob = new Blob(['\ufeff', seriesCsv(points, metric)], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob); const anchor = document.createElement('a');
    anchor.href = url; anchor.download = `${name.replace(/[^\p{L}\p{N}-]+/gu, '-').toLowerCase()}-${metric}.csv`;
    anchor.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  return <div className={styles.modalBackdrop} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
    <div ref={ref} className={styles.dataDialog} role="dialog" aria-modal="true" aria-labelledby="data-dialog-title">
      <div className={styles.dialogHeading}><div><h2 id="data-dialog-title">{meta.label}</h2><p>{name} · {points.length} months in this view</p></div>
        <IconButton label="Close data table" onClick={onClose}><X size={20} /></IconButton></div>
      <div className={styles.tableScroll}><table><caption className={styles.srOnly}>{meta.label} for {name}</caption>
        <thead><tr><th scope="col">Month</th><th scope="col">{meta.label}{meta.unit === 'currency' ? ' (CAD)' : ''}</th></tr></thead>
        <tbody>{points.map(point => <tr key={point.month}><th scope="row">{monthLabel(point.month)}</th><td>{point.value === null ? 'Not published' : formatValue(point.value, meta.unit)}</td></tr>)}</tbody>
      </table></div>
      <div className={styles.dialogFooter}><p>CSV preserves API units{meta.unit === 'ratio' ? ' (absorption is a ratio)' : ''}. Empty cells mean no published value.</p>
        <button type="button" className={styles.primaryButton} onClick={download}><Download size={16} />Download CSV</button></div>
    </div>
  </div>;
}
