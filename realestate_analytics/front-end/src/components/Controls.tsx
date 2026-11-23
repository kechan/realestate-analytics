import { AlertCircle, Info, RefreshCw } from 'lucide-react';
import { useId } from 'react';
import type { ComponentProps } from 'react';
import { METRIC_ENTRIES, METRIC_GROUPS, PROPERTY_TYPES } from '../domain/metrics';
import type { MetricName, PropertyType } from '../types';
import styles from '../styles.module.css';

export function IconButton({ label, children, className = '', ...props }: ComponentProps<'button'> & { label: string }) {
  return <button type="button" aria-label={label} title={label} className={`${styles.iconButton} ${className}`} {...props}>{children}</button>;
}
export function PropertySelect({ value, onChange, compact = false }: { value: PropertyType; onChange: (value: PropertyType) => void; compact?: boolean }) {
  const id = useId();
  return <label className={styles.selectField} htmlFor={id}>
    <span className={compact ? styles.srOnly : styles.fieldLabel}>Property type</span>
    <select id={id} aria-label={compact ? 'Preview property type' : 'Property type'} value={value} onChange={e => onChange(e.target.value as PropertyType)}>
      {PROPERTY_TYPES.map(type => <option key={type.value} value={type.value}>{type.label}</option>)}
    </select>
  </label>;
}
export function MetricSelect({ value, onChange, compact = false }: { value: MetricName; onChange: (value: MetricName) => void; compact?: boolean }) {
  const id = useId();
  return <label className={styles.metricSelect} htmlFor={id}>
    <span className={styles.srOnly}>{compact ? 'Preview metric' : 'History metric'}</span>
    <select id={id} aria-label={compact ? 'Preview metric' : 'History metric'} value={value} onChange={e => onChange(e.target.value as MetricName)}>
      {METRIC_GROUPS.map(group => <optgroup key={group} label={group}>
        {METRIC_ENTRIES.filter(([, m]) => m.group === group)
          .map(([key, meta]) => <option key={key} value={key}>{meta.label}</option>)}
      </optgroup>)}
    </select>
  </label>;
}
export function StateMessage({ title, description, onRetry, error = false, compact = false }: {
  title: string; description?: string; onRetry?: () => void; error?: boolean; compact?: boolean;
}) {
  const Icon = error ? AlertCircle : Info;
  return <div className={`${styles.stateMessage} ${compact ? styles.compactMessage : ''}`} role={error ? 'alert' : 'status'}>
    <Icon size={20} aria-hidden="true" />
    <div><strong>{title}</strong>{description && <p>{description}</p>}
      {onRetry && <button className={styles.textButton} type="button" onClick={onRetry}><RefreshCw size={14} />Try again</button>}
    </div>
  </div>;
}
export function Loading({ label = 'Loading…', compact = false }: { label?: string; compact?: boolean }) {
  return <div role="status" className={`${styles.loading} ${compact ? styles.compactLoading : ''}`}><span className={styles.spinner} aria-hidden="true" />{label}</div>;
}
