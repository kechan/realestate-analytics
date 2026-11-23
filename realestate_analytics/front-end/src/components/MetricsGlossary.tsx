import { X } from 'lucide-react';
import { useEffect, useRef } from 'react';
import { METRIC_ENTRIES, METRIC_GROUPS } from '../domain/metrics';
import type { MetricName } from '../types';
import { IconButton } from './Controls';
import styles from '../styles.module.css';

const DEFINITIONS: Record<MetricName, string> = {
  median_sold_price: 'Median of IMS SoldPrice for the reporting month, excluding missing prices.',
  median_dom: 'Median of IMS DaysOnMarket for the reporting month, excluding missing values.',
  over_ask_percentage: '100 × the number of rows where SoldPrice > ListPrice ÷ all sold rows in the group.',
  below_ask_percentage: '100 × the number of rows where SoldPrice < ListPrice ÷ all sold rows in the group.',
  sold_listing_count: 'Count of sold listing IDs in the monthly group.',
  last_mth_median_asking_price: 'Median of the raw price field across the accumulated listing set, excluding missing values. The outlier filter applies only to the minimum and maximum.',
  last_mth_min_asking_price: 'Lowest asking price remaining after the outlier filter.',
  last_mth_max_asking_price: 'Highest asking price remaining after the outlier filter.',
  last_mth_new_listings: 'Count of listings in the same accumulated set used for monthly asking prices whose addedOn date falls in the reporting month. Calculated by LastMthMetricsProcessor.',
  mth_end_snapshot_listing_count: 'Count of active for-sale RLP listings at the time of the live pull on the first day of the next month. The snapshot is labeled with the month that just ended.',
  absorption_rate: 'IMS sales during the month ÷ month-end active listings for the same area and property type. The ETL rounds the ratio to four decimal places and Markets displays it as a percentage. A zero inventory denominator leaves the value unpublished.',
  months_of_inventory: 'Month-end active listings ÷ IMS sales during the same month, rounded to two decimal places. This estimates how many months the inventory would last at that sales pace. A zero sales denominator leaves the value unpublished.',
};

export default function MetricsGlossary({ onClose }: { onClose: () => void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const element = dialog.current;
    if (!element) return;
    element.showModal();
    return () => element.close();
  }, []);
  const close = () => {
    dialog.current?.close();
    onClose();
  };

  return <dialog ref={dialog} className={styles.glossaryDialog} aria-labelledby="metrics-glossary-title"
    onCancel={event => { event.preventDefault(); close(); }}
    onKeyDown={event => {
      // Keep the Areas drawer's document-level keyboard handlers out of the modal.
      if (event.key === 'Escape' || event.key === 'Tab') event.stopPropagation();
    }}
    onClick={event => { if (event.target === event.currentTarget) close(); }}>
    <div className={styles.dialogHeading}>
      <div><h2 id="metrics-glossary-title">Metrics glossary</h2><p>How the monthly Market history metrics are calculated.</p></div>
      <IconButton label="Close metrics glossary" onClick={close}><X size={20} aria-hidden="true" /></IconButton>
    </div>
    <div className={styles.glossaryBody} tabIndex={0} role="region" aria-label="Metric definitions">
      <p className={styles.glossaryIntro}>The ETLs publish values by area and property type to <code>rlp_mkt_trends_current</code>, which the Markets API reads. All homes pools the eligible listings for that area.</p>
      {METRIC_GROUPS.map(group => <section key={group}>
        <h3>{group}</h3>
        {group === 'Sales' && <>
          <p><code>SoldMedianMetricsProcessor</code> reads <code>rlp_ims_sold_listing_current</code> and groups geocoded sales by the month of <code>WrittenDate</code> and the property type assigned at import. The source query includes detached, semi-detached, townhouse and condo sales.</p>
          <p>Both percentages use every sold row in the group as the denominator and round to two decimal places. A sale at asking contributes to neither numerator.</p>
        </>}
        {group === 'Asking prices' && <>
          <p><code>LastMthMetricsProcessor</code> accumulates daily full pulls of eligible for-sale listings from <code>rlp_listing_current</code> by province, keeping the most recently fetched record per listing ID. On the first day of the next month it computes asking prices from the accumulated set, including that day’s pull.</p>
          <p><strong>Seen during the month vs. active at month-end.</strong> A listing seen in a daily pull can still contribute to asking prices and new listings if it disappears from the feed later. The month-end active count includes only listings marked active when that separate snapshot is taken.</p>
          <p>The min and max use positive prices whose natural log is within two sample standard deviations of the mean log price. If the band is undefined, has zero width, or retains no prices, it uses all positive prices. If there are no positive prices, it takes the raw min and max.</p>
        </>}
        {group === 'Inventory' && <p><code>AbsorptionRateProcessor</code> counts active, eligible for-sale listings from <code>rlp_listing_current</code> and saves the monthly snapshots. For the ratios it aligns those snapshots with monthly sales counts rebuilt from the sold ETL’s IMS listing cache, matching by area, property type and month. A gap is an unpublished value.</p>}
        <dl>{METRIC_ENTRIES.filter(([, metric]) => metric.group === group).map(([key, metric]) =>
          <div key={key}><dt>{metric.label}</dt><dd>{DEFINITIONS[key]}</dd></div>
        )}</dl>
      </section>)}
      <section>
        <h3>Current snapshot</h3>
        <p><code>CurrentMthMetricsProcessor</code> reads the accumulated RLP listing caches from <code>LastMthMetricsProcessor</code>. It takes the median asking price across the available listings for the selected group and counts listings whose <code>addedOn</code> falls in the current calendar month.</p>
      </section>
    </div>
  </dialog>;
}
