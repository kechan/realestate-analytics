import { ChevronRight, Info } from 'lucide-react';
import type { GeoDetail, Provenance } from '../types';
import styles from '../styles.module.css';

const SECTIONS = { transport: 'Transportation', services: 'Services', character: 'Character', housing: 'Housing' };
export default function Profile({ geo, listProvenance }: { geo?: GeoDetail; listProvenance?: Provenance }) {
  const data = geo?.details.data;
  const sections = Object.entries(SECTIONS).filter(([key]) => typeof data?.profiles?.[key] === 'string' && data.profiles[key]?.trim());
  const contribution = geo?.provenance;
  const fields = [...new Set([...(contribution?.fallback_fields || []), ...(listProvenance?.fallback_fields || [])])];
  return <section className={styles.profilePanel}>
    <details className={styles.profileDisclosure}><summary><ChevronRight size={18} aria-hidden="true" /><strong>Area profile</strong>
      <span className={styles.profileSections}>{sections.map(([, label]) => label).join(' · ')}</span></summary>
      <div className={styles.profileContent}>
        {typeof data?.intro === 'string' && data.intro && <p className={styles.profileIntro}>{data.intro}</p>}
        {sections.length ? sections.map(([key, label]) => <section key={key}><h3>{geo?.details.meta?.sections?.[key] || label}</h3><p>{data?.profiles?.[key]}</p></section>)
          : <p>No written profile is available for this area.</p>}
        <details className={styles.sourceDisclosure}><summary><Info size={14} />Geography sources</summary>
      <p>{!geo && !listProvenance ? 'Geography source information is currently unavailable.' : fields.length || contribution?.source === 'geo_collection' || listProvenance?.source === 'geo_collection'
        ? 'Elasticsearch is primary. Missing geography information in this view was supplemented from the saved GeoCollection snapshot.'
        : 'Geography information in this view was supplied by Elasticsearch.'}</p>
      {fields.length > 0 && <p>Snapshot fields: {fields.join(', ')}.</p>}
      <p>Market metrics are requested separately from the API and are never supplied by the geography snapshot.</p>
        </details>
      </div>
    </details>
  </section>;
}
