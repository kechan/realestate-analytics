import AreaMap from './AreaMap';
import { PROVINCE_BOUNDARIES } from '../domain/provinceBoundaries';
import type { GeoFeature } from '../types';

export default function ProvinceMap({ province, children, loading, previewId, revision, onPreview, onLeave, onNavigate }: {
  province: string; children: GeoFeature[]; previewId?: string; revision: string;
  loading?: boolean;
  onPreview: (geo: GeoFeature, pin?: boolean) => void; onLeave: () => void;
  onNavigate: (id: string) => void;
}) {
  return <AreaMap id={`province-${province}`} home={false} parent={PROVINCE_BOUNDARIES[province]}
    children={children} loading={loading} previewId={previewId} revision={revision} onPreview={onPreview} onLeave={onLeave} onNavigate={onNavigate}
    parentAttribution='<a href="https://www.naturalearthdata.com/">Province outline: Natural Earth</a>' />;
}
