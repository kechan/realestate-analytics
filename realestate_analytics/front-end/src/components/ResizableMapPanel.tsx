import type { ComponentPropsWithoutRef } from 'react';
import { GripHorizontal } from 'lucide-react';
import usePanelResize from './usePanelResize';
import styles from '../styles.module.css';

export default function ResizableMapPanel({ children, ...props }: ComponentPropsWithoutRef<'section'>) {
  const { panel, height, handle } = usePanelResize<HTMLElement>({
    id: 'location-map', storageKey: 'market-explorer:map-height:v1', min: 0, max: 1200,
  });
  const collapsed = height === 0;

  return <div className={styles.resizablePanel}>
    <section {...props} id="location-map" ref={panel} className={styles.mapPanel}
      style={{ height }} inert={collapsed} aria-hidden={collapsed || undefined}>{children}</section>
    <div {...handle} className={styles.panelResizeHandle} aria-label="Resize map height"
      aria-valuetext={collapsed ? 'Map hidden' : undefined} title="Drag up or down to resize the map">
      <GripHorizontal size={19} aria-hidden="true" />
      {collapsed && <span aria-hidden="true">Map hidden</span>}
    </div>
  </div>;
}
