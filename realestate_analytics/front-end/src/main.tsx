import { lazy, StrictMode, Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClientProvider } from '@tanstack/react-query';
import { queryClient } from './queries';
import App from './App';
import monitorStyles from './components/ImsMappingMonitor.module.css';
import './global.css';

const ImsMappingMonitor = lazy(() => import('./components/ImsMappingMonitor'));
const path = window.location.pathname.replace(/\/$/, '');
if (path === '/ims-mappings') window.history.replaceState(null, '', `/ims-property-type-mapping${window.location.search}${window.location.hash}`);
const monitor = path === '/ims-property-type-mapping' || path === '/ims-mappings';

createRoot(document.getElementById('root')!).render(
  <StrictMode><QueryClientProvider client={queryClient}><Suspense fallback={monitor ? <div className={monitorStyles.page}>
    <header className={monitorStyles.header}><a href="/" className={monitorStyles.brand}>Market Explorer</a>
      <nav aria-label="Application"><a href="/">Markets</a><a href="/ims-property-type-mapping" aria-current="page">IMS property type mapping</a></nav>
      <span className={monitorStyles.readOnly}>Read-only</span></header>
    <main className={monitorStyles.main}><p role="status">Loading…</p></main>
  </div> : <p style={{ padding: 24 }}>Loading…</p>}>
    {monitor ? <ImsMappingMonitor /> : <App />}
  </Suspense></QueryClientProvider></StrictMode>,
);
