import { Download, Upload } from 'lucide-react';
import { useRef, useState } from 'react';
import { exportImsReportCsv, importImsReportCsv } from '../domain/imsReportCsv';
import type { MappingReport } from '../domain/imsMappings';
import styles from './ImsMappingMonitor.module.css';

type SavePicker = (options: { suggestedName: string; types: { description: string; accept: Record<string, string[]> }[] }) =>
  Promise<FileSystemFileHandle>;

async function saveReport(report: MappingReport, label: string) {
  const filename = 'ims-mapping-report-' + report.read_at.slice(0, 10) + '.csv';
  const picker = (window as Window & { showSaveFilePicker?: SavePicker }).showSaveFilePicker;
  let handle: FileSystemFileHandle | undefined;
  if (picker) {
    try {
      handle = await picker.call(window, {
        suggestedName: filename, types: [{ description: 'CSV report', accept: { 'text/csv': ['.csv'] } }],
      });
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') return;
      if (!(error instanceof DOMException) || !['SecurityError', 'NotSupportedError'].includes(error.name)) throw error;
    }
  }
  const csv = new Blob([exportImsReportCsv(report, label)], { type: 'text/csv;charset=utf-8' });
  if (handle) {
    const writer = await handle.createWritable();
    try {
      await writer.write(csv);
      await writer.close();
    } catch (error) {
      await writer.abort().catch(() => {});
      throw error;
    }
    return;
  }
  const url = URL.createObjectURL(csv);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

interface Props {
  report?: MappingReport;
  label: string;
  busy: boolean;
  onOpen: (snapshot: ReturnType<typeof importImsReportCsv>) => void;
  onImportingChange: (importing: boolean) => void;
}

export default function ImsReportControls({ report, label, busy, onOpen, onImportingChange }: Props) {
  const input = useRef<HTMLInputElement>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState('');
  const disabled = busy || pending;
  const openReport = async (file: File) => {
    setError('');
    setPending(true);
    onImportingChange(true);
    try {
      onOpen(importImsReportCsv(await file.text()));
    } catch (problem) {
      setError(problem instanceof Error ? problem.message : 'Could not open the report CSV.');
    } finally {
      onImportingChange(false);
      setPending(false);
    }
  };
  const exportReport = async () => {
    if (!report) return;
    setError('');
    setPending(true);
    try {
      await saveReport(report, label);
    } catch {
      setError('Could not save the report CSV. Try a different destination.');
    } finally {
      setPending(false);
    }
  };
  return <div className={styles.reportControls}>
    {report && <button type="button" disabled={disabled} onClick={exportReport}><Download size={14} aria-hidden="true" />Export CSV</button>}
    <button type="button" disabled={disabled} onClick={() => input.current?.click()}><Upload size={14} aria-hidden="true" />Import report</button>
    <input ref={input} type="file" hidden aria-label="Open an exported IMS report" accept=".csv,text/csv"
      disabled={disabled} onChange={event => {
        const file = event.currentTarget.files?.[0];
        event.currentTarget.value = '';
        if (file) void openReport(file);
      }} />
    {error && <p className={styles.reportFileError} role="alert">{error}</p>}
  </div>;
}
