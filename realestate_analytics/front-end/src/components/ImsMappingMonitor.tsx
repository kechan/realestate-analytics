import { useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useMutation, useQuery } from '@tanstack/react-query';
import { AlertTriangle, ArrowDown, ArrowUp, ChevronRight, CircleHelp, FileSpreadsheet, GripVertical, LockKeyhole, RefreshCw, Search, UploadCloud } from 'lucide-react';
import { api } from '../api';
import { lookupOverrideLabel, selectMappings, sourceLabels, statusLabels } from '../domain/imsMappings';
import type { LookupSource, MappingReport, MappingRow, MappingSort } from '../domain/imsMappings';
import ImsReportControls from './ImsReportControls';
import styles from './ImsMappingMonitor.module.css';

const number = new Intl.NumberFormat('en-CA');
const count = (value: number) => number.format(value);
const PAGE_SIZE = 50;
const columns: { key: MappingSort; label: string; width: number }[] = [
  { key: 'Province', label: 'Province', width: 105 },
  { key: 'Type', label: 'Type', width: 150 }, { key: 'SubType', label: 'SubType', width: 180 },
  { key: 'Style', label: 'Style', width: 190 }, { key: 'property_type', label: 'Property Type', width: 160 },
  { key: 'count', label: 'Count', width: 110 }, { key: 'algorithm_mapping', label: 'Base mapping', width: 155 },
  { key: 'override_mapping', label: lookupOverrideLabel, width: 155 }, { key: 'mapping_source', label: 'Current source', width: 160 },
  { key: 'lineage_status', label: 'Mapping check', width: 160 },
];
const MIN_WIDTH = 80;
type HelpColumn = 'property_type' | 'algorithm_mapping' | 'override_mapping';

function RawValue({ value }: { value: string | null }) {
  if (value === null) return <span className={styles.missing}>Missing</span>;
  if (value === '') return <span className={styles.missing}>Empty string</span>;
  return <><span className={styles.raw} title={JSON.stringify(value)}>{value}</span>
    {value !== value.trim() && <small className={styles.whitespace}>Whitespace</small>}</>;
}

function Source({ row }: { row: MappingRow }) {
  return <details className={styles.sourceDetail}>
    <summary>{sourceLabels[row.mapping_source]}</summary>
    <dl>
      <div><dt>{row.base_origin === 'algorithm' ? 'Algorithm (new tuple)' : 'Base mapping'}</dt><dd>{count(row.algorithm_count)}</dd></div>
      <div><dt>{lookupOverrideLabel}</dt><dd>{count(row.override_count)}</dd></div>
      <div><dt>AB duplex rule</dt><dd>{count(row.provincial_count)}</dd></div>
      <div><dt>No local match</dt><dd>{count(row.unexplained_count)}</dd></div>
    </dl>
    {row.provincial_count > 0 && <p>The AB duplex rule takes precedence over the manual lookup and base mapping. Expected type: SEMI-DETACHED.</p>}
  </details>;
}

function Lookup({ source, label }: { source: LookupSource; label: string }) {
  return <div><h3>{label}</h3><code>{source.file}</code>
    <p>{source.available ? `${count(source.rows)} rows · ${count(source.duplicate_rows)} duplicate-key rows · ${count(source.blank_rows)} blank rows`
      : `Unavailable (${source.issue || 'unknown reason'}).`}</p>
    {source.modified_at && <p>Modified {new Date(source.modified_at).toLocaleString()}</p>}
    {source.sha256 && <p className={styles.hash}>SHA-256 {source.sha256}</p>}</div>;
}

export default function ImsMappingMonitor() {
  const [search, setSearch] = useState('');
  const [conflicts, setConflicts] = useState(false);
  const [source, setSource] = useState('');
  const [storedType, setStoredType] = useState('');
  const [check, setCheck] = useState('');
  const [sort, setSort] = useState<MappingSort | null>(null);
  const [direction, setDirection] = useState<'asc' | 'desc'>('asc');
  const [widths, setWidths] = useState(() => columns.map(column => column.width));
  const [helpPosition, setHelpPosition] = useState<{ left: number; top: number; key: HelpColumn } | null>(null);
  const helpTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (helpTimer.current) clearTimeout(helpTimer.current); }, []);
  const [page, setPage] = useState(0);
  const [activeSource, setActiveSource] = useState<'csv' | 'elasticsearch'>('csv');
  const [files, setFiles] = useState<{ id: number; file: File }[]>([]);
  const hasManualFileOrder = useRef(false);
  const nextFileId = useRef(0);
  const draggedFile = useRef<number | null>(null);
  const [dropTarget, setDropTarget] = useState<number | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  const [fileError, setFileError] = useState('');
  const [dragging, setDragging] = useState(false);
  const dragDepth = useRef(0);
  const [csvResult, setCsvResult] = useState<MappingReport>();
  const [resultName, setResultName] = useState('');
  const [openedSnapshot, setOpenedSnapshot] = useState(false);
  const [importingSnapshot, setImportingSnapshot] = useState(false);
  const [groupByProvince, setGroupByProvince] = useState(false);
  const collapseUpload = () => {
    const panel = fileInput.current?.closest('details');
    if (panel) {
      if (panel.contains(document.activeElement)) panel.querySelector('summary')?.focus({ preventScroll: true });
      panel.open = false;
    }
  };
  const preview = useMutation({ mutationFn: (selected: File[]) => api.previewImsCSV(selected, new AbortController().signal), retry: false,
    onSuccess: (result, selected) => {
      setCsvResult(result);
      setResultName(selected.length === 1 ? selected[0].name : `${selected.length} CSV files`);
      setOpenedSnapshot(false);
      resetProvinceView();
      setActiveSource('csv');
      clear();
      collapseUpload();
    } });
  const openSnapshot = ({ report, label }: { report: MappingReport; label: string }) => {
    preview.reset();
    setCsvResult(report);
    setResultName(label);
    setOpenedSnapshot(true);
    resetProvinceView();
    setActiveSource('csv');
    clear();
    collapseUpload();
  };
  const report = useQuery({ queryKey: ['ims-mappings'], queryFn: ({ signal }) => api.imsMappings(signal),
    enabled: false, retry: false, staleTime: Infinity, refetchOnWindowFocus: false, refetchOnReconnect: false });
  const requestProductionScan = () => {
    if (window.confirm('Run an expensive Production ES scan?\n\nThis scans ES document source across the entire IMS sold index. It can place significant CPU and disk load on Production Elasticsearch. Use this sparingly.\n\nChoose OK to execute the full scan, or Cancel to send no request.')) {
      setActiveSource('elasticsearch'); clear();
      void report.refetch();
    }
  };
  useEffect(() => { document.title = 'IMS Property Type · Market Explorer'; }, []);
  const isCSV = activeSource === 'csv';
  const helpText: Record<HelpColumn, string> = {
    property_type: isCSV
      ? 'Computed from the CSV preview using the production mapper. It uses saved base mappings for known tuples, infers new ones, then applies any match from the override lookup. The Alberta duplex rule takes precedence where it applies.'
      : 'The property type stored on the IMS sold listing in Production Elasticsearch.',
    algorithm_mapping: isCSV
      ? 'The production mapper reuses saved base mappings first and classifies only unseen tuples. No mappings are saved.'
      : 'Saved mapper lookup for this source tuple. Existing cached values are reused and may differ from what the current algorithm would produce if run again.',
    override_mapping: 'Read from the saved ims_property_type_lookup_df dataframe. A non-null lookup override takes precedence over the base mapping. The AB duplex rule takes precedence over both.',
  };
  const data = isCSV ? csvResult : report.data;
  const provinceBreakdown = isCSV ? data?.province_breakdown : undefined;
  const provinceView = groupByProvince ? provinceBreakdown : undefined;
  const grouped = provinceView || data;
  const visibleColumns = columns.map((column, index) => ({ column, index }))
    .filter(({ column }) => column.key !== 'Province' || provinceView);
  const busy = report.isFetching || preview.isPending;
  const problem = isCSV ? preview.error : report.error;
  const addFiles = (selected: File[]) => {
    if (!selected.length) return;
    if (selected.some(file => !file.name.toLowerCase().endsWith('.csv') || file.size === 0)) {
      setFileError('Choose non-empty .csv files.');
      return;
    }
    if (files.length + selected.length > 20) {
      setFileError('Choose up to 20 CSV files.');
      return;
    }
    preview.reset();
    setFileError('');
    const added = selected.map(file => ({ id: nextFileId.current++, file }));
    const keepExistingOrder = hasManualFileOrder.current;
    const byModifiedTime = (a: typeof added[number], b: typeof added[number]) =>
      a.file.lastModified - b.file.lastModified;
    if (keepExistingOrder) added.sort(byModifiedTime);
    setFiles(current => keepExistingOrder
      ? [...current, ...added]
      : [...current, ...added].sort(byModifiedTime));
  };
  const removeFile = (index: number) => {
    preview.reset();
    setFileError('');
    if (files.length === 1) hasManualFileOrder.current = false;
    setFiles(current => current.filter((_, position) => position !== index));
    fileInput.current?.focus();
  };
  const moveFile = (id: number, to: number) => {
    if (busy || to < 0 || to >= files.length) return;
    const from = files.findIndex(entry => entry.id === id);
    if (from === -1 || from === to) return;
    const reordered = [...files];
    const [entry] = reordered.splice(from, 1);
    reordered.splice(to, 0, entry);
    hasManualFileOrder.current = true;
    preview.reset();
    setFiles(reordered);
  };
  const endFileDrag = () => {
    draggedFile.current = null;
    setDropTarget(null);
  };
  const effectiveSort = sort === 'Province' && !provinceView ? null : sort;
  const rows = useMemo(() => selectMappings(grouped?.rows || [], search, conflicts, source, storedType, check, effectiveSort, direction),
    [grouped, search, conflicts, source, storedType, check, effectiveSort, direction]);
  const types = useMemo(() => [...new Set(grouped?.rows.map(row => row.property_type).filter((value): value is string => value !== null))].sort(), [grouped]);
  const lastPage = Math.max(0, Math.ceil(rows.length / PAGE_SIZE) - 1);
  const currentPage = Math.min(page, lastPage);
  const start = currentPage * PAGE_SIZE;
  const updateFilter = (change: () => void) => { change(); setPage(0); };
  const resetProvinceView = () => {
    setGroupByProvince(false);
    if (sort === 'Province') setSort(null);
  };
  const changeSort = (column: MappingSort) => {
    if (column !== sort) { setSort(column); setDirection('asc'); }
    else if (direction === 'asc') setDirection('desc');
    else { setSort(null); setDirection('asc'); }
    setPage(0);
  };
  const resize = (index: number, width: number) => setWidths(current => current.map((value, i) => i === index ? Math.min(2000, Math.max(MIN_WIDTH, width)) : value));
  const showHelp = (element: HTMLElement, delay: number, key: HelpColumn) => {
    if (helpTimer.current) clearTimeout(helpTimer.current);
    const rect = element.getBoundingClientRect();
    helpTimer.current = setTimeout(() => setHelpPosition({ left: Math.max(8, Math.min(rect.left, window.innerWidth - 268)), top: rect.top - 8, key }), delay);
  };
  const hideHelp = (delay: number) => {
    if (helpTimer.current) clearTimeout(helpTimer.current);
    helpTimer.current = setTimeout(() => setHelpPosition(null), delay);
  };
  const heading = (column: typeof columns[number], index: number) => <th key={column.key} scope="col" aria-sort={sort === column.key ? direction === 'asc' ? 'ascending' : 'descending' : 'none'}>
    <div className={styles.headerLabel}><button type="button" onClick={() => changeSort(column.key)}>{column.label}{sort === column.key && (direction === 'asc' ? <ArrowUp size={13} /> : <ArrowDown size={13} />)}</button>
      {(column.key === 'property_type' || column.key === 'algorithm_mapping' || column.key === 'override_mapping') && <button type="button" className={styles.helpButton} aria-label={`About ${column.label.toLowerCase()}`} aria-describedby={helpPosition?.key === column.key ? 'mapping-help' : undefined}
        onMouseEnter={event => showHelp(event.currentTarget, 500, column.key as HelpColumn)} onMouseLeave={() => hideHelp(250)}
        onFocus={event => showHelp(event.currentTarget, 0, column.key as HelpColumn)} onBlur={() => hideHelp(0)}><CircleHelp size={14} aria-hidden="true" /></button>}</div>
    <div className={styles.resizeHandle} role="separator" aria-label={`Resize ${column.label} column`} aria-orientation="vertical" aria-valuemin={MIN_WIDTH} aria-valuemax={2000} aria-valuenow={widths[index]} tabIndex={0}
      onKeyDown={event => { if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') { event.preventDefault(); resize(index, widths[index] + (event.key === 'ArrowRight' ? 10 : -10)); } }}
      onPointerDown={event => { event.preventDefault(); const startX = event.clientX; const startWidth = widths[index]; const handle = event.currentTarget;
        handle.setPointerCapture(event.pointerId);
        handle.onpointermove = move => resize(index, startWidth + move.clientX - startX);
        handle.onpointerup = handle.onpointercancel = () => { handle.onpointermove = null; handle.onpointerup = null; handle.onpointercancel = null; };
      }}><span aria-hidden="true">⋮</span></div></th>;
  const clear = () => { setSearch(''); setConflicts(false); setSource(''); setStoredType(''); setCheck(''); setPage(0); };

  return <div className={styles.page}>
    <a href="#ims-content" className={styles.skip}>Skip to mapping monitor</a>
    <header className={styles.header}>
      <a href="/" className={styles.brand}>Market Explorer</a>
      <nav aria-label="Application"><a href="/">Markets</a><a href="/ims-property-type-mapping" aria-current="page">IMS property type mapping</a></nav>
      <span className={styles.readOnly}><LockKeyhole size={13} />Read-only</span>
    </header>
    <main id="ims-content" className={styles.main}>
      <div className={styles.titleRow}><div><p className={styles.eyebrow}>Read-only · IMS sold listings</p>
        <h1>IMS Property Type</h1></div>
        {!isCSV && <button className={styles.productionButton} type="button" disabled={busy || importingSnapshot} onClick={requestProductionScan}>
          <RefreshCw size={16} className={report.isFetching ? styles.spinning : ''} />
          {report.isFetching ? 'Querying Production…' : report.data ? 'Refresh from ES' : 'Query Production ES'}</button>}
      </div>

      <details className={styles.upload} open={!csvResult} aria-label="IMS CSV preview"
        onDragEnter={event => {
          if (draggedFile.current !== null || !event.dataTransfer.types.includes('Files')) return;
          event.preventDefault();
          dragDepth.current++;
          if (!busy) {
            event.currentTarget.open = true;
            setDragging(true);
          }
        }}
        onDragOver={event => {
          if (draggedFile.current === null && !event.dataTransfer.types.includes('Files')) return;
          event.preventDefault();
          event.dataTransfer.dropEffect = busy || draggedFile.current !== null ? 'none' : 'copy';
        }}
        onDragLeave={() => {
          if (draggedFile.current !== null) return;
          dragDepth.current = Math.max(0, dragDepth.current - 1);
          if (!dragDepth.current) setDragging(false);
        }}
        onDrop={event => {
          event.preventDefault();
          dragDepth.current = 0;
          setDragging(false);
          if (busy || draggedFile.current !== null || !event.dataTransfer.files.length) return;
          event.currentTarget.open = true;
          addFiles(Array.from(event.dataTransfer.files));
        }}>
        <summary className={styles.uploadSummary}>
          <h2><ChevronRight size={18} className={styles.uploadChevron} aria-hidden="true" />
            <FileSpreadsheet size={22} className={styles.uploadIcon} aria-hidden="true" />
            Preview IMS CSVs <span className={styles.uploadHint}>Drop CSVs here or expand</span>
          </h2>
        </summary>
        <p className={styles.uploadIntro}>Count source tuples and compute property types using the import workflow. No Production query or writes.</p>
        <div className={styles.uploadControls}>
          <div className={`${styles.dropZone} ${dragging && !busy ? styles.dragging : ''}`} role="region" aria-label="CSV file drop zone" aria-disabled={busy}>
            <UploadCloud size={25} aria-hidden="true" />
            <div className={styles.dropMessage}><strong>{dragging ? 'Drop to add your CSVs' : 'Drag IMS CSVs here'}</strong><span>or choose files from your computer</span></div>
            <label className={styles.filePicker}>Choose Files<input ref={fileInput} type="file" aria-label="Upload IMS CSV" accept=".csv,text/csv" multiple disabled={busy} onChange={event => {
              addFiles(Array.from(event.target.files ?? [])); event.target.value = '';
            }} /></label>
            <div className={styles.selectedFiles} aria-live="polite">
              {files.length === 0 ? <p>No file selected</p> : <ol aria-label="CSV processing order" aria-describedby="ims-csv-order">
                {files.map(({ id, file }, index) => <li key={id}
                  className={dropTarget === id ? styles.fileDropTarget : undefined}
                  onDragOver={event => {
                    if (busy || draggedFile.current === null) return;
                    event.preventDefault();
                    event.stopPropagation();
                    event.dataTransfer.dropEffect = 'move';
                    setDropTarget(draggedFile.current === id ? null : id);
                  }}
                  onDragLeave={event => {
                    if (draggedFile.current !== null && !event.currentTarget.contains(event.relatedTarget as Node | null)) {
                      setDropTarget(target => target === id ? null : target);
                    }
                  }}
                  onDrop={event => {
                    if (draggedFile.current === null) return;
                    event.preventDefault();
                    event.stopPropagation();
                    moveFile(draggedFile.current, index);
                    endFileDrag();
                  }}>
                  <div className={styles.selectedFile}>
                    {files.length > 1 && <span className={styles.fileGrip} draggable={!busy}
                      title="Drag to reorder" aria-hidden="true"
                      onDragStart={event => {
                        event.dataTransfer.setData('text/plain', file.name);
                        event.dataTransfer.effectAllowed = 'move';
                        draggedFile.current = id;
                      }}
                      onDragEnd={endFileDrag}><GripVertical size={15} /></span>}
                    <span className={styles.fileName}>{file.name}</span>
                    {files.length > 1 && <div className={styles.fileOrderButtons}>
                      <button type="button" aria-label={`Move ${file.name} up`} title="Move up"
                        disabled={busy || index === 0} onClick={() => moveFile(id, index - 1)}><ArrowUp size={14} aria-hidden="true" /></button>
                      <button type="button" aria-label={`Move ${file.name} down`} title="Move down"
                        disabled={busy || index === files.length - 1} onClick={() => moveFile(id, index + 1)}><ArrowDown size={14} aria-hidden="true" /></button>
                    </div>}
                    <button className={styles.removeFile} type="button"
                      aria-label={files.length === 1 ? 'Remove selected file' : `Remove file ${index + 1}, ${file.name}`}
                      disabled={busy} onClick={() => removeFile(index)}>Remove</button>
                  </div>
                </li>)}
              </ol>}
            </div>
          </div>
          <button className={styles.primary} type="button" disabled={!files.length || busy || importingSnapshot} onClick={() => { setActiveSource('csv'); preview.mutate(files.map(entry => entry.file)); }}>
            {preview.isPending ? 'Uploading and computing…' : 'Compute property types'}</button></div>
        <p id="ims-csv-order" className={styles.note}>Files are initially sorted by last modified, <strong>oldest at the top and newest at the bottom</strong>. Check the order using the handles or arrows. After a manual reorder, new files are added at the bottom. For duplicate listings, the last occurrence wins.</p>
        <p className={styles.note}>Batch previews require Type, SubType, Style, Province, MLSID, MLSNumber, and WrittenDate. Up to 20 files and 5 million rows combined. Files upload when you choose Compute.</p>
        {fileError && <p className={styles.error} role="alert">{fileError}</p>}
      </details>
      <div className={styles.sourceTabs} role="group" aria-label="Result source">
        <button type="button" aria-controls="ims-results" aria-pressed={isCSV} onClick={() => { setActiveSource('csv'); clear(); }}>CSV preview</button>
        <button type="button" aria-controls="ims-results" aria-pressed={!isCSV} onClick={() => { setActiveSource('elasticsearch'); clear(); }}>Production ES</button>
      </div>
      {preview.isPending && <p className={styles.progress} role="status">Uploading and computing locally on the API. Only tuple counts and classifications are returned; ES and the mapping files are not modified.</p>}
      {report.isFetching && <p className={styles.progress} role="status">ES is scanning the index and grouping exact source values. This can take a minute. Sorting and filtering the returned table will not run another ES query.</p>}
      {problem && <div className={styles.error} role="alert"><strong>{isCSV ? 'Could not compute the CSV preview.' : 'Could not complete the query.'}</strong> {problem.message}
        <p>{data ? 'The previous completed result is still shown below.' : 'If this endpoint was just added, restart FastAPI manually and try again.'} No partial result is presented as complete.</p></div>}
      <div id="ims-results" role="region" aria-label={isCSV ? 'CSV preview results' : 'Production ES results'} className={styles.results}>
      {!data && !busy && isCSV && <div className={styles.empty}>
        <p>Choose IMS CSVs to compute property types, or open a saved mapping report.</p>
        <ImsReportControls label="" busy={busy} onOpen={openSnapshot} onImportingChange={setImportingSnapshot} />
      </div>}
      {!data && !busy && !isCSV && !problem && <p className={styles.empty}>No Production result loaded. Query Production ES sparingly; each run requires confirmation.</p>}

      {data && grouped && <>
        <p className={styles.resultContext}>{isCSV ? `${openedSnapshot ? 'Saved report · ' : ''}Computed from ${resultName} · CSV preview only · Not verified against ES` : 'Production ES · Stored property types · All stored history'} · {new Date(data.read_at).toLocaleString()}</p>
        {isCSV && data.csv_input_row_count != null && <p className={styles.note}>
          {count(data.csv_input_row_count)} input rows · {count(data.csv_input_row_count - data.document_count)} duplicates removed
          {data.csv_unidentified_row_count ? ` · ${count(data.csv_unidentified_row_count)} rows without a usable listing ID were kept` : ''}
        </p>}
        {helpPosition && createPortal(<span id="mapping-help" className={styles.headerHelp} role="tooltip" style={{ left: helpPosition.left, top: helpPosition.top }}>{helpText[helpPosition.key]}</span>, document.body)}
        <dl className={styles.stats}>
          <div><dt>{isCSV ? 'Rows kept' : 'Sold records'}</dt><dd>{count(data.document_count)}</dd></div>
          <div><dt>Unique tuples</dt><dd>{count(grouped.tuple_count)}</dd></div>
          <div className={grouped.conflict_tuple_count ? styles.attention : ''}><dt>Conflicting tuples</dt><dd>{count(grouped.conflict_tuple_count)}</dd></div>
          <div><dt>{isCSV ? 'New tuples' : 'Mapping differences'}</dt><dd>{count(isCSV ? grouped.new_tuple_count || 0 : data.different_document_count)}</dd></div>
        </dl>
        {grouped.conflict_tuple_count > 0 && <div className={styles.warning} role="status"><AlertTriangle size={20} />
          <div><strong>{count(grouped.conflict_tuple_count)} {grouped.conflict_tuple_count === 1 ? 'tuple has' : 'tuples have'} more than one {isCSV ? 'computed' : 'stored'} classification{provinceView ? ' within a province' : ''}</strong>
            <p>{count(grouped.conflict_document_count)} records are associated with {grouped.conflict_tuple_count === 1 ? 'this tuple' : 'these tuples'}. Provincial rules, source conventions, or past mapping changes may explain a split; it is a review signal.</p></div>
          <button type="button" onClick={() => updateFilter(() => setConflicts(true))}>Review conflicts</button></div>}
        {(data.unexplained_document_count > 0 || data.missing_property_type_count > 0) && <p className={styles.note}>
          {count(data.unexplained_document_count)} records have no current local mapping match. {count(data.missing_property_type_count)} records have no {isCSV ? 'computed' : 'stored'} property type. Both remain included in counts.</p>}
        {(!data.sources.algorithm_cache.available || !data.sources.overrides.available) && <p className={styles.error} role="status">Local mapping comparison is incomplete because a lookup file is unavailable. ES classifications and counts are complete; see mapping file details below.</p>}
        <details className={styles.explanation}><summary>How to read mapping lineage</summary>
          <p><strong>Property type</strong> {isCSV ? 'is computed from the selected CSVs using the current import workflow. Existing saved base mappings win; only unseen tuples run through the algorithm. Computed does not mean verified in Production ES.' : 'is the classification already assigned to each sold record in Elasticsearch. Base mapping is the saved base lookup, not a fresh execution of the mapper.'} <strong>{lookupOverrideLabel}</strong> is the current override lookup.</p>
          <p>Current precedence: AB duplex rule → lookup override → base mapping. {isCSV ? 'Rows with a usable listing ID are deduplicated before grouping. The last occurrence in file and row order wins. Existing propertyType values in the CSVs are ignored.' : 'The check compares that result with the stored type, weighted by record counts. ES does not store which rule or version originally assigned a type, so this is current consistency, not proven historical provenance.'}</p>
          <p>{isCSV ? 'Tuple values are trimmed just as in the importer. Missing keys use N/A for lookup.' : 'Tuples retain exact case, punctuation, and whitespace. Missing fields are distinct from the literal string “N/A”.'} No mapping files or ES documents are changed.</p>
        </details>
        <section className={styles.tableCard} aria-label="Mapping results" aria-busy={busy}>
          <div className={styles.filters}>
            <label className={styles.search}><Search size={17} /><input aria-label="Search mappings" placeholder="Search source tuples or property types" value={search} onChange={event => updateFilter(() => setSearch(event.target.value))} /></label>
            <label>Property Type<select value={storedType} onChange={event => updateFilter(() => setStoredType(event.target.value))}>
              <option value="">All types</option>{types.map(type => <option key={type}>{type}</option>)}<option value="__missing__">Missing</option></select></label>
            <label>Current source<select value={source} onChange={event => updateFilter(() => setSource(event.target.value))}>
              <option value="">All sources</option>{Object.entries(sourceLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
            <label>Mapping check<select value={check} onChange={event => updateFilter(() => setCheck(event.target.value))}>
              <option value="">All checks</option>{Object.entries(statusLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
          </div>
          <div className={styles.tableToolbar}><label className={styles.checkbox}><input type="checkbox" checked={conflicts} onChange={event => updateFilter(() => setConflicts(event.target.checked))} />Conflicts only</label>
            {isCSV && <label className={styles.provinceSwitch} title={provinceBreakdown ? undefined : 'This report has no province breakdown'}>
              <input type="checkbox" role="switch" checked={!!provinceView} disabled={!provinceBreakdown}
                onChange={event => {
                  if (event.target.checked) setGroupByProvince(true);
                  else resetProvinceView();
                  setPage(0);
                }} />
              <span className={styles.switchTrack} aria-hidden="true" />Group by Province
            </label>}
            <span>{count(rows.length)} of {count(grouped.row_count)} grouped rows</span>
            {(search || conflicts || source || storedType || check) && <button type="button" onClick={clear}>Clear filters</button>}</div>
          <div className={styles.scroll} tabIndex={0} role="region" aria-label="Scrollable mapping table" onScroll={() => hideHelp(0)}>
            <table style={{ width: visibleColumns.reduce((total, { index }) => total + widths[index], 0) }}><caption>{isCSV ? `One row per ${provinceView ? 'province, ' : ''}normalized source tuple and computed property type. Counts cover the CSV preview only.` : 'One row per exact source tuple and stored ES property type. Counts cover all provinces and stored history.'}</caption>
              <colgroup>{visibleColumns.map(({ column, index }) => <col key={column.key} style={{ width: widths[index] }} />)}</colgroup>
              <thead><tr>{visibleColumns.map(({ column, index }) => heading(column, index))}</tr></thead>
              <tbody>{rows.slice(start, start + PAGE_SIZE).map(row => <tr key={JSON.stringify([row.Province, row.Type, row.SubType, row.Style, row.property_type])} className={row.conflict ? styles.conflictRow : undefined}>
                {provinceView && <td>{row.Province ?? <span className={styles.missing}>Unknown</span>}</td>}
                <td><RawValue value={row.Type} /></td><td><RawValue value={row.SubType} /></td><td><RawValue value={row.Style} /></td>
                <td><strong><RawValue value={row.property_type} /></strong>{row.conflict && <small className={styles.conflictBadge}><AlertTriangle size={11} />{row.property_type_count} {isCSV ? 'computed' : 'stored'} types</small>}</td>
                <td className={styles.numeric}>{count(row.count)}</td>
                <td>{row.algorithm_mapping ?? <span className={styles.missing}>No match</span>}</td>
                <td>{row.override_mapping ?? <span className={styles.missing}>None</span>}</td>
                <td><Source row={row} /></td>
                <td><span className={`${styles.status} ${styles[row.lineage_status]}`}>{statusLabels[row.lineage_status]}</span>
                  {row.different_count > 0 && <small>{count(row.different_count)} {row.different_count === 1 ? 'record differs' : 'records differ'}</small>}
                  {row.unexplained_count > 0 && <small>{count(row.unexplained_count)} unknown</small>}</td>
              </tr>)}</tbody></table>
            {rows.length === 0 && <p className={styles.noRows}>No grouped rows match these filters.</p>}
          </div>
          <div className={styles.pagination}><p>{rows.length ? `${count(start + 1)}–${count(Math.min(start + PAGE_SIZE, rows.length))}` : '0'} of {count(rows.length)} rows</p>
            <div>{isCSV && <ImsReportControls report={data} label={resultName} busy={busy} onOpen={openSnapshot} onImportingChange={setImportingSnapshot} />}
              <button type="button" disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>Previous</button>
              <span>Page {currentPage + 1} of {lastPage + 1}</span><button type="button" disabled={currentPage >= lastPage} onClick={() => setPage(currentPage + 1)}>Next</button></div></div>
        </section>
        <details className={styles.explanation}><summary>Mapping files and query details</summary>
          <p>{isCSV ? 'The API reads CSVs in chunks and deduplicates by province, MLS board, MLS number, and sale year before grouping. The production mapper is imported directly for cache-first inference and lookup overrides. Its save and audit-file hooks are disabled. The final AB exception is mirrored in the API because production embeds it inside ES-writing routines; a drift test checks both production blocks. No ES request is made, and nothing is saved to mapping files or ETL logs.' : `ES groups the entire index in ${data.scan_slices} sequential slices of one point-in-time snapshot, avoiding the gateway timeout. Only grouped counts are returned. The API joins them to these files using vectorized operations. No scalar mapper calls, dataframe apply, or listing downloads.`} This result stays in this browser until replaced or the page is closed.</p>
          {isCSV && <p>Loaded mapper version: <code className={styles.hash}>{data.rules_version}</code>. Restart FastAPI after changes to production mapper code.</p>}
          <div className={styles.lookups}><Lookup source={data.sources.algorithm_cache} label="Base lookup" /><Lookup source={data.sources.overrides} label="Lookup overrides" /></div>
        </details>
      </>}
      </div>
    </main>
  </div>;
}
