import { lookupOverrideLabel, sourceLabels, statusLabels } from './imsMappings';
import type { LookupSource, MappingReport, MappingRow, ProvinceMappingBreakdown, ProvinceMappingRow } from './imsMappings';

const headers = ['Type', 'SubType', 'Style', 'Property Type', 'Count', 'Base mapping', lookupOverrideLabel,
  'Current source', 'Mapping check', 'Row snapshot', 'Report snapshot'];
const provinceHeaders = ['View', 'Province', ...headers];
const legacyFormat = 'market-explorer-ims-report-v1';
const provinceFormat = 'market-explorer-ims-report-v2';
type ProvinceSummary = Omit<ProvinceMappingBreakdown, 'rows'>;
type ReportMetadata = Omit<MappingReport, 'rows' | 'province_breakdown'> & { province_breakdown?: ProvinceSummary };
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);
const nonnegativeInteger = (value: unknown): value is number =>
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
const optionalString = (value: unknown) => value == null || typeof value === 'string';
const optionalCount = (value: unknown) => value == null || nonnegativeInteger(value);

function validLookup(value: unknown): value is LookupSource {
  return record(value) && typeof value.file === 'string' && typeof value.available === 'boolean'
    && ['rows', 'duplicate_rows', 'blank_rows'].every(key => nonnegativeInteger(value[key]))
    && ['sha256', 'modified_at', 'issue'].every(key => optionalString(value[key]));
}

function validRow(value: unknown): value is MappingRow {
  return record(value) && ['Type', 'SubType', 'Style', 'property_type', 'algorithm_mapping', 'override_mapping']
    .every(key => value[key] === null || typeof value[key] === 'string')
    && ['count', 'algorithm_count', 'override_count', 'provincial_count', 'unexplained_count', 'different_count',
      'property_type_count'].every(key => nonnegativeInteger(value[key]))
    && typeof value.conflict === 'boolean' && typeof value.missing_property_type === 'boolean'
    && typeof value.mapping_source === 'string' && Object.hasOwn(sourceLabels, value.mapping_source)
    && typeof value.lineage_status === 'string' && Object.hasOwn(statusLabels, value.lineage_status)
    && (value.base_origin == null || ['cache', 'algorithm', 'missing'].some(origin => origin === value.base_origin));
}

function validProvinceRow(row: MappingRow): row is ProvinceMappingRow {
  return row.Province === null || typeof row.Province === 'string';
}

function validMetadata(value: unknown): value is ReportMetadata {
  if (!record(value)) return false;
  const breakdown = value.province_breakdown;
  return value.report_mode === 'csv' && value.complete === true
    && value.historical_provenance_available === false && typeof value.source_index === 'string'
    && typeof value.read_at === 'string' && Number.isFinite(Date.parse(value.read_at))
    && ['es_took_ms', 'scan_slices', 'document_count', 'tuple_count', 'row_count', 'conflict_tuple_count',
      'conflict_document_count', 'different_document_count', 'unexplained_document_count', 'missing_property_type_count']
      .every(key => nonnegativeInteger(value[key]))
    && ['csv_input_row_count', 'csv_unidentified_row_count', 'new_tuple_count'].every(key => optionalCount(value[key]))
    && optionalString(value.rules_version)
    && record(value.sources) && validLookup(value.sources.algorithm_cache) && validLookup(value.sources.overrides)
    && (breakdown === undefined || (record(breakdown)
      && ['row_count', 'tuple_count', 'new_tuple_count', 'conflict_tuple_count', 'conflict_document_count']
        .every(key => nonnegativeInteger(breakdown[key]))));
}

const quote = (value: string | number | null) => '"' + String(value ?? '').replaceAll('"', '""') + '"';

// Spreadsheet-safe display cells. The snapshot columns preserve exact source values for re-import.
function spreadsheetText(value: string | null): string | null {
  return value !== null && (/^\s*[=+\-@＝＋－＠]/u.test(value) || /^[\t\r\n]/.test(value)) ? "'" + value : value;
}

export function exportImsReportCsv(report: MappingReport, label: string) {
  const { rows, province_breakdown: breakdown, ...metadata } = report;
  const { rows: provincialRows, ...summary } = breakdown || { rows: [] };
  const snapshot = JSON.stringify({ format: breakdown ? provinceFormat : legacyFormat, label,
    report: breakdown ? { ...metadata, province_breakdown: summary } : metadata });
  const serializeRow = (row: MappingRow, view: string, reportSnapshot: string) => [
    ...(breakdown ? [view, view === 'By Province' ? spreadsheetText(row.Province ?? 'Unknown') : ''] : []),
    spreadsheetText(row.Type), spreadsheetText(row.SubType), spreadsheetText(row.Style),
    spreadsheetText(row.property_type), row.count, spreadsheetText(row.algorithm_mapping),
    spreadsheetText(row.override_mapping), sourceLabels[row.mapping_source], statusLabels[row.lineage_status],
    JSON.stringify(row), reportSnapshot,
  ].map(quote).join(',');
  const lines = rows.map((row, index) => serializeRow(row, 'Combined', index === 0 ? snapshot : ''));
  const provinceLines = provincialRows.map(row => serializeRow(row, 'By Province', ''));
  return '\ufeff' + [(breakdown ? provinceHeaders : headers).map(quote).join(','), ...lines, ...provinceLines].join('\r\n') + '\r\n';
}

const invalidReport = () => new Error('Choose an unmodified IMS mapping report CSV created by Export CSV.');

function* csvRecords(text: string) {
  const input = text.replace(/^\ufeff/, '');
  const cell = /(?:"((?:[^"]|"")*)"|([^",\r\n]*))(,|\r\n|\n|\r|$)/gy;
  let cursor = 0;
  let row: string[] = [];
  while (cursor < input.length) {
    const match = cell.exec(input);
    if (!match || cell.lastIndex <= cursor) throw invalidReport();
    cursor = cell.lastIndex;
    row.push(match[1] === undefined ? match[2] : match[1].replaceAll('""', '"'));
    if (match[3] !== ',') {
      yield row;
      row = [];
    }
  }
  if (row.length) throw invalidReport();
}

export function importImsReportCsv(text: string): { report: MappingReport; label: string } {
  const records = csvRecords(text);
  const header = records.next().value;
  const hasProvince = header?.length === provinceHeaders.length;
  const expected = hasProvince ? provinceHeaders : headers;
  if (!header || header.length !== expected.length || !expected.every((value, index) =>
    value === header[index] || (value === lookupOverrideLabel && header[index] === 'Manual override'))) {
    throw invalidReport();
  }
  const rows: MappingRow[] = [];
  const provincialRows: ProvinceMappingRow[] = [];
  let label: string | undefined;
  let metadata: ReportMetadata | undefined;
  try {
    for (const cells of records) {
      if (cells.length !== expected.length) throw invalidReport();
      const [rowSnapshot, reportSnapshot] = cells.slice(-2);
      if (!metadata) {
        const snapshot: unknown = JSON.parse(reportSnapshot);
        if (!record(snapshot) || snapshot.format !== (hasProvince ? provinceFormat : legacyFormat)
          || typeof snapshot.label !== 'string' || !validMetadata(snapshot.report)
          || !!snapshot.report.province_breakdown !== hasProvince) throw invalidReport();
        label = snapshot.label;
        metadata = snapshot.report;
      } else if (reportSnapshot) {
        throw invalidReport();
      }
      const row: unknown = JSON.parse(rowSnapshot);
      if (!validRow(row)) throw invalidReport();
      if (!hasProvince || cells[0] === 'Combined') rows.push(row);
      else if (cells[0] === 'By Province' && validProvinceRow(row)) provincialRows.push(row);
      else throw invalidReport();
    }
  } catch {
    throw invalidReport();
  }
  if (!metadata || label === undefined) throw invalidReport();
  const { province_breakdown: summary, ...reportMetadata } = metadata;
  const total = (group: MappingRow[]) => group.reduce((sum, row) => sum + row.count, 0);
  if (rows.length !== metadata.row_count || total(rows) !== metadata.document_count
    || (summary && (provincialRows.length !== summary.row_count || total(provincialRows) !== metadata.document_count))) {
    throw invalidReport();
  }
  const report: MappingReport = { ...reportMetadata, rows };
  if (summary) report.province_breakdown = { ...summary, rows: provincialRows };
  return { report, label };
}
