import { fireEvent, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import ImsMappingMonitor from './ImsMappingMonitor';
import { exportImsReportCsv, importImsReportCsv } from '../domain/imsReportCsv';
import { renderClient } from '../test/helpers';
import { response } from '../test/fixtures';
import type { MappingReport, MappingRow } from '../domain/imsMappings';

const base: MappingRow = { Type: 'Residential', SubType: 'Detached', Style: '2 Storey', property_type: 'DETACHED',
  count: 10, algorithm_mapping: 'DETACHED', override_mapping: null, algorithm_count: 10, override_count: 0,
  provincial_count: 0, unexplained_count: 0, different_count: 0, property_type_count: 1, conflict: false,
  missing_property_type: false, mapping_source: 'algorithm_cache', lineage_status: 'consistent' };
const report: MappingReport = { document_count: 18, tuple_count: 2, row_count: 3, conflict_tuple_count: 1,
  conflict_document_count: 8, different_document_count: 3, unexplained_document_count: 0,
  missing_property_type_count: 0, source_index: 'rlp_ims_sold_listing_current', read_at: '2026-11-04T12:00:00Z',
  es_took_ms: 5000, scan_slices: 16, complete: true, historical_provenance_available: false,
  sources: { algorithm_cache: { file: 'ims_property_type_mapping_v4_df', available: true, rows: 100, duplicate_rows: 0, blank_rows: 0 },
    overrides: { file: 'ims_property_type_lookup_df', available: true, rows: 2, duplicate_rows: 0, blank_rows: 0 } },
  rows: [base,
    { ...base, SubType: 'Recreational', Style: 'Park Model', property_type: 'OTHER', count: 5,
      algorithm_mapping: 'MOBILE_MINI', override_mapping: 'OTHER', algorithm_count: 0, override_count: 5,
      mapping_source: 'manual_override', conflict: true, property_type_count: 2 },
    { ...base, SubType: 'Recreational', Style: 'Park Model', property_type: 'MOBILE_MINI', count: 3,
      algorithm_mapping: 'MOBILE_MINI', override_mapping: 'OTHER', algorithm_count: 0, override_count: 3,
      mapping_source: 'manual_override', conflict: true, property_type_count: 2, lineage_status: 'different', different_count: 3 },
  ] };
async function load(data: MappingReport = report) {
  const fetch = vi.fn(async () => response(data)); vi.stubGlobal('fetch', fetch);
  const user = userEvent.setup();
  renderClient(<ImsMappingMonitor />);
  await user.click(screen.getByRole('button', { name: 'Production ES' }));
  await user.click(screen.getByRole('button', { name: 'Query Production ES' }));
  await screen.findByRole('table');
  return { user, fetch };
}

describe('IMS mapping monitor', () => {
  beforeEach(() => { vi.stubGlobal('confirm', vi.fn(() => true)); });
  it('requires explicit confirmation for both the initial scan and refresh, with no request on Cancel', async () => {
    const fetch = vi.fn(async () => response(report)); vi.stubGlobal('fetch', fetch);
    const confirm = vi.mocked(window.confirm).mockReturnValue(false);
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    await user.click(screen.getByRole('button', { name: 'Production ES' }));
    await user.click(screen.getByRole('button', { name: 'Query Production ES' }));
    expect(confirm).toHaveBeenCalledWith(expect.stringContaining('significant CPU and disk load'));
    expect(fetch).not.toHaveBeenCalled();
    confirm.mockReturnValue(true);
    await user.click(screen.getByRole('button', { name: 'Query Production ES' }));
    await screen.findByRole('table');
    expect(fetch).toHaveBeenCalledTimes(1);
    confirm.mockReturnValue(false);
    await user.click(screen.getByRole('button', { name: 'Refresh from ES' }));
    expect(confirm).toHaveBeenCalledTimes(3);
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('only queries explicitly and makes a confirmed GET for Production', async () => {
    const fetch = vi.fn(async (_url: RequestInfo | URL, _options?: RequestInit) => response(report)); vi.stubGlobal('fetch', fetch);
    renderClient(<ImsMappingMonitor />);
    expect(fetch).not.toHaveBeenCalled();
    expect(screen.queryByText(/import result/i)).toBeNull();
    expect(screen.getByLabelText('Upload IMS CSV')).toBeTruthy();
    expect(screen.queryByRole('button', { name: 'Query Production ES' })).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Production ES' }));
    expect(fetch).not.toHaveBeenCalled();
    expect(window.confirm).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: 'Query Production ES' }));
    await screen.findByRole('table');
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][0]).toBe('/api/v1/ims-mappings');
    expect(fetch.mock.calls[0][1]).toMatchObject({ method: 'GET' });
    await userEvent.click(screen.getByRole('button', { name: 'CSV preview' }));
    expect(screen.queryByRole('button', { name: 'Refresh from ES' })).toBeNull();
  });
  it('selects a CSV without uploading, then computes on demand without any ES request', async () => {
    const csvReport: MappingReport = { ...report, report_mode: 'csv', source_index: 'uploaded_csv', new_tuple_count: 1,
      scan_slices: 0, conflict_tuple_count: 0, different_document_count: 0, rows: [{ ...base, mapping_source: 'algorithm_rules', lineage_status: 'computed', base_origin: 'algorithm' }] };
    const fetch = vi.fn(async (_url: RequestInfo | URL, _options?: RequestInit) => response(csvReport)); vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    const file = new File(['Type,SubType,Style,Province\nResidential,Detached,2 Storey,ON\n'], 'IMS-drop.csv', { type: 'text/csv' });
    await user.upload(screen.getByLabelText('Upload IMS CSV'), file);
    expect(fetch).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Compute property types' }));
    await screen.findByRole('table');
    const panel = screen.getByLabelText('IMS CSV preview') as HTMLDetailsElement;
    expect(panel.open).toBe(false);
    await user.click(screen.getByText('Preview IMS CSVs', { exact: true }));
    expect(panel.open).toBe(true);
    expect(screen.getByText('IMS-drop.csv')).toBeTruthy();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][0]).toBe('/api/v1/ims-mappings/preview');
    expect(fetch.mock.calls[0][1]).toMatchObject({ method: 'POST', body: file, headers: { 'content-type': 'text/csv' } });
    expect(window.confirm).not.toHaveBeenCalled();
    expect(screen.getByText(/Computed from IMS-drop.csv/)).toBeTruthy();
    expect(within(screen.getByRole('table')).getByText('Computed')).toBeTruthy();
    expect(screen.getByText(/Counts cover the CSV preview only/, { selector: 'caption' })).toBeTruthy();
    await user.click(screen.getByRole('button', { name: 'Production ES' }));
    expect(screen.queryByRole('table')).toBeNull();
    await user.click(screen.getByRole('button', { name: 'CSV preview' }));
    expect(screen.getByRole('table')).toBeTruthy();
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('opens an exported report locally and saves every grouped row despite table filters', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    const write = vi.fn(async (_data: Blob) => {});
    const close = vi.fn(async () => {});
    const picker = vi.fn(async () => ({ createWritable: async () => ({ write, close }) }));
    vi.stubGlobal('showSaveFilePicker', picker);
    const csvReport: MappingReport = { ...report, report_mode: 'csv', new_tuple_count: 1,
      rows: [{ ...base, Type: '=Formula', SubType: '', Style: 'Quoted "style",\ncontinued', override_mapping: null }, ...report.rows.slice(1)] };
    const csv = exportImsReportCsv(csvReport, '13 CSV files');
    expect(csv.split('\r\n')[0]).toContain('"Lookup Override"');
    expect(csv).toContain('"\'=Formula"');
    expect(importImsReportCsv(csv)).toEqual({ report: csvReport, label: '13 CSV files' });
    const legacyCsv = csv.replaceAll('"Lookup Override"', '"Manual override"');
    expect(importImsReportCsv(legacyCsv)).toEqual({ report: csvReport, label: '13 CSV files' });
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    await user.upload(screen.getByLabelText('Open an exported IMS report'), new File([csv], 'saved.csv', { type: 'text/csv' }));
    await screen.findByText(/Saved report · Computed from 13 CSV files/);
    expect(screen.getByRole<HTMLInputElement>('switch', { name: 'Group by Province' }).disabled).toBe(true);
    expect(['Rows kept', 'Unique tuples', 'Conflicting tuples', 'New tuples']
      .map(label => screen.getByText(label).nextElementSibling?.textContent)).toEqual(['18', '2', '1', '1']);
    await user.type(screen.getByRole('textbox', { name: 'Search mappings' }), 'no match');
    expect(screen.getByText('No grouped rows match these filters.')).toBeTruthy();
    await user.click(screen.getByRole('button', { name: 'Export CSV' }));
    expect(picker).toHaveBeenCalledWith(expect.objectContaining({ suggestedName: 'ims-mapping-report-2026-11-04.csv' }));
    expect(close).toHaveBeenCalledTimes(1);
    expect(importImsReportCsv(await write.mock.calls[0][0].text())).toEqual({ report: csvReport, label: '13 CSV files' });
    expect(fetch).not.toHaveBeenCalled();

    await user.upload(screen.getByLabelText('Open an exported IMS report'),
      new File(['Type,SubType,Style,Province\nResidential,Detached,Bungalow,ON\n'], 'original.csv', { type: 'text/csv' }));
    expect((await screen.findByRole('alert')).textContent).toContain('unmodified IMS mapping report CSV');
    expect(screen.getByText(/Saved report · Computed from 13 CSV files/)).toBeTruthy();
    expect(fetch).not.toHaveBeenCalled();
  });
  it('switches between saved combined and provincial groups without recomputing', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    const provincialReport: MappingReport = {
      ...report, report_mode: 'csv', new_tuple_count: 0,
      province_breakdown: {
        tuple_count: 3, new_tuple_count: 0, row_count: 3,
        conflict_tuple_count: 0, conflict_document_count: 0,
        rows: report.rows.map((row, index) => ({
          ...row, Province: ['QC', 'AB', 'ON'][index], conflict: false, property_type_count: 1,
        })),
      },
    };
    const csv = exportImsReportCsv(provincialReport, '3 CSV files');
    expect(csv.split('\r\n')[0]).toContain('"View","Province"');
    expect(csv).toContain('"By Province","QC"');
    expect(importImsReportCsv(csv)).toEqual({ report: provincialReport, label: '3 CSV files' });
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    await user.upload(screen.getByLabelText('Open an exported IMS report'), new File([csv], 'saved.csv', { type: 'text/csv' }));
    const toggle = await screen.findByRole<HTMLInputElement>('switch', { name: 'Group by Province' });
    expect(toggle.disabled).toBe(false);
    expect(toggle.checked).toBe(false);
    expect(screen.queryByRole('button', { name: 'Province' })).toBeNull();
    expect(screen.getByText('Conflicting tuples').nextElementSibling?.textContent).toBe('1');
    await user.click(toggle);
    expect(screen.getByRole('button', { name: 'Province' })).toBeTruthy();
    expect(within(screen.getByRole('table')).getByText('QC')).toBeTruthy();
    expect(screen.getByText('Conflicting tuples').nextElementSibling?.textContent).toBe('0');
    expect(screen.getByText('Rows kept').nextElementSibling?.textContent).toBe('18');
    await user.click(toggle);
    expect(screen.queryByRole('button', { name: 'Province' })).toBeNull();
    expect(screen.getByText('Conflicting tuples').nextElementSibling?.textContent).toBe('1');
    expect(fetch).not.toHaveBeenCalled();
  });
  it('shows actionable CSV errors and retains the previous completed CSV result', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(response({ ...report, report_mode: 'csv' }))
      .mockResolvedValueOnce(response({ detail: 'Missing required columns: Province' }, 422)); vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    await user.upload(screen.getByLabelText('Upload IMS CSV'), new File(['Type,SubType,Style,Province\nA,B,C,ON\n'], 'drop.csv', { type: 'text/csv' }));
    await user.click(screen.getByRole('button', { name: 'Compute property types' }));
    await screen.findByRole('table');
    await user.click(screen.getByText('Preview IMS CSVs', { exact: true }));
    await user.click(screen.getByRole('button', { name: 'Compute property types' }));
    const alert = await screen.findByRole('alert');
    expect(alert.textContent).toContain('Missing required columns: Province');
    expect(screen.getByRole('table')).toBeTruthy();
    expect(alert.textContent).toContain('previous completed result');
  });
  it('removes a selected file without uploading and allows selecting it again', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    const input = screen.getByLabelText('Upload IMS CSV');
    const file = new File(['Type,SubType,Style,Province\nA,B,C,ON\n'], 'preview.csv', { type: 'text/csv' });
    const compute = screen.getByRole<HTMLButtonElement>('button', { name: 'Compute property types' });

    await user.upload(input, file);
    expect(screen.getByText('preview.csv')).toBeTruthy();
    expect(compute.disabled).toBe(false);
    await user.click(screen.getByRole('button', { name: 'Remove selected file' }));
    expect(screen.getByText('No file selected')).toBeTruthy();
    expect(compute.disabled).toBe(true);
    expect(document.activeElement).toBe(input);

    await user.upload(input, file);
    expect(screen.getByText('preview.csv')).toBeTruthy();
    expect(compute.disabled).toBe(false);
    expect(fetch).not.toHaveBeenCalled();
  });
  it('uploads dropped and picked files in the order set by dragging and the arrows', async () => {
    const fetch = vi.fn(async (_url: RequestInfo | URL, _options?: RequestInit) =>
      response({ ...report, report_mode: 'csv', csv_input_row_count: 22 })); vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    const zone = screen.getByRole('region', { name: 'CSV file drop zone' });
    const optionsForFile = { type: 'text/csv', lastModified: 1 };
    const old = new File(['older data'], 'old.csv', optionsForFile);
    const amend = new File(['amended data'], 'amend.csv', optionsForFile);
    const newest = new File(['newest data'], 'newest.csv', optionsForFile);
    fireEvent.drop(zone, { dataTransfer: { files: [newest, amend] } });
    await user.upload(screen.getByLabelText('Upload IMS CSV'), old);
    const order = screen.getByRole('list', { name: 'CSV processing order' });
    const filenames = () => within(order).getAllByRole('listitem').map(item => item.textContent);
    expect(filenames()).toEqual(['newest.csvRemove', 'amend.csvRemove', 'old.csvRemove']);
    const firstRow = within(order).getAllByRole('listitem')[0];
    const dataTransfer = { setData: vi.fn() };
    fireEvent.dragStart(within(order).getAllByTitle('Drag to reorder')[2], { dataTransfer });
    fireEvent.dragOver(firstRow, { dataTransfer });
    fireEvent.drop(firstRow, { dataTransfer });
    expect(filenames()).toEqual(['old.csvRemove', 'newest.csvRemove', 'amend.csvRemove']);
    await user.click(screen.getByRole('button', { name: 'Move amend.csv up' }));
    expect(filenames()).toEqual(['old.csvRemove', 'amend.csvRemove', 'newest.csvRemove']);
    await user.click(screen.getByRole('button', { name: 'Remove file 2, amend.csv' }));
    expect(fetch).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Compute property types' }));
    await screen.findByRole('table');
    expect(fetch).toHaveBeenCalledTimes(1);
    const options = fetch.mock.calls[0][1];
    expect(options).toMatchObject({ method: 'POST', headers: {
      'content-type': 'application/vnd.ims.csv-batch', 'x-ims-csv-sizes': `${old.size},${newest.size}`,
    } });
    expect(await (options?.body as Blob).text()).toBe('older datanewest data');
    expect(screen.getByText(/Computed from 2 CSV files/)).toBeTruthy();
    expect(screen.getByText(/22 input rows · 4 duplicates removed/)).toBeTruthy();
  });
  it('suggests an oldest-first order and preserves manual corrections on later drops', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    const zone = screen.getByRole('region', { name: 'CSV file drop zone' });
    const file = (name: string, lastModified: number) => new File(['data'], name, { lastModified });
    fireEvent.drop(zone, { dataTransfer: { files: [file('new.csv', 300), file('old.csv', 100)] } });
    const order = screen.getByRole('list', { name: 'CSV processing order' });
    const names = () => within(order).getAllByRole('listitem').map(row => row.textContent);
    expect(names()).toEqual(['old.csvRemove', 'new.csvRemove']);
    await user.upload(screen.getByLabelText('Upload IMS CSV'), file('middle.csv', 200));
    expect(names()).toEqual(['old.csvRemove', 'middle.csvRemove', 'new.csvRemove']);
    await user.click(screen.getByRole('button', { name: 'Move new.csv up' }));
    fireEvent.drop(zone, { dataTransfer: { files: [file('another.csv', 150)] } });
    expect(names()).toEqual(['old.csvRemove', 'new.csvRemove', 'middle.csvRemove', 'another.csvRemove']);
    expect(fetch).not.toHaveBeenCalled();
  });
  it('allows selecting files above the former byte cap without uploading them', () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    renderClient(<ImsMappingMonitor />);
    const file = new File(['small fixture'], 'large.csv', { type: 'text/csv' });
    Object.defineProperty(file, 'size', { value: 1024 ** 3 });
    fireEvent.drop(screen.getByRole('region', { name: 'CSV file drop zone' }), { dataTransfer: { files: [file] } });
    expect(screen.getByText('large.csv')).toBeTruthy();
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Compute property types' }).disabled).toBe(false);
    expect(fetch).not.toHaveBeenCalled();
  });
  it('opens an empty collapsed panel for file drags and accepts the drop', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); renderClient(<ImsMappingMonitor />);
    const summary = screen.getByText('Preview IMS CSVs', { exact: true });
    const panel = screen.getByLabelText('IMS CSV preview') as HTMLDetailsElement;
    await user.click(summary);
    expect(panel.open).toBe(false);
    fireEvent.dragEnter(summary, { dataTransfer: { types: ['text/plain'] } });
    expect(panel.open).toBe(false);
    const file = new File(['test'], 'dropped.csv', { type: 'text/csv' });
    const dataTransfer = { types: ['Files'], files: [file] };
    fireEvent.dragEnter(summary, { dataTransfer });
    expect(panel.open).toBe(true);
    fireEvent.drop(summary, { dataTransfer });
    expect(screen.getByText('dropped.csv')).toBeTruthy();
    expect(fetch).not.toHaveBeenCalled();
  });
  it('filters conflicts and searches locally without any extra ES calls', async () => {
    const { user, fetch } = await load();
    await user.click(screen.getByRole('button', { name: 'Review conflicts' }));
    expect(screen.getAllByText('2 stored types')).toHaveLength(2);
    expect(within(screen.getByRole('table')).queryByText('Detached')).toBeNull();
    await user.type(screen.getByRole('textbox', { name: 'Search mappings' }), 'Park Model');
    await user.selectOptions(screen.getByLabelText('Mapping check'), 'different');
    expect(within(screen.getByRole('table')).getAllByRole('row')).toHaveLength(2);
    expect(screen.getByText('3 records differ')).toBeTruthy();
    expect(fetch).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole('button', { name: 'Clear filters' }));
    expect(within(screen.getByRole('table')).getAllByRole('row')).toHaveLength(4);
  });
  it('sorts stored types and distinguishes the lookup override from the ES result', async () => {
    const { user, fetch } = await load();
    await user.click(screen.getByRole('button', { name: 'Property Type' }));
    const rows = within(screen.getByRole('table')).getAllByRole('row').slice(1);
    expect(within(rows[1]).getByText('MOBILE_MINI', { selector: 'strong span' })).toBeTruthy();
    expect(within(rows[1]).getByText('OTHER')).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Lookup Override' })).toBeTruthy();
    expect(screen.getByRole('option', { name: 'Lookup Override' })).toBeTruthy();
    await user.click(within(rows[1]).getByText('Lookup Override', { selector: 'summary' }));
    expect(within(rows[1]).getByText('AB duplex rule')).toBeTruthy();
    await user.click(screen.getByText('How to read mapping lineage'));
    expect(screen.getByText(/not proven historical provenance/)).toBeTruthy();
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('paginates the grouped rows locally and preserves exact whitespace and null values', async () => {
    const rows = Array.from({ length: 52 }, (_, i) => ({ ...base, Style: `Style ${i}`, count: i + 1 }));
    rows[51] = { ...base, Type: null, SubType: 'Detached ', Style: 'N/A', count: 1 };
    const { user, fetch } = await load({ ...report, rows, row_count: rows.length });
    expect(within(screen.getByRole('table')).getAllByRole('row')).toHaveLength(51);
    await user.click(screen.getByRole('button', { name: 'Next' }));
    expect(within(screen.getByRole('table')).getAllByRole('row')).toHaveLength(3);
    expect(screen.getByText('Whitespace')).toBeTruthy();
    expect(within(screen.getByRole('table')).getByText('Missing')).toBeTruthy();
    expect(within(screen.getByRole('table')).getByText('N/A')).toBeTruthy();
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('cycles numeric and natural text sorting back to the original order and resizes with the keyboard', async () => {
    const rows = [ { ...base, Style: 'Style 10', count: 10 }, { ...base, Style: 'Style 2', count: 2 }, { ...base, Style: 'Style 1', count: 20 } ];
    const { user, fetch } = await load({ ...report, rows });
    const values = () => within(screen.getByRole('table')).getAllByRole('row').slice(1).map(row => within(row).getAllByRole('cell')[2].textContent);
    const style = screen.getByRole('button', { name: 'Style' });
    await user.click(style); expect(values()).toEqual(['Style 1', 'Style 2', 'Style 10']);
    await user.click(style); expect(values()).toEqual(['Style 10', 'Style 2', 'Style 1']);
    await user.click(style); expect(values()).toEqual(['Style 10', 'Style 2', 'Style 1']);
    const count = screen.getByRole('button', { name: 'Count' });
    await user.click(count); expect(values()).toEqual(['Style 2', 'Style 10', 'Style 1']);
    await user.click(count); expect(values()).toEqual(['Style 1', 'Style 10', 'Style 2']);
    await user.click(count); expect(values()).toEqual(['Style 10', 'Style 2', 'Style 1']);
    const handle = screen.getByRole('separator', { name: 'Resize Type column' });
    handle.focus(); await user.keyboard('{ArrowRight}');
    expect(handle.getAttribute('aria-valuenow')).toBe('160');
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('reports API errors and never presents a partial scan as data', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => response({ detail: 'unavailable' }, 503)));
    renderClient(<ImsMappingMonitor />);
    await userEvent.click(screen.getByRole('button', { name: 'Production ES' }));
    await userEvent.click(screen.getByRole('button', { name: 'Query Production ES' }));
    await screen.findByRole('alert');
    expect(screen.queryByRole('table')).toBeNull();
    expect(screen.getByText(/No partial result/)).toBeTruthy();
  });
});
