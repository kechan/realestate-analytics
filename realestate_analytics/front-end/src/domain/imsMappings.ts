export type MappingSource = 'algorithm_cache' | 'algorithm_rules' | 'manual_override' | 'provincial_rule' | 'mixed' | 'unexplained';
export type LineageStatus = 'consistent' | 'different' | 'partial' | 'unexplained' | 'computed';
export interface MappingRow {
  Type: string | null; SubType: string | null; Style: string | null;
  Province?: string | null;
  property_type: string | null; count: number;
  algorithm_mapping: string | null; override_mapping: string | null;
  algorithm_count: number; override_count: number; provincial_count: number;
  unexplained_count: number; different_count: number;
  property_type_count: number; conflict: boolean; missing_property_type: boolean;
  mapping_source: MappingSource; lineage_status: LineageStatus;
  base_origin?: 'cache' | 'algorithm' | 'missing';
}
export interface LookupSource {
  file: string; available: boolean; rows: number; duplicate_rows: number; blank_rows: number;
  sha256?: string; modified_at?: string; issue?: string;
}
export interface MappingReport {
  report_mode?: 'elasticsearch' | 'csv'; new_tuple_count?: number; rules_version?: string;
  csv_input_row_count?: number; csv_unidentified_row_count?: number;
  source_index: string; read_at: string; es_took_ms: number; scan_slices: number;
  complete: true; historical_provenance_available: false;
  document_count: number; tuple_count: number; row_count: number;
  conflict_tuple_count: number; conflict_document_count: number;
  different_document_count: number; unexplained_document_count: number;
  missing_property_type_count: number;
  sources: { algorithm_cache: LookupSource; overrides: LookupSource };
  rows: MappingRow[];
  province_breakdown?: ProvinceMappingBreakdown;
}
export interface ProvinceMappingBreakdown {
  tuple_count: number; new_tuple_count: number; row_count: number;
  conflict_tuple_count: number; conflict_document_count: number;
  rows: ProvinceMappingRow[];
}
export type ProvinceMappingRow = MappingRow & { Province: string | null };
export const lookupOverrideLabel = 'Lookup Override';
export const sourceLabels: Record<MappingSource, string> = {
  algorithm_cache: 'Base mapping', algorithm_rules: 'Algorithm (new tuple)', manual_override: lookupOverrideLabel,
  provincial_rule: 'AB duplex rule', mixed: 'Mixed sources', unexplained: 'No local match',
};
export const statusLabels: Record<LineageStatus, string> = {
  consistent: 'Consistent', different: 'Differs', partial: 'Partly unknown', unexplained: 'Unknown', computed: 'Computed',
};
export type MappingSort = 'count' | 'Province' | 'Type' | 'SubType' | 'Style' | 'property_type' | 'algorithm_mapping' | 'override_mapping' | 'mapping_source' | 'lineage_status';
const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
export function selectMappings(rows: MappingRow[], search: string, conflicts: boolean, source: string, storedType: string,
  check: string, sort: MappingSort | null, direction: 'asc' | 'desc') {
  const term = search.toLocaleLowerCase();
  return rows.filter(row => (!conflicts || row.conflict) && (!source || row.mapping_source === source)
    && (!storedType || (storedType === '__missing__' ? row.property_type === null : row.property_type === storedType))
    && (!check || row.lineage_status === check)
    && (!term || [row.Province, row.Type, row.SubType, row.Style, row.property_type, row.algorithm_mapping, row.override_mapping]
      .some(value => value != null && value.toLocaleLowerCase().includes(term))))
    .sort((a, b) => {
      if (!sort) return 0;
      const value = (row: MappingRow) => sort === 'mapping_source' ? sourceLabels[row.mapping_source]
        : sort === 'lineage_status' ? statusLabels[row.lineage_status] : row[sort];
      const x = value(a), y = value(b);
      if (x == null) return y == null ? 0 : 1;
      if (y == null) return -1;
      const comparison = typeof x === 'number' && typeof y === 'number' ? x - y
        : collator.compare(String(x), String(y));
      return direction === 'asc' ? comparison : -comparison;
    });
}
