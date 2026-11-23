"""Public read-only IMS monitor response contract."""
from typing import List, Literal, Optional

from pydantic import BaseModel


class MappingLookup(BaseModel):
    file: str
    available: bool
    rows: int
    duplicate_rows: int
    blank_rows: int
    sha256: Optional[str] = None
    modified_at: Optional[str] = None
    issue: Optional[str] = None


class MappingSources(BaseModel):
    algorithm_cache: MappingLookup
    overrides: MappingLookup


class MappingRow(BaseModel):
    Type: Optional[str]
    SubType: Optional[str]
    Style: Optional[str]
    property_type: Optional[str]
    count: int
    algorithm_mapping: Optional[str]
    override_mapping: Optional[str]
    algorithm_count: int
    override_count: int
    provincial_count: int
    unexplained_count: int
    different_count: int
    property_type_count: int
    conflict: bool
    missing_property_type: bool
    mapping_source: Literal["algorithm_cache", "algorithm_rules", "manual_override", "provincial_rule", "mixed", "unexplained"]
    lineage_status: Literal["consistent", "different", "partial", "unexplained", "computed"]
    base_origin: Optional[Literal["cache", "algorithm", "missing"]] = None


class ProvinceMappingRow(MappingRow):
    Province: Optional[str]


class ProvinceMappingBreakdown(BaseModel):
    tuple_count: int
    new_tuple_count: int
    row_count: int
    conflict_tuple_count: int
    conflict_document_count: int
    rows: List[ProvinceMappingRow]


class MappingReport(BaseModel):
    report_mode: Literal["elasticsearch", "csv"] = "elasticsearch"
    new_tuple_count: Optional[int] = None
    csv_input_row_count: Optional[int] = None
    csv_unidentified_row_count: Optional[int] = None
    rules_version: Optional[str] = None
    source_index: str
    read_at: str
    es_took_ms: int
    scan_slices: int
    complete: Literal[True]
    historical_provenance_available: Literal[False]
    document_count: int
    tuple_count: int
    row_count: int
    conflict_tuple_count: int
    conflict_document_count: int
    different_document_count: int
    unexplained_document_count: int
    missing_property_type_count: int
    sources: MappingSources
    rows: List[MappingRow]
    province_breakdown: Optional[ProvinceMappingBreakdown] = None
