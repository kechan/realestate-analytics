"""CSV-only mapping preview using the production mapper through a read-only adapter."""
import csv
import codecs
import json
import hashlib
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from .ims_mapping_monitor import JOIN, TUPLE, _lookup
from .ims_mapper_adapter import map_groups, mapper_version, apply_provincial_exception

MAX_CSV_FILES = 20
REQUIRED_COLUMNS = TUPLE + ["Province"]
IDENTITY_COLUMNS = ["MLSID", "MLSNumber", "WrittenDate"]
MAX_CSV_ROWS = 5_000_000


class InvalidCSV(ValueError):
    pass


class MappingFilesUnavailable(RuntimeError):
    pass


def _clean(series):
    # Identical tuple trimming to the importer; preserve missing values.
    return series.where(series.isna(), series.astype(str).str.strip())


def _csv_chunks(stream, require_identity):
    """Parse complete rows, retaining only the mapping and identity columns."""
    try:
        # Incremental decoding also supports Python 3.9's SpooledTemporaryFile,
        # which does not implement the full TextIOWrapper readable() interface.
        raw_headers = next(csv.reader(codecs.iterdecode(stream, "utf-8-sig"), strict=True))
        headers = pd.Index(raw_headers).str.strip()
        stream.seek(0)
        use_identity = require_identity or headers.isin(IDENTITY_COLUMNS).any()
        columns = REQUIRED_COLUMNS + (IDENTITY_COLUMNS if use_identity else [])
        missing = pd.Index(columns).difference(headers)
        if not missing.empty:
            raise InvalidCSV("Missing required columns: " + ", ".join(missing))
        if headers.duplicated().any():
            raise InvalidCSV("CSV column names must be unique after trimming whitespace")
        dtypes = {raw: "string" for raw, name in zip(raw_headers, headers) if name in IDENTITY_COLUMNS}
        with pd.read_csv(stream, encoding="utf-8-sig", chunksize=100_000, dtype=dtypes) as reader:
            for frame in reader:
                frame.columns = headers
                yield frame[columns].copy(), use_identity
    except InvalidCSV:
        raise
    except (ValueError, UnicodeError, csv.Error, StopIteration) as exc:
        raise InvalidCSV("Could not parse this file. Use a UTF-8, comma-separated CSV with Type, SubType, Style, and Province columns.") from exc


def _keep_last(frame):
    key = frame["_listing_key"]
    return frame.loc[~(key.notna() & key.duplicated(keep="last"))]


def _count_csvs(streams):
    input_rows = 0
    chunks = []
    for number, stream in enumerate(streams, 1):
        file_rows = 0
        try:
            for frame, use_identity in _csv_chunks(stream, require_identity=len(streams) > 1):
                file_rows += len(frame)
                input_rows += len(frame)
                if input_rows > MAX_CSV_ROWS:
                    raise InvalidCSV("CSV preview supports up to 5 million rows combined")
                if use_identity:
                    # The bulk importer embeds this document ID in its write loop.
                    # Preserve original MLS identifier text, including leading zeros.
                    year = frame.pop("WrittenDate").str.extract(r"^(\d{4})-\d{2}-\d{2}(?:T| |$)", expand=False)
                    province = frame["Province"].astype("string").str.lower()
                    board = frame.pop("MLSID").str.lower()
                    listing = frame.pop("MLSNumber").str.lower()
                    key = province + "-" + board + "-" + listing + "-" + year
                    identified = province.str.strip().ne("") & board.str.strip().ne("") & listing.str.strip().ne("")
                    frame["_listing_key"] = key.where(identified)
                    frame = _keep_last(frame)
                chunks.append(frame)
            if not file_rows:
                raise InvalidCSV("The CSV contains no data rows")
        except InvalidCSV as exc:
            raise InvalidCSV(f"CSV {number}: {exc}" if len(streams) > 1 else str(exc)) from exc
    values = pd.concat(chunks, ignore_index=True, copy=False)
    chunks.clear()
    if "_listing_key" in values:
        values = _keep_last(values)
        unidentified = int(values.pop("_listing_key").isna().sum())
    else:
        unidentified = input_rows
    values[TUPLE] = values[TUPLE].transform(_clean)
    counts = values.groupby(REQUIRED_COLUMNS, dropna=False, sort=False).size().reset_index(name="count")
    return counts, len(values), input_rows, unidentified


def csv_preview(stream, cache_dir):
    return csv_batch_preview([stream], cache_dir)


def _group_mappings(values, tuple_fields):
    rows = values.groupby(tuple_fields + ["property_type"], dropna=False, sort=False).agg(
        count=("count", "sum"), algorithm_mapping=("algorithm_mapping", "first"),
        override_mapping=("override_mapping", "first"), base_origin=("base_origin", "first"),
        algorithm_count=("algorithm_count", "sum"), override_count=("override_count", "sum"),
        provincial_count=("provincial_count", "sum"), unexplained_count=("unexplained_count", "sum"),
    ).reset_index()
    rows["property_type_count"] = rows.groupby(tuple_fields, dropna=False)["property_type"].transform("nunique", dropna=False)
    rows["conflict"] = rows["property_type_count"].gt(1)
    rows["missing_property_type"] = rows["property_type"].isna()
    kinds = rows[["algorithm_count", "override_count", "provincial_count", "unexplained_count"]].gt(0).sum(axis=1)
    rows["mapping_source"] = np.select([
        kinds.gt(1), rows["provincial_count"].gt(0), rows["override_count"].gt(0),
        rows["algorithm_count"].gt(0) & rows["base_origin"].eq("algorithm"), rows["algorithm_count"].gt(0),
    ], ["mixed", "provincial_rule", "manual_override", "algorithm_rules", "algorithm_cache"], default="unexplained")
    rows["lineage_status"] = np.where(rows["missing_property_type"], "unexplained", "computed")
    rows["different_count"] = 0
    return rows.sort_values(["conflict", "count"], ascending=[False, False])


def _grouped_result(rows, tuple_fields):
    conflicts = rows.loc[rows["conflict"]]
    return {
        "row_count": len(rows),
        "conflict_tuple_count": len(conflicts.drop_duplicates(tuple_fields)),
        "conflict_document_count": int(conflicts["count"].sum()),
        "rows": json.loads(rows.to_json(orient="records", force_ascii=False)),
    }


def csv_batch_preview(streams, cache_dir):
    """Deduplicate listings before grouping and running the read-only mapper."""
    counts, document_count, input_rows, unidentified = _count_csvs(streams)

    _, base_info = _lookup(cache_dir, "ims_property_type_mapping_v4_df", "Inferred", "algorithm_mapping", keep_null_values=True)
    _, override_info = _lookup(cache_dir, "ims_property_type_lookup_df", "propertyType", "override_mapping")
    if not base_info["available"] or not override_info["available"]:
        raise MappingFilesUnavailable("CSV preview needs both current mapping files to reproduce cache-first and override behavior")
    counts[JOIN] = counts[TUPLE].fillna("N/A").to_numpy()
    keys = counts[JOIN].drop_duplicates().reset_index(drop=True)
    if len(keys) > 100_000:
        raise InvalidCSV("CSV preview supports up to 100,000 unique source tuples; split this file into smaller previews")
    mapper, original_base = map_groups(counts, cache_dir)
    # Mapping files must still match the versions that were identified above.
    # There is no write, repair, cache update, or fallback classifier here.
    try:
        if (hashlib.sha256((cache_dir / base_info["file"]).read_bytes()).hexdigest() != base_info["sha256"] or
                hashlib.sha256((cache_dir / override_info["file"]).read_bytes()).hexdigest() != override_info["sha256"]):
            raise MappingFilesUnavailable("Mapping files changed during computation; retry the preview")
    except OSError as exc:
        raise MappingFilesUnavailable("Mapping files became unavailable during computation") from exc
    rename = {"Type": "_type", "SubType": "_subtype", "Style": "_style"}
    original_keys = original_base[TUPLE].rename(columns=rename).drop_duplicates()
    joined = keys.merge(original_keys, on=JOIN, how="left", indicator=True, validate="many_to_one")
    unseen = joined.loc[joined["_merge"].eq("left_only"), JOIN].copy()
    values = mapper.df
    # The production mapper has ALREADY assigned propertyType, including manual
    # overrides. These joins annotate lineage only; they never choose that value.
    inferred = mapper.cached_mapping_df[TUPLE + ["Inferred"]].rename(columns={**rename, "Inferred": "algorithm_mapping"}).drop_duplicates(JOIN, keep="last")
    manual = mapper.override_lookup_df[TUPLE + ["propertyType"]].rename(columns={**rename, "propertyType": "override_mapping"}).drop_duplicates(JOIN, keep="last")
    values = values.merge(inferred, on=JOIN, how="left", validate="many_to_one")
    values = values.merge(manual, on=JOIN, how="left", validate="many_to_one")
    values = values.merge(unseen.assign(base_origin="algorithm"), on=JOIN, how="left", validate="many_to_one")
    values["base_origin"] = np.where(values["base_origin"].notna(), values["base_origin"],
                                     np.where(values["algorithm_mapping"].notna(), "cache", "missing"))
    # Sole duplicated business rule; its justification and source locations are
    # documented beside apply_provincial_exception, in TODO.md, and in AGENTS.md.
    provincial = apply_provincial_exception(values)
    known = values["propertyType"].notna()
    manual_used = ~provincial & values["override_mapping"].notna() & known
    base_used = ~provincial & ~manual_used & known
    values["algorithm_count"] = np.where(base_used, values["count"], 0)
    values["override_count"] = np.where(manual_used, values["count"], 0)
    values["provincial_count"] = np.where(provincial, values["count"], 0)
    values["unexplained_count"] = np.where(~known, values["count"], 0)
    values = values.rename(columns={"propertyType": "property_type"})
    rows = _group_mappings(values, TUPLE)
    result = {"document_count": document_count, "csv_input_row_count": input_rows,
            "csv_unidentified_row_count": unidentified, "tuple_count": len(keys), **_grouped_result(rows, TUPLE),
            "different_document_count": 0, "unexplained_document_count": int(rows["unexplained_count"].sum()),
            "missing_property_type_count": int(rows.loc[rows["missing_property_type"], "count"].sum()),
            "sources": {"algorithm_cache": base_info, "overrides": override_info},
            "report_mode": "csv", "source_index": "uploaded_csv", "read_at": datetime.now(timezone.utc).isoformat(),
            "es_took_ms": 0, "scan_slices": 0, "complete": True, "historical_provenance_available": False,
            "new_tuple_count": len(unseen), "rules_version": mapper_version()}
    # Normalize the display grouping after classification so the importer's
    # exact province matching for its AB rule stays intact.
    values["Province"] = values["Province"].astype("string").str.strip().str.upper().replace("", pd.NA)
    if values["Province"].notna().any():
        province_fields = ["Province", *TUPLE]
        province_keys = ["Province", *JOIN]
        result["province_breakdown"] = {
            **_grouped_result(_group_mappings(values, province_fields), province_fields),
            "tuple_count": len(values[province_keys].drop_duplicates()),
            "new_tuple_count": len(values.loc[values["base_origin"].eq("algorithm"), province_keys].drop_duplicates()),
        }
    return result
