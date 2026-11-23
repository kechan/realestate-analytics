"""Read-only IMS grouping and vectorized reconciliation with current lookups.

No mapper/ETL imports or row-wise mapping evaluation. Historical assignment
provenance is not recorded in ES; the lookup joins explain current consistency.
"""
import asyncio
import hashlib
import io
import json
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from elasticsearch import ElasticsearchException

from .repository import ESUnavailableError, UpstreamDataError
from .ims_provincial_rules import provincial_mask, AB_DUPLEX_PROPERTY_TYPE

logger = logging.getLogger(__name__)
TUPLE = ["Type", "SubType", "Style"]
GROUP = TUPLE + ["property_type"]
JOIN = ["_type", "_subtype", "_style"]
SCAN_SLICES = 16

# Text fields have no keyword subfields in PROD. Aggregate exact source strings
# once on ES, returning only grouped counts rather than downloading listings.
# The loops below run in Elasticsearch's Painless engine, not in Python.
INIT_SCRIPT = "state.groups = new HashMap();"
MAP_SCRIPT = """
def src = params._source;
String t = src['Type'] == null ? null : src['Type'].toString();
String s = src['SubType'] == null ? null : src['SubType'].toString();
String y = src['Style'] == null ? null : src['Style'].toString();
String p = src['propertyType'] == null ? null : src['propertyType'].toString();
String v = src['Province'] == null ? null : src['Province'].toString();
String key = (t == null ? '-1:' : t.length() + ':' + t)
  + (s == null ? '-1:' : s.length() + ':' + s)
  + (y == null ? '-1:' : y.length() + ':' + y)
  + (p == null ? '-1:' : p.length() + ':' + p)
  + (v == null ? '-1:' : v.length() + ':' + v);
def row = state.groups.get(key);
if (row == null) {
  state.groups.put(key, ['Type': t, 'SubType': s, 'Style': y,
    'property_type': p, 'province': v, 'count': 1L]);
} else { row.count += 1L; }
"""
REDUCE_SCRIPT = """
Map merged = new HashMap();
for (def shard : states) {
  if (shard == null) continue;
  for (def entry : shard.entrySet()) {
    def target = merged.get(entry.getKey());
    if (target == null) merged.put(entry.getKey(), new HashMap(entry.getValue()));
    else target.count += entry.getValue().count;
  }
}
return new ArrayList(merged.values());
"""


def aggregation_query(query=None):
    return {
        "size": 0, "_source": False, "track_total_hits": True,
        "timeout": "30s", "query": query or {"match_all": {}},
        "aggs": {"mapping_groups": {"scripted_metric": {
            "init_script": INIT_SCRIPT, "map_script": MAP_SCRIPT,
            "combine_script": "return state.groups;", "reduce_script": REDUCE_SCRIPT,
        }}},
    }


def _lookup(cache_dir, filename, column, output, keep_null_values=False):
    """Read a snapshot without modifying the feather file or mapper state."""
    empty = pd.DataFrame(columns=JOIN + [output])
    info = {"file": filename, "available": False, "rows": 0,
            "duplicate_rows": 0, "blank_rows": 0}
    if cache_dir is None:
        return empty, {**info, "issue": "cache_directory_not_configured"}
    path = cache_dir / filename
    try:
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
        if (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size):
            return empty, {**info, "issue": "file_changed_during_read"}
        data = pd.read_feather(io.BytesIO(raw))
        if not pd.Index(TUPLE + [column]).difference(data.columns).empty:
            return empty, {**info, "issue": "missing_columns"}
        info.update(available=True, rows=len(data),
                    duplicate_rows=int(data.duplicated(TUPLE, keep=False).sum()),
                    blank_rows=int((data[TUPLE].isna().all(axis=1) & data[column].isna()).sum()),
                    sha256=hashlib.sha256(raw).hexdigest(),
                    modified_at=datetime.fromtimestamp(after.st_mtime, timezone.utc).isoformat())
        # Match the mapper's dictionary semantics: the last duplicate wins, even
        # when its override is null. A null override is subsequently not applied.
        data = data[TUPLE + [column]].drop_duplicates(TUPLE, keep="last")
        # Cached dictionary keys are not normalized by apply_to_dataframe.
        # Preserve that behavior: null keys do not match input 'N/A' keys.
        data = data.loc[data[column].notna() | keep_null_values].rename(columns={
            "Type": "_type", "SubType": "_subtype", "Style": "_style", column: output})
        return data, info
    except Exception as exc:
        logger.warning("IMS mapping lookup unavailable: %s (%s)", filename, type(exc).__name__)
        return empty, {**info, "issue": "unreadable_file"}


def reconcile_groups(buckets, document_count, cache_dir):
    """All record processing uses merges, masks, reductions and groupby."""
    base, base_info = _lookup(cache_dir, "ims_property_type_mapping_v4_df", "Inferred", "algorithm_mapping")
    overrides, override_info = _lookup(cache_dir, "ims_property_type_lookup_df", "propertyType", "override_mapping")
    frame = pd.DataFrame(buckets, columns=GROUP + ["province", "count"])
    if not frame.empty:
        counts = pd.to_numeric(frame["count"], errors="coerce")
        if not (np.isfinite(counts) & counts.ge(1) & counts.eq(counts.round())).all():
            raise UpstreamDataError("Invalid IMS aggregation counts")
        frame["count"] = counts.astype("int64")
    if int(frame["count"].sum()) != document_count:
        raise UpstreamDataError("IMS aggregation does not cover the complete result set")
    metadata = {
        "document_count": document_count, "tuple_count": 0, "row_count": 0,
        "conflict_tuple_count": 0, "conflict_document_count": 0,
        "different_document_count": 0, "unexplained_document_count": 0,
        "missing_property_type_count": 0,
        "sources": {"algorithm_cache": base_info, "overrides": override_info},
    }
    if frame.empty:
        return {**metadata, "rows": []}

    frame[JOIN] = frame[TUPLE].fillna("N/A").to_numpy()
    frame = frame.merge(base, on=JOIN, how="left", validate="many_to_one")
    frame = frame.merge(overrides, on=JOIN, how="left", validate="many_to_one")
    provincial = provincial_mask(frame["province"], frame["SubType"])
    manual = frame["override_mapping"].notna()
    algorithm = frame["algorithm_mapping"].notna()
    frame["expected"] = np.select(
        [provincial, manual, algorithm],
        [AB_DUPLEX_PROPERTY_TYPE, frame["override_mapping"], frame["algorithm_mapping"]], default=None)
    known = frame["expected"].notna()
    frame["algorithm_count"] = np.where(~provincial & ~manual & algorithm, frame["count"], 0)
    frame["override_count"] = np.where(~provincial & manual, frame["count"], 0)
    frame["provincial_count"] = np.where(provincial, frame["count"], 0)
    frame["unexplained_count"] = np.where(~known, frame["count"], 0)
    frame["different_count"] = np.where(known & frame["property_type"].ne(frame["expected"]), frame["count"], 0)

    grouped = frame.groupby(GROUP, dropna=False, sort=False).agg(
        count=("count", "sum"), algorithm_mapping=("algorithm_mapping", "first"),
        override_mapping=("override_mapping", "first"), algorithm_count=("algorithm_count", "sum"),
        override_count=("override_count", "sum"), provincial_count=("provincial_count", "sum"),
        unexplained_count=("unexplained_count", "sum"), different_count=("different_count", "sum"),
    ).reset_index()
    grouped["property_type_count"] = grouped.groupby(TUPLE, dropna=False)["property_type"].transform("nunique", dropna=False)
    grouped["conflict"] = grouped["property_type_count"].gt(1)
    grouped["missing_property_type"] = grouped["property_type"].isna()
    kinds = grouped[["algorithm_count", "override_count", "provincial_count", "unexplained_count"]].gt(0).sum(axis=1)
    grouped["mapping_source"] = np.select(
        [kinds.gt(1), grouped["provincial_count"].gt(0), grouped["override_count"].gt(0), grouped["algorithm_count"].gt(0)],
        ["mixed", "provincial_rule", "manual_override", "algorithm_cache"], default="unexplained")
    grouped["lineage_status"] = np.select(
        [grouped["unexplained_count"].eq(grouped["count"]), grouped["different_count"].gt(0), grouped["unexplained_count"].gt(0)],
        ["unexplained", "different", "partial"], default="consistent")
    grouped = grouped.sort_values(["conflict", "count", "Type", "SubType", "Style", "property_type"],
                                  ascending=[False, False, True, True, True, True], na_position="last")
    metadata.update(
        tuple_count=len(grouped.drop_duplicates(TUPLE)), row_count=len(grouped),
        conflict_tuple_count=len(grouped.loc[grouped["conflict"]].drop_duplicates(TUPLE)),
        conflict_document_count=int(grouped.loc[grouped["conflict"], "count"].sum()),
        different_document_count=int(grouped["different_count"].sum()),
        unexplained_document_count=int(grouped["unexplained_count"].sum()),
        missing_property_type_count=int(grouped.loc[grouped["missing_property_type"], "count"].sum()),
    )
    return {**metadata, "rows": json.loads(grouped.to_json(orient="records", force_ascii=False))}


def _exact_total(result):
    if result.get("timed_out") or result.get("_shards", {}).get("failed", 0):
        raise ESUnavailableError("IMS grouping did not complete")
    total = result.get("hits", {}).get("total", {})
    if total.get("relation") != "eq":
        raise UpstreamDataError("Exact IMS document count unavailable")
    return total["value"]


async def _read_slices(client, state, slice_id=0):
    """Sequential I/O continuation; ES scans each PIT document exactly once.

    The bounded slices stay below the PROD proxy's per-request timeout. The
    Python side accumulates only aggregate buckets, never individual listings.
    """
    if slice_id == SCAN_SLICES:
        return
    body = aggregation_query()
    body.update(pit={"id": state["pit"], "keep_alive": "3m"},
                slice={"id": slice_id, "max": SCAN_SLICES})
    result = await client.search(body=body, allow_partial_search_results=False, request_timeout=45)
    state["pit"] = result.get("pit_id", state["pit"])
    state["scanned"] += _exact_total(result)
    buckets = result.get("aggregations", {}).get("mapping_groups", {}).get("value")
    if not isinstance(buckets, list):
        raise UpstreamDataError("Invalid IMS grouping response")
    state["buckets"].extend(buckets)
    state["took"] += result.get("took", 0)
    await _read_slices(client, state, slice_id + 1)


async def mapping_report(repository):
    client = repository.client
    opened = await client.open_point_in_time(index=repository.settings.ims_sold_index, keep_alive="3m")
    state = {"pit": opened["id"], "buckets": [], "scanned": 0, "took": 0}
    try:
        if opened.get("_shards", {}).get("failed", 0):
            raise ESUnavailableError("IMS snapshot could not open all shards")
        baseline = await client.search(body={"size": 0, "track_total_hits": True,
                                             "pit": {"id": state["pit"], "keep_alive": "3m"}},
                                       allow_partial_search_results=False, request_timeout=30)
        state["pit"] = baseline.get("pit_id", state["pit"])
        total = _exact_total(baseline)
        if total:
            await _read_slices(client, state)
        if state["scanned"] != total:
            raise UpstreamDataError("IMS slices do not cover the complete snapshot")
    finally:
        try:
            await client.close_point_in_time(body={"id": state["pit"]})
        except ElasticsearchException:
            logger.warning("Could not close IMS search snapshot; it will expire after 3 minutes")
    report = await asyncio.to_thread(reconcile_groups, state["buckets"], total, repository.settings.analytics_cache_dir)
    return {**report, "source_index": repository.settings.ims_sold_index,
            "read_at": datetime.now(timezone.utc).isoformat(), "es_took_ms": state["took"],
            "scan_slices": SCAN_SLICES if total else 0,
            "complete": True, "historical_provenance_available": False}
