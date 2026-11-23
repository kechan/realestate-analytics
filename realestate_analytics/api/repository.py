"""Read-only Elasticsearch access, independent of ETL data modules."""
import math
import logging
import re
from typing import Optional

from elasticsearch import NotFoundError, ElasticsearchException

from .catalog import METRICS
from .geo_cache import GeoDocument, LazyGeoCache, merge_missing, missing, provenance

logger = logging.getLogger(__name__)

MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class UpstreamDataError(Exception):
    """The upstream document does not satisfy the API contract."""


class MissingDocument(Exception):
    """The requested published document does not exist."""


class ESUnavailableError(RuntimeError):
    """The primary data service is unavailable; readiness must remain unhealthy."""


def number(value, integer=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise UpstreamDataError("Invalid numeric value")
    if integer and (value < 0 or int(value) != value):
        raise UpstreamDataError("Invalid count")
    return int(value) if integer else value


class Repository:
    def __init__(self, client, settings, geo_cache=None, geometry_processor=None):
        self.client = client
        self.settings = settings
        self.geo_cache = geo_cache if geo_cache is not None else LazyGeoCache(settings.geo_collection_path)
        self.geometry_processor = geometry_processor

    @staticmethod
    def _note_cache(record, fields, es_missing=False):
        record.setdefault("fallback_fields", set()).update(fields)
        record["es_missing"] = record.get("es_missing", False) or es_missing

    async def _cached_base(self, geog_id, source=None):
        cached = await self.geo_cache.base(geog_id)
        if cached is None:
            return None
        merged, fields = merge_missing(source or {}, cached)
        record = self._record({"_id": geog_id, "_source": merged})
        self._note_cache(record, fields, source is None)
        return record

    async def _merge_detail(self, geog_id, source, include_geometry=False, full=False, es_missing=False):
        data = (source or {}).get("data") or {}
        meta = (source or {}).get("meta") or {}
        if not isinstance(data, dict) or not isinstance(meta, dict):
            raise UpstreamDataError("Invalid geography detail document")
        level_match = re.match(r"^g(\d+)_", geog_id)
        needs_parent = level_match and int(level_match.group(1)) < 40 and missing(data.get("parent"))
        needs_geometry = include_geometry and not meta.get("geometry")
        needs_fields = full and (any(missing(data.get(k)) for k in
            ("id", "name", "province", "intro", "profiles", "centroid", "bounding_box")) or
            any(missing((data.get("profiles") or {}).get(k)) for k in ("transport", "services", "character", "housing")) or
            any(missing((data.get("centroid") or {}).get(k)) for k in ("lat", "lng")) or
            any(missing((data.get("bounding_box") or {}).get(k)) for k in ("west", "east", "south", "north")))
        if not (source is None or needs_parent or needs_geometry or needs_fields):
            return GeoDocument(source)
        cached_geometry_needed = bool(include_geometry and needs_geometry)
        cached = await self.geo_cache.detail(geog_id, cached_geometry_needed)
        if cached is None:
            return GeoDocument(source) if source is not None else None
        # Parent-only reads must not accidentally pull descriptions or polygons.
        if not full:
            cached = {"data": {"parent": (cached.get("data") or {}).get("parent")},
                      **({"meta": {"geometry": cached.get("meta", {}).get("geometry")}} if cached_geometry_needed else {})}
        merged, fields = merge_missing(source or {}, cached)
        return GeoDocument(merged, fields, es_missing=es_missing)

    async def verify(self):
        if not await self.client.ping():
            raise ESUnavailableError("Production ES is unavailable")
        nodes = await self.client.cat.nodes(format="json")
        if not any("prod" in n.get("name", "").lower() for n in nodes):
            raise RuntimeError("ES production identity could not be verified")
        for index in (self.settings.metrics_index, self.settings.geo_index,
                      self.settings.geo_details_index):
            if not await self.client.indices.exists(index=index):
                raise ESUnavailableError("Required ES index is unavailable: " + index)

    async def metrics(self, geog_id, property_type, names, start, end):
        try:
            result = await self.client.get(index=self.settings.metrics_index,
                                           id=f"{geog_id}_{property_type}")
        except NotFoundError as exc:
            raise MissingDocument from exc
        source = result.get("_source")
        if not isinstance(source, dict) or not isinstance(source.get("metrics"), dict):
            raise UpstreamDataError("Invalid metric document")
        metrics = source["metrics"]
        series = {}
        for name in names:
            if name == "current_metrics":
                continue
            raw = metrics.get(name)
            if raw is None:
                series[name] = {"available": False, "latest_available_month": None, "data": []}
                continue
            if not isinstance(raw, list):
                raise UpstreamDataError("Invalid metric series")
            points = []
            seen = set()
            integer = METRICS[name][2] == "integer"
            for item in raw:
                if not isinstance(item, dict) or not isinstance(item.get("month"), str) or not MONTH.fullmatch(item["month"]):
                    raise UpstreamDataError("Invalid metric period")
                month = item["month"]
                if month in seen:
                    raise UpstreamDataError("Duplicate metric period")
                seen.add(month)
                points.append({"month": month, "value": number(item.get("value"), integer)})
            points.sort(key=lambda p: p["month"])
            latest = points[-1]["month"] if points else None
            series[name] = {"available": True, "latest_available_month": latest,
                            "data": [p for p in points if (not start or p["month"] >= start)
                                     and (not end or p["month"] <= end)]}
        response = {"geog_id": geog_id, "property_type": property_type,
                "source_index": self.settings.metrics_index,
                "document_updated_at": source.get("last_updated"), "series": series}
        if "current_metrics" in names:
            current = metrics.get("current_metrics")
            if current is None:
                current = {}
            if not isinstance(current, dict):
                raise UpstreamDataError("Invalid current metrics")
            price, count = current.get("median_asking_price"), current.get("new_listings")
            response["current_metrics"] = {
                "median_asking_price": number(price) if price is not None else None,
                "new_listings": number(count, True) if count is not None else None}
        return response

    GEO_FIELDS = ["localLogicId", "level", "province", "city", "name", "level10En",
                  "level20En", "level30En", "level35En", "level40En"]
    # Canonical display name is determined by each document's level, then city/name.
    # A query-time sort keeps offset pagination consistent without changing ES mappings.
    GEO_SORT = [
        {"_script": {"type": "string", "order": "asc", "script": {"lang": "painless", "source": """
            String field = 'level' + doc['level'].value + 'En.keyword';
            String name = 'Unknown';
            for (String candidate : [field, 'city.keyword', 'name.keyword']) {
                if (doc.containsKey(candidate) && doc[candidate].size() > 0 &&
                    doc[candidate].value.length() > 0) {
                    name = doc[candidate].value;
                    break;
                }
            }
            return name.toLowerCase(Locale.ROOT);
        """}}},
        {"localLogicId.keyword": "asc"},
    ]

    async def _unfiltered_page(self, limit, offset):
        body = {"query": {"match_all": {}}, "_source": self.GEO_FIELDS,
                "sort": self.GEO_SORT, "track_total_hits": True, "from": offset, "size": limit}
        if offset + limit <= 10000:
            result = await self.client.search(index=self.settings.geo_index,
                allow_partial_search_results=False, body=body)
        else:
            # ES's default result window is 10,000. Traverse sort cursors for deeper
            # pages, retaining only one batch, rather than silently truncating results.
            result = await self.client.search(index=self.settings.geo_index,
                allow_partial_search_results=False,
                body={"query": {"match_all": {}}, "size": 0, "track_total_hits": True})
            total = result["hits"]["total"]["value"]
            if offset >= total:
                return [], total
            cursor = None
            remaining = offset
            while remaining:
                batch_body = {"query": {"match_all": {}}, "_source": False,
                              "sort": self.GEO_SORT, "size": min(1000, remaining)}
                if cursor is not None:
                    batch_body["search_after"] = cursor
                batch = await self.client.search(index=self.settings.geo_index,
                    allow_partial_search_results=False, body=batch_body)
                hits = batch["hits"]["hits"]
                if not hits:
                    return [], total
                cursor = hits[-1]["sort"]
                remaining -= len(hits)
            page_body = {k: v for k, v in body.items() if k != "from"}
            if cursor is not None:
                page_body["search_after"] = cursor
            result = await self.client.search(index=self.settings.geo_index,
                allow_partial_search_results=False, body=page_body)
        total = result["hits"]["total"]
        if not isinstance(total, dict) or total.get("relation") != "eq":
            raise UpstreamDataError("Incomplete geography total")
        return [await self._record_with_fallback(hit) for hit in result["hits"]["hits"]], total["value"]

    @staticmethod
    def _record(hit):
        source = hit.get("_source", {})
        gid, level = hit.get("_id"), source.get("level")
        if not gid or not isinstance(level, int):
            raise UpstreamDataError("Invalid geography document")
        name = source.get(f"level{level}En") or source.get("city") or source.get("name") or "Unknown"
        return {"geog_id": gid, "name": name, "level": level, "province": source.get("province"),
                "source": source, "parent": None, "parent_status": "unresolved"}

    async def _record_with_fallback(self, hit):
        try:
            record = self._record(hit)
        except UpstreamDataError:
            cached = await self._cached_base(hit.get("_id"), hit.get("_source", {}))
            if cached is None:
                raise
            return cached
        if not record["province"] or record["name"] == "Unknown":
            return await self._cached_base(record["geog_id"], record["source"]) or record
        return record

    async def _search_all(self, index, query, fields, scroll_from_start=False):
        # Avoid creating a scroll for the common case of a small filtered result.
        body = {"query": query, "_source": fields, "sort": ["_doc"], "track_total_hits": True}
        size = 2000 if scroll_from_start else 1000
        options = {"scroll": "1m"} if scroll_from_start else {}
        response = await self.client.search(index=index, size=size,
            allow_partial_search_results=False, body=body, **options)
        total = response.get("hits", {}).get("total")
        if not scroll_from_start and isinstance(total, dict) and total.get("relation") == "eq" and total.get("value", 0) <= size:
            return response.get("hits", {}).get("hits", [])
        if not scroll_from_start:
            response = await self.client.search(index=index, scroll="1m", size=size,
                allow_partial_search_results=False, body=body)
        scroll_id = response.get("_scroll_id")
        hits = []
        try:
            while True:
                batch = response.get("hits", {}).get("hits", [])
                if not batch:
                    break
                hits.extend(batch)
                if len(batch) < size:
                    break
                if not scroll_id:
                    raise UpstreamDataError("Missing ES scroll identifier")
                response = await self.client.scroll(scroll_id=scroll_id, scroll="1m")
                scroll_id = response.get("_scroll_id", scroll_id)
        finally:
            if scroll_id:
                await self.client.clear_scroll(scroll_id=scroll_id)
        return hits

    @staticmethod
    def _wildcard(value):
        # Escape the user's literal substring so '*' and '?' do not become operators.
        return value.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?")

    async def _base_by_id(self, geog_id):
        try:
            hit = await self.client.get(index=self.settings.geo_index, id=geog_id,
                                        _source=[*self.GEO_FIELDS, "longId"])
        except ElasticsearchException as exc:
            if not isinstance(exc, NotFoundError):
                logger.warning("Geography ES base read failed: %s", type(exc).__name__)
            cached = await self._cached_base(geog_id)
            if cached is not None:
                return cached
            if isinstance(exc, NotFoundError):
                raise MissingDocument from exc
            raise
        return await self._record_with_fallback(hit)

    async def _detail_by_id(self, geog_id, include_geometry=False, full=False):
        try:
            params = {} if include_geometry else {"_source_excludes": ["meta.geometry"]}
            if not full and not include_geometry:
                params = {"_source_includes": ["data.parent"]}
            hit = await self.client.get(index=self.settings.geo_details_index, id=geog_id, **params)
            source = hit.get("_source", {})
        except ElasticsearchException as exc:
            if not isinstance(exc, NotFoundError):
                logger.warning("Geography ES detail read failed: %s", type(exc).__name__)
            cached = await self._merge_detail(geog_id, None, include_geometry, full, es_missing=True)
            if cached is not None or isinstance(exc, NotFoundError):
                return cached
            raise
        return await self._merge_detail(geog_id, source, include_geometry, full)

    async def _resolve_parents(self, records, details):
        try:
            await self._resolve_parents_es(records, details)
        except (ElasticsearchException, UpstreamDataError):
            logger.warning("ES geography parent resolution failed")
            snapshot = await self.geo_cache.snapshot()
            if snapshot is None:
                raise
            for record in records:
                if record["parent"]:
                    continue
                detail = details.get(record["geog_id"]) or {}
                parent = (detail.get("data") or {}).get("parent")
                if parent and "data.parent" not in getattr(detail, "fallback_fields", ()):
                    record["parent"], record["parent_status"] = parent, "source"
                else:
                    parent = snapshot.parent(record["geog_id"])
                    if parent:
                        record["parent"], record["parent_status"] = parent, "cache"
                        self._note_cache(record, {"parent_geog_id"})

    async def _resolve_parents_es(self, records, details):
        # Resolve explicit source parents with one batched base-index lookup.
        parent_ids = {p for d in details.values() if isinstance(d, dict) and
                      "data.parent" not in getattr(d, "fallback_fields", ())
                      for p in [(d.get("data") or {}).get("parent")]
                      if isinstance(p, str) and p}
        existing = set()
        if parent_ids:
            ids = sorted(parent_ids)
            response = await self.client.mget(index=self.settings.geo_index,
                body={"ids": ids}, _source=False)
            docs = response.get("docs", [])
            if len(docs) != len(ids) or any(d.get("error") for d in docs):
                raise UpstreamDataError("Incomplete parent lookup")
            existing = {d["_id"] for d in docs if d.get("found")}
            for gid in parent_ids - existing:
                if await self.geo_cache.base(gid) is not None:
                    existing.add(gid)
        needed = {}
        for record in records:
            detail = details.get(record["geog_id"]) or {}
            parent = (detail.get("data") or {}).get("parent")
            if parent and "data.parent" in getattr(details.get(record["geog_id"]), "fallback_fields", ()):
                record["parent"], record["parent_status"] = parent, "cache"
                self._note_cache(record, {"parent_geog_id"})
                continue
            if isinstance(parent, str) and parent:
                if parent in existing:
                    record["parent"], record["parent_status"] = parent, "source"
                continue
            if record["level"] >= 40:
                record["parent_status"] = "root"
                continue
            for level in (20, 30, 35, 40):
                name = record["source"].get(f"level{level}En")
                if level > record["level"] and isinstance(name, str) and record["province"]:
                    needed[(record["province"], level, name.casefold())] = name
        # One ES multi-search batches unique candidate names across all returned records.
        matches = {}
        keys = list(needed)
        if keys:
            body = []
            for province, level, folded in keys:
                body.extend([{"index": self.settings.geo_index}, {"size": 1000,
                    "_source": self.GEO_FIELDS,
                    "query": {"bool": {"filter": [
                        {"term": {"level": level}},
                        {"term": {"province.keyword": {"value": province, "case_insensitive": True}}}],
                        "should": [
                            {"match_phrase": {f"level{level}En": needed[(province, level, folded)]}},
                            {"match_phrase": {"city": needed[(province, level, folded)]}},
                            {"match_phrase": {"name": needed[(province, level, folded)]}}],
                        "minimum_should_match": 1}}}])
            response = await self.client.msearch(body=body, max_concurrent_searches=5)
            if len(response.get("responses", [])) != len(keys):
                raise UpstreamDataError("Incomplete parent search")
            for key, result in zip(keys, response["responses"]):
                if result.get("error") or result.get("_shards", {}).get("failed", 0):
                    raise UpstreamDataError("Failed parent search")
                total = result.get("hits", {}).get("total", {})
                if isinstance(total, dict) and (total.get("relation") != "eq" or total.get("value", 0) > 1000):
                    raise UpstreamDataError("Too many parent candidates")
                found = [await self._record_with_fallback(h) for h in result.get("hits", {}).get("hits", [])]
                matches[key] = [r["geog_id"] for r in found
                                if r["province"] == key[0] and r["name"].casefold() == key[2]]
        for record in records:
            if record["parent_status"] != "unresolved":
                continue
            detail = details.get(record["geog_id"]) or {}
            if (detail.get("data") or {}).get("parent"):
                continue
            for level in (20, 30, 35, 40):
                name = record["source"].get(f"level{level}En")
                if level <= record["level"] or not isinstance(name, str) or not record["province"]:
                    continue
                ids = matches.get((record["province"], level, name.casefold()), [])
                if len(ids) == 1:
                    record["parent"], record["parent_status"] = ids[0], "inferred"
                    break
                if len(ids) > 1:
                    break
        for record in records:
            if record["parent_status"] != "unresolved":
                continue
            detail = details.get(record["geog_id"]) or {}
            if (detail.get("data") or {}).get("parent"):
                continue  # A nonempty ES parent link must never be replaced.
            snapshot = await self.geo_cache.snapshot()
            parent = snapshot.parent(record["geog_id"]) if snapshot else None
            if parent:
                record["parent"], record["parent_status"] = parent, "cache"
                self._note_cache(record, {"parent_geog_id"})

    async def _details_for(self, records, batch_size=500):
        details = {}
        ids = [r["geog_id"] for r in records]
        for offset in range(0, len(ids), batch_size):
            batch = ids[offset:offset + batch_size]
            try:
                result = await self.client.mget(index=self.settings.geo_details_index,
                                                 body={"ids": batch}, _source_includes=["data.parent"])
                docs = result.get("docs", [])
                if len(docs) != len(batch) or any(d.get("error") for d in docs):
                    raise UpstreamDataError("Incomplete geography details")
            except (ElasticsearchException, UpstreamDataError):
                logger.warning("ES geography parent batch read failed")
                if await self.geo_cache.snapshot() is None:
                    raise
                for gid in batch:
                    # A failed parent lookup must not discard current ES base
                    # fields just because this older snapshot lacks a new geo.
                    details[gid] = await self._merge_detail(gid, None, es_missing=True)
                continue
            for doc in docs:
                details[doc["_id"]] = await self._merge_detail(doc["_id"],
                    doc.get("_source", {}) if doc.get("found") else None,
                    es_missing=not doc.get("found"))
        return details

    @staticmethod
    def _matches_geo(record, query, province, level, parent_id):
        return ((not query or query.casefold() in record["name"].casefold()) and
                (not province or (record["province"] or "").casefold() == province.casefold()) and
                (level is None or record["level"] == level) and
                (not parent_id or record["parent"] == parent_id))

    def _snapshot_records(self, snapshot, query, province, level, parent_id):
        records = []
        for gid in snapshot.by_id:
            record = self._record({"_id": gid, "_source": snapshot.base(gid)})
            record["parent"] = snapshot.parent(gid)
            record["parent_status"] = "cache" if record["parent"] else (
                "root" if record["level"] >= 40 else "unresolved")
            if self._matches_geo(record, query, province, level, parent_id):
                self._note_cache(record, set(record["source"]), es_missing=True)
                records.append(record)
        return records

    async def list_geographies(self, query, province, level, parent_id, limit, offset):
        try:
            page, total = await self._list_geographies_es(query, province, level, parent_id, limit, offset)
        except (ElasticsearchException, MissingDocument, UpstreamDataError):
            logger.warning("ES geography search unavailable; trying snapshot")
            snapshot = await self.geo_cache.snapshot()
            if snapshot is None:
                raise
            if parent_id and parent_id not in snapshot.by_id:
                raise
            records = self._snapshot_records(snapshot, query, province, level, parent_id)
            records.sort(key=lambda r: (r["name"].lower(), r["geog_id"]))
            for record in records:
                record["es_query_failed"] = True
            return (records[offset:] if limit is None else records[offset:offset + limit]), len(records)
        if total or page:
            return page, total  # Never replace or expand a nonempty ES result set.
        snapshot = await self.geo_cache.snapshot()
        if snapshot is None:
            return page, total
        records = self._snapshot_records(snapshot, query, province, level, parent_id)
        if not records:
            return page, total
        # An empty relationship search may hide real children. Re-read only the
        # matching cached IDs so their current ES fields still take precedence.
        merged_records = []
        for offset_in_batch in range(0, len(records), 500):
            batch = records[offset_in_batch:offset_in_batch + 500]
            try:
                response = await self.client.mget(index=self.settings.geo_index,
                    body={"ids": [r["geog_id"] for r in batch]}, _source_includes=self.GEO_FIELDS)
                docs = response.get("docs", [])
                if len(docs) != len(batch) or any(d.get("error") for d in docs):
                    raise UpstreamDataError("Incomplete fallback candidate read")
            except (ElasticsearchException, UpstreamDataError):
                logger.warning("ES fallback candidate read failed")
                merged_records.extend(batch)
                continue
            for doc, record in zip(docs, batch):
                if doc.get("_id") != record["geog_id"]:
                    raise UpstreamDataError("Mismatched fallback candidate")
                if doc.get("found"):
                    source, fields = merge_missing(doc.get("_source", {}), record["source"])
                    current = self._record({"_id": record["geog_id"], "_source": source})
                    self._note_cache(current, fields)
                    merged_records.append(current)
                else:
                    merged_records.append(record)
        details = await self._details_for(merged_records)
        await self._resolve_parents(merged_records, details)
        records = [r for r in merged_records if self._matches_geo(r, query, province, level, parent_id)]
        records.sort(key=lambda r: (r["name"].lower(), r["geog_id"]))
        return (records[offset:] if limit is None else records[offset:offset + limit]), len(records)

    async def _list_geographies_es(self, query, province, level, parent_id, limit, offset):
        if not (query or province or parent_id) and level is None and limit is not None:
            page, total = await self._unfiltered_page(limit, offset)
            details = await self._details_for(page)
            await self._resolve_parents(page, details)
            return page, total
        filters = []
        if province:
            filters.append({"term": {"province.keyword": {"value": province, "case_insensitive": True}}})
        if level is not None:
            filters.append({"term": {"level": level}})
        if query:
            # Keyword wildcard's case_insensitive flag handles ASCII only.
            # Include case variants so common accented searches retain case-insensitivity.
            patterns = {"*" + self._wildcard(value) + "*" for value in (query, query.lower(), query.upper())}
            fields = ["city.keyword", "name.keyword"] + [f"level{n}En.keyword" for n in (10, 20, 30, 35, 40)]
            filters.append({"bool": {"should": [{"wildcard": {field: {"value": pattern, "case_insensitive": True}}}
                                                 for field in fields for pattern in sorted(patterns)], "minimum_should_match": 1}})
        if parent_id:
            parent = await self._base_by_id(parent_id)
            # Explicit parent links live in a separate index; find them there first.
            linked = await self._search_all(self.settings.geo_details_index,
                {"term": {"data.parent.keyword": parent_id}}, False)
            ids = [hit["_id"] for hit in linked]
            possible = [{"ids": {"values": ids}}] if ids else []
            if parent["province"]:
                possible.append({"bool": {"filter": [
                    {"term": {"province.keyword": parent["province"]}},
                    {"range": {"level": {"lt": parent["level"]}}},
                    {"wildcard": {f"level{parent['level']}En.keyword":
                        {"value": self._wildcard(parent["name"]), "case_insensitive": True}}}]}})
            if not possible:
                return [], 0
            filters.append({"bool": {"should": possible, "minimum_should_match": 1}})
        es_query = {"bool": {"filter": filters}} if filters else {"match_all": {}}
        hits = await self._search_all(self.settings.geo_index, es_query, self.GEO_FIELDS)
        records = [await self._record_with_fallback(hit) for hit in hits]
        records = [r for r in records if (not query or query.casefold() in r["name"].casefold())
                   and (not province or (r["province"] or "").casefold() == province.casefold())
                   and (level is None or r["level"] == level)]
        records.sort(key=lambda r: (r["name"].lower(), r["geog_id"]))
        if parent_id:
            details = await self._details_for(records)
            await self._resolve_parents(records, details)
            records = [r for r in records if r["parent"] == parent_id]
        total = len(records)
        page = records[offset:] if limit is None else records[offset:offset + limit]
        if not parent_id:
            details = await self._details_for(page)
            await self._resolve_parents(page, details)
        return page, total

    async def get_geography(self, geog_id, include_geometry=False, full=False):
        import asyncio
        record, detail = await asyncio.gather(self._base_by_id(geog_id),
            self._detail_by_id(geog_id, include_geometry=include_geometry, full=full))
        await self._resolve_parents([record], {geog_id: detail})
        return record, detail

    async def province_children(self, province, include_geometry=False):
        """Province navigation roots, not every city or descendant in a province.

        Provinces are virtual navigation containers. Published parent links win;
        unlinked city/region records remain accessible directly under a province.
        Only city-and-larger metadata is fetched, then only root geometries.
        """
        try:
            hits = await self._search_all(self.settings.geo_index, {"bool": {"filter": [
                {"term": {"province.keyword": province}}, {"range": {"level": {"gte": 30}}}
            ]}}, self.GEO_FIELDS, scroll_from_start=True)
            records = [await self._record_with_fallback(hit) for hit in hits]
        except ElasticsearchException:
            logger.warning("ES province navigation unavailable; trying snapshot")
            records = None
        if not records:
            snapshot = await self.geo_cache.snapshot()
            if snapshot is None:
                if records is None:
                    raise ESUnavailableError("Province navigation unavailable")
                return []
            failed = records is None
            records = [r for r in self._snapshot_records(snapshot, None, province, None, None)
                       if r["level"] >= 30]
            if failed:
                for record in records:
                    record["es_query_failed"] = True
        records = [r for r in records if r["province"] == province and r["level"] >= 30]
        # All potential parents are already in this small metadata cohort. Avoid
        # one extra search per candidate name or fetching neighbourhood metadata.
        details = await self._details_for(records, batch_size=2000) if records and not records[0].get("es_query_failed") else {}
        by_name = {}
        for record in records:
            by_name.setdefault((record["level"], record["name"].casefold()), []).append(record["geog_id"])
        levels = sorted({r["level"] for r in records})
        for record in records:
            detail = details.get(record["geog_id"]) or {}
            parent = (detail.get("data") or {}).get("parent")
            if parent and not isinstance(parent, str):
                raise UpstreamDataError("Invalid province parent link")
            if parent:
                record["parent"] = parent
                cached = "data.parent" in getattr(detail, "fallback_fields", ())
                record["parent_status"] = "cache" if cached else "source"
                if cached:
                    self._note_cache(record, {"parent_geog_id"})
            elif not record["parent"]:
                for level in levels:
                    name = record["source"].get(f"level{level}En")
                    if level <= record["level"] or not isinstance(name, str):
                        continue
                    matches = by_name.get((level, name.casefold()), [])
                    if len(matches) == 1:
                        record["parent"], record["parent_status"] = matches[0], "inferred"
                        break
                    if len(matches) > 1:
                        break
                if not record["parent"] and record["level"] >= 40:
                    record["parent_status"] = "root"
        roots = sorted((r for r in records if not r["parent"]),
                       key=lambda r: (r["name"].casefold(), r["geog_id"]))
        if not include_geometry:
            return [await self.feature(r) for r in roots]
        boundaries = await self.navigation_boundaries(
            [r["geog_id"] for r in roots],
            es_query_failed=bool(roots and roots[0].get("es_query_failed")))
        by_id = {f["properties"]["geog_id"]: f for f in boundaries}
        features = []
        for record in roots:
            boundary = by_id[record["geog_id"]]
            self._note_cache(record, boundary["provenance"]["fallback_fields"])
            feature = await self.feature(record)
            feature["geometry"] = boundary["geometry"]
            feature["properties"]["geometry_status"] = boundary["properties"]["geometry_status"]
            features.append(feature)
        return features

    async def navigation_boundaries(self, geog_ids, es_query_failed=False):
        """Read only requested polygons, without repeating hierarchy discovery."""
        import asyncio
        ids = list(dict.fromkeys(geog_ids))
        semaphore = asyncio.Semaphore(3)

        async def batch_read(batch):
            docs = {}
            async with semaphore:
                if not es_query_failed:
                    try:
                        result = await self.client.mget(index=self.settings.geo_details_index,
                            body={"ids": batch}, _source_includes=["meta.geometry"])
                        items = result.get("docs", [])
                        if (len(items) != len(batch) or any(d.get("error") for d in items)
                                or {d.get("_id") for d in items} != set(batch)):
                            raise UpstreamDataError("Incomplete navigation boundaries")
                        docs = {d["_id"]: d.get("_source", {}) for d in items if d.get("found")}
                    except ElasticsearchException:
                        logger.warning("ES boundary batch unavailable; trying snapshot")
                        if await self.geo_cache.snapshot() is None:
                            raise
                sources = []
                for gid in batch:
                    detail = docs.get(gid, {})
                    cached = False
                    if not (detail.get("meta") or {}).get("geometry"):
                        fallback = await self.geo_cache.detail(gid, include_geometry=True)
                        if fallback and (fallback.get("meta") or {}).get("geometry"):
                            detail, cached = fallback, True
                    sources.append((gid, detail, cached))
                return sources

        groups = await asyncio.gather(*(batch_read(ids[offset:offset + 100])
                                        for offset in range(0, len(ids), 100)))
        # Coordinate validation, conversion and topology-preserving simplification
        # are CPU work. Keep them off the API event loop as one bounded task.
        return await asyncio.to_thread(self._navigation_features, [item for group in groups for item in group])

    def _navigation_features(self, sources):
        features = []
        for gid, detail, cached in sources:
            geometry = self._geometry_from_detail(detail)
            features.append({"type": "Feature", "geometry": geometry,
                "properties": {"geog_id": gid, "geometry_status": "available" if geometry else "missing"},
                "provenance": provenance({"geometry"} if cached and geometry else (), es_missing=bool(cached and geometry))})
        if self.geometry_processor is not None:
            geometries = self.geometry_processor.simplify([f["geometry"] for f in features])
            return [{**feature, "geometry": geometry}
                    for feature, geometry in zip(features, geometries)]
        return self._simplify_navigation(features)

    @staticmethod
    def _simplify_navigation(features):
        from .geometry_processing import simplify_geometry
        for feature in features:
            if feature["geometry"]:
                feature["geometry"] = simplify_geometry(feature["geometry"])
        return features

    async def get_geography_detail(self, geog_id, include_geometry=False):
        """One detail-index GET; no base-index lookup or parent resolution."""
        detail = await self._detail_by_id(geog_id, include_geometry, full=True)
        if detail is None:
            raise MissingDocument()
        data = detail.get("data")
        meta = detail.get("meta", {})
        if not isinstance(data, dict) or not isinstance(meta, dict):
            raise UpstreamDataError("Invalid geography detail document")
        geometry = self._geometry_from_detail(detail) if include_geometry else None
        match = re.match(r"^g(\d+)_", geog_id)
        return {"type": "Feature", "geometry": geometry, "properties": {
            "geog_id": geog_id, "name": data.get("name"),
            "level": int(match.group(1)) if match else None,
            "province": data.get("province"),
            "geometry_status": ("available" if geometry else "missing") if include_geometry else "not_requested",
        }, "details": {**detail, "meta": {k: v for k, v in meta.items() if k != "geometry"}},
            "provenance": provenance(getattr(detail, "fallback_fields", ()), getattr(detail, "es_missing", False))}

    async def geometry(self, geog_id, record=None):
        try:
            doc = await self.client.get(index=self.settings.geo_details_index, id=geog_id,
                                        _source=["meta.geometry"])
            detail = doc.get("_source", {})
        except ElasticsearchException as exc:
            if not isinstance(exc, NotFoundError):
                logger.warning("ES geography geometry read failed: %s", type(exc).__name__)
            cached = await self.geo_cache.detail(geog_id, include_geometry=True)
            if cached is None:
                if isinstance(exc, NotFoundError):
                    return None
                raise
            if record is not None and (cached.get("meta") or {}).get("geometry"):
                self._note_cache(record, {"geometry"})
            return self._geometry_from_detail(cached)
        # ES geometry is atomic: do not even inspect cached geometry when present.
        if (detail.get("meta") or {}).get("geometry"):
            return self._geometry_from_detail(detail)
        cached = await self.geo_cache.detail(geog_id, include_geometry=True)
        if record is not None and cached and (cached.get("meta") or {}).get("geometry"):
            self._note_cache(record, {"geometry"})
        return self._geometry_from_detail(cached)

    @staticmethod
    def _geometry_from_detail(detail):
        geometry = ((detail or {}).get("meta") or {}).get("geometry")
        if not geometry:
            return None
        if not isinstance(geometry, list):
            raise UpstreamDataError("Invalid geographic geometry")
        rings = []
        for ring in geometry:
            if not isinstance(ring, list) or len(ring) < 4:
                raise UpstreamDataError("Invalid polygon ring")
            coords = []
            for point in ring:
                if not isinstance(point, (list, tuple)) or len(point) != 2:
                    raise UpstreamDataError("Invalid polygon position")
                lat, lng = number(point[0]), number(point[1])
                if not -90 <= lat <= 90 or not -180 <= lng <= 180:
                    raise UpstreamDataError("Invalid polygon position")
                coords.append([lng, lat])
            if coords[0] != coords[-1]:
                raise UpstreamDataError("Polygon ring is not closed")
            rings.append(coords)
        return {"type": "Polygon", "coordinates": [rings[0]]} if len(rings) == 1 else {
            "type": "MultiPolygon", "coordinates": [[ring] for ring in rings]}

    async def feature(self, record, include_geometry=False, detail=None, detail_loaded=False):
        if include_geometry and record.get("es_query_failed"):
            cached = await self.geo_cache.detail(record["geog_id"], include_geometry=True)
            geometry = self._geometry_from_detail(cached)
            if geometry:
                self._note_cache(record, {"geometry"})
        else:
            geometry = (self._geometry_from_detail(detail) if detail_loaded else
                        await self.geometry(record["geog_id"], record)) if include_geometry else None
        return {"type": "Feature", "geometry": geometry, "properties": {
            "geog_id": record["geog_id"], "name": record["name"], "level": record["level"],
            "longId": record["source"].get("longId"),
            "province": record["province"], "parent_geog_id": record["parent"],
            "parent_status": record["parent_status"],
            "geometry_status": ("available" if geometry else "missing") if include_geometry else "not_requested"},
            "provenance": provenance(record.get("fallback_fields", ()), record.get("es_missing", False))}
