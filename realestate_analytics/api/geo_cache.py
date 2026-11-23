"""Lazy, read-only geography fallback. No analytics-project imports."""
import asyncio
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)


class _GeoState:
    """State-only compatibility target for historical serialized Geo objects."""


class _CollectionState:
    """State-only compatibility target; overlap helpers are not needed by the API."""


def load_snapshot(path):
    # These are optional until a fallback is actually needed. The trusted snapshot
    # includes Shapely objects as well as the primitive coordinate lists we serve.
    import dill

    class SnapshotLoader(dill.Unpickler):
        def find_class(self, module, name):
            if module == "realestate_analytics.data.geo":
                if name == "Geo":
                    return _GeoState
                if name == "GeoCollection":
                    return _CollectionState
            if module.startswith("realestate_analytics."):
                raise ValueError("Snapshot references an unsupported analytics class")
            return super().find_class(module, name)

    with Path(path).open("rb") as stream:
        geos = SnapshotLoader(stream).load()
    return GeoSnapshot(geos)


class GeoSnapshot:
    def __init__(self, geos):
        self.by_id = {}
        for geo in geos:
            gid = getattr(geo, "geog_id", None)
            if not isinstance(gid, str) or gid in self.by_id:
                raise ValueError("Invalid or duplicate geography ID in snapshot")
            self.by_id[gid] = geo

    def base(self, geog_id):
        g = self.by_id.get(geog_id)
        if g is None:
            return None
        source = {"localLogicId": geog_id, "level": g.level, "province": g.province,
                  "city": getattr(g, "city", None), "name": g.name,
                  "longId": getattr(g, "long_id", None)}
        for level in (10, 20, 30, 35, 40):
            source[f"level{level}En"] = getattr(g, f"level_{level}_en", None)
        source[f"level{g.level}En"] = source.get(f"level{g.level}En") or g.name
        return source

    def parent(self, geog_id):
        g = self.by_id.get(geog_id)
        return getattr(g, "parent_id", None) if g else None

    def detail(self, geog_id, include_geometry=False):
        g = self.by_id.get(geog_id)
        if g is None:
            return None
        data = {"id": geog_id, "type": "profiles", "name": g.name, "province": g.province,
                "profiles": getattr(g, "profiles", None), "intro": getattr(g, "intro", None),
                "bounding_box": getattr(g, "bounding_box", None),
                "centroid": {"lat": getattr(g, "lat", None), "lng": getattr(g, "lng", None)},
                "parent": getattr(g, "parent_id", None)}
        meta = {}
        if include_geometry:
            meta["geometry"] = getattr(g, "geometry", None)
        return {"data": data, "meta": meta}


class LazyGeoCache:
    def __init__(self, path=None, loader=load_snapshot):
        self.path = path
        self.loader = loader
        self._snapshot = None
        self._lock = threading.Lock()

    def _load_once(self):
        # The thread lock also prevents duplicate loads if an awaiting request is
        # cancelled while its background load is still running.
        with self._lock:
            if self._snapshot is None:
                self._snapshot = self.loader(self.path)
        return self._snapshot

    async def snapshot(self):
        if self.path is None:
            return None
        if self._snapshot is not None:
            return self._snapshot
        try:
            return await asyncio.to_thread(self._load_once)
        except Exception as exc:
            logger.warning("Geography snapshot unavailable: %s", type(exc).__name__)
            return None

    async def base(self, geog_id):
        snapshot = await self.snapshot()
        return snapshot.base(geog_id) if snapshot else None

    async def detail(self, geog_id, include_geometry=False):
        snapshot = await self.snapshot()
        return snapshot.detail(geog_id, include_geometry) if snapshot else None


class GeoDocument(dict):
    def __init__(self, data, fallback_fields=(), es_missing=False):
        super().__init__(data)
        self.fallback_fields = set(fallback_fields)
        self.es_missing = es_missing


def missing(value):
    # Zero and False are valid values, not missing data. Empty polygons are the
    # explicit exception handled atomically by merge_missing.
    return value is None or value == ""


def merge_missing(primary, fallback, path=""):
    """Copy only changed dictionaries; never mutate ES or cached objects."""
    if not isinstance(primary, dict) or not isinstance(fallback, dict):
        return primary, set()
    result = primary
    used = set()
    for key, value in fallback.items():
        field = f"{path}.{key}" if path else key
        current = primary.get(key)
        absent = key not in primary or missing(current)
        if field == "meta.geometry":
            absent = absent or current == []
        if absent and not missing(value):
            if field == "meta.geometry" and not value:
                continue
            if isinstance(value, dict) and field != "meta.geometry":
                # Record leaf paths even when the whole ES object is absent.
                # Parent resolution relies on distinguishing a cached parent
                # from an explicit ES parent in source-filtered documents.
                value, leaf_fields = merge_missing({}, value, field)
                if not leaf_fields:
                    continue
            else:
                leaf_fields = {field}
            if result is primary:
                result = dict(primary)
            result[key] = value
            used.update(leaf_fields)
        elif isinstance(current, dict) and isinstance(value, dict) and field != "meta.geometry":
            merged, child_used = merge_missing(current, value, field)
            if child_used:
                if result is primary:
                    result = dict(primary)
                result[key] = merged
                used.update(child_used)
    return result, used


def provenance(fields=(), es_missing=False):
    return {"source": "geo_collection" if es_missing else ("mixed" if fields else "elasticsearch"),
            "fallback_fields": sorted(fields)}


def combined_provenance(features):
    sources = {f.get("provenance", {}).get("source", "elasticsearch") for f in features}
    fields = {p for f in features for p in f.get("provenance", {}).get("fallback_fields", [])}
    source = "geo_collection" if sources == {"geo_collection"} else ("mixed" if fields or "geo_collection" in sources else "elasticsearch")
    return {"source": source, "fallback_fields": sorted(fields)}
