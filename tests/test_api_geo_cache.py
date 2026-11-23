"""ES-first geography fallback, lazy loading, and merge contract tests."""
import asyncio
import copy
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from elasticsearch import ConnectionError, NotFoundError

from realestate_analytics.api.config import Settings
from realestate_analytics.api.geo_cache import GeoSnapshot, LazyGeoCache, merge_missing
from realestate_analytics.api.main import app, lifespan
from realestate_analytics.api.repository import Repository

RING = [[43, -79], [43, -78], [44, -78], [43, -79]]
CACHE_RING = [[45, -79], [45, -78], [46, -78], [45, -79]]


def geo(gid, name, level, parent=None):
    return SimpleNamespace(geog_id=gid, name=name, level=level, province="ON", parent_id=parent,
        city=name, lat=45, lng=-79, geometry=[CACHE_RING], intro="old intro",
        profiles={"transport": "old transport", "services": "old services", "character": "old character", "housing": "old housing"},
        bounding_box={"west": -79, "east": -78, "south": 45, "north": 46})


def fresh_detail():
    return {"data": {"id": "g30_city", "name": "Fresh Toronto", "province": "ON", "type": "profiles",
        "parent": "g40_root", "intro": "fresh intro",
        "profiles": {"transport": "fresh transport", "services": "fresh services", "character": "fresh character", "housing": "fresh housing"},
        "centroid": {"lat": 0, "lng": 0},
        "bounding_box": {"west": 0, "east": 1, "south": 0, "north": 1}},
        "meta": {"geometry": [RING], "sections": {"housing": "Housing"}}}


class GeoFallbackTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.load_count = 0
        self.snapshot = GeoSnapshot([geo("g40_root", "GTA", 40),
                                     geo("g30_city", "Old Toronto", 30, "g40_root")])
        def loader(path):
            self.load_count += 1
            return self.snapshot
        self.cache = LazyGeoCache(Path("unused/geo_collection.dill"), loader=loader)
        self.es = MagicMock()
        self.details = {"g30_city": fresh_detail()}
        self.bases = {
            "g40_root": {"level": 40, "province": "ON", "level40En": "GTA"},
            "g30_city": {"level": 30, "province": "ON", "level30En": "Fresh Toronto", "level40En": "GTA"}}
        async def get(index, id, **kwargs):
            source = (self.details if index == "rlp_geo_details_en" else self.bases).get(id)
            if source is None:
                raise NotFoundError(404, "not_found", {})
            source = copy.deepcopy(source)
            if kwargs.get("_source_excludes") and "meta" in source:
                source["meta"].pop("geometry", None)
            return {"_id": id, "_source": source}
        async def mget(index, body, **kwargs):
            source = self.details if index == "rlp_geo_details_en" else self.bases
            return {"docs": [{"_id": gid, "found": gid in source, "_source": copy.deepcopy(source.get(gid, {}))}
                              for gid in body["ids"]]}
        self.es.get = AsyncMock(side_effect=get)
        self.es.mget = AsyncMock(side_effect=mget)
        self.es.msearch = AsyncMock()
        self.es.search = AsyncMock(return_value={"hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}})
        self.repo = Repository(self.es, Settings(api_key="test"), geo_cache=self.cache)
        app.state.repository = self.repo
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test")

    async def asyncTearDown(self):
        await self.http.aclose()

    async def test_complete_es_detail_never_loads_cache(self):
        for include_geometry in ("true", "false"):
            r = await self.http.get("/api/v1/geos/g30_city", params={"include_geometry": include_geometry})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json()["details"]["data"]["intro"], "fresh intro")
            self.assertEqual(r.json()["provenance"]["source"], "elasticsearch")
        self.assertEqual(self.load_count, 0)

    async def test_only_missing_nested_values_filled_without_mutating_es(self):
        detail = self.details["g30_city"]
        detail["data"]["profiles"]["housing"] = None
        detail["data"]["bounding_box"]["north"] = None
        before = copy.deepcopy(detail)
        r = await self.http.get("/api/v1/geos/g30_city", params={"include_geometry": "true"})
        self.assertEqual(r.status_code, 200, r.text)
        d = r.json()
        self.assertEqual(d["details"]["data"]["profiles"]["housing"], "old housing")
        self.assertEqual(d["details"]["data"]["profiles"]["transport"], "fresh transport")
        self.assertEqual(d["details"]["data"]["bounding_box"]["west"], 0)
        self.assertEqual(d["details"]["data"]["bounding_box"]["north"], 46)
        self.assertEqual(d["geometry"]["coordinates"][0][0], [-79, 43])
        self.assertEqual(d["provenance"]["source"], "mixed")
        self.assertIn("data.profiles.housing", d["provenance"]["fallback_fields"])
        self.assertEqual(detail, before)
        await self.http.get("/api/v1/geos/g30_city")
        self.assertEqual(self.load_count, 1)

    async def test_missing_geometry_uses_whole_cached_geometry_only_when_requested(self):
        self.details["g30_city"]["meta"].pop("geometry")
        r = await self.http.get("/api/v1/geos/g30_city")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(self.load_count, 0)
        r = await self.http.get("/api/v1/geos/g30_city", params={"include_geometry": "true"})
        self.assertEqual(r.json()["geometry"]["coordinates"][0][0], [-79, 45])
        self.assertEqual(r.json()["details"]["data"]["intro"], "fresh intro")
        self.assertIn("meta.geometry", r.json()["provenance"]["fallback_fields"])
        self.assertEqual(self.load_count, 1)

    async def test_warm_snapshot_does_not_cache_es_results(self):
        self.details["g30_city"]["data"].pop("parent")
        first = await self.http.get("/api/v1/geos/g30_city")
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(self.load_count, 1)
        self.details["g30_city"]["data"]["intro"] = "new ES intro"
        self.details["g30_city"]["data"]["parent"] = "g40_new"
        second = await self.http.get("/api/v1/geos/g30_city")
        self.assertEqual(second.json()["details"]["data"]["intro"], "new ES intro")
        self.assertEqual(second.json()["details"]["data"]["parent"], "g40_new")
        self.assertEqual(second.json()["provenance"]["source"], "elasticsearch")
        self.assertEqual(self.es.get.await_count, 2)

    async def test_geometry_lookup_does_not_check_cache_when_es_has_geometry(self):
        g = await self.repo.geometry("g30_city")
        self.assertIsNotNone(g)
        self.assertEqual(self.load_count, 0)

    async def test_missing_detail_document_uses_cache(self):
        r = await self.http.get("/api/v1/geos/g40_root", params={"include_geometry": "true"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["properties"]["name"], "GTA")
        self.assertIsNotNone(r.json()["geometry"])
        self.assertEqual(r.json()["provenance"]["source"], "geo_collection")
        self.assertEqual(self.es.get.call_args.kwargs["index"], "rlp_geo_details_en")

    async def test_es_detail_error_uses_cache_but_unknown_id_keeps_error(self):
        self.es.get.side_effect = ConnectionError("down")
        r = await self.http.get("/api/v1/geos/g30_city")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["provenance"]["source"], "geo_collection")
        r = await self.http.get("/api/v1/geos/unknown")
        self.assertEqual(r.status_code, 503)

    async def test_missing_parent_completed_from_cache(self):
        self.details["g30_city"]["data"].pop("parent")
        r = await self.http.get("/api/v1/geos/g30_city/hierarchy")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["complete"])
        features = r.json()["features"]
        self.assertEqual([f["properties"]["name"] for f in features], ["GTA", "Fresh Toronto"])
        self.assertEqual(features[-1]["properties"]["parent_status"], "cache")
        self.assertEqual(r.json()["provenance"]["source"], "mixed")

    async def test_existing_es_parent_not_overwritten(self):
        self.bases["g40_new"] = {"level": 40, "province": "ON", "level40En": "New parent"}
        self.details["g30_city"]["data"]["parent"] = "g40_new"
        r = await self.http.get("/api/v1/geos/g30_city/hierarchy")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["features"][-1]["properties"]["parent_geog_id"], "g40_new")

    async def test_empty_parent_search_keeps_fresh_es_child_fields(self):
        self.details["g30_city"]["data"].pop("parent")
        r = await self.http.get("/api/v1/geos", params={"parent_id": "g40_root"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["total"], 1)
        child = r.json()["features"][0]
        self.assertEqual(child["properties"]["name"], "Fresh Toronto")
        self.assertEqual(child["properties"]["parent_geog_id"], "g40_root")
        self.assertEqual(child["provenance"]["source"], "mixed")

    async def test_empty_search_does_not_resurrect_old_name_over_fresh_es_name(self):
        r = await self.http.get("/api/v1/geos", params={"query": "Old Toronto"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["total"], 0)

    async def test_nonempty_search_fills_missing_field_but_keeps_es_name(self):
        self.bases["g30_city"]["province"] = None
        self.es.search.return_value = {"hits": {"total": {"value": 1, "relation": "eq"},
            "hits": [{"_id": "g30_city", "_source": self.bases["g30_city"]}]}}
        r = await self.http.get("/api/v1/geos", params={"query": "Toronto"})
        self.assertEqual(r.status_code, 200, r.text)
        child = r.json()["features"][0]
        self.assertEqual(child["properties"]["name"], "Fresh Toronto")
        self.assertEqual(child["properties"]["province"], "ON")
        self.assertIn("province", child["provenance"]["fallback_fields"])

    async def test_es_search_error_falls_back_with_limit_and_offset(self):
        self.es.search.side_effect = ConnectionError("down")
        r = await self.http.get("/api/v1/geos", params={"limit": 1, "offset": 1})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["total"], 2)
        self.assertEqual(len(r.json()["features"]), 1)
        self.assertEqual(r.json()["provenance"]["source"], "geo_collection")

    async def test_parent_batch_error_keeps_es_geos_absent_from_snapshot(self):
        self.es.search.return_value = {"hits": {"total": {"value": 2, "relation": "eq"}, "hits": [
            {"_id": "g30_city", "_source": self.bases["g30_city"]},
            {"_id": "g40_new", "_source": {"level": 40, "province": "ON", "level40En": "New ES region"}}]}}
        self.es.mget.side_effect = ConnectionError("down")
        r = await self.http.get("/api/v1/geos", params={"province": "ON"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["total"], 2)
        features = {f["properties"]["geog_id"]: f for f in r.json()["features"]}
        self.assertEqual(features["g30_city"]["properties"]["name"], "Fresh Toronto")
        self.assertEqual(features["g30_city"]["properties"]["parent_status"], "cache")
        self.assertEqual(features["g40_new"]["properties"]["name"], "New ES region")
        self.assertEqual(features["g40_new"]["provenance"]["source"], "elasticsearch")

    async def test_market_metrics_never_use_geo_fallback(self):
        self.es.get.side_effect = ConnectionError("down")
        r = await self.http.get("/api/v1/metrics/g30_city")
        self.assertEqual(r.status_code, 503)
        self.assertEqual(self.load_count, 0)

    async def test_concurrent_first_loads_share_one_snapshot(self):
        snapshots = await asyncio.gather(*(self.cache.snapshot() for _ in range(20)))
        self.assertTrue(all(s is self.snapshot for s in snapshots))
        self.assertEqual(self.load_count, 1)

    async def test_source_filtered_empty_detail_marks_parent_as_cached(self):
        self.details["g30_city"] = {}
        r = await self.http.get("/api/v1/geos/g30_city/hierarchy")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["complete"])
        child = r.json()["features"][-1]
        self.assertEqual(child["properties"]["parent_status"], "cache")
        self.assertIn("parent_geog_id", child["provenance"]["fallback_fields"])

    async def test_unavailable_snapshot_preserves_es_error(self):
        def unavailable(path):
            raise FileNotFoundError()
        self.cache.loader = unavailable
        self.es.get.side_effect = ConnectionError("down")
        self.assertEqual((await self.http.get("/api/v1/geos/g30_city")).status_code, 503)

    async def test_missing_from_both_sources_is_404(self):
        self.assertEqual((await self.http.get("/api/v1/geos/g30_absent")).status_code, 404)

    async def test_configured_fallback_allows_outage_startup_but_not_readiness(self):
        self.es.ping = AsyncMock(return_value=False)
        self.es.close = AsyncMock()
        settings = Settings(api_key="test", analytics_cache_dir=Path("unused"))
        with patch("realestate_analytics.api.main.Settings.from_environment", return_value=settings), \
             patch("realestate_analytics.api.main.AsyncElasticsearch", return_value=self.es):
            async with lifespan(app):
                self.assertIsNone(app.state.repository.geo_cache._snapshot)
                self.assertEqual((await self.http.get("/health/ready")).status_code, 503)
        self.es.close.assert_awaited_once()

    async def test_configured_fallback_does_not_bypass_identity_check(self):
        self.es.ping = AsyncMock(return_value=True)
        self.es.cat.nodes = AsyncMock(return_value=[{"name": "uat"}])
        self.es.close = AsyncMock()
        settings = Settings(api_key="test", analytics_cache_dir=Path("unused"))
        with patch("realestate_analytics.api.main.Settings.from_environment", return_value=settings), \
             patch("realestate_analytics.api.main.AsyncElasticsearch", return_value=self.es):
            with self.assertRaises(RuntimeError):
                async with lifespan(app):
                    self.fail("Wrong ES environment must fail startup")

    def test_existing_environment_setting_selects_snapshot_file(self):
        with patch.dict(os.environ, {"RLP_LISTING_ES_API_KEY": "test", "ANALYTICS_CACHE_DIR": "/deployment/geos"}, clear=True), \
             patch("realestate_analytics.api.config.load_dotenv") as load:
            settings = Settings.from_environment()
        self.assertEqual(settings.geo_collection_path, Path("/deployment/geos/geo_collection.dill"))
        self.assertEqual(load.call_args_list[0].args[0], Path.home() / ".env")
        self.assertFalse(load.call_args_list[0].kwargs["override"])
        self.assertTrue(load.call_args_list[1].kwargs["override"])

    async def test_merge_preserves_zero_false_and_atomic_geometry(self):
        geometry = [RING]
        primary = {"zero": 0, "false": False, "nested": {"a": "fresh", "b": None}, "meta": {"geometry": geometry}}
        fallback = {"zero": 7, "false": True, "nested": {"a": "old", "b": "fill"}, "meta": {"geometry": [CACHE_RING]}}
        merged, fields = merge_missing(primary, fallback)
        self.assertEqual(merged["zero"], 0)
        self.assertIs(merged["false"], False)
        self.assertEqual(merged["nested"], {"a": "fresh", "b": "fill"})
        self.assertIs(merged["meta"]["geometry"], geometry)
        self.assertEqual(fields, {"nested.b"})
        self.assertIsNone(primary["nested"]["b"])
