"""Contract tests for the standalone read-only API; no services or ETL caches."""
import os
import subprocess
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from elasticsearch import NotFoundError, ConnectionError

from realestate_analytics.api.catalog import METRICS, METRIC_CATALOG
from realestate_analytics.api.config import Settings
from realestate_analytics.api.main import app, lifespan
from realestate_analytics.api.repository import Repository


class StandaloneApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = MagicMock()
        self.client.get = AsyncMock()
        self.client.search = AsyncMock()
        self.client.scroll = AsyncMock()
        self.client.clear_scroll = AsyncMock()
        self.client.mget = AsyncMock()
        self.client.ping = AsyncMock(return_value=True)
        self.client.cat.nodes = AsyncMock(return_value=[{"name": "es-prod-1"}])
        self.client.indices.exists = AsyncMock(return_value=True)
        self.client.close = AsyncMock()
        self.settings = Settings(api_key="test-key")
        self.repo = Repository(self.client, self.settings)
        app.state.repository = self.repo
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                      base_url="http://test")

    async def asyncTearDown(self):
        await self.http.aclose()

    def set_geos(self, details=None):
        self.geos = {
            "g40_root": {"level": 40, "province": "ON", "level40En": "GTA"},
            "g30_city": {"level": 30, "province": "ON", "level30En": "Toronto", "level40En": "GTA"},
            "g20_child": {"level": 20, "province": "ON", "level20En": "Annex", "level30En": "Toronto"},
            "g30_other": {"level": 30, "province": "BC", "level30En": "Toronto"},
        }
        self.geo_details = {d["_id"]: d.get("_source", {}) for d in details or [] if d.get("found")}
        async def get(index, id, **kwargs):
            source = (self.geos if index == self.settings.geo_index else self.geo_details).get(id)
            if source is None:
                raise NotFoundError(404, "not_found", {})
            if "_source_excludes" in kwargs and "meta" in source:
                source = {**source, "meta": {k: v for k, v in source["meta"].items() if k != "geometry"}}
            return {"_id": id, "_source": source}
        async def mget(index, body, **kwargs):
            src = self.geos if index == self.settings.geo_index else self.geo_details
            return {"docs": [{"_id": id, "found": id in src, "_source": src.get(id, {})} for id in body["ids"]]}
        def matches(query, id, src):
            if "match_all" in query: return True
            if "ids" in query: return id in query["ids"]["values"]
            if "term" in query:
                field, value = next(iter(query["term"].items()))
                value = value.get("value") if isinstance(value, dict) else value
                actual = src.get(field.replace(".keyword", ""))
                if field == "data.parent.keyword": actual = src.get("data", {}).get("parent")
                return str(actual).casefold() == str(value).casefold()
            if "range" in query:
                field, value = next(iter(query["range"].items()))
                return src.get(field, 999) < value["lt"]
            if "wildcard" in query:
                field, value = next(iter(query["wildcard"].items()))
                return value["value"].strip("*").casefold() in str(src.get(field.replace(".keyword", ""), "")).casefold()
            if "match_phrase" in query:
                field, value = next(iter(query["match_phrase"].items()))
                return str(value).casefold() in str(src.get(field, "")).casefold()
            if "bool" in query:
                b = query["bool"]
                return all(matches(x, id, src) for x in b.get("filter", [])) and (not b.get("should") or any(matches(x,id,src) for x in b["should"]))
            raise AssertionError(query)
        async def search(index, body, **kwargs):
            src = self.geos if index == self.settings.geo_index else self.geo_details
            hits = [{"_id": id, "_source": value} for id, value in src.items() if matches(body["query"], id, value)]
            total = len(hits)
            if body.get("sort") == self.repo.GEO_SORT:
                hits.sort(key=lambda h: (self.repo._record(h)["name"].lower(), h["_id"]))
            start = body.get("from", 0)
            hits = hits[start:start + body.get("size", len(hits))]
            return {"hits": {"total": {"value": total, "relation": "eq"}, "hits": hits}}
        async def msearch(body, **kwargs):
            responses = []
            for item in body[1::2]:
                responses.append(await search(self.settings.geo_index, item))
            return {"responses": responses}
        self.client.get.side_effect = get
        self.client.mget.side_effect = mget
        self.client.search.side_effect = search
        self.client.msearch = AsyncMock(side_effect=msearch)

    async def test_metrics_published_values_and_independent_missing_months(self):
        self.client.get.return_value = {"_source": {"last_updated": "2026-10-30T08:00:00", "metrics": {
            "median_sold_price": [{"month": "2026-09", "value": 815000}, {"month": "2026-08", "value": 770000}],
            "absorption_rate": [{"month": "2026-08", "value": 0}, {"month": "2026-09", "value": 0.255}],
            "months_of_inventory": [{"month": "2026-09", "value": 3.92}],
            "sold_listing_count": [{"month": "2026-09", "value": 1929}],
            "current_metrics": {"new_listings": 0}}}}
        r = await self.http.get("/api/v1/metrics/g30_city", params={"start_month": "2026-09", "property_type": "all"})
        self.assertEqual(r.status_code, 200, r.text)
        data = r.json()
        self.assertEqual(len(data["series"]), 12)
        self.assertEqual(data["series"]["absorption_rate"]["data"], [{"month": "2026-09", "value": 0.255}])
        self.assertEqual(data["series"]["months_of_inventory"]["latest_available_month"], "2026-09")
        self.assertFalse(data["series"]["median_dom"]["available"])
        self.assertEqual(data["current_metrics"], {"median_asking_price": None, "new_listings": 0})
        self.client.get.assert_awaited_once_with(index="rlp_mkt_trends_current", id="g30_city_ALL")

    async def test_selected_metrics_and_zero(self):
        self.client.get.return_value = {"_source": {"metrics": {"absorption_rate": [{"month": "2026-08", "value": 0}]}}}
        r = await self.http.get("/api/v1/metrics/x", params={"metrics": "absorption_rate"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(list(r.json()["series"]), ["absorption_rate"])
        self.assertEqual(r.json()["series"]["absorption_rate"]["data"][0]["value"], 0)
        self.assertNotIn("current_metrics", r.json())

    async def test_only_explicitly_selected_current_snapshot_is_returned(self):
        self.client.get.return_value = {"_source": {"metrics": {
            "current_metrics": {"median_asking_price": 799000, "new_listings": 0},
            "last_mth_median_asking_price": [{"month": "2026-10", "value": 799000}],
        }}}
        r = await self.http.get("/api/v1/metrics/x", params={"metrics": "current_metrics"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["series"], {})
        self.assertEqual(r.json()["current_metrics"], {"median_asking_price": 799000, "new_listings": 0})
        r = await self.http.get("/api/v1/metrics/x", params=[
            ("metrics", "last_mth_median_asking_price"), ("metrics", "current_metrics")])
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(list(r.json()["series"]), ["last_mth_median_asking_price"])
        self.assertIn("current_metrics", r.json())

    async def test_monthly_selection_does_not_validate_unrequested_current_data(self):
        self.client.get.return_value = {"_source": {"metrics": {
            "current_metrics": "malformed",
            "last_mth_median_asking_price": [{"month": "2026-10", "value": 799000}],
        }}}
        r = await self.http.get("/api/v1/metrics/x", params={"metrics": "last_mth_median_asking_price"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertNotIn("current_metrics", r.json())
        r = await self.http.get("/api/v1/metrics/x", params={"metrics": "current_metrics"})
        self.assertEqual(r.status_code, 502)

    async def test_all_catalog_fields_and_other_property_type(self):
        metrics = {name: [{"month": "2026-09", "value": 1}]
                   for name in METRICS}
        metrics["current_metrics"] = {"median_asking_price": 10, "new_listings": 3}
        self.client.get.return_value = {"_source": {"metrics": metrics}}
        r = await self.http.get("/api/v1/metrics/example", params={"property_type": "OTHER"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(all(value["available"] for value in r.json()["series"].values()))
        self.client.get.assert_awaited_once_with(index="rlp_mkt_trends_current", id="example_OTHER")

    async def test_catalog_and_validation(self):
        catalog = (await self.http.get("/api/v1/metrics")).json()
        self.assertEqual(len(catalog), len(METRIC_CATALOG))
        self.assertEqual(next(m for m in catalog if m["name"] == "current_metrics")["value_type"], "object")
        for params in ({"metrics": "quarterly_median_dom"}, {"property_type": "UNKNOWN"},
                       {"start_month": "2026-13"}, {"start_month": "2026-10", "end_month": "2026-01"}):
            self.assertEqual((await self.http.get("/api/v1/metrics/x", params=params)).status_code, 422)
        self.client.get.assert_not_awaited()

    async def test_missing_and_transport_errors(self):
        self.client.get.side_effect = NotFoundError(404, "not_found", {})
        self.assertEqual((await self.http.get("/api/v1/metrics/x")).status_code, 404)
        self.client.get.side_effect = ConnectionError("unavailable")
        r = await self.http.get("/api/v1/metrics/x")
        self.assertEqual(r.status_code, 503)
        self.assertNotIn("test-key", r.text)

    async def test_bad_upstream_values(self):
        for value in [float("nan"), None, "12", True]:
            self.client.get.return_value = {"_source": {"metrics": {"median_dom": [{"month": "2026-09", "value": value}]}}}
            r = await self.http.get("/api/v1/metrics/x", params={"metrics": "median_dom"})
            self.assertEqual(r.status_code, 502, (value, r.text))

    async def test_geo_inference_search_pagination_and_hierarchy(self):
        self.set_geos()
        self.geos["g30_city"]["longId"] = "on_toronto"
        r = await self.http.get("/api/v1/geos", params={"parent_id": "g40_root", "limit": 1})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["total"], 1)
        self.assertEqual(r.json()["features"][0]["properties"]["parent_status"], "inferred")
        r = await self.http.get("/api/v1/geos/g20_child/hierarchy")
        properties = [feature["properties"] for feature in r.json()["features"]]
        self.assertEqual([p["geog_id"] for p in properties],
                         ["g40_root", "g30_city", "g20_child"])
        self.assertEqual([p["longId"] for p in properties], [None, "on_toronto", None])
        self.assertIn("longId", next(call.kwargs["_source"] for call in self.client.get.await_args_list
                                    if call.kwargs["id"] == "g30_city" and
                                    call.kwargs["index"] == self.settings.geo_index))
        self.assertTrue(r.json()["complete"])
        r = await self.http.get("/api/v1/geos", params={"query": "tor", "province": "ON"})
        self.assertEqual(r.json()["total"], 1)
        self.assertEqual((await self.http.get("/api/v1/geos", params={"query": "tor", "offset": 1, "limit": 1})).json()["total"], 2)

    async def test_blind_without_pagination_returns_every_record(self):
        self.set_geos()
        for i in range(60):
            self.geos[f"root{i}"] = {"level": 40, "province": "ON", "level40En": f"Region {i}"}
        r = await self.http.get("/api/v1/geos", params={"include_geometry": "false"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(len(r.json()["features"]), 64)
        self.assertEqual(r.json()["total"], 64)
        self.assertIsNone(r.json()["limit"])
        self.assertEqual(r.json()["offset"], 0)
        self.assertEqual({f["properties"]["geog_id"] for f in r.json()["features"]}, set(self.geos))

    async def test_blind_pagination_is_applied_in_es(self):
        self.set_geos()
        r = await self.http.get("/api/v1/geos", params={"limit": 2, "offset": 1})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["total"], 4)
        self.assertEqual([x["properties"]["geog_id"] for x in r.json()["features"]], ["g40_root", "g30_city"])
        body = self.client.search.call_args.kwargs["body"]
        self.assertEqual(body["from"], 1)
        self.assertEqual(body["size"], 2)
        self.assertTrue(body["track_total_hits"])
        self.assertEqual(body["sort"], self.repo.GEO_SORT)
        self.client.scroll.assert_not_awaited()

    async def test_offset_does_not_impose_a_limit(self):
        self.set_geos()
        r = await self.http.get("/api/v1/geos", params={"offset": 1})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertIsNone(r.json()["limit"])
        self.assertEqual(r.json()["total"], 4)
        self.assertEqual(len(r.json()["features"]), 3)

    async def test_filters_do_not_impose_a_limit(self):
        self.set_geos()
        for i in range(60):
            self.geos[f"region{i}"] = {"level": 40, "province": "ON", "level40En": f"Region {i}"}
        for params in ({"query": "Region"}, {"province": "ON"}, {"level": 40},
                       {"query": "Region", "province": "ON", "level": 40, "offset": 0}):
            r = await self.http.get("/api/v1/geos", params=params)
            self.assertEqual(r.status_code, 200, r.text)
            self.assertGreater(len(r.json()["features"]), 50)
            self.assertEqual(len(r.json()["features"]), r.json()["total"])
            self.assertIsNone(r.json()["limit"])
        limited = await self.http.get("/api/v1/geos", params={"query": "Region", "limit": 5})
        self.assertEqual(len(limited.json()["features"]), 5)
        self.assertEqual(limited.json()["total"], 60)

    async def test_deep_offset_past_total_is_empty_not_es_window_error(self):
        self.set_geos()
        r = await self.http.get("/api/v1/geos", params={"offset": 20000, "limit": 10})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["features"], [])
        self.assertEqual(r.json()["total"], 4)
        self.assertEqual(self.client.search.call_args.kwargs["body"]["size"], 0)

    async def test_deep_offset_uses_sort_cursors(self):
        results = [{"hits": {"total": {"value": 10004, "relation": "eq"}, "hits": []}}]
        for start in range(0, 10000, 1000):
            results.append({"hits": {"hits": [{"_id": str(i), "sort": [str(i), str(i)]}
                                               for i in range(start, start + 1000)]}})
        results.append({"hits": {"hits": [{"_id": "10000", "sort": ["10000", "10000"]}]}})
        results.append({"hits": {"total": {"value": 10004, "relation": "eq"}, "hits": [
            {"_id": "10001", "_source": {"level": 40, "level40En": "A"}},
            {"_id": "10002", "_source": {"level": 40, "level40En": "B"}},
        ]}})
        self.client.search.side_effect = results
        page, total = await self.repo._unfiltered_page(2, 10001)
        self.assertEqual(total, 10004)
        self.assertEqual([r["geog_id"] for r in page], ["10001", "10002"])
        body = self.client.search.call_args.kwargs["body"]
        self.assertEqual(body["search_after"], ["10000", "10000"])
        self.assertNotIn("from", body)

    async def test_unresolved_explicit_parent_is_not_inferred(self):
        self.set_geos(details=[{"_id": "g30_city", "found": True,
                                "_source": {"data": {"parent": "missing"}}}])
        r = await self.http.get("/api/v1/geos/g30_city/hierarchy")
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.json()["complete"])
        self.assertEqual(r.json()["features"][0]["properties"]["parent_status"], "unresolved")

    async def test_search_pushes_filters_to_es_and_fetches_only_selected_details(self):
        self.set_geos()
        r = await self.http.get("/api/v1/geos", params={"query": "Toronto", "province": "ON", "level": 30})
        self.assertEqual(r.status_code, 200, r.text)
        filters = self.client.search.call_args.kwargs["body"]["query"]["bool"]["filter"]
        self.assertIn({"term": {"level": 30}}, filters)
        self.assertTrue(any("wildcard" in str(f) for f in filters))
        self.assertEqual(self.client.mget.call_args.kwargs["body"]["ids"], ["g30_city"])
        self.client.scroll.assert_not_awaited()
        self.client.clear_scroll.assert_not_awaited()

    async def test_large_filtered_search_scrolls_and_cleans_up(self):
        batch = [{"_id": str(i)} for i in range(1000)]
        self.client.search.side_effect = [
            {"hits": {"total": {"value": 1001, "relation": "eq"}, "hits": batch}},
            {"_scroll_id": "s1", "hits": {"hits": batch}},
        ]
        self.client.scroll.return_value = {"_scroll_id": "s2", "hits": {"hits": [{"_id": "1000"}]}}
        hits = await self.repo._search_all("geo", {"term": {"level": 30}}, False)
        self.assertEqual(len(hits), 1001)
        self.client.clear_scroll.assert_awaited_once_with(scroll_id="s2")

    async def test_accented_uppercase_search_includes_lowercase_variant(self):
        self.set_geos()
        self.geos["qc"] = {"level": 30, "province": "QC", "level30En": "Québec"}
        result = await self.http.get("/api/v1/geos", params={"query": "QUÉBEC", "province": "QC", "level": 30})
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()["features"][0]["properties"]["name"], "Québec")
        filters = self.client.search.call_args.kwargs["body"]["query"]["bool"]["filter"]
        self.assertIn("*québec*", str(filters))

    async def test_geo_detail_includes_profiles_and_optional_geometry(self):
        self.set_geos(details=[{"_id": "g30_city", "found": True, "_source": {
            "data": {"name": "Toronto", "province": "ON", "intro": "intro", "profiles": {"housing": "housing"}, "centroid": {"lat": 43}},
            "meta": {"geometry": [[[43, -79], [43, -78], [44, -78], [43, -79]],
                                  [[45, -79], [45, -78], [46, -78], [45, -79]]]}}}])
        r = await self.http.get("/api/v1/geos/g30_city", params={"include_geometry": "true"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["geometry"]["type"], "MultiPolygon")
        self.assertEqual(r.json()["geometry"]["coordinates"][0][0][0], [-79, 43])
        self.assertEqual(r.json()["details"]["data"]["profiles"]["housing"], "housing")
        self.assertNotIn("geometry", r.json()["details"]["meta"])
        self.assertEqual(r.json()["properties"]["level"], 30)
        self.assertNotIn("parent_status", r.json()["properties"])
        self.assertNotIn("parent_geog_id", r.json()["properties"])
        self.client.get.assert_awaited_once_with(index=self.settings.geo_details_index, id="g30_city")
        self.client.mget.assert_not_awaited()
        self.client.search.assert_not_awaited()
        self.client.msearch.assert_not_awaited()
        self.client.get.reset_mock()
        r = await self.http.get("/api/v1/geos/g30_city", params={"include_geometry": "false"})
        self.assertIsNone(r.json()["geometry"])
        self.assertEqual(r.json()["properties"]["geometry_status"], "not_requested")
        self.client.get.assert_awaited_once_with(index=self.settings.geo_details_index, id="g30_city",
                                               _source_excludes=["meta.geometry"])
        r = await self.http.get("/api/v1/geos/g40_root", params={"include_geometry": "true"})
        self.assertEqual(r.status_code, 404)

    async def test_detail_does_not_require_a_base_record_or_resolve_stored_parent(self):
        self.set_geos(details=[{"_id": "g30_detail_only", "found": True, "_source": {
            "data": {"name": "Detail only", "province": "ON", "parent": "g40_absent"},
            "meta": {"sections": {"housing": "Housing"}},
        }}])
        r = await self.http.get("/api/v1/geos/g30_detail_only", params={"include_geometry": "true"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["properties"]["name"], "Detail only")
        self.assertEqual(r.json()["properties"]["geometry_status"], "missing")
        self.assertEqual(r.json()["details"]["data"]["parent"], "g40_absent")
        self.client.get.assert_awaited_once_with(index=self.settings.geo_details_index, id="g30_detail_only")
        self.client.mget.assert_not_awaited()
        self.client.msearch.assert_not_awaited()

    async def test_malformed_detail_returns_upstream_error(self):
        self.set_geos(details=[{"_id": "g30_city", "found": True, "_source": {"data": None}}])
        r = await self.http.get("/api/v1/geos/g30_city")
        self.assertEqual(r.status_code, 502)

    async def test_cycle_and_missing_parent(self):
        self.set_geos(details=[{"_id": "g30_city", "found": True, "_source": {"data": {"parent": "g20_child"}}},
                               {"_id": "g20_child", "found": True, "_source": {"data": {"parent": "g30_city"}}}])
        self.assertEqual((await self.http.get("/api/v1/geos/g20_child/hierarchy")).status_code, 502)
        self.assertEqual((await self.http.get("/api/v1/geos", params={"parent_id": "missing"})).status_code, 404)

    async def test_readiness_and_no_admin_routes(self):
        self.assertEqual((await self.http.get("/health/ready")).status_code, 200)
        self.client.cat.nodes.return_value = [{"name": "uat"}]
        self.assertEqual((await self.http.get("/health/ready")).status_code, 503)
        self.assertEqual((await self.http.get("/monitor/etl-types")).status_code, 404)
        self.assertEqual((await self.http.post("/op/historic_metrics")).status_code, 404)

    async def test_lifespan_closes_client_even_if_verification_fails(self):
        self.client.cat.nodes.return_value = [{"name": "uat"}]
        with patch("realestate_analytics.api.main.Settings.from_environment", return_value=self.settings), \
             patch("realestate_analytics.api.main.AsyncElasticsearch", return_value=self.client):
            with self.assertRaises(RuntimeError):
                async with lifespan(app):
                    pass
        self.client.close.assert_awaited_once()


class SettingsTests(unittest.TestCase):
    def test_environment_loading_and_missing_key(self):
        with patch.dict(os.environ, {"RLP_LISTING_ES_API_KEY": "exported"}, clear=True), \
             patch("realestate_analytics.api.config.load_dotenv") as load:
            self.assertEqual(Settings.from_environment().api_key, "exported")
            self.assertTrue(load.call_args.kwargs["override"])
        with patch.dict(os.environ, {}, clear=True), patch("realestate_analytics.api.config.load_dotenv"):
            with self.assertRaises(RuntimeError):
                Settings.from_environment()

    def test_serving_import_is_independent_of_etl_and_data(self):
        code = ("import sys; import realestate_analytics.api.main; "
                "assert not any(n.startswith(('realestate_analytics.etl', 'realestate_analytics.data')) "
                "for n in sys.modules)")
        subprocess.run([sys.executable, "-c", code], check=True)
