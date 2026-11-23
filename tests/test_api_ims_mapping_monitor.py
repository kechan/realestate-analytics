"""Exact grouped counts and read-only vectorized mapping reconciliation."""
import ast
import hashlib
import inspect
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import pandas as pd

from realestate_analytics.api import ims_mapping_monitor as monitor
from realestate_analytics.api.config import Settings
from realestate_analytics.api.repository import UpstreamDataError, ESUnavailableError


def bucket(subtype="Detached", style="2 Storey", prop="DETACHED", province="ON", count=3, typ="Residential"):
    return {"Type": typ, "SubType": subtype, "Style": style,
            "property_type": prop, "province": province, "count": count}


class MappingReconciliationTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.home() / "tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.base_path = self.root / "ims_property_type_mapping_v4_df"
        self.override_path = self.root / "ims_property_type_lookup_df"
        pd.DataFrame([
            ["Residential", "Detached", "2 Storey", "DETACHED"],
            ["Residential", "Recreational", "Park Model", "MOBILE_MINI"],
            ["Residential", "Duplex", "Bungalow", "OTHER"],
        ], columns=monitor.TUPLE + ["Inferred"]).to_feather(self.base_path)
        pd.DataFrame([
            ["Residential", "Recreational", "Park Model", "OTHER"],
            ["Residential", "Duplex", "Bungalow", "DETACHED"],
            [None, None, None, None], [None, None, None, None],
        ], columns=monitor.TUPLE + ["propertyType"]).to_feather(self.override_path)

    def test_conflicts_and_sources_keep_es_values_and_counts(self):
        rows = [bucket(count=3), bucket(province="BC", count=2),
                bucket("Recreational", "Park Model", "OTHER", count=7),
                bucket("Recreational", "Park Model", "MOBILE_MINI", province="BC", count=2),
                bucket("Duplex", "Bungalow", "SEMI-DETACHED", province="AB", count=4),
                bucket("Duplex", "Bungalow", "DETACHED", count=6),
                bucket("Unseen", "New format", "CONDO", count=1)]
        result = monitor.reconcile_groups(rows, 25, self.root)
        self.assertEqual(result["document_count"], 25)
        self.assertEqual(result["tuple_count"], 4)
        self.assertEqual(result["row_count"], 6)
        self.assertEqual(result["conflict_tuple_count"], 2)
        self.assertEqual(result["conflict_document_count"], 19)
        self.assertEqual(result["different_document_count"], 2)
        self.assertEqual(result["unexplained_document_count"], 1)
        frame = pd.DataFrame(result["rows"]).set_index(["SubType", "property_type"])
        self.assertEqual(frame.loc[("Detached", "DETACHED"), "count"], 5)
        self.assertEqual(frame.loc[("Detached", "DETACHED"), "mapping_source"], "algorithm_cache")
        self.assertEqual(frame.loc[("Recreational", "MOBILE_MINI"), "lineage_status"], "different")
        self.assertEqual(frame.loc[("Recreational", "MOBILE_MINI"), "override_mapping"], "OTHER")
        self.assertEqual(frame.loc[("Duplex", "SEMI-DETACHED"), "mapping_source"], "provincial_rule")
        self.assertTrue(frame.loc[("Duplex", "SEMI-DETACHED"), "conflict"])
        self.assertEqual(frame.loc[("Duplex", "DETACHED"), "mapping_source"], "manual_override")
        self.assertEqual(frame.loc[("Unseen", "CONDO"), "lineage_status"], "unexplained")
        self.assertEqual(result["sources"]["overrides"]["blank_rows"], 2)

    def test_missing_values_are_counted_and_not_lost_by_groupby(self):
        result = monitor.reconcile_groups([
            bucket(subtype=None, style=None, prop=None, typ=None, count=2),
            bucket(subtype=None, style=None, prop="OTHER", typ=None, count=3),
        ], 5, self.root)
        self.assertEqual(result["tuple_count"], 1)
        self.assertEqual(result["conflict_tuple_count"], 1)
        self.assertEqual(result["missing_property_type_count"], 2)
        self.assertEqual(result["unexplained_document_count"], 5)
        self.assertIsNone(result["rows"][0]["Type"])

    def test_null_last_override_does_not_resurrect_an_earlier_override(self):
        pd.DataFrame([
            ["Residential", "Detached", "2 Storey", "TOWNHOUSE"],
            ["Residential", "Detached", "2 Storey", None],
        ], columns=monitor.TUPLE + ["propertyType"]).to_feather(self.override_path)
        result = monitor.reconcile_groups([bucket()], 3, self.root)
        self.assertEqual(result["rows"][0]["mapping_source"], "algorithm_cache")
        self.assertEqual(result["rows"][0]["lineage_status"], "consistent")
        self.assertEqual(result["sources"]["overrides"]["duplicate_rows"], 2)

    def test_missing_source_keys_match_literal_na_cache_keys_without_merging_raw_tuples(self):
        pd.DataFrame([
            [None, "Detached", "2 Storey", "CONDO"],
            ["N/A", "Detached", "2 Storey", "DETACHED"],
        ], columns=monitor.TUPLE + ["Inferred"]).to_feather(self.base_path)
        result = monitor.reconcile_groups([bucket(typ=None), bucket(typ="N/A")], 6, self.root)
        self.assertEqual(result["tuple_count"], 2)
        self.assertEqual(result["different_document_count"], 0)
        self.assertEqual(pd.DataFrame(result["rows"])["algorithm_mapping"].unique().tolist(), ["DETACHED"])

    def test_mixed_provincial_and_algorithm_sources_have_weighted_counts(self):
        self.override_path.unlink()
        result = monitor.reconcile_groups([
            bucket("Duplex", "Bungalow", "SEMI-DETACHED", "AB", 4),
            bucket("Duplex", "Bungalow", "SEMI-DETACHED", "ON", 2),
        ], 6, self.root)
        row = result["rows"][0]
        self.assertEqual(row["mapping_source"], "mixed")
        self.assertEqual(row["provincial_count"], 4)
        self.assertEqual(row["algorithm_count"], 2)
        self.assertEqual(row["different_count"], 2)

    def test_lookup_unavailable_does_not_hide_es_groups(self):
        result = monitor.reconcile_groups([bucket()], 3, None)
        self.assertEqual(result["rows"][0]["count"], 3)
        self.assertEqual(result["rows"][0]["mapping_source"], "unexplained")
        self.assertFalse(result["sources"]["algorithm_cache"]["available"])

    def test_source_files_are_byte_identical_after_reconciliation(self):
        base_hash = hashlib.sha256(self.base_path.read_bytes()).hexdigest()
        override_hash = hashlib.sha256(self.override_path.read_bytes()).hexdigest()
        monitor.reconcile_groups([bucket()], 3, self.root)
        self.assertEqual(hashlib.sha256(self.base_path.read_bytes()).hexdigest(), base_hash)
        self.assertEqual(hashlib.sha256(self.override_path.read_bytes()).hexdigest(), override_hash)

    def test_no_python_row_iteration_or_apply(self):
        tree = ast.parse(inspect.getsource(monitor))
        self.assertFalse(any(isinstance(node, (ast.For, ast.While, ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp))
                             for node in ast.walk(tree)))
        self.assertFalse(any(isinstance(node, ast.Attribute) and node.attr in {"apply", "iterrows", "itertuples"}
                             for node in ast.walk(tree)))

    def test_empty_index_is_complete_and_incorrect_counts_are_rejected(self):
        self.assertEqual(monitor.reconcile_groups([], 0, self.root)["rows"], [])
        with self.assertRaises(UpstreamDataError):
            monitor.reconcile_groups([bucket(count=2)], 3, self.root)
        with self.assertRaises(UpstreamDataError):
            monitor.reconcile_groups([bucket(count=-1)], -1, self.root)


class MappingQueryTests(IsolatedAsyncioTestCase):
    def client(self):
        return SimpleNamespace(
            open_point_in_time=AsyncMock(return_value={"id": "snapshot"}),
            close_point_in_time=AsyncMock(), search=AsyncMock())

    @staticmethod
    def response(count, buckets=None, pit="snapshot"):
        return {"took": 20, "timed_out": False, "_shards": {"failed": 0}, "pit_id": pit,
                "hits": {"total": {"value": count, "relation": "eq"}, "hits": []},
                "aggregations": {"mapping_groups": {"value": buckets}}}

    async def test_disjoint_slices_return_only_aggregates_and_close_snapshot(self):
        client = self.client()
        client.search.side_effect = [self.response(5), self.response(3, [bucket()], "new-id"),
                                     self.response(2, [bucket(count=2)], "latest-id")]
        with patch.object(monitor, "SCAN_SLICES", 2):
            result = await monitor.mapping_report(SimpleNamespace(client=client, settings=Settings(api_key="test")))
        self.assertEqual(result["document_count"], 5)
        self.assertEqual(result["rows"][0]["count"], 5)
        self.assertFalse(result["historical_provenance_available"])
        client.open_point_in_time.assert_awaited_once_with(index="rlp_ims_sold_listing_current", keep_alive="3m")
        self.assertEqual(client.search.await_count, 3)
        arguments = client.search.call_args_list[1].kwargs
        self.assertEqual(arguments["body"]["size"], 0)
        self.assertFalse(arguments["body"]["_source"])
        self.assertFalse(arguments["allow_partial_search_results"])
        self.assertNotIn("terms", arguments["body"]["query"])
        self.assertEqual(arguments["body"]["slice"], {"id": 0, "max": 2})
        self.assertEqual(client.search.call_args_list[2].kwargs["body"]["pit"]["id"], "new-id")
        client.close_point_in_time.assert_awaited_once_with(body={"id": "latest-id"})

    async def test_partial_or_truncated_results_are_not_published(self):
        client = self.client()
        client.search.return_value = {"timed_out": True}
        repo = SimpleNamespace(client=client, settings=Settings(api_key="test"))
        with self.assertRaises(ESUnavailableError):
            await monitor.mapping_report(repo)
        client.search.return_value = {"hits": {"total": {"value": 10000, "relation": "gte"}}}
        with self.assertRaises(UpstreamDataError):
            await monitor.mapping_report(repo)
        self.assertEqual(client.close_point_in_time.await_count, 2)

    async def test_incomplete_slice_coverage_and_slice_error_close_snapshot(self):
        client = self.client()
        repo = SimpleNamespace(client=client, settings=Settings(api_key="test"))
        client.search.side_effect = [self.response(4), self.response(3, [bucket()])]
        with patch.object(monitor, "SCAN_SLICES", 1), self.assertRaises(UpstreamDataError):
            await monitor.mapping_report(repo)
        client.close_point_in_time.assert_awaited_once()
        client.search.side_effect = [self.response(3), {"timed_out": True}]
        with self.assertRaises(ESUnavailableError):
            await monitor.mapping_report(repo)
        self.assertEqual(client.close_point_in_time.await_count, 2)

    async def test_http_route_is_read_only_and_returns_unavailable_without_partial_data(self):
        import httpx
        from realestate_analytics.api.main import app
        client = self.client()
        client.search.side_effect = [self.response(3), self.response(3, [bucket()])]
        app.state.repository = SimpleNamespace(client=client, settings=Settings(api_key="test"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            with patch.object(monitor, "SCAN_SLICES", 1):
                response = await http.get("/api/v1/ims-mappings")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["document_count"], 3)
            from realestate_analytics.api.ims_mapping_schemas import MappingReport
            MappingReport.model_validate(response.json())
            self.assertEqual((await http.post("/api/v1/ims-mappings")).status_code, 405)
            client.search.side_effect = None
            client.search.return_value = {"timed_out": True}
            self.assertEqual((await http.get("/api/v1/ims-mappings")).status_code, 503)
            await app.state.repository.ims_monitor_lock.acquire()
            self.assertEqual((await http.get("/api/v1/ims-mappings")).status_code, 409)
            app.state.repository.ims_monitor_lock.release()
