"""Native navigation geometry parity, failure handling and lifecycle tests."""
import asyncio
import copy
import json
import random
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from elasticsearch import ConnectionError, NotFoundError
from shapely.geometry import shape

from realestate_analytics.api.config import Settings
from realestate_analytics.api.geo_cache import GeoSnapshot, LazyGeoCache
from realestate_analytics.api.main import app, lifespan
from realestate_analytics.api.repository import Repository
from realestate_analytics.api.geometry_processing import NavigationGeometryProcessor


_REPOSITORY_FEATURES = Repository._navigation_features


class NativeProcessor(NavigationGeometryProcessor):
    """Observe the real processor without changing its execution path."""

    def __init__(self):
        super().__init__()
        self.stats_lock = threading.Lock()
        self.active = self.peak = 0
        self.worker_threads = set()

    def _work(self, indices, geometries):
        with self.stats_lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.worker_threads.add(threading.get_ident())
        try:
            return super()._work(indices, geometries)
        finally:
            with self.stats_lock:
                self.active -= 1

    def features(self, sources):
        repo = Repository(MagicMock(), Settings(
            api_key="test"), geometry_processor=self)
        return _REPOSITORY_FEATURES(repo, sources)


class PrototypeRepository(Repository):
    def __init__(self, *args, processor, **kwargs):
        super().__init__(*args, geometry_processor=processor, **kwargs)


RING = [[43., -79.], [43., -78.], [44., -78.], [44., -79.], [43., -79.]]
HOLE = [[-79.8, 43.2], [-79.8, 43.8], [-79.2, 43.8], [-79.2, 43.2], [-79.8, 43.2]]


def normalized(obj): return json.loads(json.dumps(obj, allow_nan=False))


def baseline_features(sources): return Repository(
    MagicMock(), Settings(api_key='test'))._navigation_features(sources)


def scalar(gs): return [f['geometry']
                        for f in Repository._simplify_navigation([{'geometry': g} for g in gs])]


def result(fn):
    try:
        return ('ok', normalized(fn()))
    except Exception as exc:
        return ('error', type(exc).__name__, str(exc))


class GeometryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.processor = NativeProcessor()
    @classmethod
    def tearDownClass(cls): cls.processor.close()

    def test_missing_empty_and_order(self):
        sources = [('g30_missing', {}, False), ('g30_empty', {'meta': {'geometry': []}}, True),
                   ('g30_valid', {'meta': {'geometry': [RING]}}, False)]
        self.assertEqual(result(lambda: baseline_features(sources)),
                         result(lambda: self.processor.features(sources)))
        self.assertEqual(self.processor.features([]), [])

    def test_malformed_input_exact_exception_parity(self):
        cases = [{}, {'meta': {}}, {'meta': {'geometry': None}}, {'meta': {'geometry': False}},
                 {'meta': {'geometry': 'oops'}}, {'meta': {'geometry': {'a': 1}}}]
        bad_rings = [[], RING[:3], RING[:-1], [[43, -79], [43, -78], [44, -78], [43, -80]],
                     [[True, -79], *RING[1:]], [[43, False], *RING[1:]],
                     [['43', -79], *RING[1:]], [[None, -79], *RING[1:]],
                     [[float('nan'), -79], *RING[1:]], [[43, float('inf')], *RING[1:]],
                     [[91, -79], *RING[1:]], [[43, -181], *RING[1:]],
                     [[43], *RING[1:]], [[43, -79, 0], *RING[1:]], ['bad', *RING[1:]], tuple(RING)]
        cases += [{'meta': {'geometry': [ring]}} for ring in bad_rings]
        cases += [{'meta': {'geometry': [RING, ring]}} for ring in bad_rings]
        for i, detail in enumerate(cases):
            with self.subTest(i=i):
                sources = [('g30_case', detail, False)]
                self.assertEqual(result(lambda: baseline_features(sources)), result(
                    lambda: self.processor.features(sources)))

    def test_numeric_limits_types_and_precision(self):
        rng = random.Random(47)
        rings = [[[0, -0.0], [0, 1], [1, 1], [0, -0.0]],
                 [[90, 180], [89, 180], [89, 179], [90, 180]],
                 [[-90, -180], [-89, -180], [-89, -179], [-90, -180]],
                 [[43, -79], [43., -78.0], [44, -78], [43, -79]]]
        for _ in range(500):
            lat, lng = rng.uniform(-88, 88), rng.uniform(-178, 178)
            rings.append([[lat, lng], [lat, lng+.05],
                         [lat+.05, lng+.05], [lat+.05, lng], [lat, lng]])
        sources = [(f'g30_{i}', {'meta': {'geometry': [ring]}}, bool(i % 2))
                   for i, ring in enumerate(rings)]
        expected = baseline_features(sources)
        actual = self.processor.features(sources)
        self.assertEqual(normalized(expected), normalized(actual))
        for a, b in zip(actual, expected):
            self.assertEqual(shape(a['geometry']).wkb, shape(b['geometry']).wkb)

    def test_holes_multipart_invalid_and_empty_topology(self):
        outer = [[-80, 43], [-79, 43], [-79, 44], [-80, 44], [-80, 43]]
        other = [[-82, 43], [-81, 43], [-81, 44], [-82, 44], [-82, 43]]
        bow = [[-80, 43], [-79, 44], [-79, 43], [-80, 44], [-80, 43]]
        gs = [None, {'type': 'Polygon', 'coordinates': []},
              {'type': 'MultiPolygon', 'coordinates': []},
              {'type': 'Polygon', 'coordinates': [outer, HOLE]},
              {'type': 'MultiPolygon', 'coordinates': [[outer, HOLE], [other]]},
              {'type': 'Polygon', 'coordinates': [bow]},
              {'type': 'Polygon', 'coordinates': [[[-80, 43]]*4]},
              {'type': 'Polygon', 'coordinates': [[[-80, 43], [-79.99999, 43], [-80, 43.00001], [-80, 43]]]}]
        before = copy.deepcopy(gs)
        expected = scalar(gs)
        actual = self.processor.simplify(gs)
        self.assertEqual(normalized(actual), normalized(expected))
        for a, b in zip(actual, expected):
            if a:
                self.assertEqual(shape(a).wkb, shape(b).wkb)
        self.assertEqual(gs, before)
        self.assertEqual(len(actual[3]['coordinates']), 2)
        self.assertEqual(len(actual[4]['coordinates'][0]), 2)

    def test_no_input_mutation(self):
        sources = [('g30_one', {'meta': {'geometry': [copy.deepcopy(RING)]}}, True)]
        before = copy.deepcopy(sources)
        a = self.processor.features(sources)
        b = self.processor.features(sources)
        self.assertEqual(normalized(a), normalized(b))
        self.assertEqual(sources, before)

    def test_worker_failure_isolated_and_pool_recovers(self):
        original = self.processor._work
        def fail(indices, geoms): raise RuntimeError('injected worker failure')
        self.processor._work = fail
        try:
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                self.processor.features(
                    [('g30_one', {'meta': {'geometry': [RING]}}, False)])
        finally:
            self.processor._work = original
        self.assertEqual(self.processor.active, 0)
        self.assertEqual(len(self.processor.features(
            [('g30_one', {'meta': {'geometry': [RING]}}, False)])), 1)


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.processor = NativeProcessor()
        self.settings = Settings(api_key='test')
        self.details = {'g30_live': {'meta': {'geometry': [copy.deepcopy(RING)]}},
                        'g30_empty': {'meta': {'geometry': []}}, 'g30_bad': {'meta': {'geometry': [[[True, -79], *RING[1:]]]}}}
        snapshot = GeoSnapshot([SimpleNamespace(geog_id=gid, level=30, province='ON', name=gid, parent_id=None,
                                                geometry=[[[45, -81], [45, -80], [46, -80], [45, -81]]]) for gid in ('g30_live', 'g30_cached', 'g30_empty')])
        self.cache = LazyGeoCache(Path('unused'), loader=lambda path: snapshot)
        self.es = MagicMock()

        async def mget(index, body, **kwargs):
            await asyncio.sleep(0)
            return {'docs': [{'_id': gid, 'found': gid in self.details, '_source': copy.deepcopy(self.details.get(gid, {}))} for gid in reversed(body['ids'])]}

        async def get(index, id, **kwargs):
            if id not in self.details:
                raise NotFoundError(404, 'missing', {})
            return {'_id': id, '_source': copy.deepcopy(self.details[id])}
        self.es.mget = AsyncMock(side_effect=mget)
        self.es.get = AsyncMock(side_effect=get)
        self.base = Repository(self.es, self.settings, self.cache)
        self.fast = PrototypeRepository(
            self.es, self.settings, self.cache, processor=self.processor)
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(
            app=app, raise_app_exceptions=False), base_url='http://test')

    async def asyncTearDown(self):
        await self.http.aclose()
        self.processor.close()

    async def post(self, repo, ids):
        app.state.repository = repo
        r = await self.http.post('/api/v1/geos/boundaries', json={'geog_ids': ids})
        return r.status_code, r.json()

    async def test_full_api_response_parity_and_es_precedence(self):
        ids = ['g30_cached', 'g30_live', 'g30_unknown', 'g30_empty', 'g30_live']
        expected = await self.post(self.base, ids)
        actual = await self.post(self.fast, ids)
        self.assertEqual(actual, expected)
        self.assertEqual(actual[0], 200)
        fs = actual[1]['features']
        self.assertEqual(len(fs), 4)
        self.assertEqual(fs[1]['geometry']['coordinates'][0][0], [-79, 43])
        self.assertEqual(fs[1]['provenance']['source'], 'elasticsearch')
        self.assertEqual(fs[0]['provenance']['source'], 'geo_collection')
        self.assertEqual(fs[2]['properties']['geometry_status'], 'missing')
        for call in self.es.mget.call_args_list:
            self.assertEqual(call.kwargs['_source_includes'], ['meta.geometry'])

    async def test_bad_geometry_and_invalid_requests_match(self):
        for ids in (['g30_bad'], [], ['../bad'], ['g30_live']*2001):
            self.assertEqual(await self.post(self.base, ids), await self.post(self.fast, ids))

    async def test_upstream_outage_and_partial_errors_match(self):
        self.es.mget.side_effect = ConnectionError('offline')
        self.assertEqual(await self.post(self.base, ['g30_cached', 'g30_unknown']), await self.post(self.fast, ['g30_cached', 'g30_unknown']))
        self.es.mget.side_effect = None
        self.es.mget.return_value = {
            'docs': [{'_id': 'g30_live', 'error': {'type': 'failed'}}]}
        self.assertEqual(await self.post(self.base, ['g30_live']), await self.post(self.fast, ['g30_live']))

    async def test_exact_detail_route_unchanged(self):
        async def detail(repo):
            app.state.repository = repo
            r = await self.http.get('/api/v1/geos/g30_live?include_geometry=true')
            return r.status_code, r.json()
        self.assertEqual(await detail(self.base), await detail(self.fast))

    async def test_concurrent_api_requests_share_two_native_workers(self):
        app.state.repository = self.fast
        ids = [f'g30_{i}' for i in range(201)]
        for gid in ids:
            self.details[gid] = {'meta': {'geometry': [RING]}}
        expected_status, expected = await self.post(self.base, ids)
        app.state.repository = self.fast
        statuses = []

        async def request(i):
            r = await self.http.post('/api/v1/geos/boundaries', json={'geog_ids': ids if i % 2 else list(reversed(ids))})
            self.assertEqual(r.status_code, expected_status)
            fs = r.json()['features']
            ref = expected['features'] if i % 2 else list(
                reversed(expected['features']))
            self.assertEqual(fs, ref)
            statuses.append(r.status_code)
        await asyncio.gather(*(request(i) for i in range(8)))
        self.assertEqual(len(statuses), 8)
        self.assertLessEqual(self.processor.peak, 2)
        self.assertLessEqual(len(self.processor.worker_threads), 2)
        self.assertEqual(self.processor.active, 0)

    async def test_batch_boundaries_up_to_full_request_limit(self):
        for count in (1, 99, 100, 101, 1999, 2000):
            with self.subTest(count=count):
                ids = [f'g30_batch_{i}' for i in range(count)]
                for gid in ids:
                    self.details[gid] = {'meta': {'geometry': [RING]}}
                self.assertEqual(await self.post(self.base, ids), await self.post(self.fast, ids))
        self.assertLessEqual(self.processor.peak, 2)

    async def test_cancellation_does_not_break_followup_request(self):
        original = self.processor._work
        entered = threading.Event()
        release = threading.Event()

        def block(indices, geoms):
            entered.set()
            release.wait(timeout=5)
            return original(indices, geoms)
        self.processor._work = block
        app.state.repository = self.fast
        task = asyncio.create_task(self.http.post(
            '/api/v1/geos/boundaries', json={'geog_ids': ['g30_live']}))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 2))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            self.processor._work = original
        r = await self.http.post('/api/v1/geos/boundaries', json={'geog_ids': ['g30_live']})
        self.assertEqual(r.status_code, 200)


class ProcessorLifecycleTests(unittest.TestCase):
    def test_pool_is_lazy_and_close_is_idempotent(self):
        processor = NavigationGeometryProcessor()
        self.assertEqual(len(processor._executor._threads), 0)
        processor.close()
        processor.close()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            processor.simplify([])

    def test_close_drains_an_inflight_call(self):
        processor = NavigationGeometryProcessor()
        entered, release = threading.Event(), threading.Event()
        original = processor._work

        def block(indices, geometries):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("test worker was not released")
            return original(indices, geometries)
        processor._work = block
        geometry = Repository._geometry_from_detail({"meta": {"geometry": [RING]}})
        try:
            with ThreadPoolExecutor(max_workers=2) as callers:
                running = callers.submit(processor.simplify, [geometry])
                self.assertTrue(entered.wait(2))
                closing = callers.submit(processor.close)
                with processor._condition:
                    self.assertTrue(processor._condition.wait_for(
                        lambda: processor._closed, timeout=2))
                self.assertFalse(closing.done())
                with self.assertRaisesRegex(RuntimeError, "closed"):
                    processor.simplify([geometry])
                release.set()
                self.assertEqual(len(running.result(timeout=3)), 1)
                closing.result(timeout=3)
            self.assertTrue(all(not thread.is_alive()
                            for thread in processor._executor._threads))
        finally:
            release.set()
            processor.close()


class AppProcessorLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_processor_is_shared_and_closed_with_the_app(self):
        client = MagicMock()
        client.ping = AsyncMock(return_value=True)
        client.cat.nodes = AsyncMock(return_value=[{"name": "prod"}])
        client.indices.exists = AsyncMock(return_value=True)
        client.close = AsyncMock()
        processor = MagicMock()
        with patch("realestate_analytics.api.main.Settings.from_environment", return_value=Settings(api_key="test")), \
                patch("realestate_analytics.api.main.AsyncElasticsearch", return_value=client), \
                patch("realestate_analytics.api.main.NavigationGeometryProcessor", return_value=processor) as factory:
            async with lifespan(app):
                self.assertIs(app.state.repository.geometry_processor, processor)
                factory.assert_called_once_with()
                processor.close.assert_not_called()
        processor.close.assert_called_once_with()
        client.close.assert_awaited_once()

    async def test_startup_failure_closes_both_resources(self):
        client = MagicMock()
        client.ping = AsyncMock(return_value=True)
        client.cat.nodes = AsyncMock(return_value=[{"name": "uat"}])
        client.close = AsyncMock()
        processor = MagicMock()
        with patch("realestate_analytics.api.main.Settings.from_environment", return_value=Settings(api_key="test")), \
                patch("realestate_analytics.api.main.AsyncElasticsearch", return_value=client), \
                patch("realestate_analytics.api.main.NavigationGeometryProcessor", return_value=processor):
            with self.assertRaises(RuntimeError):
                async with lifespan(app):
                    self.fail("startup must reject UAT")
        processor.close.assert_called_once_with()
        client.close.assert_awaited_once()

    async def test_client_still_closes_if_processor_shutdown_raises(self):
        client = MagicMock()
        client.ping = AsyncMock(return_value=True)
        client.cat.nodes = AsyncMock(return_value=[{"name": "prod"}])
        client.indices.exists = AsyncMock(return_value=True)
        client.close = AsyncMock()
        processor = MagicMock()
        processor.close.side_effect = RuntimeError("injected close failure")
        with patch("realestate_analytics.api.main.Settings.from_environment", return_value=Settings(api_key="test")), \
                patch("realestate_analytics.api.main.AsyncElasticsearch", return_value=client), \
                patch("realestate_analytics.api.main.NavigationGeometryProcessor", return_value=processor):
            with self.assertRaisesRegex(RuntimeError, "close failure"):
                async with lifespan(app):
                    pass
        client.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
