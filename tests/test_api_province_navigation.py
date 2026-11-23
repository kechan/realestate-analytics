"""Province navigation stays ES-first and never pulls descendant geometries."""
import copy
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

import httpx
from elasticsearch import ConnectionError

from realestate_analytics.api.config import Settings
from realestate_analytics.api.geo_cache import GeoSnapshot, LazyGeoCache
from realestate_analytics.api.main import app
from realestate_analytics.api.repository import Repository


class ProvinceNavigationTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.es = MagicMock()
        self.bases = {
            'g40_region': {'level': 40, 'province': 'QC', 'level40En': 'Region'},
            'g35_estrie': {'level': 35, 'province': 'QC', 'level35En': 'Estrie'},
            'g30_city': {'level': 30, 'province': 'QC', 'level30En': 'Nested city'},
            'g30_direct': {'level': 30, 'province': 'QC', 'level30En': 'Direct city'},
        }
        self.details = {'g30_city': {'data': {'parent': 'g35_estrie'}}}
        self.cache = LazyGeoCache()
        self.repo = Repository(self.es, Settings(api_key='test'), self.cache)
        async def search(index, body, **kwargs):
            return {'hits': {'total': {'value': len(self.bases), 'relation': 'eq'},
                             'hits': [{'_id': gid, '_source': copy.deepcopy(src)} for gid, src in self.bases.items()]}}
        async def mget(index, body, **kwargs):
            return {'docs': [{'_id': gid, 'found': gid in self.details,
                              '_source': copy.deepcopy(self.details.get(gid, {}))} for gid in body['ids']]}
        self.es.search = AsyncMock(side_effect=search)
        self.es.mget = AsyncMock(side_effect=mget)
        self.es.get = AsyncMock()
        app.state.repository = self.repo
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test')

    async def asyncTearDown(self):
        await self.http.aclose()

    async def test_top_level_mixed_areas_exclude_linked_descendants(self):
        r = await self.http.get('/api/v1/provinces/qc/children')
        self.assertEqual(r.status_code, 200, r.text)
        data = r.json()
        self.assertEqual([g['properties']['name'] for g in data['features']], ['Direct city', 'Estrie', 'Region'])
        self.assertEqual(data['total'], 3)
        self.assertIsNone(data['limit'])
        self.assertIsNone(data['geometry_tolerance'])
        self.assertTrue(all(g['geometry'] is None for g in data['features']))
        query = self.es.search.call_args.kwargs['body']['query']
        self.assertIn({'range': {'level': {'gte': 30}}}, query['bool']['filter'])
        self.es.get.assert_not_awaited()
        self.assertEqual(self.es.mget.await_count, 1)

    async def test_infers_parent_from_same_province_metadata_without_extra_search(self):
        self.details.clear()
        self.bases['g30_city']['level35En'] = 'Estrie'
        features = await self.repo.province_children('QC')
        self.assertNotIn('g30_city', [f['properties']['geog_id'] for f in features])
        self.es.search.assert_awaited_once()
        self.es.msearch.assert_not_called()

    async def test_geometry_is_batched_source_filtered_and_simplified_only_for_navigation(self):
        ring = [[45, -73], [45, -72.99999], [45, -72.5], [46, -72.5], [45, -73]]
        self.details['g35_estrie'] = {'meta': {'geometry': [ring]}}
        before = copy.deepcopy(self.details)
        r = await self.http.get('/api/v1/provinces/QC/children?include_geometry=true')
        self.assertEqual(r.status_code, 200, r.text)
        data = r.json()
        estrie = next(g for g in data['features'] if g['properties']['name'] == 'Estrie')
        self.assertEqual(estrie['provenance']['source'], 'elasticsearch')
        self.assertLess(len(estrie['geometry']['coordinates'][0]), len(ring))
        self.assertEqual(data['geometry_tolerance'], .0025)
        self.assertEqual(before, self.details)
        geometry_read = self.es.mget.call_args.kwargs
        self.assertEqual(geometry_read['_source_includes'], ['meta.geometry'])
        self.assertNotIn('g30_city', geometry_read['body']['ids'])
        self.es.get.assert_not_awaited()

    async def test_cached_parent_and_missing_boundary_only_fill_gaps(self):
        snapshot = GeoSnapshot([SimpleNamespace(geog_id=gid, level=src['level'], province='QC', name='Old name',
            parent_id='g35_estrie' if gid == 'g30_city' else None,
            geometry=[[[45, -73], [45, -72], [46, -72], [45, -73]]]) for gid, src in self.bases.items()])
        self.repo.geo_cache = LazyGeoCache(Path('unused'), loader=lambda path: snapshot)
        self.details.clear()
        self.details['g35_estrie'] = {'meta': {'geometry': [[[44, -71], [44, -70], [45, -70], [44, -71]]]}}
        features = await self.repo.province_children('QC', True)
        estrie = next(f for f in features if f['properties']['geog_id'] == 'g35_estrie')
        self.assertEqual(estrie['properties']['name'], 'Estrie')
        self.assertEqual(estrie['geometry']['coordinates'][0][0], (-71.0, 44.0))
        self.assertNotIn('geometry', estrie['provenance']['fallback_fields'])
        self.assertNotIn('g30_city', [f['properties']['geog_id'] for f in features])
        direct = next(f for f in features if f['properties']['geog_id'] == 'g30_direct')
        self.assertIn('geometry', direct['provenance']['fallback_fields'])

    async def test_es_outage_without_snapshot_is_503(self):
        self.es.search.side_effect = ConnectionError('offline')
        r = await self.http.get('/api/v1/provinces/ON/children')
        self.assertEqual(r.status_code, 503)

    async def test_es_outage_uses_snapshot_hierarchy_and_boundary(self):
        self.es.search.side_effect = ConnectionError('offline')
        ring = [[45, -73], [45, -72], [46, -72], [45, -73]]
        snapshot = GeoSnapshot([SimpleNamespace(geog_id=gid, level=src['level'], province='QC', name=gid,
            parent_id='g35_estrie' if gid == 'g30_city' else None, geometry=[ring]) for gid, src in self.bases.items()])
        self.repo.geo_cache = LazyGeoCache(Path('unused'), loader=lambda path: snapshot)
        features = await self.repo.province_children('QC', True)
        self.assertEqual(len(features), 3)
        self.assertTrue(all(f['geometry'] and f['provenance']['source'] == 'geo_collection' for f in features))
        self.es.mget.assert_not_awaited()

    async def test_boundary_batches_are_bounded_even_for_large_provinces(self):
        self.bases = {f'g40_{i}': {'level': 40, 'province': 'QC', 'level40En': f'Region {i}'} for i in range(201)}
        features = await self.repo.province_children('QC', True)
        self.assertEqual(len(features), 201)
        boundary_calls = [c for c in self.es.mget.call_args_list if c.kwargs['_source_includes'] == ['meta.geometry']]
        self.assertEqual([len(c.kwargs['body']['ids']) for c in boundary_calls], [100, 100, 1])
        self.es.get.assert_not_awaited()

    async def test_unknown_province_rejected_before_es(self):
        r = await self.http.get('/api/v1/provinces/XX/children')
        self.assertEqual(r.status_code, 422)
        self.es.search.assert_not_awaited()

    async def test_bulk_boundaries_never_query_names_or_parents(self):
        r = await self.http.post('/api/v1/geos/boundaries', json={'geog_ids': ['g35_estrie', 'g30_direct']})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual([f['properties']['geog_id'] for f in r.json()['features']], ['g35_estrie', 'g30_direct'])
        self.es.search.assert_not_awaited()
        self.es.get.assert_not_awaited()
        self.es.mget.assert_awaited_once()
        self.assertEqual(self.es.mget.call_args.kwargs['_source_includes'], ['meta.geometry'])

    async def test_bulk_boundaries_validate_ids_and_request_cap_before_es(self):
        for ids in ([], ['../../secret'], ['g30_city'] * 2001):
            r = await self.http.post('/api/v1/geos/boundaries', json={'geog_ids': ids})
            self.assertEqual(r.status_code, 422, r.text)
        self.es.mget.assert_not_awaited()

    async def test_geometry_concurrency_is_bounded_and_result_order_stable(self):
        import asyncio
        active, peak = 0, 0
        async def read(index, body, **kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return {'docs': [{'_id': gid, 'found': False} for gid in reversed(body['ids'])]}
        self.es.mget.side_effect = read
        ids = [f'g40_{i}' for i in range(601)]
        features = await self.repo.navigation_boundaries(ids)
        self.assertEqual([f['properties']['geog_id'] for f in features], ids)
        self.assertEqual(peak, 3)
        self.assertEqual(self.es.mget.await_count, 7)

    async def test_province_scroll_consumes_first_page_once_and_cleans_up(self):
        first = [{'_id': str(i)} for i in range(2000)]
        self.es.search.return_value = {'_scroll_id': 'first', 'hits': {'hits': first, 'total': {'value': 2001, 'relation': 'eq'}}}
        self.es.search.side_effect = None
        self.es.scroll = AsyncMock(return_value={'_scroll_id': 'second', 'hits': {'hits': [{'_id': 'last'}]}})
        self.es.clear_scroll = AsyncMock()
        result = await self.repo._search_all('geo', {'match_all': {}}, ['level'], scroll_from_start=True)
        self.assertEqual(len(result), 2001)
        self.assertEqual(len({r['_id'] for r in result}), 2001)
        self.es.search.assert_awaited_once()
        self.es.scroll.assert_awaited_once()
        self.es.clear_scroll.assert_awaited_once_with(scroll_id='second')
