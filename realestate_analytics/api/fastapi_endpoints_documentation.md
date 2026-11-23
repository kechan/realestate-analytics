# Standalone Real Estate Analytics API

The serving API reads published data directly from production Elasticsearch. Geography requests can fill missing information from an optional serialized GeoCollection; market metrics always read ES directly. Dependencies are listed in `requirements.txt`, including `dill` and `shapely` for the optional snapshot. Geography and metrics serving do not import ETL scripts or archives. The optional CSV preview imports the production mapper as a read-only third-party dependency; it never runs ETL or ES-writing routines.

## Configuration and startup

The server loads `~/.env` without overriding exported values, then `~/.rlp_es_env` with `override=True`, and requires `RLP_LISTING_ES_API_KEY`. An exported key may be used if the key file does not exist. The host is `https://esp.royallepage.ca:443`; startup checks production node identity and access to the market trends, geography, and geography-details indices. Network access to the production ES host and a key with read permissions for these resources and node information are required. When geography fallback is configured, ES unavailability permits startup in degraded mode; readiness still returns 503. An unexpected ES environment identity always prevents startup.

Optional environment variables:

- `ALLOW_ORIGINS`: comma-separated browser origins; default `http://localhost:3000`.
- `API_GEO_INDEX`: geography index; default `rlp_content_geo_4`.
- `API_GEO_DETAILS_INDEX`: geography detail index; default `rlp_geo_details_en`.
- `ANALYTICS_CACHE_DIR`: optional directory containing `geo_collection.dill`. Existing local configuration uses `/Users/kechan/tmp/listing_analytics_data`. When unset, geography fallback is disabled. Deploy the trusted snapshot alongside the standalone API and configure this directory there; the ETL project is not needed for geography/metrics serving. CSV preview additionally imports the existing production mapper as a read-only dependency.

Start the serving app with the project environment:

```sh
.venv/bin/python -m uvicorn realestate_analytics.api.main:app --port 8001
```

Interactive OpenAPI documentation is at `/docs`. The old unversioned metric and geography endpoints have been replaced, and existing clients require an update.

## Read-only IMS mapping monitor

`GET /api/v1/ims-mappings` groups **all stored history** in
`rlp_ims_sold_listing_current` by exact `Type`, `SubType`, `Style`, and stored
`propertyType`. It returns counts rather than individual sold listings. Missing
values remain included; a missing property type is a distinct outcome when
detecting tuples with multiple classifications. Case and whitespace are retained.

```sh
curl --fail 'http://127.0.0.1:8013/api/v1/ims-mappings' -H 'accept: application/json'
```

The frontend page is `/ims-property-type-mapping`. Its primary workflow is CSV
preview. Choose File or drag one CSV onto the drop zone, then select **Compute
property types**. Selecting or dropping a file does not upload or compute.
Production queries run only after **Query Production ES** or **Refresh from ES**
and explicit confirmation of the full-source-scan cost. Cancel sends no request.
Filtering, sorting, result-source switching, and paging use completed results
without further ES requests. No mapping or ES edits are offered.

### Read-only CSV preview

`POST /api/v1/ims-mappings/preview` accepts one raw UTF-8 CSV (`text/csv` or
`application/octet-stream`). For a batch, concatenate the original CSV bytes
in order, use `Content-Type: application/vnd.ims.csv-batch`, and supply each
file's byte length in the comma-separated `X-IMS-CSV-Sizes` header. Every file has
its own header and may have a UTF-8 BOM. All inputs require `Type`, `SubType`,
`Style`, and `Province`. Batches also require `MLSID`, `MLSNumber`, and
`WrittenDate` in every file. Surrounding header whitespace is accepted. Missing
or duplicate headers, empty files, and parser errors are rejected. The preview
computes `propertyType` through the real mapper.

Uploads are streamed into temporary storage under `~/tmp` (spooling to disk
above 8 MiB), deleted on exit, and never retained as an application cache.
Limits are 20 files, 5 million input rows combined, and 100,000 unique normalized
source tuples. Uploads exceeding available temporary storage return HTTP 507.
The parser reads chunks and the API deduplicates
with vectorized operations before grouping. It uses the production document ID
components (province, MLS board, MLS number, sale year), preserving the
identifier text from the CSV. The last occurrence in upload and row order wins.
A legacy single file with just the four mapping columns still counts all rows.
Rows without a usable ID remain separate. The response reports
`csv_input_row_count`, retained `document_count`, and
`csv_unidentified_row_count`.

When there is a usable province value, the preview also returns
`province_breakdown` with per-province grouped rows and their summary counts.
Rows with unknown province remain included. The existing `rows` and top-level
counts still describe the combined view. A tuple can have one type in each
province and multiple types in the combined view.

**Production code is an immutable third-party dependency.** The optional CSV
path imports `IMSPropertyTypeMapper` through `api/ims_mapper_adapter.py`; it does
not vendor its inference/override rules or modify any production source file.
The adapter invokes the existing `load_dataframe()`,
`generate_full_mapping_dataframe()`, and `apply_property_types()` methods on
unique tuple/province groups. The mapper reuses its saved base mappings, infers
only unseen tuples, and applies its manual override dataframe. Its own inference
loop is reused only for unseen tuples; no per-listing mapper loop is added.
The API's joins annotate lineage, not recompute those classifications.

The adapter suppresses the mapper's two audit-file hooks, clears persistent
save destinations after inputs are loaded, and rejects `save_mapping()`. It does
not call the ES-writing bulk importer, GUID resolution, geocoding, or any save
routine. Both current mapping files must be readable. Their hashes identify the
inputs, and changed files cause an error rather than mixed-version results.

**Sole explicit exception:** the final AB duplex rule is embedded in both
`Datastore.bulk_import_ims_sold_listings()` and
`Datastore.bulk_update_ims_sold_listings()` with no pure helper to import. With
user approval, `api/ims_provincial_rules.py` mirrors it, shared by the preview and
ES lineage monitor. Its code comments, root `TODO.md`, and root `AGENTS.md`
document this temporary duplication. A drift regression test compares it with
both production blocks. A future production-helper refactor is deferred.

CSV output has `report_mode=csv` and check status `computed`, never a claim of
ES verification. The source controls keep CSV and ES reports distinct. No ES
request or write, cache save, or ETL sighting-log write occurs. The mapper code
is imported when the worker loads it: restart FastAPI after production mapper
source changes, and review the API provincial mirror if those rules change.
`rules_version` fingerprints the executed mapper methods. Only one CSV preview
runs per worker at a time. No additional frontend packages are needed.

`SubType` and `Style` are text fields without aggregatable keyword subfields in
the current Production index. An ES scripted metric therefore groups their
exact `_source` values. A full scan exceeded the gateway timeout during QA, so
the implementation opens one point-in-time snapshot, reads an exact baseline
count, then processes 16 disjoint slices sequentially. Each slice has a 30-second
ES timeout and returns only aggregates. The snapshot is closed in `finally`;
its keep-alive also expires if closing fails. This uses 19 ES requests for a
nonempty index (open, count, 16 slices, close), while each document is collected
by only one aggregation slice. Failed shards, timeout, inexact totals, or a
count mismatch cause an error rather than a successful partial report.
See [Elastic's point-in-time documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/point-in-time-api.html).

The ES monitor reads these existing files from `ANALYTICS_CACHE_DIR` afresh for
each completed report, without importing or executing the ETL/mapper:

- `ims_property_type_mapping_v4_df`: saved base `Inferred` lookup.
- `ims_property_type_lookup_df`: current manual `propertyType` overrides.

Pandas merges, masks, and grouped reductions reconcile the aggregated counts;
there is no Python per-tuple iteration or dataframe `apply`. Current precedence
matches ingestion: **AB duplex rule → non-null manual override → base lookup**.
The AB rule applies to `Duplex`, `Full Duplex`, `Half Duplex`, and
`Duplex Side By Side` subtypes and yields `SEMI-DETACHED`. Last duplicate lookup
keys win; null overrides are not applied. Missing or unreadable lookup files do
not hide ES counts, and their unavailable status is returned.

`mapping_source` and `lineage_status` describe **current lookup consistency**.
Historical mapping versions/provenance are not stored in ES, and the base cache
may itself contain prior corrections. They do not prove which rule assigned an
individual record historically. Provincial splits remain flagged as conflicts
even if the AB rule explains them. Source counters report records, not tuple
counts; no old cached `Count` column is presented as Production usage.

This optional feature uses the dependencies in `requirements-ims-monitor.txt`;
they are already installed in the current project environment. Core API startup
does not import pandas or pyarrow. No packages are installed automatically.
The API does not retain a report cache and permits one IMS scan per worker at a
time (409 if busy). It changes no index mappings or ETL code. The ES key needs
permission to read the IMS index and open/close its search snapshot.

## Metrics

`GET /api/v1/metrics` lists supported metrics, units, and descriptions: 12 monthly series and the `current_metrics` object. `GET /api/v1/metrics/{geog_id}` retrieves a published document by `{geog_id}_{property_type}` from `rlp_mkt_trends_current`.

Parameters: `property_type` defaults to `ALL`; `metrics` may be repeated to select monthly series and/or `current_metrics`; `start_month` and `end_month` are inclusive `YYYY-MM` bounds applied to monthly series only. With `metrics` omitted, the response returns all 12 monthly series plus current metrics. With an explicit selection, only those metrics are included: `current_metrics` is omitted unless explicitly selected. Monthly metrics remain under `series`; the current snapshot remains in the top-level `current_metrics` field. Selecting only `current_metrics` returns an empty `series` object.

```text
/api/v1/metrics/g30_dpz89rm7?property_type=ALL&metrics=median_sold_price&metrics=months_of_inventory&start_month=2025-01
```

Each series supplies `available`, `latest_available_month` (across the source series before date filtering), and `data`. Missing fields have `available: false`; missing current fields are null. Zero remains zero. Absorption is a ratio and sold-over/below-ask series are percentages. Asking-price bounds are filtered, not literal extrema. The document timestamp is shared across metrics and does not prove individual metric freshness. Quarterly metrics are not part of this API.

## Geography

`GET /api/v1/geos` supports `query`, `province`, `level`, `parent_id`, `include_geometry`, `limit` (optional, 1–200), and `offset` (optional). Detail and ancestry are available at `/api/v1/geos/{geog_id}` and `/api/v1/geos/{geog_id}/hierarchy`. Results are GeoJSON features, sorted by name and ID. A polygon missing from both sources is null and marked `missing`; geometry omitted by request is marked `not_requested`. Parent status identifies source, uniquely inferred, cache, unresolved, or root relationships; an incomplete hierarchy is flagged. The detail route performs a single GET against `rlp_geo_details_en` and returns that document under `details`, including its `data` and `meta` fields, with missing values supplemented as described below. It does not query the base geo index or traverse parents. Its feature properties contain ID, name, province, geometry status, and a level derived from the ID; they omit parent fields. Parent traversal remains on the hierarchy route (and parent links on list results). The source `meta.geometry` is excluded from `details`: when requested, geometry is returned once in the GeoJSON `geometry` field with longitude-first coordinates. A detail absent from ES uses the snapshot when available; absence from both sources returns 404.

Geography metadata is requested from Elasticsearch on every request. Search filters are applied in ES before fetching candidate records, and parent details are looked up only for the selected page (or parent-filter candidates). Source fields are restricted and any ES scroll is explicitly cleared. ES result caching is not used.

### Province navigation

`GET /api/v1/provinces/{province}/children?include_geometry=false` returns the immediate navigation areas for a Canadian province or territory. Codes are case-insensitive and validated. Provinces are virtual navigation containers: this endpoint returns city-and-larger records without a resolved geo parent, excluding cities linked under regions. Unresolved cities stay reachable directly under the province; missing parent links are not invented from polygon containment. This is a geography hierarchy, not a guarantee of published metrics or a non-overlapping administrative partition.

The API filters province and `level >= 30` in ES, requests only base metadata, and batches parent fields. Province discovery opens a scroll on its first search (2,000 metadata records per batch), retains that first page, and always clears the scroll. Parent-only `_mget` batches contain at most 2,000 IDs. Candidate names resolve within that metadata cohort. The frontend then sends the returned root IDs to the boundary endpoint, avoiding a repeated province search and parent lookup. No neighbourhood metadata, descendant polygons, profile prose, or metrics are fetched for this navigation request. ES remains primary and snapshot fallback fills missing data using the existing provenance rules.

With `include_geometry=true`, boundaries are simplified at **0.0025 degrees** with topology preservation, and `geometry_tolerance` reports that value. This reduces navigation response size; it does not modify ES, the snapshot, or detail endpoint geometry. Use `/geos/{geog_id}?include_geometry=true` for exact published coordinates. Missing boundaries remain null and those areas remain navigable through metadata. There is no implicit pagination cap on province children. The combined `include_geometry=true` route remains available for independent clients.

### Bulk navigation boundaries

`POST /api/v1/geos/boundaries` accepts `{"geog_ids": ["g40_dpz3tpuh"]}` and only reads data. It performs no ES writes, province search, parent lookup, or metric read. Supply 1–2,000 IDs per request. IDs are validated and deduplicated in first-occurrence order. The response is a GeoJSON FeatureCollection with ID, geometry status, provenance, and `geometry_tolerance: 0.0025`; metadata remains in the preceding province response.

Boundary reads use source-filtered `_mget` batches of 100 with at most three concurrent ES requests per API request. ES geometry is atomic and primary; a missing boundary uses the existing in-memory snapshot. Validation, conversion, and the existing topology-preserving simplification run off the event loop. Exact geo-detail output and market metric queries are unchanged. No ES-result cache is introduced.

The serving lifespan owns one `NavigationGeometryProcessor` per API process. It builds native geometries from NumPy coordinate arrays, batches Shapely validity/simplification operations, and shares a pool of **two native threads across requests**. Threads start lazily and are drained and joined on shutdown, including startup-failure cleanup. There is no Celery service, broker, custom Rust extension, or additional process to start. Multiple Uvicorn processes each have their own two-thread pool.

Strict coordinate validation still precedes array construction: strings, booleans, nonfinite values, invalid ranges, malformed points, and unclosed rings retain their existing behavior. Polygon holes, multipart geometry, missing/empty geometry, invalid topology, feature ordering, and provenance are preserved. Simplification uses the same 0.0025-degree tolerance and topology checks. NumPy is declared explicitly in this standalone API's requirements; it was already installed in the validated environment. No garbage-collection settings are changed; heavy concurrent requests can still encounter GC pauses.

### Optional geography snapshot fallback

- ES is always attempted first. On ES errors or absent documents, a matching snapshot record can supply geography information. For an existing ES document, only absent, null, or empty-string fields are filled recursively. Populated ES values, including zero and false, stay unchanged. Cached fields are not written back to ES.
- Geometry is atomic. An existing ES polygon is used as-is without inspecting cached coordinates. Only an entirely missing polygon is replaced with the complete cached geometry, and only when `include_geometry=true`. Geometry is never merged coordinate by coordinate.
- Missing parent links can come from the snapshot. Hierarchy responses can therefore connect Toronto to GTA, and an empty GTA child search can find cached child IDs. Cached search candidates are re-read from ES in batches, preserving current fields and reapplying search filters. A nonempty ES result set is not expanded with older cached records. Pagination semantics remain the same for fallback results.
- Each feature has `provenance.source` (`elasticsearch`, `mixed`, or `geo_collection`) and `provenance.fallback_fields` describing snapshot contributions. Collections also aggregate provenance. A cached parent is explicitly marked `parent_status: "cache"`.
- The snapshot is loaded only on the first fallback, in a background thread with a lock, and retained in memory until process restart. Each server worker has its own instance. Updating the file requires a worker restart. The loader restores only the serialized state needed by the API, without importing analytics data or ETL code. Use a deployment-controlled trusted dill file; dill deserialization is not intended for untrusted uploads.
- Missing/unreadable snapshots are logged and leave ES behavior in place. ES errors are logged even when a snapshot supplies a response. Readiness continues to check ES, and metric requests never consult this cache.

Pagination semantics:

- Omitting `limit` returns **all matching geography records**, with or without search filters. There is no implicit page-size cap, and the response reports `limit: null`. The normal list fields are returned, with geometry controlled by `include_geometry`.
- `offset` skips that many matching records; omitting it skips none. Providing `offset`, including `offset=0`, never imposes a result limit. Without `limit`, all remaining matches are returned.
- With no search filters and an explicit `limit`, ES performs sorting and pagination using `from`/`size`. The response's `total` is the exact number of matching documents, independent of page size. Filtered requests apply an explicit limit after resolving the matching candidates. Sorting is case-insensitive by display name with geographic ID as the tie-breaker, consistently across full downloads and pages.
- Deep offsets beyond ES's default 10,000-result window use sort cursors instead of failing or silently truncating the result. These requests can require additional ES calls.

## Errors and administration

Invalid parameters return 422; missing documents 404; ES failures 503; malformed source data 502. Error responses do not contain credentials or raw ES errors.

Existing `/op/*` and `/monitor/*` endpoints are available only through the separate ETL-server app, `realestate_analytics.api.admin:app`, which requires the ETL server's scripts, log paths, and cache configuration. Do not expose the admin app without appropriate access controls.
