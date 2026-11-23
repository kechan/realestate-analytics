# Repository Guidelines

## IMS CSV preview: production-code reuse and debugging

- Treat the main analytics/ETL project as an immutable third-party dependency
  while working on this secondary tool. Do not modify its source files to make
  preview easier. Keep implementation changes in the API/frontend and their tests.
- CSV preview imports the existing `IMSPropertyTypeMapper` through
  `api/ims_mapper_adapter.py`. Reuse its actual cache-first generation, inference,
  and `apply_property_types()` manual-override methods; do not vendor or rewrite
  those business rules. The adapter suppresses audit-file logging, removes save
  destinations after loading inputs, and blocks `save_mapping()`.
- **Sole user-approved duplication:** the final AB duplex exception is embedded
  inside `Datastore.bulk_import_ims_sold_listings()` and
  `Datastore.bulk_update_ims_sold_listings()` in `data/es.py`, with no pure helper
  to import. `api/ims_provincial_rules.py:apply_provincial_exception()` mirrors it,
  shared by the CSV adapter and ES lineage monitor.
  See the refactor TODO in `TODO.md`; do not perform that production refactor here.
- If CSV/ES classifications disagree after a production change, check the real
  mapper and both embedded provincial blocks first. Update the API exception if
  those provincial rules changed, and run `test_api_ims_csv.py`, whose drift test
  compares both live production blocks. Restart FastAPI after mapper source
  changes; Python imports remain loaded in its worker until restart.
- Verify preview changes leave mapping feather files, ETL sighting logs, caches,
  and ES untouched. Never call the ES-writing bulk import/update routines just
  to obtain classifications.

## Project Structure & Module Organization

- `realestate_analytics/`: Python package
  - `api/`: FastAPI app and endpoints (`realestate_analytics.api.main:app`).
  - `front-end/`: Standalone React/TypeScript Market Explorer; project-local Node tooling and npm dependencies.
  - `etl/`: Batch pipelines and processors (e.g., monthly metrics, absorption rate).
  - `data/`: Data access (Elasticsearch, BigQuery, caching).
  - `utils/`, `visualization/`: Shared helpers and simple visualizations.
  - `utils/metrics_viz.py`: Matplotlib helpers for plotting mkt_trends metrics (single/multi-series, YoY, spreads, heatmaps, comparisons).
  - `utils/metrics_analysis.py`: Cohort analysis utilities (outlier detection across geos for a month, summary reporting).
- `scripts/`: Operational helpers for running ETL and ops (`run_*.py`, `run_all_etl.sh`, `config.yaml`).
- `tests/`: Tests using `unittest`; many existing tests require configured services and cached data.

## Build, Test, and Development Commands

- Use the project-local `.venv/bin/python`. If `.venv` is missing, ask before using `~/Developer/python39_env` or `~/Developer/python310_env`; do not use macOS system Python.
- Dependency installation or upgrades require explicit user permission; do not run `pip install` automatically.
- Run ETL example: `.venv/bin/python scripts/run_last_mth_metrics.py --config scripts/uat_config.yaml --prov_code ON` (requires configured services and `.env`; writes metrics/caches).
- Run all tests: `.venv/bin/python -m unittest discover -s tests`
- Run a single test file: `.venv/bin/python -m unittest discover -s tests -p 'test_absorption_rate.py'`
- Before changing pipelines, read `CLAUDE.md` for operational rules and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full architecture, endpoints, and environment reference.

### Start FastAPI and Market Explorer

Run the backend and frontend in **two separate terminals**. The user manages these servers; do not start, restart, or stop them unless requested.

**Terminal 1 — FastAPI**, from the repository root:

```sh
.venv/bin/python -m uvicorn realestate_analytics.api.main:app --host 127.0.0.1 --port 8013
```

If already in `realestate_analytics/api` with the intended Python virtual environment active, the equivalent command is:

```sh
python -m uvicorn realestate_analytics.api.main:app --app-dir ../.. --host 127.0.0.1 --port 8013
```

Use **no `--reload`**: the user prefers manual restarts to avoid file-watcher CPU usage. After backend code changes, stop and relaunch the API manually. Swagger UI: <http://127.0.0.1:8013/docs>.

**Terminal 2 — frontend**, starting from the repository root:

```sh
cd realestate_analytics/front-end
npm run dev
```

Open <http://127.0.0.1:5173/>. The npm scripts automatically use this frontend's local Node 24 runtime in `.tooling/` when installed; this is separate from Python. Do not change global Node, shared Python environments, or other projects such as kefinance. Do not install npm packages automatically; provide project-local commands for the user to run. See [the frontend README](realestate_analytics/front-end/README.md) for installation and build instructions.

The Vite development server forwards `/api/v1` to `http://127.0.0.1:8013` by default; it does not launch FastAPI. To change the backend address, copy `realestate_analytics/front-end/.env.example` to `.env.local` in that same directory, set `API_PROXY_TARGET`, and restart the frontend. This optional frontend file is separate from the backend's home-directory environment files. Never put ES credentials in frontend environment variables.

Keep both terminals open while using the app. Stop each server with **Ctrl-C in its own terminal**. **Ctrl-Z suspends a server rather than terminating it**, leaving its port occupied and requests unanswered. If a port is occupied, identify the owning process before acting; do not start duplicate servers or terminate unrelated apps.

## Coding Style & Naming Conventions

- Follow PEP 8; use 4‑space indentation.
- Naming: modules/functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Keep side effects out of import time; prefer dependency injection (e.g., pass datastore and cache).
- Use `logging` for diagnostics; avoid `print` in library code.

## Testing Guidelines

- Framework: `unittest` with test files under `tests/` named `test_*.py`.
- Structure: classes `Test*` with methods `test_*`; prefer deterministic fixtures.
- Env: tests read from `.env` (examples): `ANALYTICS_CACHE_DIR`, `UAT_ES_HOST`, `UAT_ES_PORT`.
- Treat service/cache-dependent tests as integration tests. Inspect their setup and required cache keys before running them; report unavailable prerequisites separately from code failures.
- Add deterministic regression tests with mocked services and isolated caches for changed ETL behavior. Cover month-end cache retention, same-job retry through a new processor instance, and same-day consumption by `CurrentMthMetricsProcessor`.
- For listing date parsing, test valid, missing, and malformed timestamps and their effects on returned listings and metric counts/prices; parser survival alone is insufficient validation.
- Scope: cover ETL calculations and API endpoint behaviors at minimum.
- Province integrity: verify ProvinceGeogIdValidator is applied in ETLs (last_mth_metrics, historic_sold_median_metrics, absorption_rate) to prevent cross-province contamination; keep dedup keys and archive handling province-aware.
- Absorption rate: assert `MOI = 1 / absorption_rate` when sales and inventory are both positive. With zero sales and positive inventory, retain absorption_rate=0 and omit undefined MOI; with zero inventory and positive sales, omit undefined absorption_rate and retain MOI=0. With both zero, omit both undefined values. The serialized arrays can legitimately contain different sets of months.

## Commit & Pull Request Guidelines

- Commits: concise, imperative mood (e.g., "fix nearby solds edge case"); group related changes.
- PRs: clear description, rationale, and validation steps; link issues; include sample outputs or endpoint screenshots when relevant.
- Checks: ensure tests pass locally; note any configuration changes.

## Security & Configuration Tips

- Do not commit secrets. Configure via `.env` loaded by `python-dotenv`.
- Minimal `.env` example:
  - `ANALYTICS_CACHE_DIR=/path/to/cache`
  - `UAT_ES_HOST=localhost`  `UAT_ES_PORT=9200`
  - `ALLOW_ORIGINS=http://localhost:3000`
- IMS ingestion relies on `ANALYTICS_CACHE_DIR` for cached mappings and GUID resolution artifacts:
  - `ims_property_type_mapping_v4_df`, `ims_property_type_lookup_df`, `unclear_mappings.csv`
  - `geo_collection.dill`, `ims_guid_resolution_checkpoints/`

## ETL Sources and Cache Contracts

- `SoldMedianMetricsProcessor` in `historic_sold_median_metrics.py` reads IMS sold listings through `get_ims_sold_listings()`. `AbsorptionRateProcessor` reuses its province-specific `SoldMedianMetricsProcessor/{prov}_five_years_sold_listing` cache and fetches live active listings for month-end inventory snapshots.
- `NearbyComparableSoldsProcessor` must use `rlp_archive_current` through `get_sold_listings()` for comparable solds. Keep comparables on the legacy RLP pipeline; do not switch to IMS to work around an upstream archive-feed outage.
- `LastMthMetricsProcessor` uses legacy listings through `get_listings()`. Its daily query is an unbounded full pull; `delta_listing_df` and the `_delta_` checkpoint key are legacy names for today's fetch. Merge existing rows first and fresh rows last, then deduplicate by `_id` with `keep='last'`.
- On the 1st of the month, compute last month's metrics from the accumulated listings, archive the aggregated results, and push them to ES before resetting the listing cache to today's fresh pull. Preserve `LastMthMetricsProcessor/{prov}_current_listing` for the same-day `CurrentMthMetricsProcessor` run; do not delete it during the automatic month-end reset.
- Preserve the dependency order in `scripts/run_all_etl.sh`: last-month jobs precede current-month metrics; historic sold-metrics jobs precede absorption rate. Manual `cleanup()`/`reset()` is distinct from the automatic month-end cache reset.

## Elasticsearch Mapping and Index Rules

- Keep market-trends reads, writes, and admin operations pointed at `rlp_mkt_trends_current`. Confirm the alias resolves to exactly one backing index before a mapping update.
- `MKT_TRENDS_INDEX_MAPPING` is applied only when creating a new index. Before the first write of a new nested metrics field to an existing index, validate field names, period keys, nesting, and value types against the writer and inspect the target mapping. Dynamic mapping infers `object`, not `nested`; an existing field type requires reindexing to change.
- Ask for explicit user confirmation before applying `put_mapping` to PROD, since field mappings cannot be removed in place. Follow the procedure in [docs/ES_ADMIN.md](docs/ES_ADMIN.md).
- `ENABLE_QUARTERLY_METRICS` currently defaults to `False`. Before enabling it, recheck all five `quarterly_*` mappings against the writers and patch missing mappings on existing target indices before any writes. Quarterly entries use a `quarter` keyword; monthly entries use a `month` date formatted `yyyy-MM`.
- `create_mkt_trends_index()` currently hardcodes `rlp_mkt_trends_1`. Before migrating to a later backing-index version, update this function so it cannot recreate stale `_1`; update the index references in `docs/ES_ADMIN.md` and `.claude/skills/realestate-es-query/references/mkt_trends_schema.md` as described in `CLAUDE.md`.

## Elasticsearch Disk Management

Use [docs/ES_ADMIN.md](docs/ES_ADMIN.md) for disk allocation checks, deleted-document segment bloat, forcemerge, flood-stage watermarks, and read-only block recovery. Restore temporary watermark changes after recovery; keep operational commands in that runbook to avoid conflicting copies.

## GeoCollection rebuild caveat (TODO)

- TODO: add validation/warnings when rebuilding `GeoCollection` from ES if critical fields (e.g., `geometry`) are missing. Missing detail docs today leave `parent_id`/`children`/`overlaps` empty and degrade `compute_guid`. Before depending on a fresh rebuild, validate geo indices have geometry for all levels; decide whether to fail fast or drop geos without geometry. Until prod indices are corrected, consider using a serialized "golden" collection with overlaps/parents computed.

## Data Integrity Invariants

- Preserve ProvinceGeogIdValidator (utils/validators.py) and province-aware archive logic to prevent cross-province metric contamination; MonthEndListingSnapshotArchiveReader and LastMthMetricsArchiveReader require province-prefixed files and dedup with province included.
- AbsorptionRateProcessor computes months_of_inventory alongside absorption_rate and writes both metrics to ES; preserve the zero-sales/inventory semantics and independently omit undefined values as described above.
- IMS propertyType mapping uses `IMSPropertyTypeMapper` with cache-first mappings, optional manual overrides, and `unclear_mappings.csv` logging; keep provincial exceptions (e.g., AB duplex) in `realestate_analytics/data/es.py`.
- IMS sold listings ingestion uses `rlp_ims_sold_listing_current` alias, date-only `WrittenDate`, normalized IDs (`{prov}-{mlsid}-{mlsnumber}-{year}`), numeric coercion, and `MultiPassGUIDResolver` with GeoCollection cache + checkpoints to populate `guid`; `get_ims_sold_listings` reads from this index and returns canonical fields.
- IMS read path relies on `validate_ims_columns` + `rename_ims_to_canonical`; do not re-derive propertyType at read time since it is set during bulk import.
