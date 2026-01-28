# Repository Guidelines

## Project Structure & Module Organization
- `realestate_analytics/`: Python package
  - `api/`: FastAPI app and endpoints (`realestate_analytics.api.main:app`).
  - `etl/`: Batch pipelines and processors (e.g., monthly metrics, absorption rate).
  - `data/`: Data access (Elasticsearch, BigQuery, caching).
  - `utils/`, `visualization/`: Shared helpers and simple visualizations.
  - `utils/metrics_viz.py`: Matplotlib helpers for plotting mkt_trends metrics (single/multi-series, YoY, spreads, heatmaps, comparisons).
  - `utils/metrics_analysis.py`: Cohort analysis utilities (outlier detection across geos for a month, summary reporting).
- `scripts/`: Operational helpers for running ETL and ops (`run_*.py`, `run_all_etl.sh`, `config.yaml`).
- `tests/`: Unit tests using `unittest`.

## Build, Test, and Development Commands
- Create env + install: `python -m venv .venv && source .venv/bin/activate && pip install -U pip && pip install -e .`
- Run API (dev): `uvicorn realestate_analytics.api.main:app --reload --port 8001`
- Run ETL example: `python scripts/run_last_mth_metrics.py` (see `.env` requirements below).
- Run all tests: `python -m unittest`
- Run a single test: `python -m unittest tests/test_absorption_rate.py`
- System docs: `CLAUDE.md` contains full ETL architecture, endpoints, and env var reference—consult it before changing pipelines.

## Coding Style & Naming Conventions
- Follow PEP 8; use 4‑space indentation.
- Naming: modules/functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Keep side effects out of import time; prefer dependency injection (e.g., pass datastore and cache).
- Use `logging` for diagnostics; avoid `print` in library code.

## Testing Guidelines
- Framework: `unittest` with test files under `tests/` named `test_*.py`.
- Structure: classes `Test*` with methods `test_*`; prefer deterministic fixtures.
- Env: tests read from `.env` (examples): `ANALYTICS_CACHE_DIR`, `UAT_ES_HOST`, `UAT_ES_PORT`.
- Scope: cover ETL calculations and API endpoint behaviors at minimum.
- Province integrity: verify ProvinceGeogIdValidator is applied in ETLs (last_mth_metrics, historic_sold_median_metrics, absorption_rate) to prevent cross-province contamination; keep dedup keys and archive handling province-aware.
- Absorption rate: months_of_inventory metric ships with absorption_rate; tests should assert MOI=1/absorption_rate semantics and zero-sales/inventory edge cases.

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

## GeoCollection rebuild caveat (TODO)
- TODO: add validation/warnings when rebuilding `GeoCollection` from ES if critical fields (e.g., `geometry`) are missing. Missing detail docs today leave `parent_id`/`children`/`overlaps` empty and degrade `compute_guid`. Before depending on a fresh rebuild, validate geo indices have geometry for all levels; decide whether to fail fast or drop geos without geometry. Until prod indices are corrected, consider using a serialized “golden” collection with overlaps/parents computed.

## Recent Oct–Nov 2025 Changes to Preserve
- Added ProvinceGeogIdValidator (utils/validators.py) and province-aware archive logic to stop cross-province metric contamination; MonthEndListingSnapshotArchiveReader and LastMthMetricsArchiveReader now expect province-prefixed files and dedup with province included.
- AbsorptionRateProcessor now also computes months_of_inventory and writes both metrics to ES; handle zero sales/inventory as documented and keep MOI/absorption_rate arrays in sync.
- IMS propertyType mapping now uses `IMSPropertyTypeMapper` with cache-first mappings, optional manual overrides, and `unclear_mappings.csv` logging; keep provincial exceptions (e.g., AB duplex) in `realestate_analytics/data/es.py`.
- IMS sold listings ingestion now uses `rlp_ims_sold_listing_current` alias, date-only `WrittenDate`, normalized IDs (`{prov}-{mlsid}-{mlsnumber}-{year}`), numeric coercion, and `MultiPassGUIDResolver` with GeoCollection cache + checkpoints to populate `guid`; `get_ims_sold_listings` reads from this index and returns canonical fields.
- IMS read path relies on `validate_ims_columns` + `rename_ims_to_canonical`; do not re-derive propertyType at read time since it is set during bulk import.
- ETL sourcing: `HistoricSoldMedianMetricsProcessor` and `AbsorptionRateProcessor` should use IMS-backed sold listings; `NearbyComparableSoldsProcessor` continues to use `rlp_archive_current`.
