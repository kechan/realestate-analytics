# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Node / npm Convention
- **Never install npm packages globally** (`npm install -g`). Global installs cross-contaminate the system environment and can conflict across projects.
- `pptxgenjs` (and any future npm packages) are installed **locally** in the project root `node_modules/`.
- Run Node scripts with: `NODE_PATH=./node_modules node <script.js>`
- `node_modules/` is excluded from git via `.gitignore`.

## Elasticsearch Disk Full — Bulk Update Fails
If bulk updates return `0/N successful` with `cluster_block_exception` / `read-only-allow-delete` errors, the local ES disk has hit the flood-stage watermark. See **[docs/ES_ADMIN.md](docs/ES_ADMIN.md)** for the full fix (raise watermark → remove read-only blocks → forcemerge).

## New ES Metrics Fields on Existing Indices — Schema Edits Alone Don't Reach PROD

Editing `MKT_TRENDS_INDEX_MAPPING` (`realestate_analytics/data/es_index_schemas.py`) has **no effect on an
already-existing index** — `create_mkt_trends_index()` only applies it when creating a brand-new index
(`if not exists: create()`), never as an update. If a new metrics field is written for the first time against
an index that already exists (true for PROD/UAT/local's long-lived `rlp_mkt_trends_*`), Elasticsearch dynamically
infers a mapping from the first document's shape instead — arrays of `{key, value}` become plain `object`, never
`nested` (which is only ever explicit). Once that happens it's **permanent**: an existing field's type cannot be
changed without a full reindex.

**Before any feature switch that writes a new nested metrics field for the first time is turned on** (e.g.
`ENABLE_QUARTERLY_METRICS` in `historic_sold_median_metrics.py`, or any future equivalent) — before running that
code against PROD, Claude MUST:
1. Re-check that the field's mapping in `es_index_schemas.py` still matches exactly what the write code actually
   produces (field/key names, nesting, `value` type) — schema and code can silently drift apart while a feature
   sits disabled.
2. Ask the user for explicit confirmation before applying `put_mapping` to PROD's live index. This is an
   **irreversible** action — there is no API to remove a field mapping; recovering from a mistake means
   reindexing into a new index.

See **[docs/ES_ADMIN.md](docs/ES_ADMIN.md)** for the root cause, the `put_mapping` procedure, and the current
concrete example (the `quarterly_*` fields, currently switched off).

## Common Development Commands

### Environment Setup
```bash
python -m venv .venv && source .venv/bin/activate && pip install -U pip && pip install -e .
```

### Running the API
```bash
uvicorn realestate_analytics.api.main:app --reload --port 8001
```

### Testing
```bash
# Run all tests
python -m unittest

# Run a single test file
python -m unittest tests/test_absorption_rate.py
python -m unittest tests/test_last_mth_metrics.py
python -m unittest tests/test_historic_sold_median_metrics.py
python -m unittest tests/test_nearby_comparable_solds.py
```

### ETL Operations
```bash
# Run individual ETL scripts (requires .env configuration)
python scripts/run_nearby_comparable_solds.py --config scripts/prod_config.yaml --prov_code ON
python scripts/run_last_mth_metrics.py --config scripts/prod_config.yaml --prov_code ON
python scripts/run_historic_metrics.py --config scripts/prod_config.yaml --prov_code BC
python scripts/run_absorption_rate.py --config scripts/prod_config.yaml --prov_code AB
python scripts/run_current_mth_metrics.py --config scripts/prod_config.yaml

# Run all ETL processes (production script)
./scripts/run_all_etl.sh
```

## Data Source Constraints — Do Not Mix

**`NearbyComparableSoldsProcessor` MUST use `rlp_archive_current` (via `get_sold_listings()`) — NEVER IMS.**

Nearby comparable solds surfaces sold listings alongside a current active listing. For consistency, both the active listing and its comparable solds must come from the same data pipeline (the legacy RLP/archive feed). IMS (`rlp_ims_sold_listing_current`) is an aggregate of many MLS boards with different field semantics and MUST NOT be used here — mixing sources would produce inconsistent comparables.

If zero sold listings are returned from `rlp_archive_current` for ON, that is a data pipeline problem upstream (the archive feed has stopped receiving new solds). The fix is to restore the archive feed — **not** to switch to IMS.

IMS data is used exclusively in other ETL processors (historic metrics, last month metrics, absorption rate, etc.).

## Architecture Reference
Full details (package structure, API endpoints, ETL processors, IMS pipeline, env vars, data flow, deployment) → **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**
