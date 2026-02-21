# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Node / npm Convention
- **Never install npm packages globally** (`npm install -g`). Global installs cross-contaminate the system environment and can conflict across projects.
- `pptxgenjs` (and any future npm packages) are installed **locally** in the project root `node_modules/`.
- Run Node scripts with: `NODE_PATH=./node_modules node <script.js>`
- `node_modules/` is excluded from git via `.gitignore`.

## Elasticsearch Disk Full — Bulk Update Fails
If bulk updates return `0/N successful` with `cluster_block_exception` / `read-only-allow-delete` errors, the local ES disk has hit the flood-stage watermark. See **[docs/ES_ADMIN.md](docs/ES_ADMIN.md)** for the full fix (raise watermark → remove read-only blocks → forcemerge).

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

## Architecture Reference
Full details (package structure, API endpoints, ETL processors, IMS pipeline, env vars, data flow, deployment) → **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**
