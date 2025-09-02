# Repository Guidelines

## Project Structure & Module Organization
- `realestate_analytics/`: Python package
  - `api/`: FastAPI app and endpoints (`realestate_analytics.api.main:app`).
  - `etl/`: Batch pipelines and processors (e.g., monthly metrics, absorption rate).
  - `data/`: Data access (Elasticsearch, BigQuery, caching).
  - `utils/`, `visualization/`: Shared helpers and simple visualizations.
- `scripts/`: Operational helpers for running ETL and ops (`run_*.py`, `run_all_etl.sh`, `config.yaml`).
- `tests/`: Unit tests using `unittest`.

## Build, Test, and Development Commands
- Create env + install: `python -m venv .venv && source .venv/bin/activate && pip install -U pip && pip install -e .`
- Run API (dev): `uvicorn realestate_analytics.api.main:app --reload --port 8001`
- Run ETL example: `python scripts/run_last_mth_metrics.py` (see `.env` requirements below).
- Run all tests: `python -m unittest`
- Run a single test: `python -m unittest tests/test_absorption_rate.py`

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
