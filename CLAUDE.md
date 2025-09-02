# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
python scripts/run_last_mth_metrics.py --config scripts/prod_config.yaml --prov_code ON
python scripts/run_historic_metrics.py --config scripts/prod_config.yaml --prov_code BC
python scripts/run_absorption_rate.py --config scripts/prod_config.yaml --prov_code AB
python scripts/run_current_mth_metrics.py --config scripts/prod_config.yaml

# Run all ETL processes (production script)
./scripts/run_all_etl.sh
```

## Architecture Overview

### Core Package Structure
- **realestate_analytics/api/**: FastAPI application with endpoints for real estate metrics
  - Main app: `realestate_analytics.api.main:app`
  - Endpoints: historic metrics, last month metrics, current metrics, absorption rate, geos
  - ETL kickoff and monitoring endpoints
- **realestate_analytics/etl/**: Batch processing pipelines for analytics calculations
  - Per-province processing for most metrics (ON, BC, AB, SK, MB, QC, NB, NS, PE, NL, YT, NT, NU)
  - Monthly metrics, absorption rates, nearby comparable sales
- **realestate_analytics/data/**: Data access layer
  - Elasticsearch integration (`es.py`)
  - BigQuery integration (`bq.py`) 
  - Caching system (`caching.py`)
  - Geographic data utilities (`geo.py`)
- **scripts/**: Operational scripts for running ETL processes and configuration files

### Key Design Patterns
- Configuration-driven: Uses YAML config files (prod_config.yaml, uat_config.yaml, config.yaml)
- Province-based processing: Most ETL operations run per Canadian province/territory
- Dependency injection: Pass datastore and cache instances rather than global imports
- Environment-based configuration: Uses .env files with python-dotenv

### Required Environment Configuration
Create a `.env` file with:
```
ANALYTICS_CACHE_DIR=/path/to/cache
UAT_ES_HOST=localhost
UAT_ES_PORT=9200
ALLOW_ORIGINS=http://localhost:3000
```

### Data Flow
1. ETL scripts pull data from Elasticsearch and external sources
2. Data is processed per province and cached using BigQuery
3. FastAPI endpoints serve processed analytics through RESTful APIs
4. Monitoring endpoints track ETL job status and performance

### Testing Framework
- Uses Python's built-in `unittest` framework
- Test files follow `test_*.py` naming convention under `tests/` directory
- Tests cover ETL calculations and API endpoint behaviors
- Environment variables needed for test runs (see AGENTS.md for details)