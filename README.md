# realestate-analytics

A production-grade analytics platform for Canadian real estate market data. It ingests nationwide MLS/IMS (Integrated MLS System) listings, computes market metrics across all provinces and property types, and exposes them through a REST API.

---

## What It Does

- **ETL Pipelines** — Batch processors extract sold/active listing data from Elasticsearch, calculate market analytics using pandas, and write results back. Each processor runs independently per province and supports checkpoint/resume on failure.
- **Market Metrics** — Computes and stores: 5-year historical median price & days-on-market trends, last month's completed metrics, real-time current month metrics, absorption rates, and months of inventory.
- **Comparable Solds** — Finds nearby comparable sold properties for active listings using geographic proximity.
- **REST API** — FastAPI service that serves all computed metrics and geographic data to downstream consumers.
- **IMS Property Type Classification** — Classifies IMS listings into canonical property types (CONDO, DETACHED, SEMI-DETACHED, TOWNHOUSE, etc.) using a cached, vectorized rule-based mapper.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| API | FastAPI + Pydantic |
| Data Store | Elasticsearch 7.x+ |
| Historical Archive | Google BigQuery + GCS |
| Data Processing | Pandas, NumPy, GeoPandas, Shapely |
| Caching | File-based (Feather format) |
| Scheduling | Airflow (optional) |
| Presentations | Node.js + pptxgenjs (local) |

---

## Project Layout

```
realestate-analytics/
├── realestate_analytics/
│   ├── api/            # FastAPI app — endpoints, schemas, dependencies
│   ├── etl/            # Batch ETL processors
│   ├── data/           # Data access layer (ES, BigQuery, cache, geo)
│   ├── utils/          # Constants, IMS property type mapper, geo utils
│   └── visualization/  # Geo and comparable solds charts
├── scripts/            # Operational runner scripts + config YAMLs
├── tests/              # Unit tests (unittest)
└── docs/               # Architecture and admin guides
```

### Key Files

| File | Purpose |
|------|---------|
| [`realestate_analytics/api/main.py`](realestate_analytics/api/main.py) | FastAPI entry point, CORS config, route registration |
| [`realestate_analytics/etl/base_etl.py`](realestate_analytics/etl/base_etl.py) | Abstract base class — Extract/Transform/Load pattern with checkpoint/resume |
| [`realestate_analytics/etl/historic_sold_median_metrics.py`](realestate_analytics/etl/historic_sold_median_metrics.py) | 5-year trend calculations |
| [`realestate_analytics/etl/absorption_rate.py`](realestate_analytics/etl/absorption_rate.py) | Absorption rate & months of inventory |
| [`realestate_analytics/data/es.py`](realestate_analytics/data/es.py) | Elasticsearch `Datastore` class + IMS preprocessing |
| [`realestate_analytics/utils/ims.py`](realestate_analytics/utils/ims.py) | `IMSPropertyTypeMapper` — property type classifier |
| [`scripts/run_all_etl.sh`](scripts/run_all_etl.sh) | Run the full ETL pipeline |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Full technical reference |
| [`docs/ES_ADMIN.md`](docs/ES_ADMIN.md) | Elasticsearch disk management guide |

---

## Getting Started

### 1. Environment Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip && pip install -e .
```

Copy `.env.example` to `.env` and configure Elasticsearch, BigQuery, and GCS credentials.

### 2. Run the API

```bash
uvicorn realestate_analytics.api.main:app --reload --port 8001
```

API docs available at `http://localhost:8001/docs`.

### 3. Run ETL Jobs

```bash
# Individual processors (requires .env and a config YAML)
python scripts/run_historic_metrics.py --config scripts/prod_config.yaml --prov_code ON
python scripts/run_last_mth_metrics.py --config scripts/prod_config.yaml --prov_code ON
python scripts/run_absorption_rate.py  --config scripts/prod_config.yaml --prov_code BC
python scripts/run_current_mth_metrics.py --config scripts/prod_config.yaml

# Run all ETL processes sequentially
./scripts/run_all_etl.sh
```

### 4. Run Tests

```bash
python -m unittest                                         # all tests
python -m unittest tests/test_absorption_rate.py          # single file
python -m unittest tests/test_historic_sold_median_metrics.py
```

---

## API Overview

### Metrics Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/metrics/historic_sold/{geo_id}` | 5-year median price & DOM trends |
| `GET` | `/metrics/last_month/{geo_id}` | Last month's completed metrics |
| `GET` | `/metrics/current_month/{geo_id}` | Current month real-time metrics |
| `GET` | `/metrics/absorption_rate/{geo_id}` | Absorption rate & months of inventory |
| `GET` | `/geos/search?query=Toronto` | Search geographic locations |

### ETL Operation Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/op/historic_metrics` | Trigger 5-year metrics job |
| `POST` | `/op/last_mth_metrics` | Trigger last month metrics job |
| `POST` | `/op/absorption_rate` | Trigger absorption rate job |
| `POST` | `/op/nearby_comparable_solds` | Trigger comparable solds job |

### Monitoring Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/monitor/jobs/active` | Currently running job |
| `GET` | `/monitor/jobs/recent` | Recent job summaries |
| `GET` | `/monitor/job/{job_id}` | Detailed job status |
| `GET` | `/monitor/cache/diagnostics/{etl_type}` | Inspect ETL cache state |

---

## IMS Property Type Mapping

IMS listings arrive with (Type, SubType, Style) tuples that must be classified into canonical property types. The [`IMSPropertyTypeMapper`](realestate_analytics/utils/ims.py) handles this with:

- A cached lookup table (Feather format) that is updated incrementally
- Vectorized pandas operations for performance
- Ambiguous combinations logged to CSV for manual review
- Provincial subclasses with region-specific override rules

---

## Further Reading

- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — Deep dive into package structure, data flow, ETL internals, environment variables, and deployment notes
- **[docs/ES_ADMIN.md](docs/ES_ADMIN.md)** — What to do when Elasticsearch hits disk watermark limits
- **[CLAUDE.md](CLAUDE.md)** — Developer notes for AI-assisted development (Node conventions, common commands)
