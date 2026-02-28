# realestate-analytics

A Canadian real estate data pipeline and analytics API. It ingests sold listings from IMS (Integrated MLS System), computes market metrics across all 13 provinces/territories, archives monthly snapshots, and serves the results through a FastAPI REST interface.

---

## What It Does

- **Classifies** raw IMS property data into canonical types (DETACHED, CONDO, TOWNHOUSE, SEMI-DETACHED, etc.) via a rule-based mapper with manual overrides and an ML experimentation track.
- **Calculates** market metrics per geographic area: median sold price, days-on-market, over/under ask ratios, absorption rate, and months of inventory.
- **Archives** monthly snapshots for 5-year historical trend analysis.
- **Serves** all metrics through a REST API with endpoints for current month, last month, historic trends, and absorption rate.

---

## Architecture

```
IMS Data (CSV / Elasticsearch)
        ↓
  Property Type Classification (IMSPropertyTypeMapper)
        ↓
  GUID / Geo Resolution
        ↓
  Elasticsearch Index  ←──────────────────────────┐
        ↓                                          │
  ETL Processors (per province)           Bulk updates
        ↓
  Archive (monthly snapshots)
        ↓
  FastAPI REST API  →  Clients
```

For full details see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Key Components

### API — [`realestate_analytics/api/`](realestate_analytics/api/)

| File | Purpose |
|------|---------|
| [`main.py`](realestate_analytics/api/main.py) | FastAPI entry point, CORS, router registration |
| [`endpoints/`](realestate_analytics/api/endpoints/) | Metric endpoints: historic sold, last month, current month, absorption rate, geos |
| [`etl_script_kickoff_endpoints.py`](realestate_analytics/api/etl_script_kickoff_endpoints.py) | POST endpoints to trigger ETL jobs remotely |
| [`etl_monitoring_endpoints.py`](realestate_analytics/api/etl_monitoring_endpoints.py) | GET endpoints to monitor running ETL jobs |

### ETL Processors — [`realestate_analytics/etl/`](realestate_analytics/etl/)

| File | Purpose |
|------|---------|
| [`base_etl.py`](realestate_analytics/etl/base_etl.py) | Abstract base with Extract → Transform → Load lifecycle, checkpoint/resume |
| [`historic_sold_median_metrics.py`](realestate_analytics/etl/historic_sold_median_metrics.py) | 5-year median price, DOM, over/under ask |
| [`last_mth_metrics.py`](realestate_analytics/etl/last_mth_metrics.py) | Prior month metrics with end-of-month snapshot logic |
| [`current_mth_metrics.py`](realestate_analytics/etl/current_mth_metrics.py) | Real-time current month metrics (all provinces) |
| [`absorption_rate.py`](realestate_analytics/etl/absorption_rate.py) | Absorption rate + months of inventory time series |
| [`nearby_comparable_solds.py`](realestate_analytics/etl/nearby_comparable_solds.py) | Find comparable sold properties within a geo |

### Data Layer — [`realestate_analytics/data/`](realestate_analytics/data/)

| File | Purpose |
|------|---------|
| [`es.py`](realestate_analytics/data/es.py) | Elasticsearch `Datastore`: queries, bulk import/update, IMS preprocessing, schema management |
| [`bq.py`](realestate_analytics/data/bq.py) | BigQuery integration for historical data |
| [`caching.py`](realestate_analytics/data/caching.py) | File-based cache (Feather for DataFrames) with expiry and resume support |
| [`geo.py`](realestate_analytics/data/geo.py) | `GeoCollection`: geographic hierarchy, parent/child relationships |
| [`archive.py`](realestate_analytics/data/archive.py) | Monthly snapshot archival for trend analysis |

### Utilities — [`realestate_analytics/utils/`](realestate_analytics/utils/)

| File | Purpose |
|------|---------|
| [`ims.py`](realestate_analytics/utils/ims.py) | `IMSPropertyTypeMapper`: classifies IMS Type/SubType/Style → canonical property category |
| [`constants.py`](realestate_analytics/utils/constants.py) | `VALID_PROVINCES`: all 13 Canadian province/territory codes |
| [`validators.py`](realestate_analytics/utils/validators.py) | Province-aware dedup key and archive file validation |
| [`metrics_viz.py`](realestate_analytics/utils/metrics_viz.py) | Matplotlib helpers for market trend charts |
| [`ims_ml.py`](realestate_analytics/utils/ims_ml.py) | Experimental ML ensemble for property type classification |

---

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip && pip install -e .
```

Copy `.env.example` to `.env` and configure:

| Variable | Purpose |
|----------|---------|
| `ANALYTICS_CACHE_DIR` | Local cache directory (Feather files, checkpoints) |
| `UAT_ES_HOST` / `UAT_ES_PORT` | Elasticsearch connection |
| `BQ_PROJECT_ID` | BigQuery project |
| `ALLOW_ORIGINS` | CORS allowed origins |
| `ANALYTICS_ARCHIVE_DIR` | Archive directory (defaults to `ANALYTICS_CACHE_DIR/archives`) |

## Running the API

```bash
uvicorn realestate_analytics.api.main:app --reload --port 8001
```

## Running ETL

```bash
# Per-province (substitute BC, AB, SK, etc. as needed)
python scripts/run_historic_metrics.py     --config scripts/prod_config.yaml --prov_code ON
python scripts/run_last_mth_metrics.py     --config scripts/prod_config.yaml --prov_code ON
python scripts/run_absorption_rate.py      --config scripts/prod_config.yaml --prov_code ON
python scripts/run_nearby_comparable_solds.py --config scripts/prod_config.yaml --prov_code ON

# All provinces, current month
python scripts/run_current_mth_metrics.py  --config scripts/prod_config.yaml

# Full production run (all ETL, all provinces)
./scripts/run_all_etl.sh
```

## Testing

```bash
python -m unittest                                        # all tests
python -m unittest tests/test_absorption_rate.py         # single file
```

Test files live in [`tests/`](tests/).

---

## Documentation

| Doc | Contents |
|-----|---------|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Full architecture: package layout, API endpoints, ETL design, IMS pipeline, env vars, data flow, deployment |
| [`docs/ES_ADMIN.md`](docs/ES_ADMIN.md) | Elasticsearch disk management (watermark, forcemerge, read-only block recovery) |
