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
python scripts/run_nearby_comparable_solds.py --config scripts/prod_config.yaml --prov_code ON
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
  - `main.py`: Main FastAPI application with CORS configuration
  - `dependencies.py`: Dependency injection for cache, archiver, geo_collection
  - `config.py`, `schemas.py`, `utils.py`: Configuration and utilities
  - `cache_diagnostics.py`: Cache inspection and diagnostics
  - `etl_script_kickoff_endpoints.py`: POST endpoints to trigger ETL jobs (`/op/*`)
  - `etl_monitoring_endpoints.py`: GET endpoints for job monitoring (`/monitor/*`)
  - `endpoints/`: Metrics and geo endpoint modules
    - `historic_sold_metrics.py`: 5-year historical median metrics
    - `last_mth_metrics.py`: Last month's metrics
    - `current_mth_metrics.py`: Current month's metrics
    - `absorption_rate.py`: Market absorption rate calculations
    - `geos.py`: Geographic data and search endpoints

- **realestate_analytics/etl/**: Batch processing pipelines for analytics calculations
  - `base_etl.py`: Abstract base class for all ETL processors (Extract/Transform/Load pattern)
  - `nearby_comparable_solds.py`: NearbyComparableSoldsProcessor
  - `historic_sold_median_metrics.py`: SoldMedianMetricsProcessor (5-year trends)
  - `last_mth_metrics.py`: LastMthMetricsProcessor
  - `current_mth_metrics.py`: CurrentMthMetricsProcessor
  - `absorption_rate.py`: AbsorptionRateProcessor
  - `run_utils.py`: Shared utilities for ETL job management, email alerts, CSV tracking

- **realestate_analytics/data/**: Data access layer
  - `es.py`: Elasticsearch integration (Datastore class)
  - `bq.py`: BigQuery integration (BigQueryDatastore class)
  - `caching.py`: FileBasedCache system for intermediate data
  - `geo.py`: GeoCollection for geographic data management
  - `archive.py`: Archiver for historical data snapshots

- **realestate_analytics/utils/**: Shared utilities
  - `constants.py`: Project constants (VALID_PROVINCES, etc.)
  - `geo.py`: Geographic utility functions
  - `archive.py`: Archive-related utilities
  - `ims.py`: IMS Property Type Mapper for classifying IMS sold listings

- **realestate_analytics/visualization/**: Visualization tools
  - `geo_viz.py`: Geographic visualizations
  - `comparable_solds_viz.py`: Comparable sales visualizations

- **scripts/**: Operational scripts for running ETL processes
  - `run_nearby_comparable_solds.py`: Run nearby comparable sales ETL
  - `run_last_mth_metrics.py`: Run last month metrics ETL
  - `run_historic_metrics.py`: Run 5-year historical metrics ETL
  - `run_absorption_rate.py`: Run absorption rate ETL
  - `run_current_mth_metrics.py`: Run current month metrics ETL
  - `run_with_airflow.py`: Airflow integration for scheduled runs
  - `backfill_mth_end_snapshots.py`: Backfill historical monthly snapshots
  - `copy_mkt_trends_data.py`: Copy market trends data between environments
  - `upload_2_gcs.py`: Upload data to Google Cloud Storage
  - `prod_config.yaml`, `uat_config.yaml`, `config.yaml`: Environment configurations
  - `run_all_etl.sh`: Bash script to run all ETL processes sequentially

### API Endpoints

The FastAPI application provides three main endpoint groups:

#### 1. Metrics Endpoints
- `GET /metrics/historic_sold/{geo_id}`: 5-year historical median price, DOM, over/under ask trends
- `GET /metrics/last_month/{geo_id}`: Last month's completed metrics
- `GET /metrics/current_month/{geo_id}`: Current month's real-time metrics
- `GET /metrics/absorption_rate/{geo_id}`: Market absorption rate
- `GET /geos/search`: Search geographic locations
- `GET /geos/{geo_id}`: Get specific geographic location details

#### 2. ETL Operations Endpoints (`/op/*`)
POST endpoints to trigger ETL jobs (requires `ANALYTICS_ETL_SCRIPT_DIR` env var):
- `POST /op/nearby_comparable_solds`: Trigger nearby comparable sales processing
- `POST /op/historic_metrics`: Trigger 5-year historical metrics calculation
- `POST /op/last_mth_metrics`: Trigger last month metrics calculation
- `POST /op/current_mth_metrics`: Trigger current month metrics calculation
- `POST /op/absorption_rate`: Trigger absorption rate calculation

All accept query params: `config_path`, `prov_code`, `es_host`, `es_port`, `log_level`, `force_full_load` (where applicable)

#### 3. Monitoring Endpoints (`/monitor/*`)
GET endpoints for ETL job monitoring:
- `GET /monitor/etl-types`: List all valid ETL types
- `GET /monitor/job/{job_id}`: Get detailed job status with stage timings
- `GET /monitor/jobs/recent`: Get recent job summaries (limit, etl_type filters)
- `GET /monitor/jobs/history`: Get paginated job history with search
- `GET /monitor/jobs/active`: Get currently running job (if any)
- `GET /monitor/cache/diagnostics/{etl_type}`: Inspect cache keys for ETL type

### ETL Architecture

All ETL processors inherit from `BaseETLProcessor` (abstract base class) which implements:

**Standard Pattern:**
1. **Extract**: Pull data from Elasticsearch (or load from cache for resume)
2. **Transform**: Process and calculate analytics
3. **Load**: Update Elasticsearch with computed metrics

**Key Features:**
- **Checkpoint/Resume**: Each stage is marked as successful in cache; failed jobs can resume from last successful stage
- **Extra Stages**: Subclasses can add conditional stages (e.g., end-of-month processing in LastMthMetricsProcessor)
- **Province-based Processing**: Most ETL jobs run per province/territory (13 total: ON, BC, AB, SK, MB, QC, NB, NS, PE, NL, YT, NT, NU)
- **Job IDs**: Format `{prefix}_{timestamp}_{prov_code}` (e.g., `last_mth_metrics_20250115_120000_on`)
- **Monitoring**: Log files with `[MONITOR]` tags for automated status parsing

**ETL Processors:**
1. **NearbyComparableSoldsProcessor**: Find comparable sold properties within geographic areas
2. **SoldMedianMetricsProcessor**: Calculate 5-year trends (median price, DOM, over/under ask)
3. **LastMthMetricsProcessor**: Monthly metrics with end-of-month special processing
4. **CurrentMthMetricsProcessor**: Real-time current month metrics (all provinces, no province filtering)
5. **AbsorptionRateProcessor**: Calculate market absorption rates and months of inventory
   - **Absorption Rate**: Ratio of sold listings to active inventory (sold/current)
   - **Months of Inventory**: Inverse metric representing time to clear inventory (current/sold)
   - Both metrics stored as time series in `rlp_mkt_trends_current` ES index
   - Handles edge cases: zero sales → absorption_rate=0.0 included, MOI=NaN skipped; zero inventory → absorption_rate=NaN skipped, MOI=0.0 included
   - Asymmetric arrays supported (different month counts between metrics)

### Key Design Patterns
- **Configuration-driven**: Uses YAML config files (prod_config.yaml, uat_config.yaml, config.yaml)
- **Province-based processing**: Most ETL operations run per Canadian province/territory
- **Dependency injection**: Pass datastore and cache instances via API dependencies.py
- **Environment-based configuration**: Uses .env files with python-dotenv
- **Caching layers**: FileBasedCache for intermediate data, archives for historical snapshots
- **Monitoring via logs**: Structured logging with `[MONITOR]` tags for automated parsing

### Required Environment Configuration

Create a `.env` file with the following variables:

#### Required Variables
```
# Cache and storage
ANALYTICS_CACHE_DIR=/path/to/cache

# Elasticsearch
UAT_ES_HOST=localhost
UAT_ES_PORT=9200

# BigQuery
BQ_PROJECT_ID=your-project-id

# API
ALLOW_ORIGINS=http://localhost:3000

# ETL Operations (required for API kickoff/monitoring endpoints)
ANALYTICS_ETL_SCRIPT_DIR=/path/to/realestate-analytics/scripts
```

#### Optional Variables
```
# Archive directory (defaults to ANALYTICS_CACHE_DIR/archives if not set)
ANALYTICS_ARCHIVE_DIR=/path/to/archives

# Email alerts for ETL failures
ANALYTICS_ETL_SENDER_EMAIL=sender@example.com
ANALYTICS_ETL_RECEIVER_EMAILS=receiver1@example.com,receiver2@example.com
ANALYTICS_ETL_EMAIL_PASSWORD=your-email-password
```

### Data Flow

1. **Extract**: ETL scripts pull raw data from Elasticsearch
   - Uses Datastore class (realestate_analytics/data/es.py)
   - Can optionally pull from BigQuery for historical data

2. **Cache**: Intermediate data cached using FileBasedCache
   - Cache keys follow pattern: `{ProcessorClass}/{key_name}`
   - Example: `LastMthMetricsProcessor/on_current_listing`
   - Used for checkpoint/resume functionality

3. **Transform**: Data is processed per province to calculate analytics
   - Province filtering applied (except CurrentMthMetricsProcessor)
   - Calculations use pandas DataFrames

4. **Archive**: Historical snapshots saved to archive directory
   - Monthly snapshots for trend analysis
   - Managed by Archiver class

5. **Load**: Computed metrics written back to Elasticsearch
   - Bulk update operations
   - Success rate validation (must be >50%)

6. **Serve**: FastAPI endpoints serve processed analytics
   - Data retrieved from Elasticsearch
   - Optional caching via dependency injection
   - GeoCollection provides geographic context

### IMS Data Processing (Integrated MLS System)

The IMS (Integrated MLS System) dataset provides sold listings across all Canadian provinces with different field mappings than the legacy RLP system. Processing IMS data requires specialized property type classification and data transformation.

#### IMSPropertyTypeMapper

**Purpose**: Classifies IMS property types from (Type, SubType, Style) combinations into canonical categories: CONDO, SEMI-DETACHED, TOWNHOUSE, DETACHED, MOBILE_MINI, LAND_VACANT, COMMERCIAL, OTHER, or UNCLEAR.

**Location**: `realestate_analytics/utils/ims.py`

**Key Features**:
- **Cache-first strategy**: Loads cached mappings from `{ANALYTICS_CACHE_DIR}/ims_property_type_mapping_v4_df` (feather format)
- **Vectorized operations**: Replaces slow row-by-row apply() with pandas vectorized lookups for performance
- **UNCLEAR logging**: Automatically logs ambiguous property type combinations to CSV for manual review
- **Regression protection**: Prevents accidental changes to previously correct mappings when improving classification logic
- **Provincial overrides**: Base mapper handles generic classification; provincial exceptions (e.g., AB duplex → SEMI-DETACHED) applied separately in es.py

**Usage Pattern**:
```python
# Initialized automatically in Datastore.__init__()
ims_mapper = IMSPropertyTypeMapper(cached_mapping_path=cache_dir / 'ims_property_type_mapping_v4_df')

# Load IMS data and generate/update mappings
ims_mapper.load_dataframe(raw_ims_df)
ims_mapper.generate_full_mapping_dataframe()  # Updates cache with new combinations

# Apply mappings using vectorized operations
ims_mapper.apply_property_types()  # Adds 'propertyType' column to raw_ims_df

# Apply provincial overrides (example: AB duplex)
raw_ims_df.loc[ab_duplex_mask, 'propertyType'] = 'SEMI-DETACHED'
```

**UNCLEAR Mappings Review**:
- **File**: `{ANALYTICS_CACHE_DIR}/unclear_mappings.csv`
- **Columns**: Type, SubType, Style, Count, City, Province
- **Sorting**: By Count (descending) - most common combinations first
- **Behavior**:
  - Automatically updated when `generate_full_mapping_dataframe()` runs
  - Merges with existing CSV (deduplicates, keeps higher counts)
  - Never shrinks - accumulates UNCLEAR items over time for review
- **Purpose**: Manual review to improve classification rules or add edge cases

**Development Workflow**:
1. Improve `suggest_mapping()` logic in `ims.py`
2. Use `refresh_unclear_mappings()` to re-evaluate UNCLEAR items
3. Validate no regressions occurred to non-UNCLEAR mappings
4. Review changes and save updated cache with `save_mapping()`

**Cache Format**:
- **Format**: Feather (pandas fast binary format)
- **Version**: v4 (current)
- **Columns**: Type, SubType, Style, Count, Inferred
- **Uniqueness**: (Type, SubType, Style) combinations are unique
- **Preservation**: Cache never shrinks - all historical combinations preserved even if not in current data

#### IMS Data Preprocessing

**Entry Point**: `Datastore.get_ims_sold_listings()` in `realestate_analytics/data/es.py`

**Preprocessing Steps**:

1. **Validation**: `validate_ims_columns(raw_ims_df)` checks for required columns:
   - Date/Price: WrittenDate, SoldPrice, ListPrice
   - Location: Latitude, Longitude, City, Community, Province
   - Identifiers: MLSNumber, MLSID
   - Metrics: DaysOnMarket, Bedrooms, Baths
   - Property Type Fields: Type, SubType, Style

2. **Property Type Classification**:
   ```python
   ims_mapper.load_dataframe(raw_ims_df)
   ims_mapper.generate_full_mapping_dataframe()  # Updates cache + logs UNCLEAR to CSV
   ims_mapper.apply_property_types()  # Adds propertyType column
   ```

3. **Provincial Overrides** (applied after base classification):
   ```python
   # Alberta duplex exception: Duplex/Full Duplex → SEMI-DETACHED (not OTHER)
   ab_duplex_mask = (
     (raw_ims_df['Province'] == 'AB') &
     ((raw_ims_df['SubType'] == 'Duplex') | (raw_ims_df['SubType'] == 'Full Duplex'))
   )
   raw_ims_df.loc[ab_duplex_mask, 'propertyType'] = 'SEMI-DETACHED'
   ```

4. **Field Mapping**: `rename_ims_to_canonical(raw_ims_df)` transforms IMS schema to canonical format:
   - **IMS → Canonical mappings**:
     - WrittenDate → lastTransition
     - SoldPrice → soldPrice
     - ListPrice → price
     - Latitude → lat, Longitude → lng
     - DaysOnMarket → daysOnMarket (converted to float64)
     - MLSNumber → mls
     - City → city, Community → neighbourhood
     - Province → provState
     - Bedrooms → bedsInt, Baths → bathsInt

   - **Constructed fields**:
     - `_id`: Constructed as `{province}-{MLSID}-{MLSNumber}` (lowercase)
     - `beds`, `baths`: String versions of bedsInt/bathsInt for legacy compatibility
     - `listingStatus`: Hardcoded as "SOLD" for all IMS records

   - **Missing/Placeholder fields**:
     - `guid`, `computed_guid`: Not available (Boss working on geographic mapping)
     - `transitions`, `lastUpdate`: Set as None (not used downstream)
     - `listingType`: None (legacy field, not in IMS)
     - `searchCategoryType`: None (NOT used in IMS ETL logic - only for legacy RLP)

5. **Output**: Returns canonical DataFrame compatible with existing ETL processors

**Key Differences from Legacy RLP**:
- **Primary Key**: MLSNumber (instead of ES document ID)
- **Property Type**: Derived from (Type, SubType, Style, Province) instead of (searchCategoryType, listingType)
- **Geographic Coverage**: All Canadian provinces (not just GTA)
- **Data Lag**: Sold listings can arrive up to 1 year after transaction date
- **Field Names**: Requires preprocessing to match canonical schema

**Current Status**: `get_ims_sold_listings()` is a PLACEHOLDER - returns empty DataFrame until IMS ES index is available. All preprocessing logic is implemented and ready for activation.

### Testing Framework
- Uses Python's built-in `unittest` framework
- Test files follow `test_*.py` naming convention under `tests/` directory
- Tests cover ETL calculations and API endpoint behaviors
- Environment variables needed for test runs (see AGENTS.md for details)

### Production Deployment Notes

- **ETL Scheduling**: Use `run_all_etl.sh` or individual scripts via cron/Airflow
- **API Deployment**: Run uvicorn with process manager (systemd, supervisor, etc.)
- **Monitoring**: Check `/monitor/jobs/active` and `/monitor/cache/diagnostics/{etl_type}`
- **Province Processing**: Each province runs independently; failures in one don't affect others
- **Log Rotation**: ETL scripts automatically rotate log files on each run
