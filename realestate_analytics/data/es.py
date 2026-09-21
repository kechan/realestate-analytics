from __future__ import annotations
from typing import List, Dict, Any, Tuple, Union

import os, json, time, traceback, re
from datetime import datetime, timedelta
from collections import Counter
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import pandas as pd

from .caching import FileBasedCache
from realestate_analytics.utils.ims import IMSPropertyTypeMapper

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError, RequestError, TransportError
from elasticsearch.helpers import scan, bulk

from realestate_core.common.utils import load_from_pickle, save_to_pickle, join_df

import logging
from tqdm.auto import tqdm

from .ims_transformers import (
    derive_property_type, format_listingType, parse_datetime,
    validate_ims_columns, rename_ims_to_canonical,
)
from .es_index_schemas import MKT_TRENDS_INDEX_MAPPING, IMS_SOLD_LISTING_INDEX_MAPPING


class Datastore:
  def __init__(self, host: str, port: int, api_key: str = None):
    self.logger = logging.getLogger(self.__class__.__name__)

    self.es_host = host
    self.es_port = port

    if api_key:
      self.es = Elasticsearch(
        f'https://{self.es_host}:{self.es_port}',
        api_key=api_key,
        verify_certs=True,
        send_get_body_as="POST",
        timeout=30, retry_on_timeout=True, max_retries=3
      )
    else:
      self.es = Elasticsearch([f'http://{self.es_host}:{self.es_port}/'], timeout=30, retry_on_timeout=True, max_retries=3)

    # hardcode these often used indexes
    self.sold_listing_index_name = 'rlp_archive_current'
    self.listing_index_name = 'rlp_listing_current'

    self.geo_index_name = 'rlp_content_geo_current'
    self.geo_details_en_index_name = 'rlp_geo_details_en'

    # for tracking listing so we don't lose track of sold/delisted listings
    # as of now, this is used in the context of last_mth_metrics.py
    self.listing_tracking_index_name = 'rlp_listing_tracking_current'

    # for market trends time series data
    # currently, 5yr montly median sold price and days on market by geog_id and property type
    # used in SoldMedianMetricsProcessor
    self.mkt_trends_index_name = "rlp_mkt_trends_current"

    # for IMS (Integrated MLS System) sold listings across all Canadian provinces
    # stores raw IMS data with computed propertyType field
    self.ims_sold_listing_index_name = 'rlp_ims_sold_listing_current'

    if not self.ping():
      self.logger.error(f"Unable to connect to ES at {host}:{port}")
      # raise Exception("Unable to connect to Elasticsearch.")

    # get mappings
    try:
      self.sold_listing_index_mapping = self.get_mapping(index=self.sold_listing_index_name)
      self.listing_index_mapping = self.get_mapping(index=self.listing_index_name)
      self.geo_index_mapping = self.get_mapping(index=self.geo_index_name)
      self.geo_details_en_index_mapping = self.get_mapping(index=self.geo_details_en_index_name)    

      try:
        self.listing_tracking_index_mapping = self.get_mapping(index=self.listing_tracking_index_name)
      except NotFoundError:
        self.logger.warning(f"Index '{self.listing_tracking_index_name}' not found.")

      try:
        self.mkt_trends_index_mapping = self.get_mapping(index=self.mkt_trends_index_name)
      except NotFoundError:
        self.logger.warning(f"Index '{self.mkt_trends_index_name}' not found.")

      try:
        self.ims_sold_listing_index_mapping = self.get_mapping(index=self.ims_sold_listing_index_name)
      except NotFoundError:
        self.logger.warning(f"Index '{self.ims_sold_listing_index_name}' not found.")

    except ConnectionError as e:
      self.logger.error(f"Error connecting to ES at {host}:{port} mappings not retrieved.")

    # Config and instantiate cache
    self.cache = None
    _ = load_dotenv(find_dotenv())
    self.cache_dir = os.getenv('ANALYTICS_CACHE_DIR', None)
    if self.cache_dir:
      self.cache_dir = Path(self.cache_dir)
      self.cache = FileBasedCache(cache_dir=self.cache_dir)

      # Initialize IMS Property Type Mapper
      self.ims_property_type_mapping_path = self.cache_dir / 'ims_property_type_mapping_v4_df'
      self.ims_mapper = IMSPropertyTypeMapper(cached_mapping_path=self.ims_property_type_mapping_path)
      self.logger.info("IMS Property Type Mapper initialized successfully")

    else:
      self.logger.warning('ANALYTICS_CACHE_DIR not set in .env file, not instantiating cache, may cause unexpected errors.')
      self.ims_mapper = None

    # Initialize GeoCollection for GUID resolution (lazy-loaded)
    self.geo_collection = None  # Loaded on-demand by bulk_import_ims_sold_listings()

    # map from property type to the listingType and searchCategoryType encoding (the rlp_archive_current index)
    # for used in ES querying.
    self.property_type_query_mappings = [
      # CONDO
      {"bool": {
          "must": [
              {"terms": {"listingType.keyword": ["CONDO", "REC"]}},
              {"term": {"searchCategoryType": 101}}
          ]
      }},
      # SEMI-DETACHED
      {"term": {"searchCategoryType": 104}},
      # TOWNHOUSE
      {"term": {"searchCategoryType": 102}},
      # DETACHED
      {"term": {"searchCategoryType": 103}}
    ]

    
  def ping(self) -> bool:
    return self.es.ping()
  
  def get_cluster_node_names(self) -> List[str]:
    """
    Retrieves the names of the nodes in the Elasticsearch cluster.

    Returns:
    - List[str]: A list of node names in the Elasticsearch cluster.
    """
    try:
      nodes_info = self.es.cat.nodes(format="json")
      node_names = [node['name'] for node in nodes_info]
      # print(f"Name: {node['name']}, IP: {node['ip']}, Roles: {node['node.role']}, Master: {node['master']}, Heap Percent: {node['heap.percent']}, RAM Percent: {node['ram.percent']}, CPU: {node['cpu']}, Load 1m: {node['load_1m']}, Load 5m: {node['load_5m']}, Load 15m: {node['load_15m']}")
      return node_names
    except ConnectionError:
      self.logger.error(f"Error connecting to ES at {self.es_host}:{self.es_port}")
      return None
    except Exception as e:
      self.logger.error(f"Unexpected error retrieving node names: {e}")
      return None
    
  def env(self) -> str:
    """
    Determines if the Elasticsearch cluster is running on PROD or UAT (staging).

    Returns:
    - str: 'PROD' if the cluster is in the production environment,
           'UAT' if the cluster is in the staging environment.
    """
    node_names = self.get_cluster_node_names()
    if node_names:
      for name in node_names:
        if 'prod' in name.lower():
          return 'PROD'
        elif 'uat' in name.lower():
          return 'UAT'
    return 'Unknown'

  def _ensure_geo_collection(self):
    """Lazy-load GeoCollection for GUID resolution (heavy operation)."""
    if self.geo_collection is None:
      from realestate_analytics.data.geo import GeoCollection

      geo_cache_path = self.cache_dir / 'geo_collection.dill'

      # Try loading from cache first
      if geo_cache_path.exists():
        try:
          self.logger.info(f"Loading GeoCollection from cache: {geo_cache_path}")
          print(f"Loading GeoCollection from cache: {geo_cache_path}")
          self.geo_collection = GeoCollection.load(geo_cache_path, use_dill=True)
          self.logger.info(f"Loaded {len(self.geo_collection)} geos from cache")
          return self.geo_collection
        except Exception as e:
          self.logger.warning(f"Failed to load geo_collection from cache: {e}")
          self.logger.info("Falling back to loading from datastore...")

      # Fall back to loading from ES
      self.logger.info("Loading GeoCollection from datastore...")
      self.geo_collection = GeoCollection.from_datastore(
        self,
        require_details=False,  # Include geos without details
        fix_relationships=True   # CRITICAL: enables parent-child relationships (100x performance)
      )
      self.logger.info(f"Loaded {len(self.geo_collection)} geos from datastore")

      # Save to cache for next time
      try:
        self.geo_collection.save(geo_cache_path, use_dill=True)
        self.logger.info(f"Saved geo_collection to cache: {geo_cache_path}")
      except Exception as e:
        self.logger.warning(f"Failed to save geo_collection to cache: {e}")

    return self.geo_collection

  def get_alias(self) -> Dict:
    """
    Retrieves all aliases in the Elasticsearch cluster.
    """
    return self.es.indices.get_alias("*")

  def create_listing_tracking_index(self):
    index_name = "rlp_listing_tracking_1"
    alias_name = self.listing_tracking_index_name

    index_schema = {
      "mappings": {
        "properties": {
            "price": {"type": "float"},
            "addedOn": {"type": "date"},
            "lastUpdate": {"type": "date"},
            "guid": {"type": "keyword"},
            "propertyType": {"type": "keyword"}
            }
        }
    }

    # Create the index if it doesn't already exist
    if not self.es.indices.exists(index=index_name):
      self.es.indices.create(index=index_name, body=index_schema)
      self.logger.info(f"Index '{index_name}' created successfully.")
    else:
      self.logger.info(f"Index '{index_name}' already exists.")

    # Check if the alias already exists
    if not self.es.indices.exists_alias(name=alias_name):
      self.es.indices.put_alias(index=index_name, name=alias_name)
      self.logger.info(f"Alias '{alias_name}' created for index '{index_name}'.")
    else:
      self.logger.info(f"Alias '{alias_name}' already exists.")

  def create_mkt_trends_index(self):
    index_name = "rlp_mkt_trends_1"
    alias_name = self.mkt_trends_index_name

    index_schema = MKT_TRENDS_INDEX_MAPPING

    # Create the index if it doesn't already exist
    if not self.es.indices.exists(index=index_name):
      self.es.indices.create(index=index_name, body=index_schema)
      self.logger.info(f"Index '{index_name}' created successfully.")
    else:
      self.logger.info(f"Index '{index_name}' already exists.")

    # Check if the alias already exists
    if not self.es.indices.exists_alias(name=alias_name):
      self.es.indices.put_alias(index=index_name, name=alias_name)
      self.logger.info(f"Alias '{alias_name}' created for index '{index_name}'.")
    else:
      self.logger.info(f"Alias '{alias_name}' already exists.")

  def create_ims_sold_listing_index(self):
    """
    Create the IMS sold listings index with versioned name and alias.

    Index stores raw IMS (Integrated MLS System) sold listings from all Canadian provinces.
    Documents use format: {province_code}-{mlsid}-{mlsnumber}-{year} (e.g., on-20-w12193397-2024)

    The schema includes:
    - Core transaction fields (WrittenDate, SoldPrice, ListPrice, DaysOnMarket)
    - Geographic fields (Latitude, Longitude, City, Community, Province, guid)
    - Property characteristics (Bedrooms, Baths, Type, SubType, Style, propertyType)
    - Context fields (MLSNumber, MLSID, Address, PostalCode, etc.)
    - Optional fields (SoldOverAsk, SquareFoot, YearBuilt, Garage)
    - System field (last_updated)

    Note: WrittenDate uses "yyyy-MM-dd" format (date only). Bulk import code must
    convert IMS datetime strings (e.g., "2024-01-01T00:00:00") to date-only format.
    """
    index_name = "rlp_ims_sold_listing_1"
    alias_name = self.ims_sold_listing_index_name

    index_schema = IMS_SOLD_LISTING_INDEX_MAPPING

    # Create the index if it doesn't already exist
    if not self.es.indices.exists(index=index_name):
      self.es.indices.create(index=index_name, body=index_schema)
      self.logger.info(f"Index '{index_name}' created successfully.")
    else:
      self.logger.info(f"Index '{index_name}' already exists.")

    # Check if the alias already exists
    if not self.es.indices.exists_alias(name=alias_name):
      self.es.indices.put_alias(index=index_name, name=alias_name)
      self.logger.info(f"Alias '{alias_name}' created for index '{index_name}'.")
    else:
      self.logger.info(f"Alias '{alias_name}' already exists.")

  def delete_ims_sold_listing_index(self):
    """
    Delete the IMS sold listings index and its alias.

    This is a destructive operation that removes:
    1. The alias (rlp_ims_sold_listing_current)
    2. The versioned index (rlp_ims_sold_listing_1) and all its data

    Use with caution - this cannot be undone.
    """
    index_name = "rlp_ims_sold_listing_1"
    alias_name = self.ims_sold_listing_index_name

    # Delete alias first (if it exists)
    if self.es.indices.exists_alias(name=alias_name):
      # Need to get the index that the alias points to
      alias_info = self.es.indices.get_alias(name=alias_name)
      indices_with_alias = list(alias_info.keys())

      for idx in indices_with_alias:
        self.es.indices.delete_alias(index=idx, name=alias_name)
        self.logger.info(f"Alias '{alias_name}' deleted from index '{idx}'.")
    else:
      self.logger.info(f"Alias '{alias_name}' does not exist, skipping alias deletion.")

    # Delete the versioned index (if it exists)
    if self.es.indices.exists(index=index_name):
      self.es.indices.delete(index=index_name)
      self.logger.info(f"Index '{index_name}' deleted successfully.")
    else:
      self.logger.info(f"Index '{index_name}' does not exist, skipping index deletion.")

  def bulk_import_ims_sold_listings(
      self,
      csv_path: Path,
      batch_size: int = 500
  ) -> Tuple[int, List[dict]]:
    """
    Bulk import IMS sold listings from CSV file.

    Args:
        csv_path: Path to IMS CSV file with columns matching IMS schema
        batch_size: Number of documents per batch (default 500)

    Returns:
        Tuple of (success_count, failed_list) where:
        - success_count: Number of successfully indexed documents
        - failed_list: List of error dictionaries for failed operations

    CSV Required Columns:
        MLSNumber, MLSID, Province, WrittenDate (required for document ID)
        SoldPrice, ListPrice, DaysOnMarket, Latitude, Longitude, City,
        Type, SubType, Style, Address, PostalCode, FSA, District,
        Community, Bedrooms, Baths, Country, SoldOverAsk,
        ListingSellingPriceDiff, SquareFoot, YearBuilt, Garage

    Document ID Format:
        {province}-{mlsid}-{mlsnumber}-{year} (all lowercase)
        Year is extracted from WrittenDate column
        Example: "on-20-w12193397-2024"

    Note:
        - WrittenDate in CSV is "YYYY-MM-DDTHH:MM:SS" format, converted to "YYYY-MM-DD"
        - Year for document ID is parsed from WrittenDate
        - guid field is populated via MultiPassGUIDResolver (three-pass strategy)
        - propertyType is populated via IMSPropertyTypeMapper with provincial overrides
        - Numeric fields are safely coerced (invalid string values become None)
        - Invalid numeric strings (e.g., "N/A", "$1,234", "--") converted to None
        - Uses upsert behavior (updates existing, creates new)
    """
    self.logger.info(f"Starting bulk import from: {csv_path}")

    # Check if file exists
    if not csv_path.exists():
      self.logger.error(f"CSV file not found: {csv_path}")
      return 0, [{"error": f"File not found: {csv_path}"}]

    # Read CSV
    try:
      df = pd.read_csv(csv_path)
      self.logger.info(f"Loaded {len(df)} rows from CSV")
    except Exception as e:
      self.logger.error(f"Error reading CSV: {e}")
      return 0, [{"error": f"Failed to read CSV: {str(e)}"}]

    # Check if DataFrame is empty
    if df.empty:
      self.logger.warning("CSV is empty, nothing to import")
      return 0, []

    # Validate required columns exist
    required_columns = [
      'MLSNumber', 'MLSID', 'Province', 'WrittenDate',  # Required for document ID
      'SoldPrice', 'ListPrice', 'DaysOnMarket',  # Core transaction fields
      'Latitude', 'Longitude', 'City',  # Geographic fields
      'Type', 'SubType', 'Style',  # Property type fields
      'Bedrooms', 'Baths',  # Property characteristics
      'Address', 'PostalCode', 'FSA', 'District', 'Community',  # Context fields
      'Country', 'SoldOverAsk', 'ListingSellingPriceDiff',  # Optional fields
      'SquareFoot', 'YearBuilt', 'Garage'  # Bonus fields
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
      error_msg = f"Missing required columns in CSV: {missing_columns}"
      self.logger.error(error_msg)
      return 0, [{"error": error_msg}]

    # Pre-convert numeric columns to handle invalid string values safely
    # Using pd.to_numeric with errors='coerce' converts invalid values to NaN
    numeric_columns = {
      'SoldPrice': float,
      'ListPrice': float,
      'DaysOnMarket': int,
      'Latitude': float,
      'Longitude': float,
      'Bedrooms': int,
      'Baths': float,  # Float to support fractional bathrooms (e.g., 1.5, 2.5)
      'SquareFoot': int,
      'Garage': int,
      'ListingSellingPriceDiff': float
    }

    for col, dtype in numeric_columns.items():
      if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    self.logger.info("Pre-processed numeric columns for safe conversion")

    # ========== Property Type Mapping ==========
    # Populate 'propertyType' field using IMSPropertyTypeMapper
    # This field is required in ES documents (line 840)

    if self.ims_mapper is None:
      error_msg = (
        "IMS Property Type Mapper not initialized. "
        "Set ANALYTICS_CACHE_DIR in .env file and ensure cache directory exists."
      )
      self.logger.error(error_msg)
      return 0, [{"error": error_msg}]

    self.logger.info("Applying property type mappings to IMS data...")
    property_type_start = time.time()

    # Trim whitespace from Type, SubType, Style columns
    for col in ["Type", "SubType", "Style"]:
      df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip())

    # Step 1: Load DataFrame into mapper
    self.ims_mapper.load_dataframe(df)

    # Step 2: Generate full mapping DataFrame (uses cache-first strategy)
    # This updates cache with new (Type, SubType, Style) combinations
    self.ims_mapper.generate_full_mapping_dataframe()

    # Step 3: Apply property types using vectorized lookup
    # Adds 'propertyType' column to df in-place
    self.ims_mapper.apply_property_types()

    # Step 4: Apply provincial overrides (AB duplex exception)
    # Alberta business rule: Duplex → SEMI-DETACHED (not OTHER)
    ab_duplex_mask = (
      (df['Province'] == 'AB') &
      ((df['SubType'] == 'Duplex') | (df['SubType'] == 'Full Duplex') | 
       (df['SubType'] == 'Half Duplex') | (df['SubType'] == 'Duplex Side By Side'))
    )
    if ab_duplex_mask.any():
      override_count = ab_duplex_mask.sum()
      df.loc[ab_duplex_mask, 'propertyType'] = 'SEMI-DETACHED'
      self.logger.info(f"Applied AB duplex override to {override_count:,} listings")

    # Step 5: Save updated mapping cache (persists new combinations)
    # This ensures future runs don't need to re-compute these mappings
    try:
      self.ims_mapper.save_mapping()
      self.logger.info("Saved updated property type mapping cache")
    except Exception as e:
      self.logger.warning(f"Failed to save mapping cache: {e}")
      # Non-fatal - continue with bulk import

    property_type_elapsed = time.time() - property_type_start
    self.logger.info(f"Property type mapping completed in {property_type_elapsed:.2f}s")

    # Verify propertyType column exists
    if 'propertyType' not in df.columns:
      error_msg = "Property type mapping failed - 'propertyType' column not created"
      self.logger.error(error_msg)
      return 0, [{"error": error_msg}]

    # Log property type distribution
    property_type_counts = df['propertyType'].value_counts()
    self.logger.info(f"Property type distribution:\n{property_type_counts}")

    # ========== GUID Resolution ==========
    # Populate 'guid' field using MultiPassGUIDResolver
    # Uses three-pass strategy: city name matching → polygon containment → LocalLogic API

    self.logger.info("Resolving GUIDs for IMS listings...")
    guid_start = time.time()

    # Lazy-load GeoCollection (heavy operation, ~10-20 seconds)
    geo_collection = self._ensure_geo_collection()

    # Initialize resolver with checkpoint directory
    # Checkpoints enable resume if job is interrupted
    checkpoint_dir = self.cache_dir / 'ims_guid_resolution_checkpoints'

    from realestate_analytics.utils.ims import MultiPassGUIDResolver

    resolver = MultiPassGUIDResolver(
      geo_collection=geo_collection,
      checkpoint_dir=checkpoint_dir,
      checkpoint_batch_size=10000,  # Checkpoint every 10,000 listings
      use_geocoder=False,  # Don't use geocoder since it has bad rate limits
      geocoder_min_interval_sec=3.0,  # Rate limiting (2,500 requests/day limit)
      logger=self.logger
    )

    # Apply multi-pass GUID resolution
    # Modifies df in-place, adds 'guid' column and updates coordinates
    # CRITICAL: GUID resolution failures will halt processing with detailed diagnostics
    df = resolver.resolve(df, show_progress=True)

    # Log resolution statistics
    guid_resolved = df['guid'].notna().sum()
    guid_total = len(df)
    guid_rate = (guid_resolved / guid_total * 100) if guid_total > 0 else 0

    self.logger.info(f"GUID resolution: {guid_resolved:,}/{guid_total:,} listings ({guid_rate:.1f}%)")

    guid_elapsed = time.time() - guid_start
    self.logger.info(f"GUID resolution completed in {guid_elapsed:.2f}s")

    # ========== Pre-Import Validation ==========
    # Verify required columns exist before bulk indexing

    required_for_import = ['propertyType', 'guid']
    missing = [col for col in required_for_import if col not in df.columns]

    if missing:
      error_msg = f"Missing required columns for bulk import: {missing}"
      self.logger.error(error_msg)
      return 0, [{"error": error_msg}]

    self.logger.info(f"DataFrame prepared for bulk import: {len(df):,} listings, {len(df.columns)} columns")

    def generate_actions():
      """Generator that yields ES bulk action dictionaries"""
      def to_bool(value):
        """Coerce truthy/falsy strings/numbers to bool, otherwise None."""
        if pd.isna(value):
          return None
        if isinstance(value, bool):
          return value
        if isinstance(value, (int, float)):
          return bool(int(value))
        if isinstance(value, str):
          val = value.strip().lower()
          if val in ('true', '1', 'yes', 'y'):
            return True
          if val in ('false', '0', 'no', 'n'):
            return False
        return None

      for _, row in tqdm(df.iterrows(), total=len(df), desc="Preparing documents"):
        # Extract date and year from WrittenDate (convert "2024-01-01T00:00:00" to "2024-01-01" and extract year)
        written_date_raw = row.get('WrittenDate', '')
        written_date = None
        year = None

        if pd.notna(written_date_raw):
          written_date_str = str(written_date_raw)
          # Extract date part (before 'T' if present)
          if 'T' in written_date_str:
            written_date = written_date_str.split('T')[0]
          else:
            written_date = written_date_str

          # Extract year from date (format: YYYY-MM-DD)
          try:
            year = written_date.split('-')[0] if written_date else None
          except (IndexError, AttributeError):
            year = None

        # Extract date part for ListingDate if present
        listing_date_raw = row.get('ListingDate', '')
        listing_date = None
        if pd.notna(listing_date_raw):
          listing_date_str = str(listing_date_raw)
          if 'T' in listing_date_str:
            listing_date = listing_date_str.split('T')[0]
          else:
            listing_date = listing_date_str

        # Construct document ID: {province}-{mlsid}-{mlsnumber}-{year} (lowercase)
        doc_id = (
          f"{str(row.get('Province', '')).lower()}-"
          f"{str(row.get('MLSID', '')).lower()}-"
          f"{str(row.get('MLSNumber', '')).lower()}-"
          f"{year if year else 'unknown'}"
        )

        # Build document source with all fields
        doc_source = {
          # Core Transaction Fields
          "WrittenDate": written_date,
          "ListingDate": listing_date if listing_date else None,
          "Province": row.get('Province'),
          "SoldPrice": row.get('SoldPrice') if pd.notna(row.get('SoldPrice')) else None,
          "ListPrice": row.get('ListPrice') if pd.notna(row.get('ListPrice')) else None,
          "DaysOnMarket": int(row.get('DaysOnMarket')) if pd.notna(row.get('DaysOnMarket')) else None,

          # Geographic Fields
          "Latitude": row.get('Latitude') if pd.notna(row.get('Latitude')) else None,
          "Longitude": row.get('Longitude') if pd.notna(row.get('Longitude')) else None,
          "City": row.get('City'),
          "FSACity": row.get('FSACity') if pd.notna(row.get('FSACity')) else None,
          "County": row.get('County') if pd.notna(row.get('County')) else None,
          "CityProvince": row.get('CityProvince') if pd.notna(row.get('CityProvince')) else None,
          "Community": row.get('Community') if pd.notna(row.get('Community')) else None,
          "guid": row.get('guid') if pd.notna(row.get('guid')) else None,
          "PostalCode": row.get('PostalCode') if pd.notna(row.get('PostalCode')) else None,
          "FSA": row.get('FSA') if pd.notna(row.get('FSA')) else None,
          "District": row.get('District') if pd.notna(row.get('District')) else None,
          "Country": row.get('Country', 'Canada'),

          # Property Characteristics (raw values, not computed)
          "Bedrooms": int(row.get('Bedrooms')) if pd.notna(row.get('Bedrooms')) else None,
          "Baths": row.get('Baths') if pd.notna(row.get('Baths')) else None,
          "Type": row.get('Type'),
          "SubType": row.get('SubType') if pd.notna(row.get('SubType')) else None,
          "Style": row.get('Style') if pd.notna(row.get('Style')) else None,
          "SquareFoot": int(row.get('SquareFoot')) if pd.notna(row.get('SquareFoot')) else None,
          "YearBuilt": str(row.get('YearBuilt')) if pd.notna(row.get('YearBuilt')) else None,
          "Garage": int(row.get('Garage')) if pd.notna(row.get('Garage')) else None,
          "CLASA": row.get('CLASA') if pd.notna(row.get('CLASA')) else None,
          "propertyType": row.get('propertyType') if pd.notna(row.get('propertyType')) else None,  # Populated by ims_mapper before bulk import

          # Context Fields
          "MLSNumber": row.get('MLSNumber'),
          "MLSID": row.get('MLSID'),
          "Address": row.get('Address') if pd.notna(row.get('Address')) else None,

          # Optional/Bonus Fields
          "SoldOverAsk": to_bool(row.get('SoldOverAsk')),
          "ListingSellingPriceDiff": row.get('ListingSellingPriceDiff') if pd.notna(row.get('ListingSellingPriceDiff')) else None,
          "ListPricePercentOfSold": str(row.get('ListPricePercentOfSold')) if pd.notna(row.get('ListPricePercentOfSold')) else None,
          "SoldPricePercentOfList": str(row.get('SoldPricePercentOfList')) if pd.notna(row.get('SoldPricePercentOfList')) else None,
          "IsLuxuryListed": to_bool(row.get('IsLuxuryListed')),
          "IsLuxurySold": to_bool(row.get('IsLuxurySold')),

          # System Fields
          "last_updated": datetime.now().isoformat()
        }

        # Yield bulk action
        yield {
          '_op_type': 'index',  # Upsert: update if exists, create if new
          '_index': self.ims_sold_listing_index_name,
          '_id': doc_id,
          '_source': doc_source
        }

    # Execute bulk operation
    self.logger.info("Executing bulk index operation...")
    try:
      success, failed = bulk(
        client=self.es,
        actions=generate_actions(),
        raise_on_error=False,
        raise_on_exception=False,
        chunk_size=batch_size,
        request_timeout=60
      )
    except Exception as e:
      self.logger.error(f"Bulk operation failed: {e}")
      return 0, [{"error": f"Bulk operation exception: {str(e)}"}]

    # Log summary
    total_attempts = success + len(failed)
    success_rate = (success / total_attempts * 100) if total_attempts > 0 else 0

    self.logger.info(f"Bulk import complete: {success}/{total_attempts} successful ({success_rate:.2f}%)")

    if failed:
      self.logger.warning(f"{len(failed)} documents failed to index")
      # Log first few failures for debugging
      for i, failure in enumerate(failed[:5]):
        self.logger.warning(f"Sample failure {i+1}: {failure}")

    return success, failed

  
  def bulk_update_ims_sold_listings(                                                                                                                                                                   
      self,                                                                                                                                                                                            
      csv_path: Path,                                                                                                                                                                                  
      mode: str,                                                                                                                                                                                       
      batch_size: int = 500                                                                                                                                                                            
  ) -> Tuple[int, List[dict]]:                                                                                                                                                                         
    """                                                                                                                                                                                                
    Selectively update IMS sold listings in Elasticsearch.                                                                                                                                             
                                                                                                                                                                                                       
    This method allows updating only specific fields without re-processing                                                                                                                             
    expensive operations like GUID resolution or property type mapping.                                                                                                                                
                                                                                                                                                                                                       
    **IMPORTANT**: This method uses ES update operations, which means:                                                                                                                                 
    - Only specified fields are modified                                                                                                                                                               
    - All other fields in existing documents are preserved unchanged                                                                                                                                   
    - Update will FAIL if document doesn't exist (safety feature)                                                                                                                                      
    - Run full bulk_import_ims_sold_listings first to create documents                                                                                                                                 
                                                                                                                                                                                                       
    Args:                                                                                                                                                                                              
        csv_path: Path to IMS CSV file with updated data                                                                                                                                               
        mode: Update mode - one of:                                                                                                                                                                    
            - "propertyType": Update only propertyType field                                                                                                                                           
                             (runs property type mapping, skips GUID resolution)                                                                                                                       
            - "guid": Update only guid field                                                                                                                                                           
                     (runs GUID resolution, skips property type mapping)                                                                                                                               
            - "IMS": Update all IMS data fields EXCEPT guid and propertyType                                                                                                                           
                    (skips both expensive operations)                                                                                                                                                  
        batch_size: Documents per ES bulk batch (default 500)                                                                                                                                          
                                                                                                                                                                                                       
    Returns:                                                                                                                                                                                           
        (success_count, failed_list): Tuple of successful update count and list of failures                                                                                                            
                                                                                                                                                                                                       
    Raises:                                                                                                                                                                                            
        ValueError: If mode not in ["propertyType", "guid", "IMS"]                                                                                                                                     
                                                                                                                                                                                                       
    Example:                                                                                                                                                                                           
        # After fixing property type mapper logic                                                                                                                                                      
        datastore.bulk_update_ims_sold_listings(                                                                                                                                                       
            csv_path=Path('IMSData2025.csv'),                                                                                                                                                          
            mode="propertyType"                                                                                                                                                                        
        )                                                                                                                                                                                              
        # This updates ONLY propertyType field, preserving guid and all other fields                                                                                                                   
    """                                                                                                                                                                                                
    # Validate mode parameter                                                                                                                                                                          
    valid_modes = ["propertyType", "guid", "IMS"]                                                                                                                                                      
    if mode not in valid_modes:                                                                                                                                                                        
      raise ValueError(f"Invalid mode '{mode}'. Must be one of: {valid_modes}")                                                                                                                        
                                                                                                                                                                                                       
    self.logger.info(f"Starting bulk UPDATE with mode='{mode}' for {csv_path}")                                                                                                                        
    overall_start = time.time()                                                                                                                                                                        
                                                                                                                                                                                                       
    # Step 1: Load CSV                                                                                                                                                                                 
    self.logger.info(f"Loading CSV from {csv_path}")                                                                                                                                                   
    try:                                                                                                                                                                                               
      df = pd.read_csv(csv_path)                                                                                                                                                                       
      self.logger.info(f"Loaded {len(df)} rows from CSV")                                                                                                                                              
    except FileNotFoundError:                                                                                                                                                                          
      return 0, [{"error": f"CSV file not found: {csv_path}"}]                                                                                                                                         
    except Exception as e:                                                                                                                                                                             
      return 0, [{"error": f"Failed to read CSV: {str(e)}"}]                                                                                                                                           
                                                                                                                                                                                                       
    if df.empty:                                                                                                                                                                                       
      return 0, [{"error": "CSV file is empty"}]                                                                                                                                                       
                                                                                                                                                                                                       
    # Step 2: Mode-dependent column validation                                                                                                                                                         
    base_required = ['MLSNumber', 'MLSID', 'Province', 'WrittenDate']                                                                                                                                  
                                                                                                                                                                                                       
    if mode == "propertyType":                                                                                                                                                                         
      required_columns = base_required + ['Type', 'SubType', 'Style']                                                                                                                                  
    elif mode == "guid":                                                                                                                                                                               
      required_columns = base_required + ['Latitude', 'Longitude', 'City', 'Community']                                                                                                                
    elif mode == "IMS":                                                                                                                                                                                
      required_columns = base_required + [                                                                                                                                                             
        'SoldPrice', 'ListPrice', 'DaysOnMarket',                                                                                                                                                      
        'Address', 'City', 'PostalCode', 'FSA',                                                                                                                                                        
        'Bedrooms', 'Baths', 'SquareFoot', 'YearBuilt', 'Garage'                                                                                                                                       
      ]                                                                                                                                                                                                
                                                                                                                                                                                                       
    missing_cols = [col for col in required_columns if col not in df.columns]                                                                                                                          
    if missing_cols:                                                                                                                                                                                   
      return 0, [{"error": f"Missing required columns for mode '{mode}': {missing_cols}"}]                                                                                                             
                                                                                                                                                                                                       
    self.logger.info(f"Column validation passed for mode '{mode}'")                                                                                                                                    
                                                                                                                                                                                                       
    # Step 3: Numeric conversion (shared across all modes)                                                                                                                                             
    self.logger.info("Converting numeric columns...")                                                                                                                                                  
    numeric_columns = {                                                                                                                                                                                
      'SoldPrice': float,                                                                                                                                                                              
      'ListPrice': float,                                                                                                                                                                              
      'DaysOnMarket': int,                                                                                                                                                                             
      'Latitude': float,                                                                                                                                                                               
      'Longitude': float,                                                                                                                                                                              
      'Bedrooms': int,                                                                                                                                                                                 
      'Baths': float,                                                                                                                                                                                  
      'SquareFoot': int,                                                                                                                                                                               
      'Garage': int,                                                                                                                                                                                   
      'ListingSellingPriceDiff': float                                                                                                                                                                 
    }                                                                                                                                                                                                  

    # Trim whitespace from Type, SubType, Style columns
    for col in ["Type", "SubType", "Style"]:
      df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip())

    for col, dtype in numeric_columns.items():                                                                                                                                                         
      if col in df.columns:                                                                                                                                                                            
        df[col] = pd.to_numeric(df[col], errors='coerce')                                                                                                                                              
                                                                                                                                                                                                       
    # Step 4: Conditional - Property Type Mapping (only for mode="propertyType")                                                                                                                       
    if mode == "propertyType":                                                                                                                                                                         
      self.logger.info("Running property type mapping...")                                                                                                                                             
      property_type_start = time.time()                                                                                                                                                                
                                                                                                                                                                                                       
      # Initialize mapper if needed                                                                                                                                                                    
      if self.ims_mapper is None:                                                                                                                                                                      
        if self.cache_dir is None:                                                                                                                                                                     
          return 0, [{"error": "Cache directory not set - cannot initialize IMS property type mapper"}]                                                                                                
                                                                                                                                                                                                       
        cached_mapping_path = self.cache_dir / 'ims_property_type_mapping_v4_df'                                                                                                                       
        self.logger.info(f"Initializing IMS property type mapper with cache: {cached_mapping_path}")                                                                                                   
        self.ims_mapper = IMSPropertyTypeMapper(                                                                                                                                                       
          cached_mapping_path=cached_mapping_path,                                                                                                                                                     
          logger=self.logger                                                                                                                                                                           
        )                                                                                                                                                                                              
                                                                                                                                                                                                       
      # Load data and generate mappings                                                                                                                                                                
      self.ims_mapper.load_dataframe(df)                                                                                                                                                               
      self.logger.info("Generating full property type mapping...")                                                                                                                                     
      self.ims_mapper.generate_full_mapping_dataframe()                                                                                                                                                
                                                                                                                                                                                                       
      # Apply property types via vectorized lookup                                                                                                                                                     
      self.ims_mapper.apply_property_types()                                                                                                                                                           
                                                                                                                                                                                                       
      # Apply provincial overrides (AB duplex rule)                                                                                                                                                    
      ab_duplex_mask = (                                                                                                                                                                               
        (df['Province'] == 'AB') &                                                                                                                                                                     
        ((df['SubType'] == 'Duplex') | (df['SubType'] == 'Full Duplex') |                                                                                                                              
         (df['SubType'] == 'Half Duplex') | (df['SubType'] == 'Duplex Side By Side'))                                                                                                                  
      )                                                                                                                                                                                                
      if ab_duplex_mask.any():                                                                                                                                                                         
        override_count = ab_duplex_mask.sum()                                                                                                                                                          
        df.loc[ab_duplex_mask, 'propertyType'] = 'SEMI-DETACHED'                                                                                                                                       
        self.logger.info(f"Applied AB duplex override to {override_count:,} listings")                                                                                                                 
                                                                                                                                                                                                       
      # Save updated mapping cache                                                                                                                                                                     
      try:                                                                                                                                                                                             
        self.ims_mapper.save_mapping()                                                                                                                                                                 
        self.logger.info("Saved updated property type mapping cache")                                                                                                                                  
      except Exception as e:                                                                                                                                                                           
        self.logger.warning(f"Failed to save mapping cache: {e}")                                                                                                                                      
                                                                                                                                                                                                       
      # Log property type distribution                                                                                                                                                                 
      if 'propertyType' in df.columns:                                                                                                                                                                 
        type_dist = df['propertyType'].value_counts()                                                                                                                                                  
        self.logger.info(f"Property type distribution:\n{type_dist}")                                                                                                                                  
                                                                                                                                                                                                       
      property_type_elapsed = time.time() - property_type_start                                                                                                                                        
      self.logger.info(f"Property type mapping completed in {property_type_elapsed:.2f}s")                                                                                                             
                                                                                                                                                                                                       
    else:                                                                                                                                                                                              
      self.logger.info(f"Skipping property type mapping (mode={mode})")                                                                                                                                
                                                                                                                                                                                                       
    # Step 5: Conditional - GUID Resolution (only for mode="guid")                                                                                                                                     
    if mode == "guid":                                                                                                                                                                                 
      self.logger.info("Running GUID resolution...")                                                                                                                                                   
      guid_start = time.time()                                                                                                                                                                         
                                                                                                                                                                                                       
      # Lazy-load GeoCollection                                                                                                                                                                        
      geo_collection = self._ensure_geo_collection()                                                                                                                                                   
                                                                                                                                                                                                       
      # Initialize MultiPassGUIDResolver                                                                                                                                                               
      from realestate_analytics.utils.ims import MultiPassGUIDResolver                                                                                                                              
                                                                                                                                                                                                       
      checkpoint_dir = self.cache_dir / 'ims_guid_resolution_checkpoints' if self.cache_dir else None                                                                                                  
                                                                                                                                                                                                       
      resolver = MultiPassGUIDResolver(                                                                                                                                                                
        geo_collection=geo_collection,                                                                                                                                                                 
        checkpoint_dir=checkpoint_dir,                                                                                                                                                                 
        checkpoint_batch_size=10000,                                                                                                                                                                   
        use_geocoder=False,                                                                                                                                                                            
        geocoder_min_interval_sec=3.0,                                                                                                                                                                 
        logger=self.logger                                                                                                                                                                             
      )                                                                                                                                                                                                
                                                                                                                                                                                                       
      # Apply multi-pass GUID resolution                                                                                                                                                               
      self.logger.info("Resolving GUIDs...")                                                                                                                                                           
      df = resolver.resolve(df, show_progress=True)                                                                                                                                                    
                                                                                                                                                                                                       
      # Log resolution statistics                                                                                                                                                                      
      guid_total = len(df)                                                                                                                                                                             
      guid_resolved = df['guid'].notna().sum()                                                                                                                                                         
      guid_rate = (guid_resolved / guid_total * 100) if guid_total > 0 else 0                                                                                                                          
      self.logger.info(f"GUID resolution: {guid_resolved:,}/{guid_total:,} listings ({guid_rate:.1f}%)")                                                                                               
                                                                                                                                                                                                       
      guid_elapsed = time.time() - guid_start                                                                                                                                                          
      self.logger.info(f"GUID resolution completed in {guid_elapsed:.2f}s")                                                                                                                            
                                                                                                                                                                                                       
    else:                                                                                                                                                                                              
      self.logger.info(f"Skipping GUID resolution (mode={mode})")                                                                                                                                      
                                                                                                                                                                                                       
    # Step 6: Pre-update validation                                                                                                                                                                    
    if mode == "propertyType":                                                                                                                                                                         
      required_for_update = ['propertyType']                                                                                                                                                           
    elif mode == "guid":                                                                                                                                                                               
      required_for_update = ['guid']                                                                                                                                                                   
    else:  # mode == "IMS"                                                                                                                                                                             
      required_for_update = []                                                                                                                                                                         
                                                                                                                                                                                                       
    missing = [col for col in required_for_update if col not in df.columns]                                                                                                                            
    if missing:                                                                                                                                                                                        
      return 0, [{"error": f"Missing columns after processing: {missing}"}]                                                                                                                            
                                                                                                                                                                                                       
    # Step 7: Generate ES update actions (mode-specific)                                                                                                                                               
    self.logger.info(f"Generating ES update actions for mode='{mode}'...")                                                                                                                             
                                                                                                                                                                                                       
    def generate_actions():                                                                                                                                                                            
      def to_bool(value):
        """Coerce truthy/falsy strings/numbers to bool, otherwise None."""
        if pd.isna(value):
          return None
        if isinstance(value, bool):
          return value
        if isinstance(value, (int, float)):
          return bool(int(value))
        if isinstance(value, str):
          val = value.strip().lower()
          if val in ('true', '1', 'yes', 'y'):
            return True
          if val in ('false', '0', 'no', 'n'):
            return False
        return None

      for _, row in tqdm(df.iterrows(), total=len(df), desc="Generating updates"):                                                                                                                     
        # Extract document ID (same logic for all modes)                                                                                                                                               
        written_date = str(row['WrittenDate']).split('T')[0]  # YYYY-MM-DD                                                                                                                             
        year = written_date.split('-')[0]                                                                                                                                                              
                                                                                                                                                                                                       
        prov = str(row['Province']).lower()                                                                                                                                                            
        mlsid = str(row['MLSID']).lower()                                                                                                                                                              
        mlsnumber = str(row['MLSNumber']).lower()                                                                                                                                                      
                                                                                                                                                                                                       
        doc_id = f"{prov}-{mlsid}-{mlsnumber}-{year}"                                                                                                                                                  
                                                                                                                                                                                                       
        # Build doc payload based on mode                                                                                                                                                              
        if mode == "propertyType":                                                                                                                                                                     
          doc_source = {                                                                                                                                                                               
            "propertyType": row['propertyType'],                                                                                                                                                       
            "last_updated": datetime.now().isoformat()                                                                                                                                                 
          }                                                                                                                                                                                            
                                                                                                                                                                                                       
        elif mode == "guid":                                                                                                                                                                           
          doc_source = {                                                                                                                                                                               
            "guid": row['guid'] if pd.notna(row['guid']) else None,                                                                                                                                    
            "last_updated": datetime.now().isoformat()                                                                                                                                                 
          }                                                                                                                                                                                            
                                                                                                                                                                                                       
          # Update coordinates if resolver corrected them                                                                                                                                              
          if pd.notna(row.get('Latitude')) and pd.notna(row.get('Longitude')):                                                                                                                         
            doc_source['Latitude'] = float(row['Latitude'])                                                                                                                                            
            doc_source['Longitude'] = float(row['Longitude'])                                                                                                                                          
                                                                                                                                                                                                       
        elif mode == "IMS":                                                                                                                                                                            
          # All IMS fields EXCEPT propertyType and guid                                                                                                                                                
          doc_source = {                                                                                                                                                                               
            "WrittenDate": written_date,                                                                                                                                                               
            "Province": str(row['Province']) if pd.notna(row['Province']) else None,                                                                                                                   
            "SoldPrice": float(row['SoldPrice']) if pd.notna(row['SoldPrice']) else None,                                                                                                              
            "ListPrice": float(row['ListPrice']) if pd.notna(row['ListPrice']) else None,                                                                                                              
            "DaysOnMarket": int(row['DaysOnMarket']) if pd.notna(row['DaysOnMarket']) else None,                                                                                                       
            "Latitude": float(row['Latitude']) if pd.notna(row['Latitude']) else None,                                                                                                                 
            "Longitude": float(row['Longitude']) if pd.notna(row['Longitude']) else None,                                                                                                              
            "City": str(row['City']) if pd.notna(row['City']) else None,                                                                                                                               
            "FSACity": str(row.get('FSACity', '')) if pd.notna(row.get('FSACity')) else None,                                                                                                          
            "County": str(row.get('County', '')) if pd.notna(row.get('County')) else None,                                                                                                             
            "CityProvince": str(row.get('CityProvince', '')) if pd.notna(row.get('CityProvince')) else None,                                                                                           
            "Community": str(row['Community']) if pd.notna(row['Community']) else None,                                                                                                                
            "PostalCode": str(row['PostalCode']) if pd.notna(row['PostalCode']) else None,                                                                                                             
            "FSA": str(row['FSA']) if pd.notna(row['FSA']) else None,                                                                                                                                  
            "District": str(row.get('District', '')) if pd.notna(row.get('District')) else None,                                                                                                       
            "Country": str(row.get('Country', '')) if pd.notna(row.get('Country')) else None,                                                                                                          
            "Bedrooms": int(row['Bedrooms']) if pd.notna(row['Bedrooms']) else None,                                                                                                                   
            "Baths": float(row['Baths']) if pd.notna(row['Baths']) else None,                                                                                                                          
            "Type": str(row.get('Type', '')) if pd.notna(row.get('Type')) else None,                                                                                                                   
            "SubType": str(row.get('SubType', '')) if pd.notna(row.get('SubType')) else None,                                                                                                          
            "Style": str(row.get('Style', '')) if pd.notna(row.get('Style')) else None,                                                                                                                
            "SquareFoot": int(row['SquareFoot']) if pd.notna(row['SquareFoot']) else None,                                                                                                             
            "YearBuilt": str(row['YearBuilt']) if pd.notna(row['YearBuilt']) else None,                                                                                                                
            "Garage": int(row['Garage']) if pd.notna(row['Garage']) else None,                                                                                                                         
            "CLASA": str(row.get('CLASA', '')) if pd.notna(row.get('CLASA')) else None,                                                                                                                
            "MLSNumber": str(row['MLSNumber']) if pd.notna(row['MLSNumber']) else None,                                                                                                                
            "MLSID": str(row['MLSID']) if pd.notna(row['MLSID']) else None,                                                                                                                            
            "Address": str(row.get('Address', '')) if pd.notna(row.get('Address')) else None,                                                                                                          
            "SoldOverAsk": to_bool(row.get('SoldOverAsk')) if pd.notna(row.get('SoldOverAsk')) else None,                                                                                         
            "ListingSellingPriceDiff": float(row.get('ListingSellingPriceDiff')) if pd.notna(row.get('ListingSellingPriceDiff')) else None,                                                            
            "ListPricePercentOfSold": float(row.get('ListPricePercentOfSold')) if pd.notna(row.get('ListPricePercentOfSold')) else None,                                                               
            "SoldPricePercentOfList": float(row.get('SoldPricePercentOfList')) if pd.notna(row.get('SoldPricePercentOfList')) else None,                                                               
            "IsLuxuryListed": to_bool(row.get('IsLuxuryListed')) if pd.notna(row.get('IsLuxuryListed')) else None,                                                                                
            "IsLuxurySold": to_bool(row.get('IsLuxurySold')) if pd.notna(row.get('IsLuxurySold')) else None,                                                                                      
            "last_updated": datetime.now().isoformat()                                                                                                                                                 
            # NOTE: Explicitly exclude 'propertyType' and 'guid' to preserve existing values                                                                                                           
          }                                                                                                                                                                                            
                                                                                                                                                                                                       
        # Yield ES update action (not index/upsert)                                                                                                                                                    
        yield {                                                                                                                                                                                        
          '_op_type': 'update',  # Update existing doc only                                                                                                                                            
          '_index': self.ims_sold_listing_index_name,                                                                                                                                                  
          '_id': doc_id,                                                                                                                                                                               
          'doc': doc_source,  # Fields to update (partial document)                                                                                                                                    
          'doc_as_upsert': False  # Fail if document doesn't exist (safety)                                                                                                                            
        }                                                                                                                                                                                              
                                                                                                                                                                                                       
    # Step 8: Execute ES bulk update                                                                                                                                                                   
    self.logger.info(f"Starting ES bulk update ({len(df):,} documents)...")                                                                                                                            
    bulk_start = time.time()                                                                                                                                                                           
                                                                                                                                                                                                       
    try:                                                                                                                                                                                               
      success, failed = bulk(                                                                                                                                                                          
        client=self.es,                                                                                                                                                                                
        actions=generate_actions(),                                                                                                                                                                    
        raise_on_error=False,                                                                                                                                                                          
        raise_on_exception=False,                                                                                                                                                                      
        chunk_size=batch_size,                                                                                                                                                                         
        request_timeout=60                                                                                                                                                                             
      )                                                                                                                                                                                                
                                                                                                                                                                                                       
      bulk_elapsed = time.time() - bulk_start                                                                                                                                                          
      self.logger.info(f"Bulk update completed in {bulk_elapsed:.2f}s")                                                                                                                                
                                                                                                                                                                                                       
    except Exception as e:                                                                                                                                                                             
      self.logger.error(f"Bulk update operation failed: {e}")                                                                                                                                          
      return 0, [{"error": f"Bulk update exception: {str(e)}"}]                                                                                                                                        
                                                                                                                                                                                                       
    # Step 9: Log summary                                                                                                                                                                              
    total_attempts = success + len(failed)                                                                                                                                                             
    success_rate = (success / total_attempts * 100) if total_attempts > 0 else 0                                                                                                                       
                                                                                                                                                                                                       
    overall_elapsed = time.time() - overall_start                                                                                                                                                      
    self.logger.info(f"Bulk update complete: {success:,}/{total_attempts:,} successful ({success_rate:.2f}%) in {overall_elapsed:.2f}s")                                                               
                                                                                                                                                                                                       
    if failed:                                                                                                                                                                                         
      self.logger.warning(f"{len(failed):,} documents failed to update")                                                                                                                               
      # Log first few failures for debugging                                                                                                                                                           
      for i, failure in enumerate(failed[:5]):                                                                                                                                                         
        self.logger.warning(f"Sample failure {i+1}: {failure}")                                                                                                                                        
                                                                                                                                                                                                       
    return success, failed                 


  def get_mapping(self, index: str) -> dict:
    """Retrieve mappings for a specific index."""
    return self.es.indices.get_mapping(index=index)
  
  def count(self, index: str) -> int:
    """Count the number of documents in a specific index."""
    return self.es.count(index=index)['count']
  
  def search(self, 
             index: str, 
             start_time: datetime = None, 
             end_time: datetime = None, 
             filters: Dict = None,
             _id: str = None,
             selects: List[str] = None,
             size=10) -> List[dict]:
    """
    Searches an Elasticsearch index for documents within a specified time range.

    Parameters:
    - index (str): The name of the Elasticsearch index to search.
    - start_time (datetime, optional): The start of the time range for the search. Defaults to None.
    - end_time (datetime, optional): The end of the time range for the search. Defaults to None.
    - filters (dict, optional): Additional key-value pairs to filter the search. Defaults to None.
    - _id (str, optional): The document ID to search for. Defaults to None.
    - selects (List[str], optional): List of fields to retrieve. If None, all fields are retrieved.
    - size (int | None, optional): The maximum number of search hits to return. Defaults to 10. If None, returns all hits.

    Returns:
    - List[dict]: A list of dictionaries, each representing a document that matches the search criteria.

    Raises:
    - ValueError: If an invalid index name is provided.

    Note: if _id is provided, all other criteria are ignored and just retrieves that doc.
    """
    self.logger.debug(f"Adhoc searching index '{index}'")
    if _id:
      try:
        return [self.es.get(index=index, id=_id, _source=selects or True)['_source']]
      except NotFoundError:
        self.logger.warning(f"Document with ID '{_id}' not found in index '{index}'.")
        return []

    date_format, pertinent_time_field = None, None
    if index == self.sold_listing_index_name:
      date_format = '%Y-%m-%dT%H:%M:%S'
      pertinent_time_field = 'lastTransition'
    elif index == self.listing_index_name:
      # date_format = '%Y/%m/%d'
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'addedOn'
    elif index == self.mkt_trends_index_name:
      # 2024-07-12T00:00:00Z
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'last_updated'
    elif index == self.listing_tracking_index_name:
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'addedOn'
    elif index == self.ims_sold_listing_index_name:
      # IMS sold listings use date-only WrittenDate (YYYY-MM-DD)
      date_format = '%Y-%m-%d'
      pertinent_time_field = 'WrittenDate'
    elif index == self.geo_index_name or index == self.geo_details_en_index_name:
      date_format = None       # TODO: decide later on date range filtering
      pertinent_time_field = None
    elif start_time is not None or end_time is not None:
      raise NotImplementedError(f"start_time or end_time provided but {index} need date format and pertinent_time_field.")
    
    query_body = self._build_query(pertinent_time_field, start_time, end_time, date_format,
                                   filters)
    self.logger.debug(f"Constructed query: {query_body}")
    if size is None:
      results = []
      for hit in scan(self.es, index=index, query=query_body, _source=selects or True):
        results.append(hit['_source'])
      return results
    elif size > 200:
      results = []
      for hit in scan(self.es, index=index, query=query_body, _source=selects or True):
        results.append(hit['_source'])
        if len(results) >= size:
          break
      return results
    else:
      response = self.es.search(index=index, body=query_body, size=size, _source=selects or True)
      return [hit['_source'] for hit in response['hits']['hits']]

  def get_sold_listings(self, start_time: datetime = None, end_time: datetime = None, 
                        date_field: str = "lastTransition", selects: List[str]=None) -> Tuple[bool, pd.DataFrame]:
    """
    Query sold listings from the past year filtered by property types:
    - CONDO, SEMI-DETACHED, TOWNHOUSE, DETACHED
    Parameters:
    - start_time (datetime, optional): The start of the time range for the search. Defaults to one year ago.
    - end_time (datetime, optional): The end of the time range for the search. Defaults to None (current time).
    - date_field (str, optional): The field to use for date filtering. Defaults to "lastTransition".
    - selects (List[str], optional): List of fields to retrieve. If None, all fields are retrieved.
    Returns:
    Tuple[bool, pd.DataFrame]: A tuple containing:
        - A boolean indicating success (True) or failure (False)
        - A DataFrame representing the sold listings.
    """
    
    self.logger.info(f"Fetching sold listings from {start_time if start_time else 'unspecified'} to {end_time if end_time else 'unspecified'}")
    start_process_time = time.time()

    try:
      must_clauses = [
        {
          "match": {"listingStatus": "SOLD"},
        },
        {
          "match": {"transactionType": "SALE"}
        },
        {
          "bool": {
            "should": self.property_type_query_mappings
          }
        }
      ]

      if start_time is None:
        start_time = datetime.now() - timedelta(days=365)  # one year ago

      if date_field == "lastTransition":
        date_format = '%Y-%m-%dT%H:%M:%S'
      elif date_field == "lastUpdate":
        date_format = '%y-%m-%d:%H:%M:%S'
      else:
        raise ValueError(f"Unsupported date_field: {date_field}")

      time_range_filter = {
        "range": {
          date_field: {
            "gte": start_time.strftime(date_format)
          }
        }
      }

      #  Add end_time to the query if provided
      if end_time:
        time_range_filter["range"][date_field]["lte"] = end_time.strftime(date_format)

      must_clauses.append(time_range_filter)
      
      query_body = {
        "query": {
          "bool": {
            "must": must_clauses
          }
        },
        "_source": selects or True  # Select all fields if no fields are specified
      }
      self.logger.debug(f"Constructed query: {query_body}")
      
      scroll_id = None
      sources = []
    
      page = self.es.search(index=self.sold_listing_index_name, body=query_body, scroll='5m', size=500)
      scroll_id = page['_scroll_id']
      total_hits = page['hits']['total']['value']
      self.logger.info(f"Total hits for sold listings: {total_hits}")

      hits = page['hits']['hits']

      while hits:
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']
    except ConnectionError as e:
      self.logger.error(f"Connection error while fetching sold listings: {str(e)}")
      return False, pd.DataFrame()
    
    except Exception as e:
      self.logger.error(f"Unexpected error in get_sold_listing: {e}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
    
    finally:
      self._safe_remove_scroll_id(scroll_id)

    if not sources:
      self.logger.warning("No sold listings found - this is highly unexpected and should be investigated ASAP")
      return True, pd.DataFrame()
    
    self.logger.info(f"Fetched {len(sources)} sold listings")

    def serialize_dict_cols(x: Any):
      return json.dumps(x) if isinstance(x, dict) else x
    
    # creating the dataframe
    try:
      sold_listings_df = pd.DataFrame(sources)
      if 'listingType' in sold_listings_df.columns:
        sold_listings_df.listingType = sold_listings_df.listingType.apply(format_listingType)

      # Add property type
      sold_listings_df['propertyType'] = sold_listings_df.apply(derive_property_type, axis=1)

      for col in ['climateInfo', 'NBHDEnEntry', 'NBHDFrEntry']:
        if col in sold_listings_df.columns:
          sold_listings_df[col] = sold_listings_df[col].apply(serialize_dict_cols)

      # format known datetime column 
      if 'lastTransition' in sold_listings_df.columns:
        sold_listings_df.lastTransition = pd.to_datetime(sold_listings_df.lastTransition)


      process_time = time.time() - start_process_time
      self.logger.info(f"Completed fetching sold listings in {process_time:.2f} seconds")
      return True, sold_listings_df
    except Exception as e:
      self.logger.error(f"Error converting sold listings to DataFrame: {e}")
      return False, pd.DataFrame()


  def get_ims_sold_listings(self, start_time: datetime = None, end_time: datetime = None,
                           date_field: str = "WrittenDate", selects: List[str] = None,
                           prov_code: str = None) -> Tuple[bool, pd.DataFrame]:
    """
    Query sold listings from the IMS (Integrated MLS System) dataset.

    This method queries the IMS ES index for sold listings across all Canadian provinces.
    The IMS dataset uses different field mappings compared to the current rlp_archive_current index:
    - Primary Key: MLSNumber (instead of ES document ID)
    - Property Type: Already populated in ES during bulk import (no runtime derivation needed)
    - Field Mappings: WrittenDate → lastTransition, SoldPrice → soldPrice, etc.
    - Geographic Coverage: All Canadian provinces (not just GTA)
    - Data Lag: Sold listings can arrive up to 1 year after transaction date

    Parameters:
    - start_time (datetime, optional): The start of the time range for the search. Defaults to one year ago.
    - end_time (datetime, optional): The end of the time range for the search. Defaults to None (current time).
    - date_field (str, optional): The field to use for date filtering. Defaults to "WrittenDate"
                                 (IMS native field, will be renamed to lastTransition in output).
    - selects (List[str], optional): List of fields to retrieve. If None, all fields are retrieved.
                                    Note: Field names should match the canonical interface for compatibility.
    - prov_code (str, optional): Province code to filter listings (e.g., 'ON', 'BC', 'AB').
                                If None, returns data for all provinces.

    Returns:
    Tuple[bool, pd.DataFrame]: A tuple containing:
        - A boolean indicating success (True) or failure (False)
        - A DataFrame representing the sold listings with same column structure as get_sold_listings()

    Notes:
    - propertyType is already populated in ES docs during bulk import (via ims_mapper)
    - This method does NOT derive property types - it just reads and transforms field names
    - Province filtering is optional (None = all provinces)
    - Returns canonical schema (WrittenDate→lastTransition, SoldPrice→soldPrice, etc.)
    """

    # Apply same default as get_sold_listings
    if start_time is None:
      start_time = datetime.now() - timedelta(days=365)  # one year ago

    self.logger.info(f"Fetching IMS sold listings from {start_time} to {end_time if end_time else 'unspecified'}{f' for province {prov_code}' if prov_code else ''}")
    start_process_time = time.time()

    try:
      must_clauses = []

      # Property type filter (IMS docs already have propertyType field populated during bulk import)
      # Filter for the 4 main residential property types
      must_clauses.append({
        "terms": {"propertyType": ["CONDO", "SEMI-DETACHED", "TOWNHOUSE", "DETACHED"]}
      })

      # Province filter (optional)
      if prov_code:
        must_clauses.append({
          "term": {"Province": prov_code}
        })

      # Date range filter
      date_format = '%Y-%m-%d'  # IMS WrittenDate format
      time_range_filter = {
        "range": {
          date_field: {
            "gte": start_time.strftime(date_format)
          }
        }
      }

      # Add end_time to the query if provided
      if end_time:
        time_range_filter["range"][date_field]["lte"] = end_time.strftime(date_format)

      must_clauses.append(time_range_filter)

      query_body = {
        "query": {
          "bool": {
            "must": must_clauses
          }
        },
        "_source": True  # Get all fields (we'll apply selects after transformation)
      }
      self.logger.debug(f"Constructed query: {query_body}")

      scroll_id = None
      sources = []

      page = self.es.search(index=self.ims_sold_listing_index_name, body=query_body, scroll='5m', size=500)
      scroll_id = page['_scroll_id']
      total_hits = page['hits']['total']['value']
      self.logger.info(f"Total hits for IMS sold listings: {total_hits}")

      hits = page['hits']['hits']

      while hits:
        for hit in hits:
          doc = hit['_source']
          # Note: IMS uses MLSNumber as logical primary key, but we'll include ES _id for consistency
          doc['_id'] = hit['_id']
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

    except ConnectionError as e:
      self.logger.error(f"Connection error while fetching IMS sold listings: {str(e)}")
      return False, pd.DataFrame()

    except Exception as e:
      self.logger.error(f"Unexpected error in get_ims_sold_listings: {e}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()

    finally:
      self._safe_remove_scroll_id(scroll_id)

    if not sources:
      self.logger.info("No IMS sold listings found")
      if selects:
        columns = list(selects)
        if '_id' not in columns:
          columns.append('_id')
      else:
        columns = [
          '_id', 'mls', 'lastTransition', 'soldPrice', 'price', 'daysOnMarket',
          'lat', 'lng', 'city', 'neighbourhood', 'provState',
          'beds', 'bedsInt', 'baths', 'bathsInt',
          'listingStatus', 'guid', 'computed_guid', 'transitions', 'lastUpdate',
          'listingType', 'searchCategoryType', 'propertyType'
        ]
      return True, pd.DataFrame(columns=columns)

    self.logger.info(f"Fetched {len(sources)} IMS sold listings")

    # Convert to DataFrame
    try:
      raw_ims_df = pd.DataFrame(sources)

      # Validate required IMS columns exist
      is_valid, missing = validate_ims_columns(raw_ims_df)
      if not is_valid:
        self.logger.error(f"IMS sold listings data is missing required columns: {missing}")
        return False, pd.DataFrame()

      # Transform IMS schema to canonical schema
      # Note: propertyType already exists in raw_ims_df (populated during bulk import)
      # No need to call ims_mapper here - it was applied during bulk import
      canonical_df = rename_ims_to_canonical(raw_ims_df, keep_bonus_cols=False)

      # Ensure lastTransition is datetime for downstream ETL comparisons
      if 'lastTransition' in canonical_df.columns:
        canonical_df['lastTransition'] = pd.to_datetime(
          canonical_df['lastTransition'],
          errors='coerce'
        )

      # Apply selects filter if specified (after transformation to canonical names)
      if selects:
        # Only keep columns that exist in both selects and canonical_df
        available_cols = [col for col in selects if col in canonical_df.columns]
        if '_id' in canonical_df.columns and '_id' not in available_cols:
          available_cols.append('_id')
        canonical_df = canonical_df[available_cols]

      process_time = time.time() - start_process_time
      self.logger.info(f"Completed fetching IMS sold listings in {process_time:.2f} seconds")
      return True, canonical_df

    except Exception as e:
      self.logger.error(f"Error converting IMS sold listings to DataFrame: {e}")
      return False, pd.DataFrame()


  def get_current_active_listings(self, selects: List[str], prov_code: str=None,
                                  addedOn_start_time: datetime=None, addedOn_end_time: datetime=None,
                                  updated_start_time: datetime=None, updated_end_time: datetime=None,
                                  use_script_for_last_update: bool=False,  # TODO: should comment out later after dev
                                  active=True
                                  ):
    """
    This is used in context of nearby_comparable_solds.NearbyComparableSoldsProcessor ETL pipeline.
    Fetches current active or non-active listings based on specified filters for a NearbyComparableSoldsProcessor ETL pipeline.

    Parameters:
    - selects (List[str]): Fields to retrieve.
    - prov_code (str, optional): Province code to filter listings by.
    - addedOn_start_time (datetime, optional): Start time for filtering listings added on.
    - addedOn_end_time (datetime, optional): End time for filtering listings added on.
    - updated_start_time (datetime, optional): Start time for filtering listings last updated.
    - updated_end_time (datetime, optional): End time for filtering listings last updated.
    - use_script_for_last_update (bool, optional): If True, uses a script for filtering by last update time. Requires both updated_start_time and updated_end_time to be set.
    - active (bool, optional): If True, fetches active listings. If False, fetches non-active listings.

    Returns:
    Tuple[bool, pd.DataFrame]: A tuple containing a boolean indicating if the fetch was successful, and a DataFrame of the listings.
    """

    self.logger.info(f'Fetching current {"active" if active else "non active"} listings for province: {prov_code}')
    start_process_time = time.time()

    try:
      must_filters = [
          {"match": {"transactionType": "SALE"}},
          {"bool": {"should": self.property_type_query_mappings}}
      ]
      if active:
        must_filters.append({"match": {"listingStatus": "ACTIVE"}})
      else:
        must_filters.append({"bool": {"must_not": {"match": {"listingStatus": "ACTIVE"}}}})

      if prov_code is not None:
        must_filters.append({"match": {"provState": prov_code}})

      # Prepare time range filters
      time_filters = []

      if addedOn_start_time or addedOn_end_time:
        added_range = {}
        if addedOn_start_time:
          added_range["gte"] = addedOn_start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')
        if addedOn_end_time:
          added_range["lte"] = addedOn_end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')
        time_filters.append({"range": {"addedOn": added_range}})

      if updated_start_time or updated_end_time:
        if use_script_for_last_update:
          if updated_start_time is None or updated_end_time is None:
            raise ValueError("Both updated_start_time and updated_end_time must be provided when using script query for lastUpdate")

          script_query = {
            "script": {
              "script": {
                "source": """
                  DateTimeFormatter formatter = DateTimeFormatter.ofPattern('yy-MM-dd:HH:mm:ss');
                  LocalDateTime start = LocalDateTime.parse(params.start, formatter);
                  LocalDateTime end = LocalDateTime.parse(params.end, formatter);
                  if (doc['lastUpdate.keyword'].size() == 0) {
                    return false;
                  }
                  LocalDateTime lastUpdate = LocalDateTime.parse(doc['lastUpdate.keyword'].value, formatter);
                  return lastUpdate.isAfter(start) && lastUpdate.isBefore(end);
                """,
                "params": {
                  "start": updated_start_time.strftime('%y-%m-%d:%H:%M:%S'),
                  "end": updated_end_time.strftime('%y-%m-%d:%H:%M:%S')
                }
              }
            }
          }
          
          time_filters.append(script_query)
        else:
          updated_range = {}
          if updated_start_time:
            updated_range["gte"] = updated_start_time.strftime('%y-%m-%d:%H:%M:%S')
          if updated_end_time:
            updated_range["lte"] = updated_end_time.strftime('%y-%m-%d:%H:%M:%S')
          time_filters.append({"range": {"lastUpdate": updated_range}})

      # Add time filters to the query if any are specified
      if time_filters:
        must_filters.append({"bool": {"should": time_filters}})
      
      query_body = {
        "query": {
          "bool": {
            "must": must_filters
          }
        },
        "_source": selects or True  # Select all fields if no fields are specified
      }
      
      self.logger.debug(f"Query body: {query_body}")

      scroll_id = None
      sources = []
    
      page = self.es.search(index=self.listing_index_name, body=query_body, scroll='5m', size=500)
      scroll_id = page['_scroll_id']
      total_hits = page['hits']['total']['value']
      self.logger.info(f"Total hits for current {'active' if active else 'non active'} listings: {total_hits}")
      hits = page['hits']['hits']

      while hits:
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

      self.logger.info(f"Fetched {len(sources)} current {'active' if active else 'non active'} listings")

    except ConnectionError as e:
      self.logger.error(f"Connection error while fetching current active listings: {str(e)}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
    
    except RequestError as e:
      self.logger.error(f"Request error while fetching current active listings: {str(e)}")
      self.logger.error(f"Query body: {query_body}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
  
    except Exception as e:
      self.logger.error(f"Unexpected error in get_current_active_listings: {e}")
      self.logger.error(f"Query body: {query_body}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
    
    finally:
      self._safe_remove_scroll_id(scroll_id)

    if not sources:
      # A query that executed successfully and legitimately matched zero documents is not a
      # failure - it's a valid result. Returning False here would make it indistinguishable
      # from the real error paths above (ConnectionError/RequestError/Exception), forcing
      # every caller to guess whether "False" means "confirmed zero" or "query broke".
      self.logger.info("No current active listings found")
      return True, pd.DataFrame(columns=list(selects) + ['_id', 'propertyType'])

    try:

      listings_df = pd.DataFrame(sources)
      if 'listingType' in listings_df.columns:
        listings_df.listingType = listings_df.listingType.apply(format_listingType)

      # Add property type
      listings_df['propertyType'] = listings_df.apply(derive_property_type, axis=1)

      # format known datetime column
      if 'addedOn' in listings_df.columns:
        # listings_df.addedOn = pd.to_datetime(listings_df.addedOn)
        listings_df.addedOn = listings_df.addedOn.apply(parse_datetime)
      if 'lastUpdate' in listings_df.columns:
        parsed_lastupdate = pd.to_datetime(listings_df.lastUpdate, format='%y-%m-%d:%H:%M:%S', errors='coerce')
        bad_lastupdate = parsed_lastupdate.isna() & listings_df.lastUpdate.notna()   # present but unparseable; missing stays NaT as before
        listings_df.lastUpdate = parsed_lastupdate
        if bad_lastupdate.any():
          self.logger.warning(f"Dropping {bad_lastupdate.sum()} listing(s) with unparseable lastUpdate: "
                              f"{listings_df.loc[bad_lastupdate, '_id'].tolist()}")
          listings_df = listings_df[~bad_lastupdate].reset_index(drop=True)

      process_time = time.time() - start_process_time
      self.logger.info(f"Completed fetching current active listings in {process_time:.2f} seconds")
      return True, listings_df
    except Exception as e:
      self.logger.error(f"Error converting current active listings to DataFrame: {e}")
      return False, pd.DataFrame()
    

  def get_listings(self, selects: List[str]=None, prov_code: str=None, 
                   addedOn_start_time: datetime=None, addedOn_end_time: datetime=None,
                   updated_start_time: datetime=None, updated_end_time: datetime=None,
                   use_script_for_last_update: bool=False  # TODO: should comment out later after dev
                   ):    
    """
    Retrieves listings from Elasticsearch based on various filters and returns them as a DataFrame.

    Parameters:
    - selects (List[str], optional): A list of fields to include in the returned documents. If None, all fields are included.
    - prov_code (str, optional): The province or state code to filter listings by.
    - addedOn_start_time (datetime, optional): The start time for filtering listings based on their added date.
    - addedOn_end_time (datetime, optional): The end time for filtering listings based on their added date.
    - updated_start_time (datetime, optional): The start time for filtering listings based on their last update date.
    - updated_end_time (datetime, optional): The end time for filtering listings based on their last update date.

    Returns:
    - Tuple[bool, pd.DataFrame]: A tuple containing:
      - A boolean indicating success (True) or failure (False)
      - A pandas DataFrame of the listings.

    Note:
    - The method applies filters for transaction type and property type by default.
    - Time filters for 'addedOn' and 'lastUpdate' are applied if corresponding start and end times are provided.
    - The method handles pagination internally using Elasticsearch's scroll API.
    """
  
    self.logger.info(f"Fetching listings for province: {prov_code}")
    start_process_time = time.time()

    must_filters = [
      {"match": {"transactionType": "SALE"}},
      {"bool": {"should": self.property_type_query_mappings}}
    ]
    if prov_code is not None:
      must_filters.append({"match": {"provState": prov_code}})

    # Prepare time range filters
    time_filters = []

    if addedOn_start_time or addedOn_end_time:
      added_range = {}
      if addedOn_start_time:
        added_range["gte"] = addedOn_start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')
      if addedOn_end_time:
        added_range["lte"] = addedOn_end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')
      time_filters.append({"range": {"addedOn": added_range}})

    if updated_start_time or updated_end_time:
      if use_script_for_last_update:
        if updated_start_time is None or updated_end_time is None:
          raise ValueError("Both updated_start_time and updated_end_time must be provided when using script query for lastUpdate")
        script_query = {
          "script": {
              "script": {
                "source": """
                  DateTimeFormatter formatter = DateTimeFormatter.ofPattern('yy-MM-dd:HH:mm:ss');
                  LocalDateTime start = LocalDateTime.parse(params.start, formatter);
                  LocalDateTime end = LocalDateTime.parse(params.end, formatter);
                  if (doc['lastUpdate.keyword'].size() == 0) {
                    return false;
                  }
                  LocalDateTime lastUpdate = LocalDateTime.parse(doc['lastUpdate.keyword'].value, formatter);
                  return lastUpdate.isAfter(start) && lastUpdate.isBefore(end);
                """,
                "params": {
                  "start": updated_start_time.strftime('%y-%m-%d:%H:%M:%S'),
                  "end": updated_end_time.strftime('%y-%m-%d:%H:%M:%S')
                }
              }
            }
        }
        time_filters.append(script_query)
      else:
        updated_range = {}
        if updated_start_time:
          updated_range["gte"] = updated_start_time.strftime('%y-%m-%d:%H:%M:%S')
        if updated_end_time:
          updated_range["lte"] = updated_end_time.strftime('%y-%m-%d:%H:%M:%S')
        time_filters.append({"range": {"lastUpdate": updated_range}})

    # Add time filters to the query if any are specified
    if time_filters:
      must_filters.append({"bool": {"should": time_filters}})

    query_body = {
      "query": {
        "bool": {
          "must": must_filters
        }
      },
      "_source": selects or True  # Select all fields if no fields are specified
    }

    self.logger.debug(f"Query body: {query_body}")

    scroll_id = None
    sources = []

    try:
      page = self.es.search(index=self.listing_index_name, body=query_body, scroll='5m', size=500)
      scroll_id = page['_scroll_id']
      total_hits = page['hits']['total']['value']
      self.logger.info(f"Total hits for listings: {total_hits}")
      hits = page['hits']['hits']
    
      while hits:
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

      self.logger.info(f"Fetched {len(sources)} current listings")
    
    except ConnectionError as e:
      self.logger.error(f"Connection error while fetching listings: {str(e)}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
    
    except RequestError as e:
      self.logger.error(f"Request error while fetching listings: {str(e)}")
      self.logger.error(f"Query body: {query_body}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()

    except Exception as e:
      self.logger.error(f"Unexpected error in get_listings: {e}")
      self.logger.error(f"Query body: {query_body}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()

    finally:
      self._safe_remove_scroll_id(scroll_id)

    if not sources:
      self.logger.info("No listings found")
      return True, pd.DataFrame()

    try:
      listings_df = pd.DataFrame(sources)
      if 'listingType' in listings_df.columns:
        listings_df.listingType = listings_df.listingType.apply(format_listingType)

      # Add property type
      listings_df['propertyType'] = listings_df.apply(derive_property_type, axis=1)

      # format known datetime column
      if 'addedOn' in listings_df.columns:
        # listings_df.addedOn = pd.to_datetime(listings_df.addedOn)
        listings_df.addedOn = listings_df.addedOn.apply(parse_datetime)
      if 'lastUpdate' in listings_df.columns:
        parsed_lastupdate = pd.to_datetime(listings_df.lastUpdate, format='%y-%m-%d:%H:%M:%S', errors='coerce')
        bad_lastupdate = parsed_lastupdate.isna() & listings_df.lastUpdate.notna()   # present but unparseable; missing stays NaT as before
        listings_df.lastUpdate = parsed_lastupdate
        if bad_lastupdate.any():
          self.logger.warning(f"Dropping {bad_lastupdate.sum()} listing(s) with unparseable lastUpdate: "
                              f"{listings_df.loc[bad_lastupdate, '_id'].tolist()}")
          listings_df = listings_df[~bad_lastupdate].reset_index(drop=True)
  
      process_time = time.time() - start_process_time
      self.logger.info(f"Completed fetching listings in {process_time:.2f} seconds")

      return True, listings_df
    except Exception as e:
      self.logger.error(f"Error converting listings to DataFrame: {e}")
      return False, pd.DataFrame()


  def get_geo_entry_df(self) -> pd.DataFrame:
    """
    Returns a dedup dataframe of geo_entry table rows from geo db 
    This is read from a tab delimited text dump
    """
    return self.cache.get('all_geo_entry')


  # dev workaround, need to periodically manually update the guid in sold listings cache
  def update_geo_entry_df(self, data_dir: Union[str, Path]):
    """
    Updated geo_entry table dump is read from a tab delimited text file
    This is part of the solution until guid is properly stamped to new future sold listings
    """
    data_dir = Path(data_dir)
    all_geo_entry_df = pd.read_csv(data_dir/'all_geo_entry.txt', sep='\t')  # read in new .txt dump
    self.logger.info(f"Read in {len(all_geo_entry_df)} rows from updated all_geo_entry.txt")

    # combine with previous geo_entry_df
    prev_geo_entry_df = self.cache.get('all_geo_entry')
    self.logger.info(f"Read in {len(prev_geo_entry_df)} rows from previous all_geo_entry cache") 

    all_geo_entry_df = pd.concat([prev_geo_entry_df, all_geo_entry_df], ignore_index=True)
    # drop duplicates and keep last
    all_geo_entry_df.drop_duplicates(subset=['MLS', 'CITY', 'PROV_STATE'], keep='last', inplace=True)
    all_geo_entry_df.reset_index(drop=True, inplace=True)

    self.logger.info(f"Output {len(all_geo_entry_df)} rows to updated all_geo_entry cache")

    self.cache.set(key='all_geo_entry', value=all_geo_entry_df)


  def fix_guid_in_sold_listing_cache(self):
    """
    This is a temporary fix to update guid in sold listing cache
    """
    
    for sold_listing_cache_key in ['SoldMedianMetricsProcessor/five_years_sold_listing', 'NearbyComparableSoldsProcessor/one_year_sold_listing']:

      sold_listing_df = self.cache.get(sold_listing_cache_key)
      geo_entry_df = self.cache.get('all_geo_entry')

      if sold_listing_df is None or geo_entry_df is None:
        self.logger.error("Sold listings or geo entry df not found in cache.")
        return
      
      len_b4_sold_listing = len(sold_listing_df)
      self.logger.info(f'len of {sold_listing_cache_key} before fixing guid: {len_b4_sold_listing}')

      geo_entry_df.drop_duplicates(subset=['MLS', 'CITY', 'PROV_STATE'], keep='last', inplace=True)

      # Merge the geo_entry_df with the sold_listing_df
      sold_listing_df = join_df(sold_listing_df, 
                                geo_entry_df[['MLS', 'CITY', 'PROV_STATE', 'GEOGRAPHIES']], 
                                left_on=['mls', 'city', 'provState'], 
                                right_on=['MLS', 'CITY', 'PROV_STATE'], 
                                how='left')

      # sold_listing_df['guid'] = sold_listing_df['GEOGRAPHIES']
      for index, row in sold_listing_df.iterrows():
        if pd.isna(row['guid']) or row['guid'] == '' or row['guid'] == 'None':
          sold_listing_df.at[index, 'guid'] = row['GEOGRAPHIES']

      sold_listing_df.drop(columns=['MLS', 'CITY', 'PROV_STATE', 'GEOGRAPHIES'], inplace=True)  # remove the extra columns from geo_entry
      len_after_sold_listing = len(sold_listing_df)
      self.logger.info(f'len of {sold_listing_cache_key} after fixing guid: {len_after_sold_listing}')

      assert len_b4_sold_listing == len_after_sold_listing, "Length of sold listing df changed after fixing guid"

      self.cache.set(key=sold_listing_cache_key, value=sold_listing_df)

      perc_guid_null = round(sold_listing_df.guid.isnull().sum()/len(sold_listing_df) * 100, 2)
      self.logger.info(f"% of guid null in {sold_listing_cache_key}: {perc_guid_null}%")

 
  def _update_es_sold_listings_with_guid(self):
    '''
    Fix legacy sold listings with guid (aka geog_id).
    Run this only after carefully preparing five_years_sold_listing
    '''
    sold_listing_df = self.cache.get('SoldMedianMetricsProcessor/five_years_sold_listing')
    if sold_listing_df is None or sold_listing_df.empty:
      self.logger.error("Failed to retrieve 5-year sold listing data from cache.")
      return

    # Query for documents with valid coordinates, recent transactions, and missing guids
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "range": {"lat": {"gte": -90.0, "lte": 90.0}}
            },
            {
              "range": {"lng": {"gte": -180.0, "lte": 180.0}}
            },
            {
              "range": {
                "lastTransition": {
                  "gte": "now-90d/d",   # last 90 days
                  "lte": "now"
                }
              }
            }
          ],
          "must_not": {
            "exists": {
              "field": "guid.keyword"
            }
          }
        }
      }
    }

    # Use the scan helper to efficiently retrieve all matching documents
    documents_to_update = scan(
      self.es,
      index=self.sold_listing_index_name,
      query=query,
      _source=["_id"]
    )

    def generate_updates():
      for doc in tqdm(documents_to_update, desc="Updating documents"):
        doc_id = doc["_id"]
        _df = sold_listing_df.q("_id == @doc_id")
        if not _df.empty:
          guid = _df.iloc[0].guid
          if pd.notna(guid):
            yield {
              '_op_type': 'update',
              '_index': self.sold_listing_index_name,
              '_id': doc_id,
              'doc': {
                'guid': guid
              }
            }

    # Perform bulk update
    success, failed = bulk(self.es, generate_updates(), stats_only=True, raise_on_error=False)
    self.logger.info(f"Successfully updated {success} documents with guid.")
    if failed:
      self.logger.error(f"Failed to update {failed} documents")

    return success, failed


  def _delete_computed_guid(self, index_name: str):
    sold_listing_cache_keys = ['SoldMedianMetricsProcessor/five_years_sold_listing', 'NearbyComparableSoldsProcessor/one_year_sold_listing']

    for sold_listing_cache_key in sold_listing_cache_keys:
      sold_listing_df = self.cache.get(sold_listing_cache_key)

      sold_listing_df.loc[sold_listing_df['computed_guid'].notnull(), ['guid', 'computed_guid']] = None

      self.cache.set(sold_listing_cache_key, sold_listing_df)


  def get_geos(self, selects: List[str]=None, return_df=True):
    body = {"query": {"match_all": {}}}
    scroller = scan(self.es, index=self.geo_index_name, query=body)
    
    doc_ids, sources = [], []
    for hit in scroller:
      doc_id = hit['_id']
      source = hit['_source']
      doc_ids.append(doc_id)
      sources.append(source)    
    
    if return_df:
      geo_df = pd.DataFrame(sources)
      geo_df['geog_id'] = doc_ids
        
      return geo_df
    else:
      return doc_ids, sources

  def get_geo_details(self, selects: List[str] = None, return_df=True):
    # fetch geo details docs for a province, name, or lang and return them as a dataframe

    body = {
        "query": {"match_all": {}},
        "_source": selects or True
    }
      
    scroller = scan(self.es, index=self.geo_details_en_index_name, query=body)
    
    doc_ids, sources = [], []
    for hit in scroller:
      doc_id = hit['_id']
      source = hit['_source']
      doc_ids.append(doc_id)
      sources.append(source)
      
    if return_df:
      geo_details_df = pd.DataFrame([src['data'] for src in sources])
      geo_details_df['geog_id'] = doc_ids
      
      return geo_details_df
    else:
      return doc_ids, sources
    
  def _build_query(self, 
                  pertinent_time_field: str,
                  start_time: datetime = None, 
                  end_time: datetime = None, 
                  date_format: str = '%Y-%m-%dT%H:%M:%S',
                  filters: Dict = None
                  ):
    """
    Builds a time range query for Elasticsearch based on the provided start and end times.

    Parameters:
    - pertinent_time_field (str): The field in Elasticsearch documents to query against.
    - start_time (datetime, optional): The start time for the range query.
    - end_time (datetime, optional): The end time for the range query.
    - date_format (str, optional): The strftime format to convert datetime objects to strings.
    - filters (dict, optional): Additional key-value pairs to filter the search.

    Returns:
    - dict: An Elasticsearch query dict.
    """
    
    must_clauses = []

    # Add time range query if start_time or end_time is provided
    range_query = {}
    if start_time:
      range_query['gte'] = start_time.strftime(date_format)
    if end_time:
      range_query['lte'] = end_time.strftime(date_format)

    if range_query:
      must_clauses.append({"range": {pertinent_time_field: range_query}})

    # Add filters if provided
    if filters:
      for key, value in filters.items():
        if isinstance(value, str):
          must_clauses.append({"match_phrase": {key: value}})
        else:
          must_clauses.append({"term": {key: value}})

    if must_clauses:
      return {
        "query": {
          "bool": {
            "must": must_clauses
          }
        }
      }
    else:
      return {"query": {"match_all": {}}}
    

  def _safe_remove_scroll_id(self, scroll_id: str = None):
    if scroll_id:
      try:
        self.es.clear_scroll(scroll_id=scroll_id)
      except Exception as e:
        self.logger.error(f"Error removing scroll ID: {e}")




  def close(self):
    if self.es:
      self.es.close()

  # helper to summarize update and delete failures
  def summarize_update_failures(self, update_failures):
    reasons = Counter()

    for failure in update_failures:
      error = failure['update'].get('error', {})
      reason = error.get('type', 'unknown')
      reasons[reason] += 1

    self.logger.info("ES failures summary:")
    self.logger.info("\tSummary,\t\tCount")
    self.logger.info("\t-----------------")
    for reason, count in reasons.items():
      self.logger.info(f"\t\"{reason}\", {count}")
    self.logger.info("\t-----------------")

  def summarize_delete_failures(self, delete_failures):
    reasons = Counter()

    for failure in delete_failures:
      error = failure.get('delete', {}).get('result', {})
      reasons[error] += 1

    self.logger.info("ES failures summary:")
    self.logger.info("\tSummary,\t\tCount")
    self.logger.info("\t-----------------")
    for reason, count in reasons.items():
      self.logger.info(f"\t\"{reason}\", {count}")
    self.logger.info("\t-----------------")
