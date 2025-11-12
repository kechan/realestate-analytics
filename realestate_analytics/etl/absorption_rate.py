from typing import Dict, List, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import pytz, logging, calendar

from .base_etl import BaseETLProcessor
from ..data.caching import FileBasedCache
from ..data.es import Datastore
from ..data.archive import Archiver
from ..utils.validators import ProvinceGeogIdValidator
from elasticsearch.helpers import scan, bulk

import realestate_core.common.class_extensions
from realestate_core.common.class_extensions import *


class AbsorptionRateProcessor(BaseETLProcessor):
  # https://www.cgprealestateconsulting.com/post/calculating-absorption-rate-real-estate
  def __init__(self, job_id: str, datastore: Datastore, prov_code: str = 'ON'):
    super().__init__(job_id=job_id, datastore=datastore, prov_code=prov_code)

    self.logger.info(f'Province: {self.prov_code}')
    self.logger.info("Last run is expectedly None since we don't do any extract from ES for this ETL job.")

    self.geog_id_validator = ProvinceGeogIdValidator(
      archive_dir=self.archiver.archive_dir,
      prov_code=self.prov_code
    )

    # Raw data loaded from cache
    self.sold_listing_df = None                 # 5-year sold listings from SoldMedianMetricsProcessor (IMS data)
    self.listing_df = None                      # Current active listings from NearbyComparableSoldsProcessor (for month-end snapshots)
    self.current_counts_ts_df = None            # Persistent cache for historic current listing snapshots (long format: geog_id, propertyType, year-month, current_count)

    # Time series data structures (pivot tables)
    self.current_counts_ts_pivot = None         # Current counts pivot: index=(geog_id, propertyType), columns=year-month, values=current_count
    self.sold_counts_ts_df = None               # Sold counts pivot: index=(geog_id, propertyType), columns=year-month, values=sold_count

    # Absorption rates time series for ground truth diffing
    self.absorption_rates_ts_df = None          # Absorption rates pivot: index=(geog_id, propertyType), columns=year-month, values=absorption_rate
    self.months_of_inventory_ts_df = None       # Months of inventory pivot: index=(geog_id, propertyType), columns=year-month, values=months_of_inventory

    # self.absorption_rates = None   # old

    self.LOAD_SUCCESS_THRESHOLD = 0.5
    self.ABSORPTION_RATE_ROUND_DIGITS = 4
    self.MONTHS_OF_INVENTORY_ROUND_DIGITS = 2

  def extract(self, from_cache=True):
    # ignore the from_cache flag, always load from cache
    # this is to conform to the signature of extract(...) that the parent class expects

    self._load_from_cache()
    self._mark_success('extract')
    # else:
    #   self.logger.error("Direct extraction from datastore not implemented. Use from_cache=True.")
    #   self.logger.error("Cached data from NearbyComparableSoldsProcessor should be used.")
    #   raise NotImplementedError("Direct extraction from datastore not implemented. 
    # Cached data from NearbyComparableSoldsProcessor and SoldMedianMetricsProcessor should be used.")
     

  def transform(self):
    if self._was_success('transform'):
      self.logger.info("Transform already completed successfully. Loading diff from cache for recovery")
      # Load the diff for recovery rerun (not the full absorption rates)
      checkpoint_key = f"{self.cache_prefix}{self.job_id}_absorption_rates_changes"
      self.changes_df = self.cache.get(checkpoint_key)
      if self.changes_df is None:
        self.changes_df = pd.DataFrame(columns=['geog_id', 'propertyType'])
      return

    try:
      # Step 1: Load previous rates for comparison (the "old" baseline)
      old_absorption_rates_flat = self.cache.get(f'{self.cache_prefix}{self.prov_code.lower()}_absorption_rates_time_series')
      old_absorption_rates_ts_df = None
      if old_absorption_rates_flat is not None and not old_absorption_rates_flat.empty:
        old_absorption_rates_ts_df = old_absorption_rates_flat.set_index(['geog_id', 'propertyType'])

      # **NEW**: Load previous months_of_inventory for comparison
      old_months_of_inventory_flat = self.cache.get(f'{self.cache_prefix}{self.prov_code.lower()}_months_of_inventory_time_series')
      old_months_of_inventory_ts_df = None
      if old_months_of_inventory_flat is not None and not old_months_of_inventory_flat.empty:
        old_months_of_inventory_ts_df = old_months_of_inventory_flat.set_index(['geog_id', 'propertyType'])

      # Step 2: Compute from scratch using latest sold/current listings
      self._prepare_sold_data()
      self._create_sold_counts_time_series()
      self._prepare_current_counts_time_series()
      self._calculate_absorption_rates_time_series()  # produces self.absorption_rates_ts_df and self.months_of_inventory_ts_df

      # Step 2.5: Handle month-end snapshot if needed
      self._handle_month_end_snapshot()

      # Step 3: Compare and diff current vs prior run
      self.changes_df = self._detect_changes(old_absorption_rates_ts_df, self.absorption_rates_ts_df)

      # Step 4: Cache the "diff" for recovery purpose
      checkpoint_key = f"{self.cache_prefix}{self.job_id}_absorption_rates_changes"  # Note: checkpoint uses job_id so no prov_code prefix needed
      if self.changes_df is not None and not self.changes_df.empty:
        self.cache.set(checkpoint_key, self.changes_df)

      # Step 5: Cache the latest absorption_rates_ts_df (ground truth), overwriting the previous old
      if hasattr(self, 'absorption_rates_ts_df') and self.absorption_rates_ts_df is not None:
        absorption_rates_flat = self.absorption_rates_ts_df.reset_index()           # Reset index for feather compatibility
        self.cache.set(f'{self.cache_prefix}{self.prov_code.lower()}_absorption_rates_time_series', absorption_rates_flat)
        self.logger.info("Successfully cached absorption rates time series.")
      else:
        self.logger.error("absorption_rates_ts_df not found or is None.")
        raise ValueError("absorption_rates_ts_df not found or is None.")

      # Step 5b: Cache the latest months_of_inventory_ts_df (ground truth), overwriting the previous old
      if hasattr(self, 'months_of_inventory_ts_df') and self.months_of_inventory_ts_df is not None:
        months_of_inventory_flat = self.months_of_inventory_ts_df.reset_index()
        self.cache.set(f'{self.cache_prefix}{self.prov_code.lower()}_months_of_inventory_time_series', months_of_inventory_flat)
        self.logger.info("Successfully cached months of inventory time series.")
        self._mark_success('transform')
      else:
        self.logger.error("months_of_inventory_ts_df not found or is None.")
        raise ValueError("months_of_inventory_ts_df not found or is None.")

    except Exception as e:
      self.logger.error(f"Unexpected error in transform: {e}")
      raise


  def load(self):
    if self._was_success('load'):
      self.logger.info("Load already successful.")
      return

    absorption_success = 0
    absorption_failed = []
    snapshot_success = 0
    snapshot_failed = []

    # Handle absorption rate changes (existing logic)
    if hasattr(self, 'changes_df') and not self.changes_df.empty:
      absorption_success, absorption_failed = self.update_mkt_trends_ts_index()
      absorption_total = absorption_success + len(absorption_failed)

      if absorption_total != 0 and absorption_success / absorption_total < self.LOAD_SUCCESS_THRESHOLD:
        self.logger.error(f"Absorption rate update: Less than 50% success rate. Only {absorption_success} out of {absorption_total} documents updated.")
        self.datastore.summarize_update_failures(absorption_failed)
    else:
      self.logger.info("No absorption rate changes detected - skipping absorption rate ES updates")

    # Handle month-end snapshot updates (new logic - always check)
    if hasattr(self, 'month_end_snapshot_data'):
      snapshot_success, snapshot_failed = self.update_month_end_snapshot_in_es(
        self.month_end_snapshot_data['current_counts'],
        self.month_end_snapshot_data['last_month_yearmonth']
      )
      
      snapshot_total = snapshot_success + len(snapshot_failed)
      if snapshot_total > 0:
        if snapshot_success / snapshot_total < self.LOAD_SUCCESS_THRESHOLD:
          self.logger.error(f"Month-end snapshot update: Less than 50% success rate. Only {snapshot_success} out of {snapshot_total} documents updated.")
          self.datastore.summarize_update_failures(snapshot_failed)
        else:
          self.logger.info(f"Month-end snapshot update: Successfully updated {snapshot_success} documents")
    else:
      self.logger.info("No month-end snapshot to update")

    # Mark success based on ALL relevant operations
    absorption_ok = (not hasattr(self, 'changes_df') or self.changes_df.empty) or \
                    (absorption_success + len(absorption_failed) > 0 and 
                    absorption_success / (absorption_success + len(absorption_failed)) >= self.LOAD_SUCCESS_THRESHOLD)
    
    snapshot_total = snapshot_success + len(snapshot_failed)
    snapshot_ok = (not hasattr(self, 'month_end_snapshot_data')) or \
              (snapshot_total == 0) or \
              (snapshot_success / snapshot_total >= self.LOAD_SUCCESS_THRESHOLD)

    if absorption_ok and snapshot_ok:
      self._mark_success('load')

    return absorption_success, absorption_failed 

  def load_old(self):
    if self._was_success('load'):
      self.logger.info("Load already successful.")
      return

    # Check if we have changes to process
    if not hasattr(self, 'changes_df') or self.changes_df.empty:
      self.logger.info("No absorption rate changes detected - skipping ES updates")
      self._mark_success('load')
      return 0, []
    
    success, failed = self.update_mkt_trends_ts_index()
    total_attempts = success + len(failed)

    if total_attempts != 0 and success / total_attempts < self.LOAD_SUCCESS_THRESHOLD:
      self.logger.error(f"Less than 50% success rate. Only {success} out of {total_attempts} documents updated.")

      self.datastore.summarize_update_failures(failed)
    else:
      self._mark_success('load')

    return success, failed


  def _handle_month_end_snapshot(self):
    """Archive current listing snapshot and update persistent cache if month boundary"""
    current_date = self.get_current_datetime()
    is_month_boundary = (current_date.day == 1)
    
    if is_month_boundary:
      last_month = current_date.replace(day=1) - timedelta(days=1)
      last_month_yearmonth = last_month.strftime('%Y-%m')
      
      # Create snapshot for archival
      def expand_guid(df):
        return df.assign(geog_id=df['guid'].str.split(',')).explode('geog_id')

      expanded_current = expand_guid(self.listing_df)

      # Filter out cross-province geog_ids to prevent contamination
      if not self.geog_id_validator.bypass_mode:
        before_count = len(expanded_current)
        expanded_current = expanded_current[expanded_current['geog_id'].isin(self.geog_id_validator.valid_geog_ids)].reset_index(drop=True)
        after_count = len(expanded_current)

        contaminated_count = before_count - after_count
        if contaminated_count > 0:
          contamination_pct = 100 * contaminated_count / before_count
          self.logger.warning(
            f"[CONTAMINATION] Filtered {contaminated_count} cross-province geog_ids "
            f"({contamination_pct:.2f}%) for province {self.prov_code} in month-end snapshot"
          )
        else:
          self.logger.info(f"No cross-province contamination detected for {self.prov_code} in month-end snapshot")
      else:
        self.logger.warning(
          f"[VALIDATION BYPASS] Skipping geog_id validation for {self.prov_code} month-end snapshot - "
          f"validator is in bypass mode. Contamination checks are NOT active."
        )

      current_counts = expanded_current.groupby(['geog_id', 'propertyType']).size().reset_index(name='current_count')
      
      # Archive the snapshot
      self.archiver.archive(current_counts, f'{self.prov_code.lower()}_current_listing_counts_{last_month_yearmonth}')
      
      # Append to persistent cache time series
      self._append_to_current_counts_cache(current_counts, last_month_yearmonth)

      # Create version WITH 'ALL' for ES updates
      all_property_types = current_counts.groupby('geog_id')['current_count'].sum().reset_index()
      all_property_types['propertyType'] = 'ALL'
      current_counts_with_all = pd.concat([current_counts, all_property_types], ignore_index=True)
      
      # Store ES version for load()
      self.month_end_snapshot_data = {
        'current_counts': current_counts_with_all,  # This version has 'ALL'
        'last_month_yearmonth': last_month_yearmonth
      }

  def _append_to_current_counts_cache(self, current_counts, last_month_yearmonth):
    """
    Append new month-end snapshot to the persistent current counts time series cache
    
    Args:
      current_counts: DataFrame with columns [geog_id, propertyType, current_count] 
      last_month_yearmonth: String like '2024-07' for the month being archived
    """
    # Convert last_month_yearmonth to end-of-month datetime to match existing format
    year, month = map(int, last_month_yearmonth.split('-'))
    last_day = calendar.monthrange(year, month)[1]
    month_end_date = pd.to_datetime(f"{year}-{month:02d}-{last_day:02d}")
    
    # Add year-month column to new snapshot data
    new_snapshot = current_counts.copy()
    new_snapshot['year-month'] = month_end_date
    
    # Load existing cached time series
    existing_cache = self.cache.get(f'{self.cache_prefix}{self.prov_code.lower()}_current_counts_time_series')
    
    if existing_cache is not None and not existing_cache.empty:
      # Check if this month already exists (avoid duplicates on reruns)
      existing_months = existing_cache['year-month'].dt.strftime('%Y-%m').unique()
      
      if last_month_yearmonth in existing_months:
        self.logger.info(f"Month {last_month_yearmonth} already exists in cache - skipping append")
        return
      
      # Append new snapshot to existing data
      updated_cache = pd.concat([existing_cache, new_snapshot], ignore_index=True)
    else:
      # First snapshot ever
      updated_cache = new_snapshot
    
    # Sort by year-month to maintain chronological order
    updated_cache = updated_cache.sort_values(['year-month', 'geog_id', 'propertyType']).reset_index(drop=True)
    
    # Update the persistent cache
    self.cache.set(f'{self.cache_prefix}{self.prov_code.lower()}_current_counts_time_series', updated_cache)
    
    self.logger.info(f"Appended month-end snapshot for {last_month_yearmonth} to current counts cache")
    self.logger.info(f"Cache now contains {updated_cache['year-month'].nunique()} months of data")
  

  def _load_from_cache(self):
    super()._load_from_cache()
    
    # Load sold listings from new IMS source (5 years)
    # TODO: when IMS data is ready, this should be:
    # self.sold_listing_df = self.cache.get(f'SoldMedianMetricsProcessor/{self.prov_code.lower()}_five_years_sold_listing')    
    self.sold_listing_df = self.cache.get('SoldMedianMetricsProcessor/on_five_years_sold_listing')  # formerly f'{nearby_comparable_solds_prefix}one_year_sold_listing'

    self.listing_df = self.cache.get(f'NearbyComparableSoldsProcessor/{self.prov_code.lower()}_listing')  # TODO: need prov_code param later

    # Load cached current counts time series
    # IMPORTANT: this has to be pre-seeded from current staging run 
    self.current_counts_ts_df = self.cache.get(f'{self.cache_prefix}{self.prov_code.lower()}_current_counts_time_series')
    
    if self.sold_listing_df is None or self.listing_df is None:
      self.logger.error(f"Missing sold_listing_df or listing_df for {self.prov_code}.")
      raise ValueError(f"Missing sold_listing_df or listing_df for {self.prov_code}. ")
    
    if self.current_counts_ts_df is None:
      self.logger.warning(f"No current_counts_ts_df found for {self.prov_code} - this may be first run or no snapshots exist yet")

    
    self.logger.info(f"Loaded {len(self.sold_listing_df)} sold listings and {len(self.listing_df)} current listings from cache.")


  def update_mkt_trends_ts_index_old(self):
    """Update ES with incremental absorption rate changes"""
    def generate_actions():
      current_date = self.get_current_datetime()
      last_month = current_date.replace(day=1) - timedelta(days=1)
      last_month_str = last_month.strftime('%Y-%m')

      for _, row in self.absorption_rates.iterrows():
        property_type = row['propertyType'] if row['propertyType'] != 'ALL' else None
        composite_id = f"{row['geog_id']}_{property_type or 'ALL'}"

        # Handle NaN values and round to 3 decimal places
        absorption_rate = row['absorption_rate']
        if pd.isna(absorption_rate):
          absorption_rate = None
        else:
          absorption_rate = round(float(absorption_rate), self.ABSORPTION_RATE_ROUND_DIGITS)

        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            if (ctx._source.metrics.absorption_rate == null) {
              ctx._source.metrics.absorption_rate = [];
            }
            // Check if an entry for this month already exists
            int existingIndex = -1;
            for (int i = 0; i < ctx._source.metrics.absorption_rate.size(); i++) {
              if (ctx._source.metrics.absorption_rate[i].month == params.new_metric.month) {
                existingIndex = i;
                break;
              }
            }
            if (existingIndex >= 0) {
              // Replace existing entry
              if (params.new_metric.value != null) {
                ctx._source.metrics.absorption_rate[existingIndex] = params.new_metric;
              } else {
                ctx._source.metrics.absorption_rate.remove(existingIndex);
              }
            } else {
              // Append new entry
              if (params.new_metric.value != null) {
                ctx._source.metrics.absorption_rate.add(params.new_metric);
              }
            }
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_metric": {
                "month": last_month_str,
                "value": absorption_rate
              },
              "geog_id": row['geog_id'],
              "propertyType": property_type or "ALL",
              "geo_level": int(row['geog_id'].split('_')[0][1:]),
              "last_updated": current_date.isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": property_type or "ALL",
            "geo_level": int(row['geog_id'].split('_')[0][1:]),
            "metrics": {
              "absorption_rate": [] if absorption_rate is None else [{
                "month": last_month_str,
                "value": absorption_rate
              }]
            },
            "last_updated": current_date.isoformat()
          }
        }

    # Perform bulk update
    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)

    self.logger.info(f"Successfully updated {success} documents")
    if failed:
      self.logger.error(f"Failed to update {len(failed)} documents")
      # self.datastore.summarize_update_failures(failed)

    return success, failed

 
  def update_mkt_trends_ts_index(self):
    """Update ES by replacing entire absorption_rate and months_of_inventory time series for changed geog_id/propertyType combinations"""

    def generate_actions():
      current_date = self.get_current_datetime()

      # Process each changed geog_id/propertyType combination
      for _, change in self.changes_df.iterrows():
        geog_id = change['geog_id']
        propertyType = change['propertyType']

        property_type = propertyType if propertyType != 'ALL' else None
        composite_id = f"{geog_id}_{property_type or 'ALL'}"

        # Get the complete absorption_rate time series for this combination
        absorption_rate_array = []
        if hasattr(self, 'absorption_rates_ts_df') and not self.absorption_rates_ts_df.empty:
          try:
            time_series_row = self.absorption_rates_ts_df.loc[(geog_id, propertyType)]

            # Convert to list of month/value objects, filtering out NaN values
            for month, value in time_series_row.items():
              if pd.notna(value):
                absorption_rate_array.append({
                  'month': month,
                  'value': round(float(value), self.ABSORPTION_RATE_ROUND_DIGITS)
                })

            # Sort by month to ensure chronological order
            absorption_rate_array.sort(key=lambda x: x['month'])

          except KeyError:
            # This combination was removed in new data
            absorption_rate_array = []

        # Get the complete months_of_inventory time series for this combination
        months_of_inventory_array = []
        if hasattr(self, 'months_of_inventory_ts_df') and not self.months_of_inventory_ts_df.empty:
          try:
            time_series_row = self.months_of_inventory_ts_df.loc[(geog_id, propertyType)]

            # Convert to list of month/value objects, filtering out NaN values
            for month, value in time_series_row.items():
              if pd.notna(value):
                months_of_inventory_array.append({
                  'month': month,
                  'value': round(float(value), self.MONTHS_OF_INVENTORY_ROUND_DIGITS)
                })

            # Sort by month to ensure chronological order
            months_of_inventory_array.sort(key=lambda x: x['month'])

          except KeyError:
            # This combination was removed in new data
            months_of_inventory_array = []

        # Create ES update operation that replaces both absorption_rate and months_of_inventory arrays
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            // Safely create metrics object if it doesn't exist
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }

            // Replace entire absorption_rate array with new time series
            ctx._source.metrics.absorption_rate = params.absorption_rate_array;

            // Replace entire months_of_inventory array with new time series
            ctx._source.metrics.months_of_inventory = params.months_of_inventory_array;

            // Update document metadata
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "absorption_rate_array": absorption_rate_array,
              "months_of_inventory_array": months_of_inventory_array,
              "geog_id": geog_id,
              "propertyType": property_type or "ALL",
              "geo_level": int(geog_id.split('_')[0][1:]),
              "last_updated": current_date.isoformat()
            }
          }
        }

    # Perform bulk update
    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)

    self.logger.info(f"Updated {success} documents with absorption_rate and months_of_inventory for {len(self.changes_df)} changed combinations")
    if failed:
      self.logger.error(f"Failed to update {len(failed)} documents")
      self.datastore.summarize_update_failures(failed)

    return success, failed


  def update_month_end_snapshot_in_es(self, current_counts, last_month_yearmonth):
    """
    Update ES with new month-end current listing count snapshot.
    Only called at month boundaries.
    
    Args:
      current_counts: DataFrame with columns [geog_id, propertyType, current_count]
      last_month_yearmonth: String like '2024-07' for the month being archived
    """
    from elasticsearch.helpers import bulk
    
    def generate_actions():
      current_date = self.get_current_datetime()
      
      for _, row in current_counts.iterrows():
        geog_id = row['geog_id']
        propertyType = row['propertyType']
        current_count = int(row['current_count'])
        
        property_type = propertyType if propertyType != 'ALL' else None
        composite_id = f"{geog_id}_{property_type or 'ALL'}"
        
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            // Safely create metrics object if it doesn't exist
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            if (ctx._source.metrics.mth_end_snapshot_listing_count == null) {
              ctx._source.metrics.mth_end_snapshot_listing_count = [];
            }
            
            // Check if an entry for this month already exists (prevent duplicates on reruns)
            int existingIndex = -1;
            for (int i = 0; i < ctx._source.metrics.mth_end_snapshot_listing_count.size(); i++) {
              if (ctx._source.metrics.mth_end_snapshot_listing_count[i].month == params.new_snapshot.month) {
                existingIndex = i;
                break;
              }
            }
            
            if (existingIndex >= 0) {
              // Replace existing entry (rerun protection)
              ctx._source.metrics.mth_end_snapshot_listing_count[existingIndex] = params.new_snapshot;
            } else {
              // Append new entry
              ctx._source.metrics.mth_end_snapshot_listing_count.add(params.new_snapshot);
            }
            
            // Update document metadata
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_snapshot": {
                "month": last_month_yearmonth,
                "value": current_count
              },
              "geog_id": geog_id,
              "propertyType": property_type or "ALL",
              "geo_level": int(geog_id.split('_')[0][1:]),
              "last_updated": current_date.isoformat()
            }
          },
          "upsert": {
            "geog_id": geog_id,
            "propertyType": property_type or "ALL",
            "geo_level": int(geog_id.split('_')[0][1:]),
            "metrics": {
              "mth_end_snapshot_listing_count": [{
                "month": last_month_yearmonth,
                "value": current_count
              }]
            },
            "last_updated": current_date.isoformat()
          }
        }

    # Perform bulk update
    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)
    
    self.logger.info(f"Month-end snapshot update: Successfully updated {success} documents for {last_month_yearmonth}")
    if failed:
      self.logger.error(f"Month-end snapshot update: Failed to update {len(failed)} documents")
      self.datastore.summarize_update_failures(failed)

    return success, failed


  def delete_all_absorption_rates(self):
    """
    Deletes the 'absorption_rate' field from all documents in the market trends time series index.
    """
    def generate_actions():
      query = {
        "query": {
          "exists": {
            "field": "metrics.absorption_rate"
          }
        }
      }
      for hit in scan(self.datastore.es, index=self.datastore.mkt_trends_index_name, query=query):
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": hit["_id"],
          "script": {
            "source": """
            if (ctx._source.metrics != null) {
              ctx._source.metrics.remove('absorption_rate');
            }
            """
          }
        }

    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)
    
    self.logger.info(f"Delete absorption rates: Successfully updated {success} documents")
    if failed:
      self.logger.error(f"Delete absorption rates: Failed to update {len(failed)} documents")
      self.datastore.summarize_update_failures(failed)

    return success, failed

  


  def delete_checkpoints_data(self):
    # Clean up temporary checkpoint data
    checkpoint_key = f"{self.cache_prefix}{self.job_id}_absorption_rates_changes"
    self.cache.delete(checkpoint_key)
    self.logger.info(f"Cleaned up checkpoint: {checkpoint_key}")


  def _prepare_sold_data(self):
    """Step 2: Clean and filter sold data to months where we have snapshots"""
    if self.sold_listing_df is None:
      # raise ValueError("sold_listing_df is None")
      self.logger.info(f"No sold data available for {self.prov_code} - skipping sold data preparation")
      return
    
    if self.current_counts_ts_df is None or len(self.current_counts_ts_df) == 0:
      # raise ValueError("current_counts_ts_df is None or empty - need snapshots to determine valid months")
      self.logger.info(f"No current counts snapshots available for {self.prov_code} - no months to intersect with, clearing sold data")
      # Clear sold data since there's no intersection possible
      self.sold_listing_df = self.sold_listing_df.iloc[0:0]  # Empty DataFrame with same structure
      return
    
    # Ensure datetime format
    self.sold_listing_df['lastTransition'] = pd.to_datetime(self.sold_listing_df['lastTransition'])
    
    # Get the earliest year-month from snapshots to determine valid range
    earliest_snapshot_month = self.current_counts_ts_df['year-month'].min()
    earliest_yearmonth = earliest_snapshot_month.strftime('%Y-%m')
    
    # Add year-month to sold data for filtering (using consistent naming)
    self.sold_listing_df['year-month'] = self.sold_listing_df['lastTransition'].dt.strftime('%Y-%m')
    
    original_count = len(self.sold_listing_df)
    
    # Filter sold data to only months where we have snapshots (>= earliest snapshot month)
    # Drop rows that don't meet the date criteria
    date_filter_mask = self.sold_listing_df['year-month'] < earliest_yearmonth
    self.sold_listing_df.drop(self.sold_listing_df[date_filter_mask].index, inplace=True)
    
    # Clean data - remove invalid geog_ids
    invalid_guid_mask = self.sold_listing_df['guid'].isin(["None", None])
    self.sold_listing_df.drop(self.sold_listing_df[invalid_guid_mask].index, inplace=True)
    
    self.logger.info(f"Filtered sold data from {original_count} to {len(self.sold_listing_df)} records (>= {earliest_yearmonth})")
    if len(self.sold_listing_df) > 0:
      self.logger.info(f"Sold data range: {self.sold_listing_df['lastTransition'].min()} to {self.sold_listing_df['lastTransition'].max()}")


  def _create_sold_counts_time_series(self):
    """Step 3: Create sold counts time series from filtered sold data"""
    if self.sold_listing_df is None or len(self.sold_listing_df) == 0:
      self.logger.warning("No sold listings available after filtering")
      self.sold_counts_ts_df = pd.DataFrame()
      return
    
    # Expand guid to geog_id (keep original sold_listing_df in case needed)
    def expand_guid(df):
      return df.assign(geog_id=df['guid'].str.split(',')).explode('geog_id')
    
    expanded_sold = expand_guid(self.sold_listing_df)
    
    # Group by geog_id, propertyType, year-month and count (year-month already exists)
    sold_counts = expanded_sold.groupby(['geog_id', 'propertyType', 'year-month']).size().reset_index(name='sold_count')

    # Group by geog_id, year-month only for 'ALL' property type
    sold_counts_all = expanded_sold.groupby(['geog_id', 'year-month']).size().reset_index(name='sold_count')
    sold_counts_all['propertyType'] = 'ALL'

    # Combine both datasets
    sold_counts = pd.concat([sold_counts, sold_counts_all], ignore_index=True)
    
    # Create pivot table: index=(geog_id, propertyType), columns=year-month, values=sold_count
    self.sold_counts_ts_df = sold_counts.pivot_table(
      index=['geog_id', 'propertyType'],
      columns='year-month',
      values='sold_count',
      fill_value=0
    )
    
    self.logger.info(f"Created sold counts time series: {self.sold_counts_ts_df.shape[0]} (geog_id, propertyType) combinations")
    self.logger.info(f"Time series columns: {list(self.sold_counts_ts_df.columns)}")


  def _prepare_current_counts_time_series(self):
    """Convert cached current counts to time series format matching sold counts"""
    if self.current_counts_ts_df is None or len(self.current_counts_ts_df) == 0:
      # raise ValueError("current_counts_ts_df is None or empty")
      self.logger.info(f"No current counts time series found for {self.prov_code}")
      self.current_counts_ts_pivot = pd.DataFrame()  # Create empty DataFrame
      return
    
    # Convert year-month datetime to YYYY-MM string format to match sold data
    year_month_str = self.current_counts_ts_df['year-month'].dt.strftime('%Y-%m')
    
    # # Create pivot table: index=(geog_id, propertyType), columns=year-month strings, values=current_count
    # self.current_counts_ts_pivot = self.current_counts_ts_df.pivot_table(
    #   index=['geog_id', 'propertyType'],
    #   columns=year_month_str,  # Use the series directly, not a new column
    #   values='current_count',
    #   fill_value=0
    # )

    # Create 'ALL' property type by grouping without propertyType
    current_counts_all = self.current_counts_ts_df.groupby(['geog_id', year_month_str])['current_count'].sum().reset_index()
    current_counts_all['propertyType'] = 'ALL'

    # Combine original data with 'ALL' aggregation
    current_counts_combined = pd.concat([
      self.current_counts_ts_df[['geog_id', 'propertyType', 'current_count']].assign(year_month=year_month_str),
      current_counts_all.rename(columns={year_month_str.name: 'year_month'})
    ], ignore_index=True)

    # Create pivot table from combined data
    self.current_counts_ts_pivot = current_counts_combined.pivot_table(
      index=['geog_id', 'propertyType'],
      columns='year_month',
      values='current_count',
      fill_value=0
    )
    
    self.logger.info(f"Prepared current counts time series: {self.current_counts_ts_pivot.shape[0]} (geog_id, propertyType) combinations")
    self.logger.info(f"Time series columns: {list(self.current_counts_ts_pivot.columns)}")



  def _calculate_absorption_rates_time_series(self):
    """Calculate absorption rates for time intersection of sold data and snapshots"""
    if self.sold_counts_ts_df.empty:
      self.logger.warning("No sold counts data - cannot calculate absorption rates")
      self.absorption_rates_ts_df = pd.DataFrame()
      self.months_of_inventory_ts_df = pd.DataFrame()
      return
    
    if self.current_counts_ts_pivot.empty:
      self.logger.warning("No current counts time series - cannot calculate absorption rates")
      self.absorption_rates_ts_df = pd.DataFrame()
      self.months_of_inventory_ts_df = pd.DataFrame()
      return
    
    # Find intersection: should be much closer now since we pre-filtered sold data
    common_index = self.sold_counts_ts_df.index.intersection(self.current_counts_ts_pivot.index)
    common_columns = self.sold_counts_ts_df.columns.intersection(self.current_counts_ts_pivot.columns)
    
    if len(common_index) == 0:
      self.logger.error("No common geog_id/propertyType between sold and current counts")
      raise ValueError("Cannot align sold and current counts time series")
    
    if len(common_columns) == 0:
      self.logger.warning("No common months between sold data and current count snapshots")
      self.absorption_rates_ts_df = pd.DataFrame()
      self.months_of_inventory_ts_df = pd.DataFrame()
      return
    
    # Should have much better alignment now
    sold_aligned = self.sold_counts_ts_df.loc[common_index, common_columns]
    current_aligned = self.current_counts_ts_pivot.loc[common_index, common_columns]
    
    # Calculate absorption rates (element-wise division)
    self.absorption_rates_ts_df = sold_aligned / current_aligned

    # Calculate months of inventory (inverse of absorption rate)
    self.months_of_inventory_ts_df = current_aligned / sold_aligned
    
    # Handle division by zero and infinity
    self.absorption_rates_ts_df = self.absorption_rates_ts_df.replace([np.inf, -np.inf], np.nan)
    self.months_of_inventory_ts_df = self.months_of_inventory_ts_df.replace([np.inf, -np.inf], np.nan)
    
    self.logger.info(f"Calculated absorption rates time series: {self.absorption_rates_ts_df.shape}")
    self.logger.info(f"Calculated months of inventory time series: {self.months_of_inventory_ts_df.shape}")
    self.logger.info(f"Months with absorption rates: {list(self.absorption_rates_ts_df.columns)}")    
    
    # Should have fewer or no missing months now
    missing_snapshots = self.sold_counts_ts_df.columns.difference(common_columns)
    if len(missing_snapshots) > 0:
      self.logger.warning(f"Months with sold data but no snapshots (unexpected): {list(missing_snapshots)}")


  # delta diff helpers
  def _detect_changes(self, old_rates_df, new_rates_df):
    """
    Compare old vs new absorption rates and identify which geog_id/propertyType combinations changed.
    
    Args:
      old_rates_df: Previous absorption rates DataFrame (MultiIndex) or None
      new_rates_df: New absorption rates DataFrame (MultiIndex)
      
    Returns:
      DataFrame with columns [geog_id, propertyType] for combinations that changed
    """
    if old_rates_df is None:
      self.logger.info("No previous absorption rates found - marking all as new")
      return self._mark_all_combinations_as_changed(new_rates_df)
    
    if new_rates_df is None or new_rates_df.empty:
      self.logger.warning("No new absorption rates calculated")
      return pd.DataFrame(columns=['geog_id', 'propertyType'])
    
    # Find all unique combinations that need to be checked
    all_combinations = old_rates_df.index.union(new_rates_df.index)
    changed_combinations = []
    
    for geog_id, propertyType in all_combinations:
      has_old = (geog_id, propertyType) in old_rates_df.index
      has_new = (geog_id, propertyType) in new_rates_df.index
      
      if not has_old and has_new:
        # New combination - definitely changed
        changed_combinations.append({'geog_id': geog_id, 'propertyType': propertyType})
      elif has_old and not has_new:
        # Combination removed - definitely changed  
        changed_combinations.append({'geog_id': geog_id, 'propertyType': propertyType})
      elif has_old and has_new:
        # Both exist - compare the entire time series row
        old_row = old_rates_df.loc[(geog_id, propertyType)]
        new_row = new_rates_df.loc[(geog_id, propertyType)]
        
        # Get all columns (months) from both rows
        all_columns = old_row.index.union(new_row.index)
        
        # Check if ANY month value changed
        row_changed = False
        for month in all_columns:
          old_value = old_row.get(month, np.nan)
          new_value = new_row.get(month, np.nan)
          
          # Compare values accounting for NaN
          if pd.isna(old_value) and pd.isna(new_value):
            continue  # Both NaN - no change
          elif pd.isna(old_value) or pd.isna(new_value):
            row_changed = True  # One is NaN, other isn't - changed
            break
          elif not np.isclose(old_value, new_value, equal_nan=False):
            row_changed = True  # Values differ - changed
            break
        
        if row_changed:
          changed_combinations.append({'geog_id': geog_id, 'propertyType': propertyType})
    
    changes_df = pd.DataFrame(changed_combinations)
    self.logger.info(f"Detected {len(changes_df)} geog_id/propertyType combinations with changes")
    
    return changes_df

  def _mark_all_combinations_as_changed(self, new_rates_df):
    """Mark all geog_id/propertyType combinations in new_rates_df as changed"""
    changes_list = []
    
    for geog_id, propertyType in new_rates_df.index:
      changes_list.append({
        'geog_id': geog_id,
        'propertyType': propertyType
      })
    
    return pd.DataFrame(changes_list)


# for dev
if __name__ == '__main__':
  job_id = 'absorption_rate_xxx'
  uat_datastore = Datastore(host='localhost', port=9201)
  prod_datastore = Datastore(host='localhost', port=9202)

  processor = AbsorptionRateProcessor(
    job_id=job_id,
    datastore=uat_datastore
  )
  processor.run()

  # datastore.search(index=datastore.mkt_trends_index_name, _id='g30_dpz89rm7_DETACHED')[0]


""" Example absorption_rates_ts_df:
+-------------+---------------+----------+----------+----------+----------+----------+----------+----------+----------+
|             |               | 2024-08  | 2024-09  | 2024-10  | 2024-11  | 2024-12  | 2025-01  | 2025-02  | 2025-04  |
| geog_id     | propertyType  |          |          |          |          |          |          |          |          |
+-------------+---------------+----------+----------+----------+----------+----------+----------+----------+----------+
| g10_dpsbjvx6| ALL           | 0.000000 | 0.005952 | 0.004608 | 0.004049 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
|             | DETACHED      | 0.000000 | 0.008621 | 0.000000 | 0.006098 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
|             | SEMI-DETACHED | 0.000000 | 0.000000 | 0.076923 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| g10_dpsbmv3x| ALL           | 0.000000 | 0.000000 | 0.066667 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
|             | DETACHED      | 0.000000 | 0.000000 | 0.125000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| ...         | ...           | ...      | ...      | ...      | ...      | ...      | ...      | ...      | ...      |
| g40_f24gqr8n| TOWNHOUSE     | NaN      | 0.500000 | 0.333333 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | NaN      |
| g40_f8hydcw8| ALL           | NaN      | NaN      | NaN      | 24.000000| 0.000000 | 0.000000 | 0.000000 | 0.000000 |
|             | DETACHED      | NaN      | NaN      | NaN      | 24.000000| 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| g40_fb6qq622| ALL           | 0.000000 | 0.000000 | 0.500000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
|             | DETACHED      | 0.000000 | 0.000000 | 0.500000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
+-------------+---------------+----------+----------+----------+----------+----------+----------+----------+----------+
"""

""" Example current_counts_ts_df:
+--------+------------+-------------+---------------+---------------+
|        | year-month | geog_id     | propertyType  | current_count |
+--------+------------+-------------+---------------+---------------+
| 0      | 2024-08-31 | g10_dhx588zc| CONDO         | 2             |
| 1      | 2024-08-31 | g10_dhx58ktq| CONDO         | 1             |
| 2      | 2024-08-31 | g10_dpmr7tfy| DETACHED      | 13            |
| 3      | 2024-08-31 | g10_dpmr7tfy| SEMI-DETACHED | 2             |
| 4      | 2024-08-31 | g10_dpsbjvx6| CONDO         | 25            |
| ...    | ...        | ...         | ...           | ...           |
| 52993  | 2025-07-31 | g40_f24gqr8n| CONDO         | 7             |
| 52994  | 2025-07-31 | g40_f24gqr8n| DETACHED      | 8             |
| 52995  | 2025-07-31 | g40_f24gqr8n| SEMI-DETACHED | 3             |
| 52996  | 2025-07-31 | g40_f24gqr8n| TOWNHOUSE     | 2             |
| 52997  | 2025-07-31 | g40_fb6qq622| DETACHED      | 1             |
+--------+------------+-------------+---------------+---------------+
"""

""" Example current_counts_ts_pivot:
+-------------+---------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
|             |               | year_month |            |            |            |            |            |            |            |            |            |            |            |
| geog_id     | propertyType  | 2024-08    | 2024-09    | 2024-10    | 2024-11    | 2024-12    | 2025-01    | 2025-02    | 2025-03    | 2025-04    | 2025-05    | 2025-06    | 2025-07    |
+-------------+---------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
| g10_9tbqcm3w| ALL           | 0          | 0          | 0          | 0          | 0          | 0          | 1          | 1          | 1          | 0          | 0          | 0          |
|             | DETACHED      | 0          | 0          | 0          | 0          | 0          | 0          | 1          | 1          | 1          | 0          | 0          | 0          |
| g10_9tbr15q7| ALL           | 0          | 0          | 0          | 0          | 0          | 0          | 1          | 1          | 1          | 0          | 0          | 0          |
|             | DETACHED      | 0          | 0          | 0          | 0          | 0          | 0          | 1          | 1          | 1          | 0          | 0          | 0          |
| g10_c3nfk0vf| ALL           | 0          | 1          | 1          | 1          | 1          | 0          | 0          | 0          | 0          | 0          | 0          | 0          |
| ...         | ...           | ...        | ...        | ...        | ...        | ...        | ...        | ...        | ...        | ...        | ...        | ...        | ...        |
| g60_drdhpv86| ALL           | 2          | 2          | 2          | 2          | 2          | 0          | 0          | 0          | 0          | 0          | 0          | 0          |
|             | DETACHED      | 2          | 2          | 2          | 2          | 2          | 0          | 0          | 0          | 0          | 0          | 0          | 0          |
| g60_dru6dx8s| ALL           | 2          | 2          | 2          | 2          | 2          | 1          | 1          | 1          | 0          | 0          | 0          | 0          |
|             | DETACHED      | 2          | 2          | 1          | 2          | 2          | 1          | 0          | 0          | 0          | 0          | 0          | 0          |
|             | TOWNHOUSE     | 0          | 0          | 1          | 0          | 0          | 0          | 1          | 1          | 0          | 0          | 0          | 0          |
+-------------+---------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
"""

""" Example sold_counts_ts_df:
+-------------+---------------+------------+----------+----------+----------+----------+----------+----------+----------+----------+
|             |               | year_month | 2024-08  | 2024-09  | 2024-10  | 2024-11  | 2024-12  | 2025-01  | 2025-02  | 2025-04  |
| geog_id     | propertyType  |            |          |          |          |          |          |          |          |          |
+-------------+---------------+------------+----------+----------+----------+----------+----------+----------+----------+----------+
| g10_dpsbjvx6| ALL           |            | 0        | 1        | 1        | 1        | 0        | 0        | 0        | 0        |
|             | DETACHED      |            | 0        | 1        | 0        | 1        | 0        | 0        | 0        | 0        |
|             | SEMI-DETACHED |            | 0        | 0        | 1        | 0        | 0        | 0        | 0        | 0        |
| g10_dpsbmv3x| ALL           |            | 0        | 0        | 1        | 0        | 0        | 0        | 0        | 0        |
|             | DETACHED      |            | 0        | 0        | 1        | 0        | 0        | 0        | 0        | 0        |
| ...         | ...           |            | ...      | ...      | ...      | ...      | ...      | ...      | ...      | ...      |
| g40_f8jjv80t| ALL           |            | 55       | 54       | 53       | 47       | 0        | 0        | 0        | 0        |
|             | DETACHED      |            | 53       | 53       | 50       | 46       | 0        | 0        | 0        | 0        |
|             | SEMI-DETACHED |            | 2        | 1        | 3        | 1        | 0        | 0        | 0        | 0        |
| g40_fb6qq622| ALL           |            | 0        | 0        | 1        | 0        | 0        | 0        | 0        | 0        |
|             | DETACHED      |            | 0        | 0        | 1        | 0        | 0        | 0        | 0        | 0        |
+-------------+---------------+------------+----------+----------+----------+----------+----------+----------+----------+----------+
"""



