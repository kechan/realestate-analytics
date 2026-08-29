from typing import Callable
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

from pathlib import Path

from ..etl.base_etl import BaseETLProcessor
from ..data.caching import FileBasedCache
from ..data.es import Datastore
from ..data.bq import BigQueryDatastore
from ..data.archive import Archiver
from ..utils.validators import ProvinceGeogIdValidator
from .run_utils import send_etl_error_alert
from elasticsearch.helpers import scan, bulk
from elasticsearch.exceptions import NotFoundError, ConnectionError, RequestError, TransportError

import logging, sys, pytz


class LastMthMetricsProcessor(BaseETLProcessor):
  def __init__(self, job_id: str, datastore: Datastore, 
               bq_datastore: BigQueryDatastore = None,
               prov_code: str = 'ON'
               ):
    super().__init__(job_id=job_id, datastore=datastore, bq_datastore=bq_datastore, prov_code=prov_code)

    self.geog_id_validator = ProvinceGeogIdValidator(
      archive_dir=self.archiver.archive_dir,
      prov_code=self.prov_code
    )
    
    self.listing_df = None
    self.delta_listing_df = None
    self.last_mth_metrics_results = None

    self.listing_selects = ['listingType', 'searchCategoryType', 
                'guid',
                'beds', 'bedsInt', 
                'baths', 'bathsInt',
                'lat', 'lng',
                'price',
                'addedOn',
                'lastUpdate'
                ]

    self.LOAD_SUCCESS_THRESHOLD = 0.5
    self.ENABLE_TRACKING_INDEX = False  # if false, skip updating tracking index

    # self.es_store_script_name  = "update_last_month_metrics"
    # self.ensure_stored_script_exists()

  def setup_extra_stages(self,
                         add_extra_stage: Callable[[str], None],
                         add_extra_cleanup_stage: Callable[[str], None]) -> None:
    add_extra_stage('end_of_mth_run')

  def extract(self, from_cache=False):
    try:
      if from_cache:
        self._load_from_cache()
      else:
        self._extract_from_datastore()
        self._mark_success('extract')

    except (ConnectionError, RequestError, TransportError) as e:
      self.logger.error(f"Elasticsearch error during extract: {e}", exc_info=True)
      raise
    except Exception as e:
      self.logger.error(f"Unexpected error during extract: {e}", exc_info=True)
      raise


  def transform(self):
    if self._was_success('transform'):
      self.logger.info("Transform stage already completed. Loading checkpoints from cache.")
      self.delta_listing_df = self.cache.get(f'{self.cache_prefix}{self.job_id}_delta_{self.prov_code.lower()}_current_listing')
      return

    # there's no transform needed
    if self.delta_listing_df is None:
      self.logger.info("No transform needed. Just loading checkpoints from cache.")
      self.delta_listing_df = self.cache.get(f'{self.cache_prefix}{self.job_id}_delta_{self.prov_code.lower()}_current_listing')
    
    self._mark_success('transform')

  
  def load(self):
    if self._was_success('load'):
      self.logger.info("Load already successful.")
      return 0, []
      
    try:
      success, failed = self.update_es_tracking_index()
      total_attempts = success + len(failed)

      if total_attempts != 0 and success / total_attempts < self.LOAD_SUCCESS_THRESHOLD:
        self.logger.error(f"Less than 50% success rate. Only {success} out of {total_attempts} documents updated.")
        self.datastore.summarize_update_failures(failed)
      else:
        self._mark_success('load')

      return success, failed
    
    except (ConnectionError, RequestError, TransportError) as e:
      self.logger.error(f"Elasticsearch error during load: {e}", exc_info=True)
      raise

    except Exception as e:
      self.logger.error(f"Unexpected error during load: {e}", exc_info=True)
      raise


  def _extract_from_datastore(self):
    """
    Time-blind full pull: fetch every current listing for the province (no addedOn/updated
    bounds), merge onto whatever's already cached, and dedupe. Run every day regardless of
    month-end. A missing/empty cache (first-ever run, or right after reset_listing_cache())
    is just a fresh start, not a special case - concat onto an empty frame.
    """
    if self._was_success('extract'):
      self.logger.info("Extract stage already completed. Loading from cache.")
      self._load_from_cache()
      return

    end_time = self.get_current_datetime()

    success, self.delta_listing_df = self.datastore.get_listings(
      selects=self.listing_selects,
      prov_code=self.prov_code
    )

    if not success:
      self.logger.error(f"Failed to load listings for {self.prov_code}")
      raise ValueError("Failed to load listings from datastore")

    if len(self.delta_listing_df) == 0:
      self.logger.warning(f"No listings found for {self.prov_code}")
      self.delta_listing_df = pd.DataFrame(columns=self.listing_selects + ['_id'])
      self.delta_listing_df.reset_index(drop=True, inplace=True)
    else:
      self.logger.info(f'Fetched {len(self.delta_listing_df)} listings for {self.prov_code}')

    existing_listing_df = self.cache.get(f'{self.cache_prefix}{self.prov_code.lower()}_current_listing')
    if existing_listing_df is None:
      # no cache (first-ever run, or the month just rolled over via reset_listing_cache())
      existing_listing_df = pd.DataFrame(columns=self.listing_selects + ['_id'])

    # existing rows first, fresh pull appended after - keep='last' then always keeps the
    # fresh pull's row for any _id present in both, without needing to sort by lastUpdate
    # (which es.py's own schema notes flag as oddly formatted and unreliable to sort on).
    self.listing_df = pd.concat([existing_listing_df, self.delta_listing_df], ignore_index=True)
    self.listing_df = self.listing_df.drop_duplicates('_id', keep='last').reset_index(drop=True)

    self._save_to_cache()
    self.cache.set(key=self.last_run_key, value=end_time)

    # checkpoint today's fetch, such that this can be picked up on rerun
    self.cache.set(key=f'{self.cache_prefix}{self.job_id}_delta_{self.prov_code.lower()}_current_listing', value=self.delta_listing_df)


  def end_of_mth_run(self):
    """
    Runs only on the 1st of the month - no self-heal, no retry window, no cross-day markers.
    Compute last month's metrics, archive them (the manual-recovery artifact if the push below
    fails), then push to ES - if any step fails, log + email + re-raise so the existing
    halt-gate (is_last_run_successful()/last_mth_metrics_run.csv)
    applies, same as any other ETL failure in this codebase. Devops reruns the job manually;
    safe to do since a rerun just redoes the same steps against the current cache, with no
    state to reconcile.
    """
    self.pre_end_of_mth_run()
    try:
      current_date = self.get_current_datetime()
      if current_date.day != 1:
        self.logger.info(f"Not the 1st of the month for {self.prov_code} - skipping end of month run.")
        self._mark_success('end_of_mth_run')   # nothing to do today
        return

      expected_yearmonth = (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')

      try:
        self.compute_last_month_metrics()
        self._archive_results()   # before the push - this is the manual-recovery artifact if the push below fails

        success, failed = self.update_mkt_trends()
        total_attempts = success + len(failed)
        if total_attempts != 0 and success / total_attempts < 0.5:
          raise ValueError(f"Failed to update market trends index: only {success}/{total_attempts} documents succeeded.")
      except Exception as e:
        self.logger.error(f"End of month run failed for {self.prov_code}/{expected_yearmonth}: {e}", exc_info=True)
        send_etl_error_alert(self.job_id, f"LastMthMetricsProcessor/{self.prov_code} end_of_mth_run/{expected_yearmonth}", str(e))
        raise   # propagate -- halts subsequent runs via the existing halt-gate, same as any other failure

      self._mark_success('end_of_mth_run')

      # Reset is best-effort and must never affect today's ETL status - even though it has
      # no ES calls, cache.delete() is still file I/O and could in principle raise. Swallow
      # (don't re-raise): an uncaught exception here would propagate out of run()'s stage loop
      # and skip its own _mark_success('all') check entirely, incorrectly halting tomorrow's
      # run despite the metrics having genuinely succeeded.
      try:
        self.reset_listing_cache(expected_yearmonth)
      except Exception as e:
        self.logger.error(f"Cache reset failed unexpectedly for {self.prov_code}/{expected_yearmonth} "
                           f"(metrics already succeeded - this does not affect today's ETL status): {e}", exc_info=True)

    finally:
      self.post_end_of_mth_run()

      
  def update_es_tracking_index(self):

    if self.delta_listing_df is None:
      self.delta_listing_df = self.cache.get(f'{self.cache_prefix}{self.job_id}_delta_{self.prov_code.lower()}_current_listing')
      if self.delta_listing_df is not None:
        self.logger.info(f"Loaded {len(self.delta_listing_df)} delta listings from cache.")

    if not self.ENABLE_TRACKING_INDEX:
      self.logger.info("Tracking index is disabled. Skipping update.")
      return 0, []

    if self.delta_listing_df is not None:
      to_be_updated_listing_df = self.delta_listing_df
    else:
      self.logger.info("No delta listings found in either cache or prior computation. Updating all listings.")
      to_be_updated_listing_df = self.listing_df

    def generate_actions():
      for _, row in to_be_updated_listing_df.iterrows():
        yield {
            "_op_type": "update",
            "_index": self.datastore.listing_tracking_index_name,
            "_id": row['_id'],
            "doc": {
                "price": float(row['price']),
                "addedOn": row['addedOn'].isoformat(),
                "lastUpdate": row['lastUpdate'].isoformat(),
                "guid": row['guid'],
                "propertyType": row['propertyType']
            },
            "doc_as_upsert": True
        }

     # Perform bulk update    
    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)

    self.logger.info(f"Successfully updated {success} documents")
    if failed:
      self.logger.error(f"Failed to update {len(failed)} documents")

    return success, failed

  
  def compute_last_month_metrics(self):
    """
    This is to be run at the 1st min of a new month, so the last month metrics can be calculated.
    It calculates the median price and new listings count for each geog_id-propertyType pair.
    There's also entry of propertType 'ALL' which is the sum of all property types within the geog_id.
    """
    # Determine the last month
    today = self.get_current_datetime()
    last_month = (today.replace(day=1) - pd.Timedelta(days=1)).strftime('%Y-%m')
    self.logger.info(f'Calculating metrics for {last_month}')

    # listing_df is already deduped by _extract_from_datastore() on every daily merge.

    # Convert 'addedOn' to datetime if it's not already
    self.listing_df.addedOn = pd.to_datetime(self.listing_df.addedOn)

    # Expand guid column
    expanded_df = self.listing_df.assign(geog_id=self.listing_df['guid'].str.split(',')).explode('geog_id')

    # Filter out cross-province geog_ids to prevent contamination
    if not self.geog_id_validator.bypass_mode:
      before_count = len(expanded_df)
      expanded_df = expanded_df[expanded_df['geog_id'].isin(self.geog_id_validator.valid_geog_ids)].reset_index(drop=True)
      after_count = len(expanded_df)

      contaminated_count = before_count - after_count
      if contaminated_count > 0:
        contamination_pct = 100 * contaminated_count / before_count
        self.logger.warning(
            f"[CONTAMINATION] Filtered {contaminated_count} cross-province geog_ids "
            f"({contamination_pct:.2f}%) for province {self.prov_code}"
        )
      else:
        self.logger.info(f"No cross-province contamination detected for {self.prov_code}")
    else:
      self.logger.warning(
          f"[VALIDATION BYPASS] Skipping geog_id validation for {self.prov_code} - "
          f"validator is in bypass mode. Contamination checks are NOT active."
      )


    # Function to calculate metrics for both specific property types and 'ALL'
    def calculate_metrics(group):
        prices = group['price']
        positive_prices = prices[prices > 0]   # log() is undefined for non-positive prices

        if positive_prices.empty:
          # No valid positive prices at all (shouldn't happen in practice) - fall back to raw.
          min_price = prices.min()
          max_price = prices.max()
        else:
          # 2-standard-deviation bandpass filter, applied in log-space: asking prices are
          # right-skewed/log-normal-ish, not normally distributed, so a raw-dollar mean +/- 2*std
          # band is unusable (a single extreme listing inflates the std enough that the lower
          # bound goes negative, meaning it can never exclude anything on the low end). Taking
          # log(price) first turns "how many times more expensive" into a fixed distance, so the
          # band is naturally always positive once exponentiated back, and a single outlier no
          # longer dominates the variance calculation the way it does in raw-dollar space.
          log_prices = np.log(positive_prices)
          log_mean = log_prices.mean()
          log_std = log_prices.std()

          if pd.isna(log_std) or log_std == 0:
            filtered_prices = positive_prices
          else:
            lower_bound = np.exp(log_mean - 2 * log_std)
            upper_bound = np.exp(log_mean + 2 * log_std)
            filtered_prices = positive_prices[(positive_prices >= lower_bound) & (positive_prices <= upper_bound)]
            if filtered_prices.empty:
              filtered_prices = positive_prices

          min_price = filtered_prices.min()
          max_price = filtered_prices.max()

        return pd.Series({
            'median_price': prices.median(),
            'min_price': min_price,
            'max_price': max_price,
            'new_listings_count': (group['addedOn'].dt.to_period('M').astype(str) == last_month).sum()
        })

    if expanded_df.empty:
      # Create empty results with expected columns
      results = pd.DataFrame(columns=['geog_id', 'propertyType', 'median_price', 'min_price', 'max_price', 'new_listings_count'])
      all_property_types = pd.DataFrame(columns=['geog_id', 'propertyType', 'median_price', 'min_price', 'max_price', 'new_listings_count'])
    else:
      # Calculate metrics for specific property types
      results = expanded_df.groupby(['geog_id', 'propertyType']).apply(calculate_metrics).reset_index()

      # Calculate metrics for 'ALL' property type
      all_property_types = expanded_df.groupby('geog_id').apply(calculate_metrics).reset_index()
      all_property_types['propertyType'] = 'ALL'

    # Combine results
    final_results = pd.concat([results, all_property_types], ignore_index=True)

    # Reorder columns
    final_results = final_results[['geog_id', 'propertyType', 'median_price', 'min_price', 'max_price', 'new_listings_count']]

    final_results.new_listings_count = final_results.new_listings_count.fillna(0).astype(int)

    self.logger.info(f'Calculated metrics for {len(final_results)} (geog_id, propertyType) pairs, including "ALL" property type')

    self.last_mth_metrics_results = final_results
  

  def reset_listing_cache(self, expected_yearmonth: str):
    """
    Runs once, immediately after last month's metrics are computed, archived (the small,
    aggregated last_mth_metrics_results - see _archive_results()), and pushed to ES. Deletes
    the raw listing_df cache key outright - no archiving of listing_df itself. At
    tens/hundreds of thousands of rows per province, archiving the raw listing cache every
    month indefinitely is a real disk-space/maintenance hazard for a dataframe nothing
    downstream ever reads back - the only value it had was "just in case" per-listing
    forensics, deliberately traded away here in favour of a single delete with no failure mode
    to reason about. The very next daily _extract_from_datastore() call sees a missing cache
    and treats it exactly like the original first-ever-run case (fresh start, no concat),
    naturally repopulating via the regular time-blind full pull.
    """
    self.cache.delete(f'{self.cache_prefix}{self.prov_code.lower()}_current_listing')
    self.listing_df = None

    self.logger.info(f"Reset listing cache for {self.prov_code}/{expected_yearmonth}")


  def update_mkt_trends(self):
    """
    Update the market trends index with last month's median asking price and new listings count.
    """
    
    last_month = (self.get_current_datetime().replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    # Modified last_month to simulate a diff month
    # last_month = (datetime.now().replace(day=1) + timedelta(days=1)).strftime('%Y-%m')

    self.logger.info(f"Updating last month's ({last_month}) median asking price on ES")
    success_price, failed_price = self.update_last_mth_median_asking_price(last_month)

    self.logger.info(f"Updating last month's ({last_month}) min asking price on ES")
    success_min, failed_min = self.update_last_mth_min_asking_price(last_month)

    self.logger.info(f"Updating last month's ({last_month}) max asking price on ES")
    success_max, failed_max = self.update_last_mth_max_asking_price(last_month)

    self.logger.info(f"Updating last month's ({last_month}) new listings count on ES")
    success_listings, failed_listings = self.update_last_mth_new_listings(last_month)

    total_success = success_price + success_min + success_max + success_listings
    total_failed = len(failed_price) + len(failed_min) + len(failed_max) + len(failed_listings)

    # Perform bulk update with error handling
    self.logger.info(f"Successfully updated {total_success} documents in market trends index")
    if total_failed > 0:
      self.logger.error(f"Failed to update {total_failed} documents in market trends index")

    return total_success, failed_price + failed_min + failed_max + failed_listings
  

  def update_last_mth_median_asking_price(self, last_month):
    def generate_actions():
      for _, row in self.last_mth_metrics_results.iterrows():
        composite_id = f"{row['geog_id']}_{row['propertyType']}"
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            if (ctx._source.metrics.last_mth_median_asking_price == null) {
              ctx._source.metrics.last_mth_median_asking_price = [];
            }
            // Check if an entry for this month already exists
            int existingIndex = -1;
            for (int i = 0; i < ctx._source.metrics.last_mth_median_asking_price.size(); i++) {
              if (ctx._source.metrics.last_mth_median_asking_price[i].month == params.new_metric.month) {
                existingIndex = i;
                break;
              }
            }
            if (existingIndex >= 0) {
              // Replace existing entry
              ctx._source.metrics.last_mth_median_asking_price[existingIndex] = params.new_metric;
            } else {
              // Append new entry
              ctx._source.metrics.last_mth_median_asking_price.add(params.new_metric);
            }
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_metric": {
                "month": last_month,
                "value": float(row['median_price'])
              },
              "geog_id": row['geog_id'],
              "propertyType": row['propertyType'],
              "geo_level": int(row['geog_id'].split('_')[0][1:]),
              "last_updated": self.get_current_datetime().isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": row['propertyType'],
            "geo_level": int(row['geog_id'].split('_')[0][1:]),
            "metrics": {
              "last_mth_median_asking_price": [{
                "month": last_month,
                "value": float(row['median_price'])
              }]
            },
            "last_updated": self.get_current_datetime().isoformat()
          }
        }
  
    return bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)


  def update_last_mth_min_asking_price(self, last_month):
    def generate_actions():
      for _, row in self.last_mth_metrics_results.iterrows():
        composite_id = f"{row['geog_id']}_{row['propertyType']}"
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            if (ctx._source.metrics.last_mth_min_asking_price == null) {
              ctx._source.metrics.last_mth_min_asking_price = [];
            }
            // Check if an entry for this month already exists
            int existingIndex = -1;
            for (int i = 0; i < ctx._source.metrics.last_mth_min_asking_price.size(); i++) {
              if (ctx._source.metrics.last_mth_min_asking_price[i].month == params.new_metric.month) {
                existingIndex = i;
                break;
              }
            }
            if (existingIndex >= 0) {
              // Replace existing entry
              ctx._source.metrics.last_mth_min_asking_price[existingIndex] = params.new_metric;
            } else {
              // Append new entry
              ctx._source.metrics.last_mth_min_asking_price.add(params.new_metric);
            }
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_metric": {
                "month": last_month,
                "value": float(row['min_price'])
              },
              "geog_id": row['geog_id'],
              "propertyType": row['propertyType'],
              "geo_level": int(row['geog_id'].split('_')[0][1:]),
              "last_updated": self.get_current_datetime().isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": row['propertyType'],
            "geo_level": int(row['geog_id'].split('_')[0][1:]),
            "metrics": {
              "last_mth_min_asking_price": [{
                "month": last_month,
                "value": float(row['min_price'])
              }]
            },
            "last_updated": self.get_current_datetime().isoformat()
          }
        }

    return bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)


  def update_last_mth_max_asking_price(self, last_month):
    def generate_actions():
      for _, row in self.last_mth_metrics_results.iterrows():
        composite_id = f"{row['geog_id']}_{row['propertyType']}"
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            if (ctx._source.metrics.last_mth_max_asking_price == null) {
              ctx._source.metrics.last_mth_max_asking_price = [];
            }
            // Check if an entry for this month already exists
            int existingIndex = -1;
            for (int i = 0; i < ctx._source.metrics.last_mth_max_asking_price.size(); i++) {
              if (ctx._source.metrics.last_mth_max_asking_price[i].month == params.new_metric.month) {
                existingIndex = i;
                break;
              }
            }
            if (existingIndex >= 0) {
              // Replace existing entry
              ctx._source.metrics.last_mth_max_asking_price[existingIndex] = params.new_metric;
            } else {
              // Append new entry
              ctx._source.metrics.last_mth_max_asking_price.add(params.new_metric);
            }
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_metric": {
                "month": last_month,
                "value": float(row['max_price'])
              },
              "geog_id": row['geog_id'],
              "propertyType": row['propertyType'],
              "geo_level": int(row['geog_id'].split('_')[0][1:]),
              "last_updated": self.get_current_datetime().isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": row['propertyType'],
            "geo_level": int(row['geog_id'].split('_')[0][1:]),
            "metrics": {
              "last_mth_max_asking_price": [{
                "month": last_month,
                "value": float(row['max_price'])
              }]
            },
            "last_updated": self.get_current_datetime().isoformat()
          }
        }

    return bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)


  def update_last_mth_new_listings(self, last_month):
    def generate_actions():
      for _, row in self.last_mth_metrics_results.iterrows():
        composite_id = f"{row['geog_id']}_{row['propertyType']}"
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            if (ctx._source.metrics.last_mth_new_listings == null) {
              ctx._source.metrics.last_mth_new_listings = [];
            }
            // Check if an entry for this month already exists
            int existingIndex = -1;
            for (int i = 0; i < ctx._source.metrics.last_mth_new_listings.size(); i++) {
              if (ctx._source.metrics.last_mth_new_listings[i].month == params.new_metric.month) {
                existingIndex = i;
                break;
              }
            }
            if (existingIndex >= 0) {
              // Replace existing entry
              ctx._source.metrics.last_mth_new_listings[existingIndex] = params.new_metric;
            } else {
              // Append new entry
              ctx._source.metrics.last_mth_new_listings.add(params.new_metric);
            }
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_metric": {
                "month": last_month,
                "value": int(row['new_listings_count'])
              },
              "geog_id": row['geog_id'],
              "propertyType": row['propertyType'],
              "geo_level": int(row['geog_id'].split('_')[0][1:]),
              "last_updated": self.get_current_datetime().isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": row['propertyType'],
            "geo_level": int(row['geog_id'].split('_')[0][1:]),
            "metrics": {
              "last_mth_new_listings": [{
                "month": last_month,
                "value": int(row['new_listings_count'])
              }]
            },
            "last_updated": self.get_current_datetime().isoformat()
          }
        }
  
    return bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)


  def remove_last_mth_metrics(self):
    """
    Remove 'last_mth_median_asking_price' and 'last_mth_new_listings' from all items
    in the mkt_trends_ts index on ES.
    """
    def generate_actions():
      query = {
        "query": {
          "match_all": {}
        }
      }
      for hit in scan(self.datastore.es, 
                      index=self.datastore.mkt_trends_index_name, 
                      query=query):
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": hit["_id"],
          "script": {
            "source": """
            if (ctx._source.metrics.containsKey('last_mth_median_asking_price')) {
              ctx._source.metrics.remove('last_mth_median_asking_price')
            }
            if (ctx._source.metrics.containsKey('last_mth_new_listings')) {
              ctx._source.metrics.remove('last_mth_new_listings')
            }
            """
          }
        }

    success, failed = bulk(self.datastore.es, generate_actions(), 
                           raise_on_error=False, 
                           raise_on_exception=False)
    
    self.logger.info(f"Successfully removed last_mth metrics from {success} documents")
    if failed:
      self.logger.error(f"Failed to remove last_mth metrics from {len(failed)} documents")
    
    return success, failed


  def _load_from_cache(self):
    super()._load_from_cache()

    # regard less of first_load, we will always load from the same cache
    self.listing_df = self.cache.get(f'{self.cache_prefix}{self.prov_code.lower()}_current_listing')

    if self.listing_df is None:
      # no cache yet (first-ever run, or right after reset_listing_cache()) -- fresh start
      self.listing_df = pd.DataFrame(columns=self.listing_selects + ['_id'])

    self.logger.info(f"Loaded {len(self.listing_df)} listings from cache.")


  def _save_to_cache(self):
    super()._save_to_cache()

    # reset index before saving to cache to avoid "serializing a non-default index"
    # before saving to cache
    self.listing_df.reset_index(drop=True, inplace=True)   

    self.cache.set(f'{self.cache_prefix}{self.prov_code.lower()}_current_listing', self.listing_df)
    self.logger.info(f"Saved {len(self.listing_df)} listings to cache.")


  def _archive_results(self):
    if hasattr(self, 'last_mth_metrics_results') and self.last_mth_metrics_results is not None:
      # Archive the last month's metrics results
      if not self.archiver.archive(self.last_mth_metrics_results, f'{self.prov_code.lower()}_last_mth_metrics_results'):
        self.logger.error("Failed to archive last month's metrics results")
        raise ValueError(f"Fatal error archiving {self.prov_code.lower()}_last_mth_metrics_results. This must be successful to proceed.")
      else:
        self.logger.info("Successfully archived last month's metrics results")
    else:
      self.logger.error(f"Fatal error: {self.prov_code.lower()}_last_mth_metrics_results not found or is None. Unable to archive.")
      raise ValueError(f"Fatal error: {self.prov_code.lower()}_last_mth_metrics_results not found or is None")


  def cleanup(self):
    super().cleanup()
    cache_key = f'{self.cache_prefix}{self.prov_code.lower()}_current_listing'
    self.cache.delete(cache_key)

    # reset instance variables
    self.listing_df = None
    self.delta_listing_df = None
    self.last_mth_metrics_results = None

    self.logger.info("Cleanup completed. All cached data has been cleared and instance variables reset.")


  def delete_checkpoints_data(self):
    self.cache.delete(f'{self.cache_prefix}{self.job_id}_delta_{self.prov_code.lower()}_current_listing')

  def pre_end_of_mth_run(self):
    if getattr(self, 'simulate_failure_at', None) == 'end_of_mth_run':
      raise Exception("Simulated failure in end_of_mth_run")
    
  def post_end_of_mth_run(self):
    pass

# ES Update solution using stored script (this may not be needed and appeared more complex)
  def ensure_stored_script_exists(self):
    try:
      # Check if the script already exists
      self.datastore.es.get_script(id=self.es_store_script_name)
      self.logger.info(f"Stored script '{self.es_store_script_name}' already exists")
    except NotFoundError:
      # Script doesn't exist, so create it
      self.create_stored_script()
    except Exception as e:
      self.logger.error(f"Error checking for stored script: {e}")

  def create_stored_script(self):
    script = {
      "script": {
        "lang": "painless",
        "source": """
          if (ctx._source.metrics == null) {
            ctx._source.metrics = new HashMap();
          }
          if (ctx._source.metrics[params.metric_name] == null) {
            ctx._source.metrics[params.metric_name] = [];
          }
          def metrics = ctx._source.metrics[params.metric_name];
          def existingIndex = metrics.indexOf(params.new_entry);
          if (existingIndex != -1) {
            metrics[existingIndex] = params.new_entry;
          } else {
            metrics.add(params.new_entry);
          }
          ctx._source.last_updated = params.last_updated;
        """
      }
    }
    
    self.datastore.es.put_script(id=self.es_store_script_name, body=script)
    self.logger.info(f"Stored script '{self.es_store_script_name}' created successfully")

  def delete_stored_script(self):
    try:
      self.datastore.es.delete_script(id=self.es_store_script_name)
      self.logger.info(f"Stored script '{self.es_store_script_name}' deleted successfully")
    except NotFoundError:
      self.logger.info(f"Stored script '{self.es_store_script_name}' not found, nothing to delete")
    except Exception as e:
      self.logger.error(f"Error deleting stored script '{self.es_store_script_name}': {e}")


# For dev
if __name__ == "__main__":
  job_id = 'last_mth_metrics_xxx'

  uat_datastore = Datastore(host='localhost', port=9201)
  prod_datastore = Datastore(host='localhost', port=9202)
  bq_datastore = BigQueryDatastore()


  processor = LastMthMetricsProcessor(
    job_id=job_id,
    datastore=prod_datastore,
    bq_datastore=bq_datastore
  )
  # during dev, we extract from PROD but update the UAT as a temporary workaround
  processor.simulate_failure_at = 'transform'  
  processor.run()
  
  # during dev, continue with the UAT datastore
  processor = LastMthMetricsProcessor(
    job_id=job_id,
    datastore=uat_datastore,
    bq_datastore=bq_datastore
  )
  processor.run()  

  # Repeat the above about once a day (to ensure we capture listings before they get deleted)
  # end of month won't run unless the day is on the 1st of the month

""" Sample last_mth_metrics_results dataframe
+--------------+----------------+---------------+-------------------+
| geog_id      | propertyType   | median_price  | new_listings_count|
+--------------+----------------+---------------+-------------------+
| g30_dpz89rm7 | SEMI-DETACHED  | 1150000.0     | 48.0              |
+--------------+----------------+---------------+-------------------+
"""

""" Sample json in rlp_mkt_trends_current
  {'geog_id': 'g30_dpz89rm7',
  'propertyType': 'SEMI-DETACHED',
  'geo_level': 30,
  'metrics': {'median_price': [{'month': '2023-01', 'value': 1035000.0},
    {'month': '2023-10', 'value': 1192000.0},
    {'month': '2023-11', 'value': 1037500.0},
    {'month': '2023-12', 'value': 976000.0},
    {'month': '2024-01', 'value': 1075000.0},
    {'month': '2024-02', 'value': 1201054.0},
    {'month': '2024-03', 'value': 1175333.0},
    {'month': '2024-04', 'value': 1159444.0},
    {'month': '2024-06', 'value': 915000.0}],
   'median_dom': [{'month': '2023-01', 'value': 3},
    {'month': '2023-10', 'value': 9},
    {'month': '2023-11', 'value': 15},
    {'month': '2023-12', 'value': 33},
    {'month': '2024-01', 'value': 9},
    {'month': '2024-02', 'value': 8},
    {'month': '2024-03', 'value': 8},
    {'month': '2024-04', 'value': 8},
    {'month': '2024-06', 'value': 49}],
   'last_mth_median_asking_price': {'month': '2024-06', 'value': 1150000.0},
   'last_mth_new_listings': {'month': '2024-06', 'value': 48}},
  'last_updated': '2024-07-24T21:33:28.443159'}

"""

''' 
    def generate_actions():
      for _, row in self.last_mth_metrics_results.iterrows():
        doc_id = f"{row['geog_id']}_{row['propertyType']}"
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": doc_id,
          "script": {
            "source": """
              if (ctx._source.metrics == null) {
                ctx._source.metrics = new HashMap();
              }
              
              // Update last_mth_median_asking_price
              ctx._source.metrics.last_mth_median_asking_price = params.median_price;
              
              // Update last_mth_new_listings
              ctx._source.metrics.last_mth_new_listings = params.new_listings_count;
              
              ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "median_price": {
                "month": last_month,
                "value": row['median_price']
              },
              "new_listings_count": {
                "month": last_month,
                "value": int(row['new_listings_count'])
              },
              "last_updated": self.get_current_datetime().isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": row['propertyType'],
            "geo_level": int(row['geog_id'].split('_')[0][1:]),
            "metrics": {
              "last_mth_median_asking_price": {
                "month": last_month,
                "value": row['median_price']
              },
              "last_mth_new_listings": {
                "month": last_month,
                "value": int(row['new_listings_count'])
              }
            },
            "last_updated": self.get_current_datetime().isoformat()
          }
        }
'''