from typing import Dict, List, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging, gc

from .base_etl import BaseETLProcessor
from ..data.caching import FileBasedCache
from ..data.es import Datastore
from ..utils.constants import VALID_PROVINCES
from elasticsearch.helpers import bulk, scan

class CurrentMthMetricsProcessor(BaseETLProcessor):
  def __init__(self, job_id: str, datastore: Datastore):
    super().__init__(job_id=job_id, datastore=datastore)

    self.logger.info("Last run is expectedly None since we don't do any extract from ES for this ETL job.")

    # We don't need bq_datastore since we're reusing cached data
    self.listing_df = None
    self.current_metrics_results = None
    
    self.LOAD_SUCCESS_THRESHOLD = 0.5

  def extract(self, from_cache=True):
    # Like AbsorptionRateProcessor, we always load from cache
    # since we're reusing LastMthMetricsProcessor's data
    self._load_from_cache()
    self._mark_success('extract')

  def _load_from_cache(self):
    """Load current listings from ALL LastMthMetricsProcessor province caches."""
    all_listing_dfs = []
    for prov in VALID_PROVINCES:
      cache_key = f'LastMthMetricsProcessor/{prov.lower()}_current_listing'
      listing_df = self.cache.get(cache_key)    

      if listing_df is None or listing_df.empty:
        self.logger.warning(f"Cache {cache_key} is missing or empty; skipping.")
        continue

      listing_df = listing_df.sort_values('lastUpdate').drop_duplicates('_id', keep='last')
      all_listing_dfs.append(listing_df)

    if not all_listing_dfs:
      self.logger.error(
            "No listing data found in any LastMthMetricsProcessor province caches. "
            "Ensure at least one LastMthMetricsProcessor has run successfully before this job."
        )
      raise ValueError("No listing data found in any LastMthMetricsProcessor province caches.")
        
    self.listing_df = pd.concat(all_listing_dfs, ignore_index=True)
    

    self.logger.info(f"Loaded {len(self.listing_df)} current listings from {len(all_listing_dfs)} province caches.")
    del all_listing_dfs; gc.collect();
    
    # Filter out soft-deleted listings if is_deleted column exists
    if 'is_deleted' in self.listing_df.columns:
      self.listing_df = self.listing_df[~self.listing_df['is_deleted']]
      self.listing_df.reset_index(drop=True, inplace=True)

    self.logger.info(f"Loaded {len(self.listing_df)} current listings from cache.")

  def transform(self):
    if self._was_success('transform'):
      self.logger.info("Transform already successful. Loading checkpoints from cache.")
      self.current_metrics_results = self.cache.get(f"{self.cache_prefix}{self.job_id}_transform_results")
      return

    try:
      self.compute_current_metrics()

      # Save the current metrics to "global" cache, this is to enable endpoints to query this.
      self.cache.set(f"{self.cache_prefix}current_mth_metrics_results", 
                    self.current_metrics_results)
      
      # Cache transform results for recovery, will be removed by delete_checkpoints_data()
      self.cache.set(f"{self.cache_prefix}{self.job_id}_transform_results", 
                    self.current_metrics_results)
      
      self._mark_success('transform')
    except Exception as e:
      self.logger.error(f"Error occurred during transform: {e}")
      raise

  def load(self):
    if self._was_success('load'):
      self.logger.info("Load already successful.")
      return 0, []

    success, failed = self.update_mkt_trends()
    total_attempts = success + len(failed)

    if total_attempts != 0 and success / total_attempts < self.LOAD_SUCCESS_THRESHOLD:
      self.logger.error(f"Less than 50% success rate. Only {success} out of {total_attempts} documents updated.")
      self.datastore.summarize_update_failures(failed)
    else:
      self._mark_success('load')

    return success, failed


  def compute_current_metrics(self):
    """
    Compute current snapshot metrics:
    - Current median asking price
    - Count of new listings in current month
    Grouped by geog_id and propertyType, ensuring all property types are represented
    
    OPTIMIZED VERSION: Memory-efficient approach using vectorized operations
    """
    
    # STEP 1: Get complete list of geog_ids from ES market trends index
    # Memory impact: ~10-50MB (same as original)
    all_geog_ids = set()
    query = {
      "query": {"match_all": {}},
      "_source": ["geog_id"]
    }
    
    for hit in scan(self.datastore.es,
                    index=self.datastore.mkt_trends_index_name,
                    query=query):
      all_geog_ids.add(hit["_source"]["geog_id"])
    
    self.logger.info(f"Found {len(all_geog_ids)} unique geog_ids in market trends index")
    
    # STEP 2: Prepare listing data - SLIM DOWN before explode (MAJOR MEMORY OPTIMIZATION)
    # Only keep columns we actually need for metrics calculation
    cols_needed = ['guid', 'price', 'addedOn', 'propertyType']
    slim_df = self.listing_df[cols_needed].copy()
    slim_df['addedOn'] = pd.to_datetime(slim_df['addedOn'])
    
    current_date = self.get_current_datetime()
    current_month = current_date.strftime('%Y-%m')
    
    self.logger.info(f"Slimmed DataFrame from {self.listing_df.shape[1]} columns to {len(cols_needed)} columns before explode")
    
    # STEP 3: Expand guid column for listings that exist  
    # Memory impact: 3-5x increase but ONLY on essential columns (much more efficient)
    expanded_df = (
      slim_df.assign(geog_id=slim_df['guid'].str.split(','))
            .explode('geog_id')
    )
    del slim_df
    gc.collect()
    
    self.logger.info(f"Expanded to {len(expanded_df)} listing-geog_id combinations")
    
    # Define standard property types
    standard_property_types = ['DETACHED', 'SEMI-DETACHED', 'TOWNHOUSE', 'CONDO']
    
    # Function to calculate metrics (same as original)
    def calculate_metrics(group):
      if group.empty:
        return pd.Series({
          'median_asking_price': None,
          'new_listings': 0
        })
      
      return pd.Series({
        'median_asking_price': group['price'].median(),
        'new_listings': (
          group['addedOn'].dt.to_period('M').astype(str) == current_month
        ).sum()
      })
    
    # STEP 4: Pre-calculate all metrics using vectorized pandas operations
    # Memory impact: Much smaller than original (only actual data combinations)
    self.logger.info("Calculating metrics for specific property types...")
    specific_metrics = expanded_df.groupby(['geog_id', 'propertyType']).apply(
      calculate_metrics
    ).reset_index()
    
    self.logger.info("Calculating metrics for 'ALL' property type...")
    all_metrics = expanded_df.groupby('geog_id').apply(
      calculate_metrics
    ).reset_index()
    all_metrics['propertyType'] = 'ALL'
    
    # Combine calculated metrics
    calculated_metrics = pd.concat([specific_metrics, all_metrics], ignore_index=True)
    self.logger.info(f"Calculated metrics for {len(calculated_metrics)} actual (geog_id, propertyType) pairs")
    
    # STEP 5: Create complete combinations matrix using vectorized operations
    # Memory impact: MUCH more efficient than original nested list comprehension
    
    # Convert set to sorted list for consistent ordering
    geog_id_list = sorted(list(all_geog_ids))
    num_geog_ids = len(geog_id_list)
    num_prop_types = len(standard_property_types) + 1  # +1 for 'ALL'
    
    self.logger.info(f"Creating complete matrix: {num_geog_ids} geog_ids × {num_prop_types} property types = {num_geog_ids * num_prop_types} total combinations")
    
    # VECTORIZED APPROACH: Create arrays using numpy (much faster than loops)
    # Each geog_id repeated 5 times: [geog1, geog1, geog1, geog1, geog1, geog2, geog2, ...]
    geog_id_array = np.repeat(geog_id_list, num_prop_types)
    
    # Property types tiled: [DETACHED, SEMI, TOWN, CONDO, ALL, DETACHED, SEMI, ...]  
    prop_type_array = np.tile(standard_property_types + ['ALL'], num_geog_ids)
    
    # Create base DataFrame with default values (FAST - vectorized DataFrame creation)
    complete_combinations = pd.DataFrame({
      'geog_id': geog_id_array,
      'propertyType': prop_type_array,
      'median_asking_price': None,  # Default value for missing data
      'new_listings': 0             # Default value for missing data
    })
    
    self.logger.info(f"Created complete combinations matrix with {len(complete_combinations)} rows")
    
    # STEP 6: Merge with calculated metrics (EFFICIENT - pandas optimized merge)
    # This replaces the massive memory-bomb merge from original code
    self.logger.info("Merging calculated metrics with complete combinations...")
    
    final_results = complete_combinations.merge(
      calculated_metrics[['geog_id', 'propertyType', 'median_asking_price', 'new_listings']],
      on=['geog_id', 'propertyType'],
      how='left',
      suffixes=('_default', '_calculated')
    )
    
    # STEP 7: Fill in actual values where they exist, keep defaults where they don't
    # Use calculated values when available, otherwise keep defaults (None, 0)
    final_results['median_asking_price'] = final_results['median_asking_price_calculated'].combine_first(
      final_results['median_asking_price_default']
    )
    final_results['new_listings'] = final_results['new_listings_calculated'].fillna(
      final_results['new_listings_default']
    ).astype(int)
    
    # Clean up - keep only the final columns
    final_results = final_results[['geog_id', 'propertyType', 'median_asking_price', 'new_listings']]
    
    # STEP 8: Final processing (same as original)
    final_results['median_asking_price'] = final_results['median_asking_price'].map(
      lambda x: None if pd.isna(x) else float(x)
    )
    
    self.logger.info(
      f'Calculated current metrics for {len(final_results)} '
      f'(geog_id, propertyType) pairs, including "ALL" property type'
    )
    
    # Add debug logging to show distribution
    active_geog_ids = expanded_df['geog_id'].unique()
    self.logger.debug(
      f'Distribution of geog_ids: '
      f'{len(active_geog_ids)} active, '
      f'{len(all_geog_ids - set(active_geog_ids))} with no current listings'
    )
    
    self.current_metrics_results = final_results


  def update_mkt_trends(self):
    """
    Update the market trends index with current metrics.
    Updates each metric separately to handle failures independently.
    """
    success_price, failed_price = self.update_current_median_asking_price()
    success_listings, failed_listings = self.update_current_new_listings()

    total_success = success_price + success_listings
    total_failed = len(failed_price) + len(failed_listings)

    self.logger.info(f"Successfully updated {total_success} documents")
    if total_failed > 0:
      self.logger.error(f"Failed to update {total_failed} documents")

    return total_success, failed_price + failed_listings
  
  def update_current_median_asking_price(self):
    """Update just the current median asking price metric."""
    def generate_actions():
      for _, row in self.current_metrics_results.iterrows():
        composite_id = f"{row['geog_id']}_{row['propertyType']}"
        median_price = None if pd.isna(row['median_asking_price']) else float(row['median_asking_price'])

        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            if (ctx._source.metrics.current_metrics == null) {
              ctx._source.metrics.current_metrics = new HashMap();
            }
            if (params.median_price === null) {
              ctx._source.metrics.current_metrics.remove('median_asking_price');
            } else {
              ctx._source.metrics.current_metrics.median_asking_price = params.median_price;
            }
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "median_price": median_price,
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
              "current_metrics": {
                "median_asking_price": median_price
              } if median_price is not None else {}
            },
            "last_updated": self.get_current_datetime().isoformat()
          }
        }

    return bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)


  def update_current_new_listings(self):
    """Update just the current new listings count metric."""
    def generate_actions():
      for _, row in self.current_metrics_results.iterrows():
        composite_id = f"{row['geog_id']}_{row['propertyType']}"
        new_listings = int(row['new_listings'])

        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            if (ctx._source.metrics.current_metrics == null) {
              ctx._source.metrics.current_metrics = new HashMap();
            }
            ctx._source.metrics.current_metrics.new_listings = params.new_listings;
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_listings": new_listings,
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
              "current_metrics": {
                "new_listings": new_listings
              }
            },
            "last_updated": self.get_current_datetime().isoformat()
          }
        }

    return bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)


  def cleanup(self):
    """Cleanup any temporary data and reset instance variables."""
    super().cleanup()
    
    self.listing_df = None
    self.current_metrics_results = None
    
    self.logger.info("Cleanup completed. Instance variables reset.")


  def delete_checkpoints_data(self):
    """Remove temporary transform checkpoints."""
    self.cache.delete(f"{self.cache_prefix}{self.job_id}_transform_results")

  
  def remove_current_metrics(self):
    """
    Remove 'current_metrics' field from all documents in the market trends index.
    Useful for cleanup or reset operations.
    """
    def generate_actions():
      query = {
        "query": {
          "exists": {
            "field": "metrics.current_metrics"
          }
        }
      }
      
      for hit in scan(
        self.datastore.es,
        index=self.datastore.mkt_trends_index_name,
        query=query
      ):
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_index_name,
          "_id": hit["_id"],
          "script": {
            "source": """
            if (ctx._source.metrics != null) {
              ctx._source.metrics.remove('current_metrics');
            }
            """
          }
        }

    return bulk(
      self.datastore.es,
      generate_actions(),
      raise_on_error=False,
      raise_on_exception=False
    )
  