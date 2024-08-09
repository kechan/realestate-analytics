from typing import Dict, List, Union
import pandas as pd
import numpy as np

import logging

from datetime import datetime, date, timedelta
import pytz
from tqdm.auto import tqdm

from .base_etl import BaseETLProcessor
from ..data.caching import FileBasedCache
from ..data.es import Datastore
from elasticsearch.helpers import scan, bulk

import realestate_core.common.class_extensions
from realestate_core.common.class_extensions import *
from realestate_core.common.utils import load_from_pickle, save_to_pickle, join_df

# Function to compute metrics for a given geog level
def compute_metrics(df, date_mask, geo_level):
  # Group by geog_id, property type, and month, then calculate metrics
  grouped = df[date_mask].groupby([f'geog_id_{geo_level}', 'propertyType', pd.Grouper(key='lastTransition', freq='M')])
  metrics = grouped.agg({
      'soldPrice': 'median',
      'daysOnMarket': 'median',
      'sold_over_ask': lambda x: (x.sum() / len(x)) * 100  # Percentage over ask
  }).reset_index()

  # Calculate metrics for all property types combined
  grouped_all = df[date_mask].groupby([f'geog_id_{geo_level}', pd.Grouper(key='lastTransition', freq='M')])
  metrics_all = grouped_all.agg({
    'soldPrice': 'median',
    'daysOnMarket': 'median',
    'sold_over_ask': lambda x: (x.sum() / len(x)) * 100  # Percentage over ask
  }).reset_index()
  # Add a 'propertyType' column with None value for the combined metrics
  metrics_all['propertyType'] = None

  # Concatenate the property-specific and combined metrics
  metrics_combined = pd.concat([metrics, metrics_all], ignore_index=True)
  
  # Pivot the data to create time series
  price_series = metrics_combined.pivot(index=[f'geog_id_{geo_level}', 'propertyType'], 
                                columns='lastTransition', 
                                values='soldPrice')
  dom_series = metrics_combined.pivot(index=[f'geog_id_{geo_level}', 'propertyType'], 
                              columns='lastTransition', 
                              values='daysOnMarket')
  over_ask_series = metrics_combined.pivot(index=[f'geog_id_{geo_level}', 'propertyType'], 
                                columns='lastTransition', 
                                values='sold_over_ask')
  
  # Rename columns to YYYY-MM format
  for series in [price_series, dom_series, over_ask_series]:
    series.columns = series.columns.strftime('%Y-%m')

    # Remove the 'lastTransition' label from the column index
    series.columns.name = None

    # Reset index to make geog_id and propertyType regular columns
    # Rename the geog_id_<N> uniformly to geog_id
    series.reset_index(inplace=True)
    series.rename(columns={f'geog_id_{geo_level}': 'geog_id'}, inplace=True)

    # Replace NaN with None in the propertyType 
    series['propertyType'] = series['propertyType'].where(series['propertyType'].notna(), None)

  # price_series.columns = price_series.columns.strftime('%Y-%m')
  # dom_series.columns = dom_series.columns.strftime('%Y-%m')

  # # Remove the 'lastTransition' label from the column index
  # price_series.columns.name = None
  # dom_series.columns.name = None
  
  # # Reset index to make geog_id and propertyType regular columns
  # # Rename the geog_id_<N> uniformly to geog_id
  # price_series = price_series.reset_index().rename(columns={f'geog_id_{geo_level}': 'geog_id'})
  # dom_series = dom_series.reset_index().rename(columns={f'geog_id_{geo_level}': 'geog_id'})

  # # Replace NaN with None in the propertyType 
  # price_series['propertyType'] = price_series['propertyType'].where(price_series['propertyType'].notna(), None)
  # dom_series['propertyType'] = dom_series['propertyType'].where(dom_series['propertyType'].notna(), None)

  return price_series, dom_series, over_ask_series


class SoldMedianMetricsProcessor(BaseETLProcessor):
  def __init__(self, job_id: str, datastore: Datastore):
    super().__init__(job_id=job_id, datastore=datastore)

    self.sold_listing_df = None
    self.geo_entry_df = None

    self.final_price_series = None
    self.final_dom_series = None
    self.final_over_ask_series = None

    self.diff_price_series = None
    self.diff_dom_series = None
    self.diff_over_ask_series = None

    self.sold_listing_selects = [
      'mls',
      'lastTransition', 'transitions', 
      'listingType', 'searchCategoryType', 
      'listingStatus',
      'beds', 'bedsInt', 'baths', 'bathsInt',        
      'price','soldPrice',
      'daysOnMarket',
      'lat', 'lng',
      'city','neighbourhood',
      'provState',
      'guid'
    ]


  def extract(self, from_cache=False):
    super().extract(from_cache=from_cache)
  
  def _extract_from_datastore(self):
    if self._was_success('extract'):
      self.logger.info("Extract stage already completed. Loading from cache.")
      self._load_from_cache()
      return
    
    last_run = self.cache.get(self.last_run_key)
    self.logger.info(f"Last run: {last_run}")

    try:       
      if last_run is None:       # first run

        end_time = self.get_current_datetime()
        start_time = datetime(end_time.year - 5, end_time.month, 1)

        success, self.sold_listing_df = self.datastore.get_sold_listings(
          start_time = start_time,
          end_time = end_time,
          selects=self.sold_listing_selects
        )
        if not success:
          self.logger.error(f"Failed to retrieve sold listings from {start_time} to {end_time}")
          raise ValueError("Failed to retrieve sold listings")

      else:   # inc/delta load
        # Load existing data from cache
        self.sold_listing_df = self.cache.get('five_years_sold_listing')
        if self.sold_listing_df is None:
          self.logger.error("Cache is inconsistent. Missing prior sold_listing_df.")
          raise ValueError("Cache is inconsistent. Missing prior sold_listing_df.")
        
        # get the sold listings from last run till now
        start_time = last_run - timedelta(days=21)    # load from 21 days before last run to have bigger margin of safety.
        end_time = self.get_current_datetime()

        success, delta_sold_listing_df = self.datastore.get_sold_listings(
          start_time=start_time,
          end_time=end_time,
          selects=self.sold_listing_selects
        )
        if not success:
          self.logger.error(f"Failed to retrieve delta sold listings from {start_time} to {end_time}")
          raise ValueError("Failed to retrieve delta sold listings")
        self.logger.info(f'Loaded {len(delta_sold_listing_df)} sold listings from {start_time} to {end_time}')

        # merge delta with whole
        self.sold_listing_df = pd.concat([self.sold_listing_df, delta_sold_listing_df], axis=0, ignore_index=True)
        self.sold_listing_df.drop_duplicates(subset=['_id'], inplace=True, keep='last')

        # get rid of stuff older than 5 years (but incl. the full 1st month)
        len_sold_listing_df_before_delete = len(self.sold_listing_df)
        five_years_ago_from_now = datetime(end_time.year - 5, end_time.month, 1)
        drop_idxs = self.sold_listing_df.q("lastTransition < @five_years_ago_from_now").index
        self.sold_listing_df.drop(index=drop_idxs, inplace=True)
        self.sold_listing_df.reset_index(drop=True, inplace=True)
        len_sold_listing_df_after_delete = len(self.sold_listing_df)
        self.logger.info(f'removed {len_sold_listing_df_before_delete - len_sold_listing_df_after_delete} sold listings older than 5 years')

      # if all operations are successful, update the cache
      self._save_to_cache()
      self.cache.set(key=self.last_run_key, value=end_time)
      self._mark_success('extract')

    except Exception as e:
      self.logger.error(f"Error occurred during extract: {e}")
      raise

 
  def _load_from_cache(self):
    super()._load_from_cache() 

    self.sold_listing_df = self.cache.get('five_years_sold_listing')

    self.geo_entry_df = self.cache.get('all_geo_entry')
    self.geo_entry_df.drop_duplicates(subset=['MLS', 'CITY', 'PROV_STATE'], keep='last', inplace=True)

    if self.sold_listing_df is None or self.geo_entry_df is None:
      self.logger.error("Missing sold_listing_df or geo_entry_df.")
      raise ValueError("Missing sold_listing_df or geo_entry_df.")

    self.logger.info(f"Loaded {len(self.sold_listing_df)} sold listings and {len(self.geo_entry_df)} geo entries from cache.")

  def _save_to_cache(self):
    super()._save_to_cache()

    # Remove 'sold_over_ask' column if it exists before saving to cache
    # this col. is a pre-computed col. in 
    if 'sold_over_ask' in self.sold_listing_df.columns:
      cache_df = self.sold_listing_df.drop(columns=['sold_over_ask'])
    else:
      cache_df = self.sold_listing_df
    
    self.cache.set('five_years_sold_listing', cache_df)

    # self.cache.set('five_years_sold_listing', self.sold_listing_df)
    self.logger.info(f"Saved {len(self.sold_listing_df)} sold listings to cache.")


  def transform(self):

    if self._was_success('transform'):
      self.logger.info("Transform already successful. Loading checkpoint from cache.")
      self.diff_price_series = self.cache.get(f"{self.job_id}_diff_price_series")
      self.diff_dom_series = self.cache.get(f"{self.job_id}_diff_dom_series")
      self.diff_over_ask_series = self.cache.get(f"{self.job_id}_diff_over_ask_series")
      return

    try:
      # Add geog_ids to sold listings, this is needed only for legacy data
      self.add_geog_ids_to_sold_listings()
      self.compute_5_year_metrics()

      # optimize such that we don't update on things that didnt change from last run
      prev_price_series = self.cache.get('five_years_price_series')
      prev_dom_series = self.cache.get('five_years_dom_series')
      prev_over_ask_series = self.cache.get('five_years_over_ask_series')

      if prev_price_series is None or prev_dom_series is None or prev_over_ask_series is None:
        self.logger.info("No previous times series found. Keeping entire currently computed time series.")
        self.diff_price_series = self.final_price_series
        self.diff_dom_series = self.final_dom_series
        self.diff_over_ask_series = self.final_over_ask_series
      else:
        self.logger.info("Previous times series found. Computing update delta.")
        self.diff_price_series = self._get_delta_dataframe(self.final_price_series, prev_price_series)
        self.diff_dom_series = self._get_delta_dataframe(self.final_dom_series, prev_dom_series)
        self.diff_over_ask_series = self._get_delta_dataframe(self.final_over_ask_series, prev_over_ask_series)

      self.logger.info(f'Prepared to update {len(self.diff_price_series)} price time series, '
                         f'{len(self.diff_dom_series)} DOM time series, and '
                         f'{len(self.diff_over_ask_series)} over-ask percentage time series.')

      # Cache the current data for the next run
      self.cache.set('five_years_price_series', self.final_price_series)
      self.cache.set('five_years_dom_series', self.final_dom_series)
      self.cache.set('five_years_over_ask_series', self.final_over_ask_series)

      # checkpoint the diff_*_series
      self.diff_price_series.reset_index(drop=True, inplace=True)
      self.diff_dom_series.reset_index(drop=True, inplace=True)
      self.diff_over_ask_series.reset_index(drop=True, inplace=True)

      self.cache.set(f"{self.job_id}_diff_price_series", self.diff_price_series)
      self.cache.set(f"{self.job_id}_diff_dom_series", self.diff_dom_series)
      self.cache.set(f"{self.job_id}_diff_over_ask_series", self.diff_over_ask_series)

      self._mark_success('transform')

    except Exception as e:
      self.logger.error(f"Error occurred during transform: {e}")
      raise


  def load(self):
    if self._was_success('load'):
      self.logger.info("Load already successful.")
      return 0, []
    
    success, failed = self.update_mkt_trends_ts_index()
    total_attempts = success + len(failed)

    if total_attempts != 0 and success / total_attempts < 0.5:
      self.logger.error(f"Less than 50% success rate. Only {success} out of {total_attempts} documents updated.")

      self.datastore.summarize_update_failures(failed)
    else:
      self._mark_success('load')

    return success, failed
  

  def cleanup(self):
    super().cleanup()
    
    cache_keys = ['five_years_sold_listing',
                  'five_years_dom_series_df',
                  'five_years_price_series_df'
                  ]
    
    for key in cache_keys:
      try:
        self.cache.delete(key)
      except Exception as e:
        self.logger.error(f"Error deleting cache key {key}: {e}")

    self.logger.info("Cache cleanup complete.")

    # remove metrics from mkt_trends_ts index
    # updated, failures = self.remove_metrics_from_mkt_trends_ts()
    # self.logger.info(f"Removed metrics from {updated} doc in mkt_trends_ts index with {len(failures)} failures.")
      

  def compute_5_year_metrics(self):
    # Construct date range for the past 60 full months plus the current month to date.

    current_date = self.get_current_datetime().date()
    current_month_start = date(current_date.year, current_date.month, 1)
    start_date = (current_month_start.replace(day=1) - pd.DateOffset(months=12*5)).date()
    self.logger.info(f'Computing 5 yr metrics for start_date: {start_date} to current_date: {current_date}')

    self.sold_listing_df.lastTransition = pd.to_datetime(self.sold_listing_df.lastTransition)

    # Add 'sold_over_ask' (a boolean column)
    self.sold_listing_df['sold_over_ask'] = self.sold_listing_df['soldPrice'] > self.sold_listing_df['price']

    # Create a boolean mask for the date range filter
    date_mask = (
        (self.sold_listing_df['lastTransition'].dt.date >= start_date) &
        (self.sold_listing_df['lastTransition'].dt.date < current_date)
    )

    if not date_mask.any():
      self.logger.error("No data available for the specified date range")
      raise ValueError("No data available for the specified date range")

    # filter sold listings for the required date range
    # df = self.sold_listing_df[
    #   (self.sold_listing_df['lastTransition'].dt.date >= start_date) &
    #   (self.sold_listing_df['lastTransition'].dt.date < current_date)
    # ]

    # if df.empty:
    #   self.logger.error("No data available for the specified date range")
    #   raise ValueError("No data available for the specified date range")

    # Compute metrics for each geographic level and concat into a single dataframe
    levels = [10, 20, 30, 40]
    all_price_series = []
    all_dom_series = []
    all_over_ask_series = []

    for level in levels:
      price_series, dom_series, over_ask_series = compute_metrics(self.sold_listing_df, date_mask, level)

      price_series['geo_level'] = level
      dom_series['geo_level'] = level
      over_ask_series['geo_level'] = level

      all_price_series.append(price_series)
      all_dom_series.append(dom_series)
      all_over_ask_series.append(over_ask_series)

    # Combine results from all levels
    self.final_price_series = pd.concat(all_price_series, ignore_index=True)
    self.final_dom_series = pd.concat(all_dom_series, ignore_index=True)
    self.final_over_ask_series = pd.concat(all_over_ask_series, ignore_index=True)


  def add_geog_ids_to_sold_listings(self):    
    legacy_data_path = False   # TODO: remove this when guid bug is fixed on the ES.

    # Function to parse geog_ids
    def parse_geog_ids(geog_string):
      if pd.isna(geog_string):
        return {}
      geog_ids = geog_string.split(',')
      parsed = {}
      for geog_id in geog_ids:
        match = re.match(r'g(\d+)_\w+', geog_id)
        if match:
          level = match.group(1)
          parsed[f'geog_id_{level}'] = geog_id
      return parsed
    
    # TODO: we need to remove legacy path before deployment, this shouldnt be here.
    if legacy_data_path:
      # Merge the geo_entry_df with the sold_listing_df
      self.sold_listing_df = join_df(self.sold_listing_df, 
                                    self.geo_entry_df[['MLS', 'CITY', 'PROV_STATE', 'GEOGRAPHIES']], 
                                    left_on=['mls', 'city', 'provState'], 
                                    right_on=['MLS', 'CITY', 'PROV_STATE'], 
                                    how='left')

      # Apply parsing function and create new columns
      geog_data = self.sold_listing_df.GEOGRAPHIES.apply(parse_geog_ids)
      for level in ['10', '20', '30', '40']:
        self.sold_listing_df[f'geog_id_{level}'] = geog_data.apply(lambda x: x.get(f'geog_id_{level}'))
    else:
      if 'guid' in self.sold_listing_df.columns:
        geo_data = self.sold_listing_df.guid.apply(parse_geog_ids)
        for level in ['10', '20', '30', '40']:
          self.sold_listing_df[f'geog_id_{level}'] = geo_data.apply(lambda x: x.get(f'geog_id_{level}'))
      else:
        self.logger.error("No GUID column found in sold_listing_df. Cannot proceed with geog_id mapping.")
        raise ValueError("No GUID column found in sold_listing_df. Cannot proceed with geog_id mapping.")
    
    
  
  def update_mkt_trends_ts_index(self):
    """
    Updates the market trends time series index in ES with new median price,
    days on market (DOM), and over-ask percentage metrics.

    Returns:
        tuple: A tuple containing the number of successfully updated documents and a list of any failures.    
    """
    def generate_actions():
      # Process price series
      for _, row in self.diff_price_series.iterrows():
        new_metrics = {
          "median_price": []
        }

        for col in self.diff_price_series.columns:
          if col.startswith('20'):
            date = col
            value = row[col]
            if pd.notna(value):
              new_metrics["median_price"].append({
                "date": date,
                "value": float(value)
              })

        property_type_id = "ALL" if row['propertyType'] is None else row['propertyType']            
        composite_id = f"{row['geog_id']}_{property_type_id}"

        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_ts_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            ctx._source.metrics.median_price = params.new_metrics.median_price;
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_metrics": new_metrics,
              "geog_id": row['geog_id'],
              "propertyType": property_type_id,
              "geo_level": int(row['geo_level']),
              "last_updated": self.get_current_datetime().isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": property_type_id,
            "geo_level": int(row['geo_level']),
            "metrics": new_metrics,
            "last_updated": self.get_current_datetime().isoformat()
          }
        }

      # Process DOM series
      for _, row in self.diff_dom_series.iterrows():
        new_metrics = {
          "median_dom": []
        }

        for col in self.diff_dom_series.columns:
          if col.startswith('20'):
            date = col
            value = row[col]
            if pd.notna(value):
              new_metrics["median_dom"].append({
                "date": date,
                "value": float(value)
              })

        property_type_id = "ALL" if row['propertyType'] is None else row['propertyType']            
        composite_id = f"{row['geog_id']}_{property_type_id}"

        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_ts_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            ctx._source.metrics.median_dom = params.new_metrics.median_dom;
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_metrics": new_metrics,
              "geog_id": row['geog_id'],
              "propertyType": property_type_id,
              "geo_level": int(row['geo_level']),
              "last_updated": self.get_current_datetime().isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": property_type_id,
            "geo_level": int(row['geo_level']),
            "metrics": new_metrics,
            "last_updated": self.get_current_datetime().isoformat()
          }
        }

      # Process over-ask % series
      for _, row in self.diff_over_ask_series.iterrows():
        new_metrics = {
          "over_ask_percentage": []
        }

        for col in self.diff_over_ask_series.columns:
          if col.startswith('20'):
            date = col
            value = row[col]
            if pd.notna(value):
              new_metrics["over_ask_percentage"].append({
                "date": date,
                "value": round(float(value), 2)
              })

        property_type_id = "ALL" if row['propertyType'] is None else row['propertyType']
        composite_id = f"{row['geog_id']}_{property_type_id}"

        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_ts_index_name,
          "_id": composite_id,
          "script": {
            "source": """
            if (ctx._source.metrics == null) {
              ctx._source.metrics = new HashMap();
            }
            ctx._source.metrics.over_ask_percentage = params.new_metrics.over_ask_percentage;
            ctx._source.geog_id = params.geog_id;
            ctx._source.propertyType = params.propertyType;
            ctx._source.geo_level = params.geo_level;
            ctx._source.last_updated = params.last_updated;
            """,
            "params": {
              "new_metrics": new_metrics,
              "geog_id": row['geog_id'],
              "propertyType": property_type_id,
              "geo_level": int(row['geo_level']),
              "last_updated": self.get_current_datetime().isoformat()
            }
          },
          "upsert": {
            "geog_id": row['geog_id'],
            "propertyType": property_type_id,
            "geo_level": int(row['geo_level']),
            "metrics": new_metrics,
            "last_updated": self.get_current_datetime().isoformat()
          }
        }

    # Perform bulk update
    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)

    self.logger.info(f"Successfully updated {success} docs")
    if failed: self.logger.error(f"Failed to update {len(failed)} docs")

    return success, failed

  
  def delete_all_mkt_trends_ts(self):
    """
    Deletes all documents from the market trends time series index 
    This probably shouldnt be done often, since it also removes
     - monthly median price
     - monthly new listing
     - absorption rate
     - etc.
    """
    query = {
      "query": {
        "match_all": {}
      }
    }
    response = self.datastore.es.delete_by_query(index=self.datastore.mkt_trends_ts_index_name, body=query)
    deleted_count = response["deleted"]
    self.logger.info(f"Deleted {deleted_count} documents from {self.datastore.mkt_trends_ts_index_name}")


  def remove_metrics_from_mkt_trends_ts(self):
    def generate_actions():
      # Use scan to efficiently retrieve all documents that have a 'metrics' field
      for hit in scan(self.datastore.es, 
                      index=self.datastore.mkt_trends_ts_index_name, 
                      query={"query": {"exists": {"field": "metrics"}}}):
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_ts_index_name,
          "_id": hit["_id"],
          "script": {
            "source": "ctx._source.remove('metrics')",
          }
        }

    # Perform bulk update
    success, failed = bulk(self.datastore.es, generate_actions(), stats_only=True, raise_on_error=False)

    self.logger.info(f"Successfully updated {success} documents")
    if failed:
      self.logger.error(f"Failed to update {len(failed)} documents")

    return success, failed


  def _update_es_sold_listings_with_guid(self) -> None:
    '''
    Fix legacy sold listings with guid (aka geog_id).
    Run this only after carefully preparing self.sold_listing_df
    '''
    def generate_updates():
      for _, row in self.sold_listing_df.iterrows():
        yield {
          '_op_type': 'update',
          '_index': self.datastore.sold_listing_index_name,
          '_id': row['_id'],
          'doc': {
            'guid': row['GEOGRAPHIES'] if pd.notna(row['GEOGRAPHIES']) else None
          }
        }

    # Perform bulk update
    bulk(self.datastore.es, generate_updates())


  def _get_delta_dataframe(self, current_df, prev_df) -> pd.DataFrame:
    """
    Identify the delta changes between the current and previous DataFrames.

    This function compares the current DataFrame to the previous DataFrame and identifies
    rows that are either new or have changed. If the columns have changed between the
    two DataFrames, it considers the entire current DataFrame as changed. It ensures
    that the resulting delta DataFrame has the same columns as the original current DataFrame.

    Parameters:
    current_df (pd.DataFrame): The current DataFrame with the latest data.
    prev_df (pd.DataFrame): The previous DataFrame with the data to compare against.

    Returns:
    pd.DataFrame: A DataFrame containing only the rows that are new or have changed,
                  with the columns in the same order as the original current DataFrame.
    """
    # Step 1: Check if columns have changed
    if set(prev_df.columns) != set(current_df.columns):
      # Columns have changed, consider everything as changed
      delta_df = current_df.copy()
    else:
      original_columns = current_df.columns.tolist()
      # Columns have not changed, proceed with finding the delta
      # Ensure both dataframes have the same columns
      all_columns = list(set(prev_df.columns).union(set(current_df.columns)))

      prev_df = prev_df.reindex(columns=all_columns, fill_value=np.nan)
      current_df = current_df.reindex(columns=all_columns, fill_value=np.nan)

      # Sort dataframes to ensure alignment when comparing
      prev_df = prev_df.sort_values(by=['geog_id', 'propertyType']).reset_index(drop=True)
      current_df = current_df.sort_values(by=['geog_id', 'propertyType']).reset_index(drop=True)

      # Merge the dataframes on composite keys to identify new and changed rows
      merged_df = pd.merge(prev_df, current_df, on=['geog_id', 'propertyType'], how='outer', suffixes=('_prev', '_curr'), indicator=True)

      # Identify new rows and changed rows
      def row_changed(row):
        for col in all_columns:
          if col not in ['geog_id', 'propertyType', '_merge']:
            if not pd.isna(row[f'{col}_prev']) or not pd.isna(row[f'{col}_curr']):
              if row[f'{col}_prev'] != row[f'{col}_curr']:
                return True
        return False

      new_or_changed_rows = merged_df[
          (merged_df['_merge'] == 'right_only') | 
          ((merged_df['_merge'] == 'both') & merged_df.apply(row_changed, axis=1))
      ]

      # Extract the relevant columns for the delta dataframe
      relevant_columns = ['geog_id', 'propertyType'] + [col for col in current_df.columns if col not in ['geog_id', 'propertyType']]
      delta_df = new_or_changed_rows[['geog_id', 'propertyType'] + [f'{col}_curr' for col in relevant_columns if col not in ['geog_id', 'propertyType']]].copy()
      delta_df.columns = ['geog_id', 'propertyType'] + [col.replace('_curr', '') for col in delta_df.columns if col not in ['geog_id', 'propertyType']]

      # Drop the rows where all values except 'geog_id' and 'propertyType' are NaN (if such rows are not meaningful)
      delta_df = delta_df.dropna(how='all', subset=[col for col in delta_df.columns if col not in ['geog_id', 'propertyType']])

      # Rearrange columns to match the original order of current_df
      delta_df = delta_df[original_columns]
      current_df = current_df[original_columns]
      prev_df = prev_df[original_columns]
    
    return delta_df

  

  def delete_checkpoints_data(self):
    self.cache.delete(f"{self.job_id}_diff_price_series")
    self.cache.delete(f"{self.job_id}_diff_dom_series")
    self.cache.delete(f"{self.job_id}_diff_over_ask_series")


if __name__ == '__main__':
  job_id = 'hist_median_metrics_xxx'
  uat_datastore = Datastore(host='localhost', port=9201)
  prod_datastore = Datastore(host='localhost', port=9202)

  processor = SoldMedianMetricsProcessor(
    job_id=job_id,
    datastore=prod_datastore
  )

  # during dev, we extract from PROD but update the UAT as a temporary workaround
  processor.simulate_failure_at = 'transform'
  try:
    processor.run()
  except Exception as e:
    print(e)

  # during dev, continue with the UAT datastore
  processor = SoldMedianMetricsProcessor(
    job_id=job_id,
    datastore=uat_datastore
  )
  processor.run()

  # repeat the above steps every few days or weekly, this will allow a partial current month calculation.


""" Sample doc from the mkt_trends_ts index: 
{'geog_id': 'g30_dpz89rm7',
 'propertyType': 'DETACHED',
 'geo_level': 30,
 'metrics': {'median_price': [{'date': '2023-01', 'value': 1789000.0},
   {'date': '2023-02', 'value': 1320000.0},
   {'date': '2023-03', 'value': 1352500.0},
   {'date': '2023-04', 'value': 1550000.0},
   etc. etc. 
   {'date': '2024-05', 'value': 1435000.0},
   {'date': '2024-06', 'value': 1350000.0},
   {'date': '2024-07', 'value': 1261750.0}],
  'median_dom': [{'date': '2023-01', 'value': 2.0},
   {'date': '2023-02', 'value': 7.0},
   {'date': '2023-03', 'value': 7.0},
   {'date': '2023-04', 'value': 8.0},
   etc. etc.
   {'date': '2024-05', 'value': 9.0},
   {'date': '2024-06', 'value': 9.0},
   {'date': '2024-07', 'value': 14.5}],
  'last_mth_median_asking_price': {'date': '2024-07', 'value': 1500000.0},
  'last_mth_new_listings': {'date': '2024-07', 'value': 887},
  'absorption_rate': [{'date': '2024-07', 'value': 0.0399}],
  'over_ask_percentage': [{'date': '2023-01', 'value': 66.67},
   {'date': '2023-02', 'value': 53.85},
   {'date': '2023-03', 'value': 45.74},
   {'date': '2023-04', 'value': 40.0},
   etc. etc.
   {'date': '2024-05', 'value': 50.8},
   {'date': '2024-06', 'value': 50.41},
   {'date': '2024-07', 'value': 36.67}]},
 'last_updated': '2024-08-08T03:59:24.281335'}
"""

""" Sample price times series df
+--------------+-------------+----------+----------+----------+------+----------+----------+----------+----------+
| geog_id      | propertyType| 2023-01  | 2023-02  | 2023-03  | ...  | 2024-05  | 2024-06  | 2024-07  | geo_level|
+--------------+-------------+----------+----------+----------+------+----------+----------+----------+----------+
| g40_f8543be9 | None        | NaN      | 162550.0 | 367450.0 | ...  | 289000.0 | 389900.0 | 300000.0 | 40       |
| g40_f8543be9 | DETACHED    | NaN      | 162550.0 | 367450.0 | ...  | 286000.0 | 389900.0 | 300000.0 | 40       |
| g40_f85dce8h | None        | NaN      | 74900.0  | 165625.0 | ...  | 196000.0 | 367000.0 | 252500.0 | 40       |
+--------------+-------------+----------+----------+----------+------+----------+----------+----------+----------+

Sample DOM times series df
+--------------+-------------+----------+----------+----------+------+----------+----------+----------+----------+
| geog_id      | propertyType| 2023-01  | 2023-02  | 2023-03  | ...  | 2024-05  | 2024-06  | 2024-07  | geo_level|
+--------------+-------------+----------+----------+----------+------+----------+----------+----------+----------+
| g40_f8543be9 | None        | NaN      | 22.0     | 23.5     | ...  | 83.0     | 30.0     | 24.0     | 40       |
| g40_f8543be9 | DETACHED    | NaN      | 22.0     | 23.5     | ...  | 67.0     | 30.0     | 24.0     | 40       |
| g40_f85dce8h | None        | NaN      | 16.0     | 18.0     | ...  | 32.5     | 33.0     | 19.0     | 40       |
+--------------+-------------+----------+----------+----------+------+----------+----------+----------+----------+

"""