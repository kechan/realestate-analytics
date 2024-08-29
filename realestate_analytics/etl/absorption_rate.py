from typing import Dict, List, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import pytz, logging

from .base_etl import BaseETLProcessor
from ..data.caching import FileBasedCache
from ..data.es import Datastore
from ..data.archive import Archiver
from elasticsearch.helpers import scan, bulk

import realestate_core.common.class_extensions
from realestate_core.common.class_extensions import *


class AbsorptionRateProcessor(BaseETLProcessor):
  # https://www.cgprealestateconsulting.com/post/calculating-absorption-rate-real-estate
  def __init__(self, job_id: str, datastore: Datastore):
    super().__init__(job_id=job_id, datastore=datastore)

    self.logger.info("Last run is expectedly None since we don't do any extract from ES for this ETL job.")
    self.sold_listing_df = None
    self.listing_df = None
    self.absorption_rates = None

    self.LOAD_SUCCESS_THRESHOLD = 0.5
    self.ABSORPTION_RATE_ROUND_DIGITS = 4

  def extract(self, from_cache=True):
    # ignore the from_cache flag, always load from cache
    # this is to conform to the signature of extract(...) that the parent class expects

    self._load_from_cache()
    self._mark_success('extract')
    # else:
    #   self.logger.error("Direct extraction from datastore not implemented. Use from_cache=True.")
    #   self.logger.error("Cached data from NearbyComparableSoldsProcessor should be used.")
    #   raise NotImplementedError("Direct extraction from datastore not implemented. Cached data from NearbyComparableSoldsProcessor should be used.")
     

  def transform(self):
    if self._was_success('transform'):
      self.logger.info("Transform already completed successfully. Loading from archive")
      self.absorption_rates = self.archiver.retrieve('absorption_rates')
      return

    try:
      self._calculate_absorption_rates()

      # archive the absorption rates, and it wants to be successful before transform is marked as successful
      if hasattr(self, 'absorption_rates') and self.absorption_rates is not None:
        if not self.archiver.archive(self.absorption_rates, 'absorption_rates'):
          self.logger.error("Failed to archive absorption rates")
          raise ValueError("Fatal error archiving absorption_rates. This must be successful to proceed.")
        else:
          self.logger.info("Successfully archived absorption rates.")
          self._mark_success('transform')
      else:
        self.logger.error("absorption_rates not found or is None. Unable to archive.")
        raise ValueError("absorption_rates not found or is None. Unable to archive.")
      
    except Exception as e:
      self.logger.error(f"Unexpected error in transform: {e}")
      raise

   
  def load(self):
    if self._was_success('load'):
      self.logger.info("Load already successful.")
      return

    success, failed = self.update_mkt_trends_ts_index()
    total_attempts = success + len(failed)

    if total_attempts != 0 and success / total_attempts < self.LOAD_SUCCESS_THRESHOLD:
      self.logger.error(f"Less than 50% success rate. Only {success} out of {total_attempts} documents updated.")

      self.datastore.summarize_update_failures(failed)
    else:
      self._mark_success('load')

    return success, failed

  
  def _calculate_absorption_rates(self):
    if self.sold_listing_df is None or self.listing_df is None:
      self.logger.error("Missing sold_listing_df or listing_df. Cannot calculate absorption rates.")
      raise ValueError("Missing sold_listing_df or listing_df. Cannot calculate absorption rates.")
    
    try:
      # drop geog_id of "None" or None
      self.sold_listing_df.drop(self.sold_listing_df[self.sold_listing_df['guid'].isin(["None", None])].index, inplace=True)
      self.listing_df.drop(self.listing_df[self.listing_df['guid'].isin(["None", None])].index, inplace=True)

        
      # Ensure datetime columns are in datetime format
      self.sold_listing_df['lastTransition'] = pd.to_datetime(self.sold_listing_df['lastTransition'])
      self.listing_df['addedOn'] = pd.to_datetime(self.listing_df['addedOn'])

      # Get the current date and the start of the previous month
      current_date = self.get_current_datetime()
      last_month = current_date.replace(day=1) - timedelta(days=1)    
      last_month_yearmonth = last_month.strftime('%Y-%m')
      self.logger.info(f'Calculating absorption rates for month: {last_month_yearmonth}')

      # Add a year-month column to the sold_listing_df
      self.sold_listing_df['yearmonth'] = self.sold_listing_df['lastTransition'].dt.strftime('%Y-%m')

      # Filter sold listings for the previous month
      prev_month_sold = self.sold_listing_df[self.sold_listing_df['yearmonth'] == last_month_yearmonth]

      self.logger.info(f"Found {len(prev_month_sold)} sold listings for {last_month_yearmonth}.")

      def expand_guid(df):
        return df.assign(geog_id=df['guid'].str.split(',')).explode('geog_id')

      is_month_boundary = (current_date.day == 1)

      # Expand guid for both sold and current listings
      expanded_sold = expand_guid(prev_month_sold)

      if is_month_boundary:
        expanded_current = expand_guid(self.listing_df)
        current_counts = expanded_current.groupby(['geog_id', 'propertyType']).size().reset_index(name='current_count')
        self.archiver.archive(current_counts, f'current_listing_counts_{last_month_yearmonth}')
        self.logger.info(f"Archived current listing counts for {last_month_yearmonth}.")
      else:
        current_counts = self.archiver.retrieve(f'current_listing_counts_{last_month_yearmonth}')   # load from archive
        if current_counts is None:
          # TODO: during dev, the snapshot for last mth may indeed be missing, we just use the current listing_df.
          # This should be removed after a few successful end of mth runs after deployment, and probably even raise 
          # an error if current_counts is not found.
          expanded_current = expand_guid(self.listing_df)
          current_counts = expanded_current.groupby(['geog_id', 'propertyType']).size().reset_index(name='current_count')
          self.logger.warning(f"Current listing counts for {last_month_yearmonth} not found in archive. Using current listing_df.")

      # Group by geog_id and propertyType
      sold_counts = expanded_sold.groupby(['geog_id', 'propertyType']).size().reset_index(name='sold_count')
      # current_counts = expanded_current.groupby(['geog_id', 'propertyType']).size().reset_index(name='current_count')

      # Merge the counts
      merged_counts = pd.merge(sold_counts, current_counts, on=['geog_id', 'propertyType'], how='outer').fillna(0)

      # Calculate absorption rate
      merged_counts['absorption_rate'] = merged_counts['sold_count'] / merged_counts['current_count']
      merged_counts['absorption_rate'] = merged_counts['absorption_rate'].replace([np.inf, -np.inf], np.nan)

      self.absorption_rates = merged_counts

      self.logger.info(f"Calculated absorption rates for {len(self.absorption_rates)} geog_id-propertyType combinations.")

      # Calculate metrics for 'ALL' property type
      all_property_types = merged_counts.groupby('geog_id').agg({
        'sold_count': 'sum',
        'current_count': 'sum'
      }).reset_index()
      all_property_types['propertyType'] = 'ALL'
      all_property_types['absorption_rate'] = all_property_types['sold_count'] / all_property_types['current_count']
      all_property_types['absorption_rate'] = all_property_types['absorption_rate'].replace([np.inf, -np.inf], np.nan)

      # Combine results
      self.absorption_rates = pd.concat([self.absorption_rates, all_property_types], ignore_index=True)

      self.absorption_rates.sold_count = self.absorption_rates.sold_count.astype(int)
      self.absorption_rates.current_count = self.absorption_rates.current_count.astype(int)

      self.logger.info(f"Final absorption rates calculated for {len(self.absorption_rates)} geog_id-propertyType combinations, including 'ALL' property type.")
    except Exception as e:
      self.logger.error(f"Unexpected error calculating absorption rates: {e}")
      raise


  def _load_from_cache(self):
    super()._load_from_cache()
    self.sold_listing_df = self.cache.get('one_year_sold_listing')
    self.listing_df = self.cache.get('on_listing')   # from NearbyComparableSoldsProcessor
    
    if self.sold_listing_df is None or self.listing_df is None:
      self.logger.error("Missing sold_listing_df or listing_df.")
      raise ValueError("Missing sold_listing_df or listing_df.")
    
    self.logger.info(f"Loaded {len(self.sold_listing_df)} sold listings and {len(self.listing_df)} current listings from cache.")


  def update_mkt_trends_ts_index(self):
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
    pass

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


""" Sample absorption_rates dataframe:
+-------------+---------------+---------------+--------------+----------------+
| geog_id     | propertyType  | sold_count    | current_count| absorption_rate|
+-------------+---------------+---------------+--------------+----------------+
| g30_dpz89rm7| CONDO         | 54.0          | 3768.0       | 0.014331       |
| g30_dpz89rm7| DETACHED      | 51.0          | 1363.0       | 0.037417       |
| g30_dpz89rm7| SEMI-DETACHED | 12.0          | 201.0        | 0.059701       |
| g30_dpz89rm7| TOWNHOUSE     | 11.0          | 501.0        | 0.021956       |
| g30_dpz89rm7| ALL           | 128.0         | 5833.0       | 0.021944       |
+-------------+---------------+---------------+--------------+----------------+
"""

""" Sample doc from the mkt_trends_current index: 
{'geog_id': 'g30_dpz89rm7',
 'propertyType': 'DETACHED',
 'geo_level': 30,
 'metrics': {'median_price': [{'month': '2023-01', 'value': 1789000.0},
   {'month': '2023-02', 'value': 1320000.0},
   {'month': '2023-03', 'value': 1352500.0},
   {'month': '2023-04', 'value': 1550000.0},
   {'month': '2023-05', 'value': 1900000.0},
   {'month': '2023-06', 'value': 1915000.0},
      etc
   {'month': '2024-06', 'value': 1350000.0},
   {'month': '2024-07', 'value': 1261750.0}],

  'median_dom': [{'month': '2023-01', 'value': 2.0},
   {'month': '2023-02', 'value': 7.0},
   {'month': '2023-03', 'value': 7.0},
   {'month': '2023-04', 'value': 8.0},
   {'month': '2023-05', 'value': 11.5},
   {'month': '2023-06', 'value': 9.0},
   {'month': '2023-07', 'value': 11.0},
      etc.
   {'month': '2024-06', 'value': 9.0},
   {'month': '2024-07', 'value': 14.5}],

  'last_mth_median_asking_price': [{'month': '2024-07', 'value': 1500000.0}],
  'last_mth_new_listings': [{'month': '2024-07', 'value': 887}],

  'absorption_rate': [{'month': '2024-07', 'value': 0.0374}]},

 'last_updated': '2024-08-02T04:51:04.199475'}
"""  