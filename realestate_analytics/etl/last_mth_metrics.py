from typing import Dict, List, Union, Callable
import pandas as pd
from datetime import datetime, date, timedelta

from pathlib import Path
import logging

from ..etl.base_etl import BaseETLProcessor
from ..data.caching import FileBasedCache
from ..data.es import Datastore
from ..data.bq import BigQueryDatastore
from ..data.archive import Archiver
from elasticsearch.helpers import scan, bulk
from elasticsearch.exceptions import NotFoundError, ConnectionError, RequestError, TransportError

import logging, sys, pytz


class LastMthMetricsProcessor(BaseETLProcessor):
  def __init__(self, job_id: str, datastore: Datastore, bq_datastore: BigQueryDatastore = None):
    super().__init__(job_id=job_id, datastore=datastore, bq_datastore=bq_datastore)
    
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

  def setup_extra_stages(self, 
                         add_extra_stage: Callable[[str], None], 
                         add_extra_cleanup_stage: Callable[[str], None]) -> None:
    add_extra_stage('end_of_mth_run')
    add_extra_cleanup_stage('compute_last_month_metrics')
    add_extra_cleanup_stage('remove_deleted_listings')
    add_extra_cleanup_stage('update_mkt_trends')
  
  """
  def run(self):
    if self._was_success('all'):
      self.logger.info(f"Job {self.job_id} has already run successfully.")
      return

    self._extract()
    self._transform()
    success, failed = self._load()

    self.end_of_mth_run()

    # if the whole job is successful, clean up the temporary checkpoints and markers

    stages = ['extract', 'transform', 'load', 'end_of_mth_run']
    all_stages_successful = all(self._was_success(stage) for stage in stages)

    if all_stages_successful:
      # remove all possible checkpt markers
      for stage in ['extract', 'transform', 'load', 'compute_last_month_metrics', 'remove_deleted_listings', 'update_mkt_trends', 'end_of_mth_run']:
        self._unmark_success(stage)

      # remove intermediate result cached by transform
      self.delete_checkpoints_data()
  
      # finally mark it as all success
      self._mark_success('all')

      self.logger.info(f"Job {self.job_id} completed successfully.")
  """

  def extract(self, from_cache=False):
    try:      
      if from_cache:
        self._load_from_cache()
      else:
        last_run = self.cache.get(self.last_run_key)
        self.logger.info(f"last_run: {last_run}")

        if last_run is None:
          self._initialize_extract_from_datastore()
        else:
          self._delta_extract_from_datastore(last_run=last_run)

        # is_deleted is updated during end of month run, we shouldnt consider
        # is_deleted = True during all stages. 
        self.listing_df = self.listing_df[~self.listing_df['is_deleted']]
        self.listing_df.reset_index(drop=True, inplace=True)

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
      self.delta_listing_df = self.cache.get(f'{self.job_id}_delta_on_current_listing')
      return

    # there's no transform needed
    if self.delta_listing_df is None:
      self.logger.info("No transform needed. Just loading checkpoints from cache.")
      self.delta_listing_df = self.cache.get(f'{self.job_id}_delta_on_current_listing')
    
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


  def _initialize_extract_from_datastore(self):
    if self._was_success('extract'):
      self.logger.info("Extract stage already completed. Loading from cache.")
      self._load_from_cache()
      return

    # get everything till now
    start_time = datetime(1970, 1, 1)     # distant past
    end_time = self.get_current_datetime()
    success, self.listing_df = self.datastore.get_listings(
      use_script_for_last_update=True,    # TODO: Undo after dev.

      addedOn_start_time=start_time,
      updated_start_time=start_time,
      
      addedOn_end_time=end_time,
      updated_end_time=end_time,

      selects=self.listing_selects,
      prov_code='ON'                
    )
    
    if not success:
      self.logger.error(f"Failed to load listings from {start_time} to {end_time}")
      raise ValueError("Failed to load listings from datastore")

    # Add is_deleted column, for tracking soft-deleted listings
    self.listing_df['is_deleted'] = False
    
    self.logger.info(f"Initially extracted {len(self.listing_df)} listings.")
    
    self._save_to_cache()
    self.cache.set(key=self.last_run_key, value=end_time)

    # checkpoint such that listing_df can be picked on on rerun 
    self.cache.set(key=f'{self.job_id}_delta_on_current_listing', value=self.listing_df)


    
  def _delta_extract_from_datastore(self, last_run: datetime):
    if self._was_success('extract'):
      self.logger.info("Extract stage already completed. Loading from cache.")
      self._load_from_cache()
      return

    start_time = last_run
    end_time = self.get_current_datetime()

    success, self.delta_listing_df = self.datastore.get_listings(
      use_script_for_last_update=True,    # TODO: Undo after dev.
      addedOn_start_time=start_time,
      addedOn_end_time=end_time,
      updated_start_time=start_time,
      updated_end_time=end_time,
                
      selects=self.listing_selects,
      prov_code='ON'                
    )
    
    if not success:
      self.logger.error(f"Failed to load delta listings from {start_time} to {end_time}")
      raise ValueError("Failed to load delta listings from datastore")
    
    if len(self.delta_listing_df) == 0:
      self.logger.warning(f"No new listings found between {start_time} and {end_time}")
      # Create an empty DataFrame with the expected structure, but no dummy row
      self.delta_listing_df = pd.DataFrame(columns=self.listing_selects + ['_id'])
      self.delta_listing_df.reset_index(drop=True, inplace=True)
    else:
      self.logger.info(f'Loaded {len(self.delta_listing_df)} listings from {start_time} to {end_time}')

    listing_df = self.cache.get('on_current_listing')
    self.delta_listing_df['is_deleted'] = False  # New delta listings are not deleted

    self.listing_df = pd.concat([listing_df, self.delta_listing_df], ignore_index=True)

    # if all operations are successful, update the cache
    self._save_to_cache()
    self.cache.set(key=self.last_run_key, value=end_time)

    # checkpoint the delta cache, such that this can be picked on on rerun 
    self.cache.set(key=f'{self.job_id}_delta_on_current_listing', value=self.delta_listing_df)


  def end_of_mth_run(self):
    self.logger.info("End of month run")
    self.pre_end_of_mth_run()

    if self._was_success('end_of_mth_run'):
      self.logger.info("End of month run already successful.")
      return

    today = self.get_current_datetime()
    if today.day != 1:
      self.logger.warning("Today is not the 1st of the month. Skipping end of month run.")
      self._mark_success('end_of_mth_run')   # 'cos nothing needs to be done, this is still marked a "success"
      return

    try:
      # (1) Compute last month's metrics
      if not self._was_success('compute_last_month_metrics'):
        self.compute_last_month_metrics()
        self._archive_results()
        self._mark_success('compute_last_month_metrics')

      # (2) Remove deleted listings from df and ES tracking index
      if not self._was_success('remove_deleted_listings'):
        success, failed = self.remove_deleted_listings()
        total_attempts = success + len(failed)
        if total_attempts != 0 and success / total_attempts < self.LOAD_SUCCESS_THRESHOLD:
          self.logger.error(f"Less than 50% success rate. Only {success} out of {total_attempts} documents deleted.")
          raise ValueError("Failed to remove deleted listings. This must be successful to proceed.")      
        else:
          self._mark_success('remove_deleted_listings')

      # (3) Update market trends index
      if not self._was_success('update_mkt_trends'):
        if self.last_mth_metrics_results is None:
          # load from archive
          self.last_mth_metrics_results = self.archiver.retrieve('last_mth_metrics_results')
          self.logger.info("Loaded last month's metrics results from archive.")

        success, failed = self.update_mkt_trends()
        total_attempts = success + len(failed)
        if success / total_attempts < 0.5:
          self.logger.error(f"Less than 50% success rate. Only {success} out of {total_attempts} documents updated.")
          raise ValueError("Failed to update market trends index. This must be successful to proceed.")
        else:
          self._mark_success('update_mkt_trends')

      self._mark_success('end_of_mth_run')

    except Exception as e:
      self.logger.error(f"Unexpected error during end of month run: {e}", exc_info=True)
      raise

    finally:
      self.post_end_of_mth_run()

      
  def update_es_tracking_index(self):

    if self.delta_listing_df is None:
      self.delta_listing_df = self.cache.get('delta_on_current_listing')
      if self.delta_listing_df is not None:
        self.logger.info(f"Loaded {len(self.delta_listing_df)} delta listings from cache.")

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

    # Deduplicate listings based on _id, keeping the last (most current) entry
    self.listing_df = self.listing_df.sort_values('lastUpdate').drop_duplicates('_id', keep='last')

    # This is now done during extract
    # Filter out soft-deleted listings
    # active_listings = self.listing_df[~self.listing_df['is_deleted']]
    # self.logger.info(f'# of listing before filtering out soft-deleted: {len(self.listing_df)}')
    # self.logger.info(f'# of listing after filtering out soft-deleted: {len(active_listings)}')

    # Convert 'addedOn' to datetime if it's not already
    self.listing_df.addedOn = pd.to_datetime(self.listing_df.addedOn)

    # Expand guid column
    expanded_df = self.listing_df.assign(geog_id=self.listing_df['guid'].str.split(',')).explode('geog_id')

    # Function to calculate metrics for both specific property types and 'ALL'
    def calculate_metrics(group):
        return pd.Series({
            'median_price': group['price'].median(),
            'new_listings_count': (group['addedOn'].dt.to_period('M').astype(str) == last_month).sum()
        })

    # Calculate metrics for specific property types
    results = expanded_df.groupby(['geog_id', 'propertyType']).apply(calculate_metrics).reset_index()

    # Calculate metrics for 'ALL' property type
    all_property_types = expanded_df.groupby('geog_id').apply(calculate_metrics).reset_index()
    all_property_types['propertyType'] = 'ALL'

    # Combine results
    final_results = pd.concat([results, all_property_types], ignore_index=True)

    # Reorder columns
    final_results = final_results[['geog_id', 'propertyType', 'median_price', 'new_listings_count']]

    final_results.new_listings_count = final_results.new_listings_count.fillna(0).astype(int)

    self.logger.info(f'Calculated metrics for {len(final_results)} (geog_id, propertyType) pairs, including "ALL" property type')

    self.last_mth_metrics_results = final_results
  

  def remove_deleted_listings(self):
    """
    Remove deleted listings from Elasticsearch tracking index.
    this is to be run at the 1st min of a new month after compute_last_month_metrics(), 
    We do not want deleted stuff to roll over to next month.
    """

    current_date = self.get_current_datetime()

    first_day_current_month = current_date.replace(day=1)
    last_month_start = (first_day_current_month - timedelta(days=1)).replace(day=1)
    last_month_end = first_day_current_month - timedelta(days=1)

    last_month_start = last_month_start.date()
    last_month_end = last_month_end.date()

    # len_before = len(self.listing_df)
    self.logger.info(f'Remove deleted listings from {last_month_start} to {last_month_end}')
    deleted_listings_df = self.bq_datastore.get_deleted_listings(start_time=last_month_start, end_time=last_month_end)
    self.logger.info(f'Found {len(deleted_listings_df)} deleted listings')

    if len(deleted_listings_df) == 0:  # no deleted listings
      return

    # (1) Soft delete from self.listing_df
    deleted_listing_ids = set(deleted_listings_df['listingId'].tolist())
    existing_listing_ids = set(self.listing_df['_id'])
    soft_deleted_ids = deleted_listing_ids.intersection(existing_listing_ids)
    # idxs_to_remove = self.listing_df.q("_id.isin(@deleted_listing_ids)").index
    # self.listing_df.drop(index=idxs_to_remove, inplace=True)
    # self.listing_df.reset_index(drop=True, inplace=True)

    # len_after = len(self.listing_df)
    # self.logger.info(f'Removed {len_before - len_after} deleted listings from current listings')

    self.listing_df.loc[self.listing_df['_id'].isin(soft_deleted_ids), 'is_deleted'] = True
    self.logger.info(f'Soft deleted {len(soft_deleted_ids)} listings in self.listing_df')

    # (2) Remove from Elasticsearch tracking index
    def generate_actions():
      for _, row in deleted_listings_df.iterrows():
        yield {
          "_op_type": "delete",
          "_index": self.datastore.listing_tracking_index_name,
          "_id": row['listingId']
        }

    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)
    self.logger.info(f"Successfully deleted {success} documents from Elasticsearch")
    if failed:
      self.logger.error(f"Failed to delete {len(failed)} documents from Elasticsearch")

    if self.cache.cache_dir:
      self._save_to_cache()

    return success, failed


  def update_mkt_trends(self):
    """
    Update the market trends index with last month's median asking price and new listings count.
    """
    
    last_month = (self.get_current_datetime().replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    # Modified last_month to simulate a diff month
    # last_month = (datetime.now().replace(day=1) + timedelta(days=1)).strftime('%Y-%m')

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
            
            // Function to update or append metric
            void updateMetric(String metricName, Map newEntry) {
              if (ctx._source.metrics[metricName] == null) {
                ctx._source.metrics[metricName] = [];
              }
              int existingIndex = -1;
              for (int i = 0; i < ctx._source.metrics[metricName].size(); i++) {
                if (ctx._source.metrics[metricName][i].month == newEntry.month) {
                  existingIndex = i;
                  break;
                }
              }
              if (existingIndex >= 0) {
                ctx._source.metrics[metricName][existingIndex] = newEntry;
              } else {
                ctx._source.metrics[metricName].add(newEntry);
              }
            }
            
            // Update last_mth_median_asking_price
            updateMetric('last_mth_median_asking_price', params.median_price);
            
            // Update last_mth_new_listings
            updateMetric('last_mth_new_listings', params.new_listings_count);
            
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
              "last_mth_median_asking_price": [{
                "month": last_month,
                "value": row['median_price']
              }],
              "last_mth_new_listings": [{
                "month": last_month,
                "value": int(row['new_listings_count'])
              }]
            },
            "last_updated": self.get_current_datetime().isoformat()
          }
        }
 
    # Perform bulk update with error handling
    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)
    
    self.logger.info(f"Successfully updated {success} documents in market trends index")
    if failed:
      self.logger.error(f"Failed to update {len(failed)} documents in market trends index")
    
    return success, failed
  

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
    self.listing_df = self.cache.get('on_current_listing')
    self.logger.info(f"Loaded {len(self.listing_df)} listings from cache.")


  def _save_to_cache(self):
    super()._save_to_cache()
    self.cache.set('on_current_listing', self.listing_df)
    self.logger.info(f"Saved {len(self.listing_df)} listings to cache.")


  def _archive_results(self):
    if hasattr(self, 'last_mth_metrics_results') and self.last_mth_metrics_results is not None:
      if not self.archiver.archive(self.last_mth_metrics_results, 'last_mth_metrics_results'):
        self.logger.error("Failed to archive last month's metrics results")
        raise ValueError("Fatal error archiving last_mth_metrics_results. This must be successful to proceed.")
      else:
        self.logger.info("Successfully archived last month's metrics results")
    else:
      self.logger.error("Fatal error: last_mth_metrics_results not found or is None. Unable to archive.")
      raise ValueError("Fatal error: last_mth_metrics_results not found or is None")


  def cleanup(self):
    super().cleanup()
    self.cache.delete('on_current_listing')
    # self.cache.delete('delta_on_current_listing')

    # reset instance variables
    self.listing_df = None
    self.delta_listing_df = None
    self.last_mth_metrics_results = None

    self.logger.info("Cleanup completed. All cached data has been cleared and instance variables reset.")


  def delete_checkpoints_data(self):
    self.cache.delete(f'{self.job_id}_delta_on_current_listing')

  def pre_end_of_mth_run(self):
    if getattr(self, 'simulate_failure_at', None) == 'end_of_mth_run':
      raise Exception("Simulated failure in end_of_mth_run")
    
  def post_end_of_mth_run(self):
    pass

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