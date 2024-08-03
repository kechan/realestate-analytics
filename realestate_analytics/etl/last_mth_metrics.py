from typing import Dict, List, Union
import pandas as pd
from datetime import datetime, date, timedelta

from pathlib import Path
import logging

from ..data.caching import FileBasedCache
from ..data.es import Datastore
from ..data.bq import BigQueryDatastore
from ..data.archive import Archiver
from elasticsearch.helpers import scan, bulk
from elasticsearch.exceptions import NotFoundError, ConnectionError, RequestError, TransportError

import logging, sys, pytz

# Set up logging to write to stdout
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class LastMthMetricsProcessor:
  def __init__(self, datastore: Datastore, 
               bq_datastore: BigQueryDatastore = None,
               cache_dir: Union[str, Path] = None,
               archive_dir: Union[str, Path] = None):
    self.logger = logging.getLogger(self.__class__.__name__)

    self.datastore = datastore
    self.bq_datastore = bq_datastore

    self.listing_df = None
    self.delta_listing_df = None

    self.cache_dir = Path(cache_dir) if cache_dir else None
    if self.cache_dir:
      self.cache_dir.mkdir(parents=True, exist_ok=True)

    self.cache = FileBasedCache(cache_dir=self.cache_dir)
    self.logger.info(f'Cache dir: {self.cache.cache_dir}')

    self.last_run_key = 'LastMthMetrics_last_run'
    last_run = self.cache.get(self.last_run_key)
    self.logger.info(f"Last run: {last_run}")

    self.archiver = Archiver(archive_dir or self.cache.cache_dir / 'archives')

    self.use_utc = True  # Set this to True to use UTC


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

        self._save_to_cache()

    except (ConnectionError, RequestError, TransportError) as e:
      self.logger.error(f"Elasticsearch error during extract: {e}", exc_info=True)
      raise
    except Exception as e:
      self.logger.error(f"Unexpected error during extract: {e}", exc_info=True)
      raise

  def load(self):
    try:
      success, failed = self.update_es_tracking_index()
      return success, failed
    except (ConnectionError, RequestError, TransportError) as e:
      self.logger.error(f"Elasticsearch error during load: {e}", exc_info=True)
      raise
    except Exception as e:
      self.logger.error(f"Unexpected error during load: {e}", exc_info=True)
      raise

  def _initialize_extract_from_datastore(self):
    # get everything till now
    start_time = datetime(1970, 1, 1)     # distant past
    end_time = self.get_current_datetime()
    success, self.listing_df = self.datastore.get_listings(
      use_script_for_last_update=True,    # TODO: Undo after dev.

      addedOn_start_time=start_time,
      updated_start_time=start_time,
      
      addedOn_end_time=end_time,
      updated_end_time=end_time,

      selects=['listingType', 'searchCategoryType', 
                'guid',
                'beds', 'bedsInt', 
                'baths', 'bathsInt',
                'lat', 'lng',
                'price',
                'addedOn',
                'lastUpdate'
                ],
                prov_code='ON'                
                )
    
    if not success:
      self.logger.error(f"Failed to load listings from {start_time} to {end_time}")
      raise ValueError("Failed to load listings from datastore")
    
    self.logger.info(f"Initially extracted {len(self.listing_df)} listings.")
    
    self.cache.set(key=self.last_run_key, value=end_time)

    
  def _delta_extract_from_datastore(self, last_run: datetime):
    start_time = last_run
    end_time = self.get_current_datetime()

    success, self.delta_listing_df = self.datastore.get_listings(
      use_script_for_last_update=True,    # TODO: Undo after dev.
      addedOn_start_time=start_time,
      addedOn_end_time=end_time,
      updated_start_time=start_time,
      updated_end_time=end_time,
                
      selects=['listingType', 'searchCategoryType', 
                'guid',
                'beds', 'bedsInt', 
                'baths', 'bathsInt',
                'lat', 'lng',
                'price',
                'addedOn',
                'lastUpdate'
                ],
                prov_code='ON'                
                )
    
    if not success:
      self.logger.error(f"Failed to load delta listings from {start_time} to {end_time}")
      raise ValueError("Failed to load delta listings from datastore")
    
    self.logger.info(f'Loaded {len(self.delta_listing_df)} listings from {start_time} to {end_time}')

    listing_df = self.cache.get('on_current_listing')
    self.listing_df = pd.concat([listing_df, self.delta_listing_df], ignore_index=True)

    # if all operations are successful, update the cache
    self.cache.set(key=self.last_run_key, value=end_time)

    # save the delta by itself to cache
    # TODO: this is needed only during dev where we extract from PROD but load to UAT
    self.cache.set(key='delta_on_current_listing', value=self.delta_listing_df)


  def end_of_mth_run(self):
    today = self.get_current_datetime()
    if today.day == 1:
      self.compute_last_month_metrics()

      if hasattr(self, 'last_mth_metrics_results') and self.last_mth_metrics_results is not None:
        if not self.archiver.archive(self.last_mth_metrics_results, 'last_mth_metrics_results'):
          self.logger.error("Failed to archive last month's metrics results")
          raise ValueError("Fatal error archiving last_mth_metrics_results. This must be successful to proceed.")
        else:
          self.logger.info("Successfully archived last month's metrics results")
      else:
        self.logger.error("Fatal error last_mth_metrics_results not found or is None. Unable to archive.")
        raise ValueError("Fatal error archiving last_mth_metrics_results. This must be successful to proceed.")

      success, failed = self.remove_deleted_listings()  # TODO: should we handle success and failure here?
      success, failed = self.update_mkt_trends()
    else:
      self.logger.warning("Today is not the 1st of the month.")


  def update_es_tracking_index(self):

    # TODO: this is needed on during dev as we store delta in cache, instead of running
    # straight through
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

    len_before = len(self.listing_df)
    self.logger.info(f'Remove deleted listings from {last_month_start} to {last_month_end}')
    deleted_listings_df = self.bq_datastore.get_deleted_listings(start_time=last_month_start, end_time=last_month_end)
    self.logger.info(f'Found {len(deleted_listings_df)} deleted listings')

    if len(deleted_listings_df) == 0:  # no deleted listings
      return

    deleted_listing_ids = deleted_listings_df['listingId'].tolist()
    idxs_to_remove = self.listing_df.q("_id.isin(@deleted_listing_ids)").index
    self.listing_df.drop(index=idxs_to_remove, inplace=True)
    self.listing_df.reset_index(drop=True, inplace=True)
    len_after = len(self.listing_df)

    self.logger.info(f'Removed {len_before - len_after} deleted listings from current listings')

    # Remove from Elasticsearch tracking index
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
          "_index": self.datastore.mkt_trends_ts_index_name,
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
                "date": last_month,
                "value": row['median_price']
              },
              "new_listings_count": {
                "date": last_month,
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
                "date": last_month,
                "value": row['median_price']
              },
              "last_mth_new_listings": {
                "date": last_month,
                "value": int(row['new_listings_count'])
              }
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
    in the mkt_trends_ts index on Elasticsearch.
    """
    def generate_actions():
      query = {
        "query": {
          "match_all": {}
        }
      }
      for hit in scan(self.datastore.es, 
                      index=self.datastore.mkt_trends_ts_index_name, 
                      query=query):
        yield {
          "_op_type": "update",
          "_index": self.datastore.mkt_trends_ts_index_name,
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
    # cache_file = self.cache_dir / f"{'full' if first_load else 'delta'}_listing_cache_df"
    if not self.cache.cache_dir:
      raise ValueError("Cache directory not set. Cannot load from cache.")
    
    # regard less of first_load, we will always load from the same cache
    self.listing_df = self.cache.get('on_current_listing')
    self.logger.info(f"Loaded {len(self.listing_df)} listings from cache.")
    
    
  def _save_to_cache(self):
    if not self.cache.cache_dir:
      self.logger.error("Cache directory not set. Cannot save to cache.")
      raise ValueError("Cache directory not set. Cannot save to cache.")
    self.cache.set(key='on_current_listing', value=self.listing_df)

    self.logger.info(f"Saved {len(self.listing_df)} listings to cache.")
    
  def cleanup(self):
    """
    Clean up all cached data and reset instance variables.
    """
    # Clear cache entries
    self.cache.delete(self.last_run_key)
    self.cache.delete('on_current_listing')
    self.cache.delete('delta_on_current_listing')

    # Reset instance variables
    self.listing_df = None
    self.delta_listing_df = None

    self.logger.info("Cleanup completed. All cached data has been cleared and instance variables reset.")


  def close(self):
    self.datastore.close()
    self.bq_datastore.close()


  def get_current_datetime(self):
    """
    Returns the current datetime in UTC or local time based on the use_utc flag.
    """
    if self.use_utc:
      return datetime.now(pytz.utc).replace(tzinfo=None)
    else:
      return datetime.now()
    
# For dev
if __name__ == "__main__":
  datastore = Datastore()  # Assume this is properly initialized

  metrics = LastMthMetricsProcessor(datastore=datastore)
  
  metrics.extract(from_cache=False)   # run this against PROD
  # switch SSH tunneling/IP if needed.
  metrics.extract(from_cache=True)    # run this against UAT
  success, failed = metrics.load()
  
  # Repeat the above about once a day (to ensure we capture listings before they get deleted)/

  # At the 1st min of a new month
  metrics.end_of_mth_run()


""" Sample last_mth_metrics_results dataframe
+--------------+----------------+---------------+-------------------+
| geog_id      | propertyType   | median_price  | new_listings_count|
+--------------+----------------+---------------+-------------------+
| g30_dpz89rm7 | SEMI-DETACHED  | 1150000.0     | 48.0              |
+--------------+----------------+---------------+-------------------+
"""

""" Sample json in rlp_mkt_trends_ts_current
  {'geog_id': 'g30_dpz89rm7',
  'propertyType': 'SEMI-DETACHED',
  'geo_level': 30,
  'metrics': {'median_price': [{'date': '2023-01', 'value': 1035000.0},
    {'date': '2023-10', 'value': 1192000.0},
    {'date': '2023-11', 'value': 1037500.0},
    {'date': '2023-12', 'value': 976000.0},
    {'date': '2024-01', 'value': 1075000.0},
    {'date': '2024-02', 'value': 1201054.0},
    {'date': '2024-03', 'value': 1175333.0},
    {'date': '2024-04', 'value': 1159444.0},
    {'date': '2024-06', 'value': 915000.0}],
   'median_dom': [{'date': '2023-01', 'value': 3},
    {'date': '2023-10', 'value': 9},
    {'date': '2023-11', 'value': 15},
    {'date': '2023-12', 'value': 33},
    {'date': '2024-01', 'value': 9},
    {'date': '2024-02', 'value': 8},
    {'date': '2024-03', 'value': 8},
    {'date': '2024-04', 'value': 8},
    {'date': '2024-06', 'value': 49}],
   'last_mth_median_asking_price': {'date': '2024-06', 'value': 1150000.0},
   'last_mth_new_listings': {'date': '2024-06', 'value': 48}},
  'last_updated': '2024-07-24T21:33:28.443159'}

"""