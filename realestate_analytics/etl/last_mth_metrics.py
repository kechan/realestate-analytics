from typing import Dict, List, Union
import pandas as pd
from datetime import datetime, date

from pathlib import Path

from ..data.caching import FileBasedCache
from ..data.es import Datastore
from elasticsearch.helpers import scan, bulk

class LastMthMetricsProcessor:
  def __init__(self, datastore: Datastore, cache_dir: Union[str, Path] = None):
    self.datastore = datastore
    self.listing_df = None
    self.delta_listing_df = None

    self.cache_dir = Path(cache_dir) if cache_dir else None
    if self.cache_dir:
      self.cache_dir.mkdir(parents=True, exist_ok=True)

    self.cache = FileBasedCache(cache_dir=self.cache_dir)
    print(self.cache.cache_dir)

    self.last_run_key = 'LastMthMetrics_last_run'

  def extract(self, first_load=False, from_cache=False):
    if from_cache:
      self._load_from_cache(first_load)
    else:
      if first_load:
        self._initialize_extract_from_datastore()
      else:
        last_run = self.cache.get(self.last_run_key)
        self._delta_extract_from_datastore(last_run=last_run)

      self._save_to_cache(first_load)

  def load(self):
    success, failed = self.update_es_tracking_index()
    return success, failed

  def _initialize_extract_from_datastore(self):
    # get everything
    end_time = datetime.now()
    _, self.listing_df = self.datastore.get_listings(
      selects=['listingType', 'searchCategoryType', 
                'guid',
                'beds', 'bedsInt', 
                'baths', 'bathsInt',
                'lat', 'lng',
                'price',
                'addedOn',
                'lastUpdate'
                ],
                prov_code='ON',
                addedOn_end_time=end_time,
                updated_end_time=end_time
                )
    
    self.cache.set(key='on_current_listing', value=self.listing_df)
    self.cache.set(key=self.last_run_key, value=end_time)

    
  def _delta_extract_from_datastore(self, last_run: datetime):
    start_time = last_run
    end_time = datetime.now()

    _, self.delta_listing_df = self.datastore.get_listings(selects=['listingType', 'searchCategoryType', 
                                                      'guid',
                                                      'beds', 'bedsInt', 
                                                      'baths', 'bathsInt',
                                                      'lat', 'lng',
                                                      'price',
                                                      'addedOn',
                                                      'lastUpdate'
                                                      ],
                                                      prov_code='ON',
                                                      addedOn_start_time=start_time,
                                                      addedOn_end_time=end_time,
                                                      updated_start_time=start_time,
                                                      updated_end_time=end_time
                                                      )
    print(f'Loaded {len(self.delta_listing_df)} listings from {start_time} to {end_time}')

    # save the delta to cache
    # TODO: this is needed only during dev where we extract from PROD but load to UAT
    self.cache.set(key='delta_on_current_listing', value=self.delta_listing_df)

    # concat to the existing cache
    listing_df = self.cache.get('on_current_listing')
    self.listing_df = pd.concat([listing_df, self.delta_listing_df], ignore_index=True)

    self.cache.set(key=self.last_run_key, value=end_time)


  def update_es_tracking_index(self):

    # TODO: this is needed on during dev as we store delta in cache, instead of running
    # straight through
    if self.delta_listing_df is None:
      self.delta_listing_df = self.cache.get('delta_on_current_listing')

    if self.delta_listing_df is not None:
      to_be_updated_listing_df = self.delta_listing_df
    else:
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

    print(f"Successfully updated {success} documents")
    if failed:
      print(f"Failed to update {len(failed)} documents")

    return success, failed


  def compute_last_month_metrics(self):
    """
    this is to be run at the 1st min of a new month, so the last month can be calculated.
    """
    # Determine the last month
    today = datetime.now()
    last_month = (today.replace(day=1) - pd.Timedelta(days=1)).strftime('%Y-%m')
    print(f'Calculating metrics for {last_month}')

    # Deduplicate listings based on _id, keeping the last (most current) entry
    self.listing_df = self.listing_df.sort_values('lastUpdate').drop_duplicates('_id', keep='last')

    # Convert 'addedOn' to datetime if it's not already
    self.listing_df.addedOn = pd.to_datetime(self.listing_df.addedOn)

    # Expand guid column
    expanded_df = self.listing_df.assign(geog_id=self.listing_df['guid'].str.split(',')).explode('geog_id')

    # Calculate median price
    median_prices = expanded_df.groupby(['geog_id', 'propertyType'])['price'].median().reset_index()

    # Calculate new listings count
    new_listings = expanded_df[expanded_df['addedOn'].dt.to_period('M').astype(str) == last_month]
    new_listings_count = new_listings.groupby(['geog_id', 'propertyType']).size().reset_index(name='new_listings_count')

    # Merge median prices and new listings count
    results = pd.merge(median_prices, new_listings_count, on=['geog_id', 'propertyType'], how='outer')

    # Fill NaN values with 0 for new_listings_count
    results['new_listings_count'] = results['new_listings_count'].fillna(0)

    # Calculate metrics for 'ALL' property type
    all_property_types = results.groupby('geog_id').agg({
        'price': 'median',
        'new_listings_count': 'sum'
    }).reset_index()
    all_property_types['propertyType'] = 'ALL'

    # Combine results
    final_results = pd.concat([results, all_property_types], ignore_index=True)

    # Reorder columns
    final_results = final_results[['geog_id', 'propertyType', 'price', 'new_listings_count']]

    # Rename 'price' column to 'median_price'
    final_results.rename(columns={'price': 'median_price'}, inplace=True)

    return final_results

    
  def _load_from_cache(self, first_load):
    # cache_file = self.cache_dir / f"{'full' if first_load else 'delta'}_listing_cache_df"
    if not self.cache.cache_dir:
      raise ValueError("Cache directory not set. Cannot load from cache.")
    
    # regard less of first_load, we will always load from the same cache
    self.listing_df = self.cache.get('on_current_listing')
    print(f"Loaded {len(self.listing_df)} listings from cache.")
    
    
  def _save_to_cache(self, first_load):
    # cache_file = self.cache_dir / f"{'full' if first_load else 'delta'}_listing_cache_df"
    # self.listing_df.to_feather(cache_file)

    self.cache.set(key='on_current_listing', value=self.listing_df)

    print(f"Saved {len(self.listing_df)} listings to cache.")
    

# For dev
if __name__ == "__main__":
  datastore = Datastore()  # Assume this is properly initialized

  metrics = LastMthMetricsProcessor(datastore=datastore)
  
  # First run

  # run this with production ES
  metrics.extract(first_load=True)

  # run this with test ES
  metrics.extract(first_load=True, from_cache=True)
  success, failed = metrics.load()
  
  # Subsequent runs (do this later)
  # metrics.extract(first_load=False)
  # metrics.load()