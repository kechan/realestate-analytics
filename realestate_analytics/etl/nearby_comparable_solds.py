from typing import Dict, List, Tuple, Union

import time, math, json, logging
from collections import defaultdict
import numpy as np
import pandas as pd
from math import radians, sin, cos, sqrt, atan2
from pathlib import Path
from datetime import datetime, timedelta
import pytz

from .base_etl import BaseETLProcessor
from ..data.caching import FileBasedCache
from ..data.es import Datastore
from ..data.bq import BigQueryDatastore
from elasticsearch.helpers import scan, bulk

import realestate_core.common.class_extensions
from realestate_core.common.class_extensions import *

def scalar_calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
  R = 6371.0  # Earth radius in kilometers
  lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
  dlon = lon2 - lon1
  dlat = lat2 - lat1
  a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
  c = 2 * atan2(sqrt(a), sqrt(1 - a))
  distance = R * c
  return distance

def calculate_distance(coords1: np.ndarray, coords2: np.ndarray) -> np.ndarray:
  """
    Calculate the Haversine distance matrix between two sets of coordinates.
    
    This function computes the pairwise distances between two sets of geographical coordinates
    using the Haversine formula. The coordinates are provided as numpy arrays where each row
    represents a point, with the first column being the latitude and the second column the longitude.
    
    Parameters:
    - coords1 (np.ndarray): A numpy array of shape (n, 2) containing n points with latitude and longitude.
    - coords2 (np.ndarray): A numpy array of shape (m, 2) containing m points with latitude and longitude.
    
    Returns:
    - np.ndarray: A numpy array of shape (n, m) containing the distances between each pair of points
      from coords1 and coords2, measured in kilometers.
  """
  R = 6371.0  # Earth radius in kilometers
  
  # Convert latitude and longitude from degrees to radians
  coords1 = np.radians(coords1)
  coords2 = np.radians(coords2)
  
  lat1, lon1 = coords1[:, 0][:, None], coords1[:, 1][:, None]
  lat2, lon2 = coords2[:, 0], coords2[:, 1]

  dlon = lon2 - lon1
  dlat = lat2 - lat1
  
  a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
  c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
  
  distance_matrix = R * c
  
  return distance_matrix

def compare_comparable_results(prev_result: Dict[str, Dict[str, List]], 
                               current_result: Dict[str, Dict[str, List]]) -> Dict[str, Dict[str, List]]:
  """
  Compare previous and current comparable sold listings results,
  keeping differences and new entries.

  Args:
  prev_result (Dict): The previous comparable sold listings result.
  current_result (Dict): The current comparable sold listings result.

  Returns:
  Dict: A dictionary containing the differences and new entries.
  """
  diff_result = {}

  for listing_id, current_data in current_result.items():
    if listing_id not in prev_result:
      # New listing, add it to the diff result
      diff_result[listing_id] = current_data
    else:
      prev_data = prev_result[listing_id]
      if (set(current_data['sold_listing_id']) != set(prev_data['sold_listing_id']) or
          len(current_data['distance_km']) != len(prev_data['distance_km']) or
          any(not math.isclose(c, p, rel_tol=1e-9, abs_tol=1e-12) 
              for c, p in zip(current_data['distance_km'], prev_data['distance_km']))):
        # Listing exists in both, but data is different
        diff_result[listing_id] = current_data

  return diff_result

class NearbyComparableSoldsProcessor(BaseETLProcessor):
  def __init__(self, job_id: str, datastore: Datastore, bq_datastore: BigQueryDatastore = None, check_bq_deletions = False):
    super().__init__(job_id=job_id, datastore=datastore, bq_datastore=bq_datastore)

    self.check_bq_deletions = check_bq_deletions

    self.sold_listing_df = None
    self.listing_df = None    

    self.sold_listing_selects = [
      'mls',
      'lastTransition', 'transitions', 
      'lastUpdate',
      'listingType', 'searchCategoryType',
      'listingStatus',       
      'beds', 'bedsInt', 'baths', 'bathsInt',
      'price','soldPrice',      
      'daysOnMarket',
      'lat', 'lng',
      'city', 'neighbourhood',
      'provState',
      'guid'
    ]

    self.current_listing_selects = [
      'listingType', 'searchCategoryType',
      'guid',
      'beds', 'bedsInt', 
      'baths', 'bathsInt',
      'lat', 'lng',
      'price',
      'addedOn',
      'lastUpdate',
      'listingStatus'
    ]

    # definition of "comparables"
    # In totality, the vicinity of the property and time of sale are also involved, but these are the simpler ones
    # so we partition our sold listing set by comparable_criteria first.
    self.comparable_criteria = ['propertyType', 'bedsInt', 'bathsInt']
    
    self.listing_profile_partition = None #self.sold_listing_df.groupby(self.comparable_criteria).size().reset_index(name='population')

    # Flag to determine whether to use UTC or local time
    self.use_utc = True  # Set this to True to use UTC

    # The number of days to look back for sold and current listings in the delta load
    # this creates a margin of safety for not missing anything.
    self.DELTA_SOLD_LISTINGS_LOOKBACK_DAYS = 21
    self.DELTA_CURRENT_LISTINGS_LOOKBACK_DAYS = 3

    # Constants for get_sold_listings method
    self.MAX_DISTANCE = 1.0  # km, max distance to consider 
    self.MAX_COMPS = 12      # incl. this maximum number of comparables to return for each current listing

    self.LOAD_SUCCESS_THRESHOLD = 0.5  # minimum success rate for upsert to ES to be considered successful
    self.DISTANCE_ROUNDING_DECIMALS = 1


  def extract(self, from_cache=False):
    super().extract(from_cache=from_cache)


  def _extract_from_datastore(self, force_full_load_current=False):
    """
    Extract sold and current listings data from the datastore.

    This method performs either a full load or an incremental load of data,
    depending on whether it's the first run or a subsequent run. It handles
    both sold listings and current active listings.

    For sold listings:
    - Always performs an incremental load if not the first run.
    - Retrieves data for the past year.

    For current listings:
    - Performs a full load on first run or when force_full_load_current is True.
    - Otherwise, performs an incremental load.

    The method also handles inactive listings and removes known deletions.

    Args:
        force_full_load_current (bool, optional): If True, forces a full load
            of current listings even if it's not the first run. Defaults to False.

    Raises:
        ValueError: If there's an error retrieving data from the datastore
            or if the cache is inconsistent.

    Side Effects:
        - Updates self.sold_listing_df and self.listing_df with new data.
        - Updates the cache with the latest data.
        - Sets the last_run timestamp in the cache.
        - Marks the 'extract' stage as successful upon completion.

    Note:
        This method uses a lookback period (DELTA_SOLD_LISTINGS_LOOKBACK_DAYS and
        DELTA_CURRENT_LISTINGS_LOOKBACK_DAYS) to ensure no data is missed in
        incremental loads.
    """
    if self._was_success('extract'):  # load the cache and skip instead
      self.logger.info("Extract stage already completed. Loading from cache.")
      self._load_from_cache()
      return

    last_run = self.cache.get(self.last_run_key)
    self.logger.info(f"last_run: {last_run}")

    try:      
      if last_run is None:   # first run
        end_time = self.get_current_datetime()

        # get the sold listings from the last year till now
        success, self.sold_listing_df = self.datastore.get_sold_listings(
          start_time=end_time - timedelta(days=365),
          end_time=end_time,
          selects=self.sold_listing_selects
        )
        if not success:
          self.logger.error(f"Failed to get sold listings from {end_time - timedelta(days=365)} to {end_time}")
          raise ValueError("Failed to get sold listings.")
        
        # get every active listings up till now    
        start_time = datetime(1970, 1, 1)     # distant past
        success, self.listing_df = self.datastore.get_current_active_listings(
          use_script_for_last_update=True,      # TODO: remove after dev 
          addedOn_start_time=start_time,
          updated_start_time=start_time,
          addedOn_end_time=end_time,
          updated_end_time=end_time,
          selects=self.current_listing_selects,
          prov_code='ON',
          active=True
        )
        if not success:
          self.logger.error(f"failed to get current listings from {start_time} to {end_time}")
          raise ValueError("Failed to get current listings.")

      else:    # incremental/delta load 
        # Load existing data from cache
        self.sold_listing_df = self.cache.get(f'{self.cache_prefix}one_year_sold_listing')
        self.listing_df = self.cache.get(f'{self.cache_prefix}on_listing')

        if self.sold_listing_df is None or self.listing_df is None:
          self.logger.error("Cache is inconsistent. Missing prior sold_listing_df or listing_df.")
          raise ValueError("Cache is inconsistent. Missing prior sold_listing_df or listing_df.")

        # get the sold listings from last run till now
        start_time = last_run - timedelta(days=self.DELTA_SOLD_LISTINGS_LOOKBACK_DAYS)   # load from 21 days before last run to have bigger margin of safety.
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
        
        # Get the current ACTIVE listings
        if force_full_load_current:
          start_time = datetime(1970, 1, 1)     # distant past for full load of active listings
        else:
          start_time = last_run - timedelta(days=self.DELTA_CURRENT_LISTINGS_LOOKBACK_DAYS)   # load from 3 days before last run to have some margin of safety.
        # end time is same as above
        success, delta_listing_df = self.datastore.get_current_active_listings(
          use_script_for_last_update=True,      # TODO: remove after dev
          addedOn_start_time=start_time,
          addedOn_end_time=end_time,
          updated_start_time=start_time,
          updated_end_time=end_time,
          selects=self.current_listing_selects,
          prov_code='ON',
          active=True
          )
        if not success:
          self.logger.error(f"Failed to retrieve delta current active listings from {start_time} to {end_time}")
          raise ValueError("Failed to retrieve delta current active listings")
        self.logger.info(f'Loaded {len(delta_listing_df)} current active listings from {start_time} to {end_time}')

        # Get the current NON ACTIVE listings from last run till now
        success, delta_inactive_listing_df = self.datastore.get_current_active_listings(
          use_script_for_last_update=True,      # TODO: remove after dev
          addedOn_start_time=start_time,
          addedOn_end_time=end_time,
          updated_start_time=start_time,
          updated_end_time=end_time,
          selects=self.current_listing_selects,
          prov_code='ON',
          active=False
        )
        if not success:
          self.logger.error(f"Failed to retrieve delta current non active listings from {start_time} to {end_time}")
          raise ValueError("Failed to retrieve delta current non active listings")
        self.logger.info(f'Loaded {len(delta_inactive_listing_df)} current non active listings from {start_time} to {end_time}')

        # Merge delta with whole

        # Merging delta for sold listings
        self.sold_listing_df = pd.concat([self.sold_listing_df, delta_sold_listing_df], axis=0, ignore_index=True)
        self.sold_listing_df.drop_duplicates(subset=['_id'], inplace=True, keep='first')   # TODO: keep first to preserve hacked guid, undo this later
        
        # Filter out stuff thats older than a year
        len_sold_listing_df_before_delete = len(self.sold_listing_df)
        one_year_ago_from_now = end_time - timedelta(days=365)
        drop_idxs = self.sold_listing_df.q("lastTransition < @one_year_ago_from_now").index
        self.sold_listing_df.drop(index=drop_idxs, inplace=True)
        self.sold_listing_df.reset_index(drop=True, inplace=True)
        len_sold_listing_df_after_delete = len(self.sold_listing_df)
        self.logger.info(f'removed {len_sold_listing_df_before_delete - len_sold_listing_df_after_delete} sold listings older than a year')

        # Merging for active listings
        if force_full_load_current:
          self.listing_df = delta_listing_df
        else:
          self.listing_df = pd.concat([self.listing_df, delta_listing_df], axis=0)   # delta is active at time of this query
          self.listing_df.drop_duplicates(subset=['_id'], inplace=True, keep='last')

        # For managing non active listings
        self.listing_df = pd.concat([self.listing_df, delta_inactive_listing_df], axis=0)   # delta is non active at time of this query
        self.listing_df.drop_duplicates(subset=['_id'], inplace=True, keep='last')
        # drop the non active listings
        inactive_indices = self.listing_df.q("listingStatus != 'ACTIVE'").index
        self.logger.info(f"Dropping {len(inactive_indices)} non active listings")
        self.listing_df.drop(inactive_indices, inplace=True)

        # Remove known deletions from BQ since last run, gauranteed finally by checking ES.
        if self.check_bq_deletions:
          # Note: for the purpose of computing nearby solds, it isnt critical to remove these, 
          # as updating an already deleted listing should be a no op for ES update
          # But this is also used in Absorption Rate ETL, so we need to ensure it is at least done at the end of month snapshot
          # The responsiblity of this schedule should on the run script that invoke this class.
          self.logger.info("Checking for deleted listings in BigQuery since last run... and removed them from current listings.")
          deleted_listings_df = self.bq_datastore.get_deleted_listings(start_time=last_run, end_time=end_time)
          if len(deleted_listings_df) > 0:
            len_listing_df_before_delete = len(self.listing_df)

            deleted_listing_ids = deleted_listings_df['listingId'].tolist()
            idxs_to_remove = self.listing_df.q("_id.isin(@deleted_listing_ids)").index
            self.listing_df.drop(index=idxs_to_remove, inplace=True)
            self.listing_df.reset_index(drop=True, inplace=True)
            len_listing_df_after_delete = len(self.listing_df)
            self.logger.info(f'removed {len_listing_df_before_delete - len_listing_df_after_delete} deleted listings since last run')
          else:
            self.logger.info("No deleted listings found in BigQuery for the specified time range.")

          self.delete_by_checking_es()   # we are piggbacking on this to remove from ES as well, although this isnt about BQ
        else:
          self.logger.info("Skipping check for deleted listings in BigQuery (as per configuration).")

        self.listing_df.reset_index(drop=True, inplace=True)

      # if all operations are successful, update the data cache 
      self._save_to_cache()
      self.cache.set(self.last_run_key, end_time)   # save to cache: last run time, and the data.
      self._mark_success('extract')

    except Exception as e:
      self.logger.error(f"Error occurred during extract: {e}")
      raise  


  def _load_from_cache(self):
    super()._load_from_cache()
    
    self.sold_listing_df = self.cache.get(f'{self.cache_prefix}one_year_sold_listing')
    self.listing_df = self.cache.get(f"{self.cache_prefix}on_listing")

    if self.sold_listing_df is None or self.listing_df is None:
      self.logger.error("Missing sold_listing_df or listing_df in cache.")
      raise ValueError("Missing sold_listing_df or listing_df in cache.")

    self.logger.info(f"Loaded {len(self.sold_listing_df)} sold listings and {len(self.listing_df)} current listings from cache.")


  def _save_to_cache(self):
    super()._save_to_cache()
    
    self.cache.set(f'{self.cache_prefix}one_year_sold_listing', self.sold_listing_df)
    self.cache.set(f'{self.cache_prefix}on_listing', self.listing_df)

    self.logger.info(f"Saved {len(self.sold_listing_df)} sold listings and {len(self.listing_df)} current listings to cache.")


  def transform(self):
    if self._was_success('transform'):  # load the cache and skip ahead
      self.logger.info("Transform already successful. Loading checkpoint from cache.")
      diff_result_json = self.cache.get(f"{self.cache_prefix}{self.job_id}_transform_diff_result").replace("'", '"')
      diff_result_json = diff_result_json.replace("'", '"')
      self.diff_result = json.loads(diff_result_json)
      return

    try:
      self._process_listing_profile_partition()
      self.comparable_sold_listings_result, _ = self.get_sold_listings()

      # optimize such that we dont update on things that didnt change from last run.
      # Retrieve previous result from cache
      # prev_result = eval(self.cache.get(f'{self.cache_prefix}comparable_sold_listings_result')) # it deserializes back to dict
      prev_result_json = self.cache.get(f'{self.cache_prefix}comparable_sold_listings_result')
      if prev_result_json:
        prev_result_json = prev_result_json.replace("'", '"')
        self.logger.info("Found previous comparable_sold_listings_result in cache. Deserializing...")
        try:
          prev_result = json.loads(prev_result_json)   # Deserialize JSON safely
          self.logger.info("Successfully deserialized comparable_sold_listings_result.")
        except json.JSONDecodeError:        
          prev_result = None
          self.logger.error("Failed to deserialize comparable_sold_listings_result. Setting to None. Please investigate")
      else:
        prev_result = None
        self.logger.info("No previous comparable_sold_listings_result found in cache.")

      if prev_result is not None:    
        self.diff_result = compare_comparable_results(prev_result, self.comparable_sold_listings_result)
      else:
        self.diff_result = self.comparable_sold_listings_result
      
      # Cache the full current result for next time
      self.cache.set(f'{self.cache_prefix}comparable_sold_listings_result', self.comparable_sold_listings_result)
      self.logger.info(f"{len(self.diff_result)} listings to be updated.")
      
      # checkpoint the diff results (this will be deleted when the entire ETL is completed)
      self.cache.set(f"{self.cache_prefix}{self.job_id}_transform_diff_result", self.diff_result)  # save the diff result in case a recovery is needed.
      self._mark_success('transform')

    except Exception as e:
      self.logger.error(f"Error occurred during transform: {e}")
      raise


  def load(self):
    if self._was_success('load'): 
      self.logger.info("Load already successful.")
      return 0, []  # nothing to do 

    success, failed = self.update_datastore(self.diff_result)
    total_attempts = success + len(failed)

    if total_attempts != 0 and (success / total_attempts) < self.LOAD_SUCCESS_THRESHOLD:
      self.logger.error(f"Less than {self.LOAD_SUCCESS_THRESHOLD*100}% success rate. Only {success} out of {total_attempts} documents updated.")

      self.datastore.summarize_update_failures(failed)
    else:
      self._mark_success('load')

    return success, failed


  def cleanup(self):
    super().cleanup()
    
    # remove resident cache data
    self.cache.delete(f'{self.cache_prefix}one_year_sold_listing')
    self.cache.delete(f'{self.cache_prefix}on_listing')
    self.cache.delete(f'{self.cache_prefix}comparable_sold_listings_result')

    self.delete_checkpoints_data()

    # Reset instance variables
    self.sold_listing_df = None
    self.listing_df = None
    self.listing_profile_partition = None
    self.comparable_sold_listings_result = None
    self.diff_result = None

    self.logger.info("Cleanup completed. All cached data has been cleared and instance variables reset.")

    # Manipulating or deleting from ES should be more deliberately rather than bundling them under cleanup().
    # updated, failures = self.delete_comparable_sold_listings()
    # self.logger.info(f"Removed comparable_sold_listings from {updated} documents with {len(failures)} failures.")


  def get_sold_listings(self):
    """
      Find comparable sold listings for each current listing based on property profiles.

      Args:
      listing_profile_partition (pd.DataFrame): DataFrame containing unique property profiles (type, bedsInt, bathsInt).
      listing_df (pd.DataFrame): DataFrame containing current property listings.
      sold_listing_df (pd.DataFrame): DataFrame containing sold property listings.

      Returns:
      Tuple[Dict[str, Dict[str, List]], Dict[str, float]]: 
      - A dictionary where keys are current listing IDs and values are dictionaries
        containing lists of comparable sold listing IDs and their distances.
      - A dictionary of profile timings, where keys are profile strings and values are processing times in seconds.

      Example:
      {
        '21483963': {
          'sold_listing_id': ['on-17-c7289144', 'on-17-c8123830', ...],
          'distance_km': [1.2, 0.8, ...]
        },
        '21969076': {
          'sold_listing_id': ['on-13-1353569', 'on-17-c7018686', ...],
          'distance_km': [0.5, 0.3, ...]
        },
        ...
      }
    """
    results = {}
    profile_timings = defaultdict(float)

    for _, row in self.listing_profile_partition.iterrows():
      property_type, bedsInt, bathsInt = row['propertyType'], row['bedsInt'], row['bathsInt']
      profile_key = f'{property_type}_{bedsInt}_{bathsInt}'
      profile_start_time = time.time()

      sold_partition_df = self._get_partitioned_df(self.sold_listing_df, property_type, bedsInt, bathsInt)
      listing_partition_df = self._get_partitioned_df(self.listing_df, property_type, bedsInt, bathsInt)

      if not listing_partition_df.empty:
        self.logger.info(f'Profile: {profile_key}, # of sold: {len(sold_partition_df)}, # of current: {len(listing_partition_df)}')
        
        filtered_distances, filtered_indices = self._compute_nearest_sold_properties(
            listing_partition_df[['lat', 'lng']].values,
            sold_partition_df[['lat', 'lng']].values
        )

        # filtered_indices are relative to sold_partition_df, so we need to map them back to the original dataframe        
        profile_results = self._postprocess_results(filtered_distances, filtered_indices, listing_partition_df, sold_partition_df)
        results.update(profile_results)

      profile_timings[profile_key] = time.time() - profile_start_time

    self.logger.info(f"Processed {len(results)} listings")

    return results, profile_timings
  

  def update_datastore(self, active_listing_2_sold_listings: Dict):
    """
      Update datastore (Elasticsearch) with the computed comparable sold listings.

      Args:
      active_listing_2_sold_listings (Dict): A dictionary containing comparable sold listings for each current listing.
      This should directly come from the output of self.get_sold_listings(...)
    """
    # self.datastore.es 
    def generate_actions():
      for current_listing_id, comparables in active_listing_2_sold_listings.items():
        rounded_distances = [round(d, self.DISTANCE_ROUNDING_DECIMALS) for d in comparables['distance_km']]    # round to 1 decimal place
        yield {
            '_op_type': 'update',
            '_index': self.datastore.listing_index_name,
            '_id': current_listing_id,
            'doc': {
                'comparable_sold_listings': {
                    'sold_listing_ids': comparables['sold_listing_id'],
                    'distances_km': rounded_distances
                }
            }
        }
    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)
    self.logger.info(f"Successfully updated {success} documents")
    if failed:
      self.logger.error(f"Failed to update {len(failed)} documents")

    # TODO: detail tracking of succcess
    # all_ids = set(self.active_listing_2_sold_listings.keys()) 
    # failed_ids = {failure['update']['_id'] for failure in failed}           
    # all_ids - failed_ids    # what succeeded?

    return success, failed


  def _get_partitioned_df(self, df: pd.DataFrame, property_type: str, bedsInt: int, bathsInt: int) -> pd.DataFrame:
    """
      Filter the dataframe based on property type, number of beds, and baths.

      Args:
      df (pd.DataFrame): The input dataframe containing property listings.
      property_type (str): The type of property to filter for.
      bedsInt (int): The number of bedrooms to filter for.
      bathsInt (int): The number of bathrooms to filter for.

      Returns:
      pd.DataFrame: A filtered dataframe containing only the rows matching the specified criteria,
                    and only the 'lat', 'lng', and '_id' columns.
    """
    mask = (df.propertyType == property_type) & (df.bedsInt == bedsInt) & (df.bathsInt == bathsInt)
    return df.loc[mask, ['lat', 'lng', '_id']]
  

  def _compute_nearest_sold_properties(self, current_coords: np.ndarray, sold_coords: np.ndarray):
    """
      Calculate distance matrix between current and sold listings' coords, and keep only maximum of 12 comparables sold within 1 km 
      of each current listing. (i.e. results should be same length as current_coords)

      Args:
      current_coords (np.ndarray): Array of shape (n, 2) containing lat/lng coordinates of current listings.
      sold_coords (np.ndarray): Array of shape (m, 2) containing lat/lng coordinates of sold listings.
      max_distance (float): Maximum distance (in km) to consider for comparable properties. Default is 1.0 km.
      max_comps (int): Maximum number of comparables to return for each current listing. Default is 12.

      Returns:
      Tuple[np.ndarray, np.ndarray]: Two arrays, each of shape (n, max_comps):
      - filtered_distances: Distances to the closest comparables. Non-comparable entries are np.nan.
      - filtered_indices: Indices of the closest comparables. Non-comparable entries are np.nan.
    """
    D = calculate_distance(current_coords, sold_coords)
    sorted_indices = np.argsort(D, axis=1)[:, :self.MAX_COMPS]
    sorted_distances = np.sort(D, axis=1)[:, :self.MAX_COMPS]
    mask = sorted_distances < self.MAX_DISTANCE
    return np.where(mask, sorted_distances, np.nan), np.where(mask, sorted_indices, np.nan)
  

  def _postprocess_results(self, 
                             filtered_distances: np.ndarray, 
                             filtered_indices: np.ndarray, 
                             listing_partition_df: pd.DataFrame, 
                             sold_partition_df: pd.DataFrame) -> Dict[str, Dict[str, List]]:
    """
      Generate a dictionary of comparable results for each current listing.

      Args:
      filtered_distances (np.ndarray): Array of shape (n, max_comps) containing distances to comparable properties.
      filtered_indices (np.ndarray): Array of shape (n, max_comps) containing indices of comparable properties relatively to sold_partition_df.
      listing_partition_df (pd.DataFrame): DataFrame containing current listings (partitioned by profile)
      sold_partition_df (pd.DataFrame): DataFrame containing sold listings (partitioned by profile)

      Returns:
      Dict[str, Dict[str, List]]: A dictionary where keys are current listing IDs and values are dictionaries
                                  containing lists of comparable sold listing IDs and their distances.
    """
    results = {}
    for i, (distances, indices) in enumerate(zip(filtered_distances, filtered_indices)):
      valid_indices = ~np.isnan(indices)
      if np.any(valid_indices):
        jumpId = listing_partition_df.iloc[i]._id
        valid_distances = distances[valid_indices]
        valid_indices = indices[valid_indices].astype(int)
        results[jumpId] = {
            'sold_listing_id': sold_partition_df.iloc[valid_indices]._id.tolist(),
            'distance_km': valid_distances.tolist()
        }
    return results
  

  def _process_listing_profile_partition(self):
    """
    Partition the sold listings DataFrame into unique property profiles.

    This method groups the `sold_listing_df` DataFrame by the criteria defined in
    `self.comparable_criteria` (e.g., property type, number of bedrooms, and number of bathrooms)
    and calculates the population size for each unique profile. The result is stored in
    `self.listing_profile_partition`.

    The resulting DataFrame contains:
    - Columns for each criterion in `self.comparable_criteria`.
    - A `population` column indicating the number of sold listings in each profile.

    This partitioning is used to optimize the process of finding comparable sold listings
    by reducing the search space for each current listing.

    Side Effects:
    - Updates the `self.listing_profile_partition` attribute with the grouped DataFrame.

    Example:
    If `self.comparable_criteria = ['propertyType', 'bedsInt', 'bathsInt']`, the resulting
    `self.listing_profile_partition` might look like:
    
        propertyType  bedsInt  bathsInt  population
        Condo         2        2         150
        Detached      3        2         200
        Townhouse     3        3         120
    """
    self.listing_profile_partition = self.sold_listing_df.groupby(self.comparable_criteria).size().reset_index(name='population')


  def delete_comparable_sold_listings(self):
    """
    Delete the 'comparable_sold_listings' node from all docs in the listing index using bulk.
    This method can be used in conjunction with cleanup() to reset everything from scratch.
    """
    def generate_actions():
      query = {
        "query": {
          "exists": {
            "field": "comparable_sold_listings"
          }
        }
      }
      for hit in scan(self.datastore.es,
                      index=self.datastore.listing_index_name,
                      query=query):
        yield {
          "_op_type": "update",
          "_index": self.datastore.listing_index_name,
          "_id": hit["_id"],
          "script": {
            "source": "ctx._source.remove('comparable_sold_listings')"
          }
        }

    success, failed = bulk(self.datastore.es, generate_actions(),
                          raise_on_error=False,
                          raise_on_exception=False)
    
    self.logger.info(f"Successfully removed 'comparable_sold_listings' from {success} documents")
    if failed:
      self.logger.error(f"Failed to remove 'comparable_sold_listings' from {len(failed)} documents")
    
    return success, failed
    

  def delete_checkpoints_data(self):
    self.cache.delete(f"{self.cache_prefix}{self.job_id}_transform_diff_result")

 
  def delete_by_checking_es(self) -> List[str]:
    """
    Check listing status against ES and delete those not found or inactive.
    This provides a comprehensive cleanup beyond BQ deletions table.
    Return a list of listing IDs that were deleted.
    """
    if not hasattr(self, 'listing_df') or self.listing_df is None:
      self.logger.warning("No listing_df available for verification")
      return []
        
    listing_ids = list(set(self.listing_df['_id'].tolist()))
    batch_size = 1000
    inactive_listings = []
    
    for i in range(0, len(listing_ids), batch_size):
      batch_ids = listing_ids[i:i + batch_size]
      query = {
        "query": {
          "terms": {
            "_id": batch_ids
          }
        }
      }
      
      active_ids = {
        hit["_id"] for hit in scan(
          self.datastore.es,
          index=self.datastore.listing_index_name,
          query=query,
          _source=["listingStatus"]
        ) if hit["_source"]["listingStatus"] == "ACTIVE"
      }
      
      inactive_listings.extend(set(batch_ids) - active_ids)

    if inactive_listings:
      len_before = len(self.listing_df)
      self.listing_df = self.listing_df[~self.listing_df['_id'].isin(inactive_listings)]
      self.listing_df.reset_index(drop=True, inplace=True)
      self.logger.info(f"Removed {len_before - len(self.listing_df)} listings based on ES verification")

    return inactive_listings
  
if __name__ == '__main__':
  job_id = 'nearby_solds_xxx'

  uat_datastore = Datastore(host='localhost', port=9201)
  prod_datastore = Datastore(host='localhost', port=9202)
  bq_datastore = BigQueryDatastore()
  
  processor = NearbyComparableSoldsProcessor(
    job_id=job_id,
    datastore=prod_datastore, 
    bq_datastore=bq_datastore
  )

  # during dev, we extract from PROD but update the UAT as a temporary workaround
  processor.simulate_failure_at = 'transform'
  try:
    processor.run()
  except Exception as e:
    print(f"Exception occurred: {e}")

  # during dev, continue to run against UAT
  processor = NearbyComparableSoldsProcessor(
    job_id=job_id,
    datastore=uat_datastore,
    bq_datastore=bq_datastore
  )
  
  processor.run()

  # this can be run daily.
  # since we have a constant stream of new sold data and new listings.

""" Sample json appended to listing in ES:
  {
   etc. etc. 
   'comparable_sold_listings': 
    {'sold_listing_ids': ['on-17-e8018264', 'on-17-e8069306', 'on-17-e8142602'],
  'distances_km': [0.7, 0.9, 0.9]
  }
"""

""" Sample comparable_sold_listings_result:
{'21969076': {'sold_listing_id': ['on-13-1353569'],
  'distance_km': [0.45767571332477264]},
 '21483963': {'sold_listing_id': ['on-17-c7280822'],
  'distance_km': [0.39653338615963163]},
 '21954935': {'sold_listing_id': ['on-13-1373402',
   'on-17-x7294514',
   'on-13-1369153'],
  'distance_km': [0.21228468386441926,
   0.25941478772296306,
   0.26068223623936243]},
 '22073425': {'sold_listing_id': ['on-17-x7029200'],
  'distance_km': [0.17871279268886067]},
  

}
"""

""" testing compare_comparable_results
from realestate_analytics.etl.nearby_comparable_solds import compare_comparable_results

''' Test cases '''
prev_result = {
    '21483963': {'sold_listing_id': ['on-17-c7280822'], 'distance_km': [0.39653338615963163]},
    '21969076': {'sold_listing_id': ['on-13-1353569'], 'distance_km': [0.45767571332477264]},
    '21000001': {'sold_listing_id': ['on-17-c1111111', 'on-17-c2222222'], 'distance_km': [0.1, 0.2]},
    '21000002': {'sold_listing_id': ['on-17-c3333333'], 'distance_km': [0.3]},
    '21000003': {'sold_listing_id': ['on-17-c4444444'], 'distance_km': [0.4]},
}

current_result = {
    '21483963': {'sold_listing_id': ['on-17-c7280822'], 'distance_km': [0.39653338615963163]},  # Unchanged
    '21969076': {'sold_listing_id': ['on-13-1353569', 'on-17-new123'], 'distance_km': [0.45767571332477264, 0.5]},  # Changed
    '21549819': {'sold_listing_id': ['on-17-c6693166', 'on-17-c6680498'], 'distance_km': [0.1, 0.2]},  # New
    '21000001': {'sold_listing_id': ['on-17-c1111111', 'on-17-c2222222'], 'distance_km': [0.1, 0.20000001]},  # Slight change in distance
    '21000002': {'sold_listing_id': ['on-17-c3333333', 'on-17-c5555555'], 'distance_km': [0.3, 0.5]},  # Added new sold listing
    '21000004': {'sold_listing_id': [], 'distance_km': []},  # New empty listing
}

# diff_result = compare_comparable_results(prev_result, current_result)
# pprint(diff_result)
"""
