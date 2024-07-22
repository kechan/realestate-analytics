from typing import Dict, List, Tuple, Union

import time, math
from collections import defaultdict
import numpy as np
import pandas as pd
from math import radians, sin, cos, sqrt, atan2
from pathlib import Path
from datetime import datetime, timedelta

from ..data.caching import FileBasedCache
from ..data.es import Datastore
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

class NearbyComparableSoldsProcessor:
  def __init__(self, datastore: Datastore, cache_dir: Union[str, Path] = None):
    self.datastore = datastore
    self.cache_dir = Path(cache_dir) if cache_dir else None
    if self.cache_dir:
      self.cache_dir.mkdir(parents=True, exist_ok=True)

    self.cache = FileBasedCache(cache_dir=self.cache_dir)
    print(self.cache.cache_dir)

    self.last_run_key = 'NearbyComparableSoldsProcessor_last_run'

    self.sold_listing_df = None
    self.listing_df = None

    # definition of "comparables"
    # In totality, the vicinity of the property and time of sale are also involved, but these are the simpler ones
    # so we partition our sold listing set by comparable_criteria first.
    self.comparable_criteria = ['propertyType', 'bedsInt', 'bathsInt']

    self.listing_profile_partition = None #self.sold_listing_df.groupby(self.comparable_criteria).size().reset_index(name='population')

  def extract(self, from_cache=False):
    if from_cache:
      self._load_from_cache()
    else:
      self._extract_from_datastore()
      if self.cache.cache_dir:
        self._save_to_cache()
    
    self._process_listing_profile_partition()


  def _extract_from_datastore(self):
    last_run = self.cache.get(self.last_run_key)

    # first run
    if last_run is None:
      end_time = datetime.now()

      # get the sold listings from the last year till now
      _, self.sold_listing_df = self.datastore.get_sold_listings(
        start_time=end_time - timedelta(days=365),
        end_time=end_time,
        selects=[
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
        ])
        
      # get everything up till now    
      _, self.listing_df = self.datastore.get_current_active_listings(
        selects=[
        'listingType', 'searchCategoryType',
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

      self.cache.set(self.last_run_key, end_time)   # save last run to cache
    else:    # incremental/delta load 
      # Load existing data from cache
      self.sold_listing_df = self.cache.get('one_year_sold_listing')
      self.listing_df = self.cache.get('on_listing')

      if self.sold_listing_df is None or self.listing_df is None:
        raise ValueError("Cache is inconsistent. Missing sold_listing_df or listing_df.")

      start_time = last_run - timedelta(days=21)   # load from 21 days before last run to have bigger margin of safety.
      end_time = datetime.now()

      # get the sold listings from last run till now
      _, delta_sold_listing_df = self.datastore.get_sold_listings(
        start_time=start_time,
        end_time=end_time,
        selects=[
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
        ])

      # get the current listings from last run till now
      _, delta_listing_df = self.datastore.get_current_active_listings(
        selects=[
        'listingType', 'searchCategoryType',
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
      
      # merge delta with whole
      self.sold_listing_df = pd.concat([self.sold_listing_df, delta_sold_listing_df], axis=0)
      self.sold_listing_df.drop_duplicates(subset=['_id'], inplace=True, keep='last')
      # get rid of stuff thats older than a year
      one_year_ago_from_now = end_time - timedelta(days=365)
      drop_idxs = self.sold_listing_df.q("lastTransition < @one_year_ago_from_now").index
      self.sold_listing_df.drop(index=drop_idxs, inplace=True)

      self.listing_df = pd.concat([self.listing_df, delta_listing_df], axis=0)
      self.listing_df.drop_duplicates(subset=['_id'], inplace=True, keep='last')

      self.cache.set(self.last_run_key, end_time)   # save last run to cache

  def _load_from_cache(self):
    if not self.cache.cache_dir:
      raise ValueError("Cache directory not set. Cannot load from cache.")
    
    last_run = self.cache.get(self.last_run_key)
    self.sold_listing_df = self.cache.get('one_year_sold_listing')
    self.listing_df = self.cache.get("on_listing")

    if self.sold_listing_df is None or self.listing_df is None:
      raise ValueError("Missing sold_listing_df or listing_df in cache.")

    print(f"Loaded {len(self.sold_listing_df)} sold listings and {len(self.listing_df)} current listings from cache.")

  def _save_to_cache(self):
    if not self.cache.cache_dir:
      raise ValueError("Cache directory not set. Cannot save to cache.")
    
    self.cache.set('one_year_sold_listing', self.sold_listing_df)
    self.cache.set('on_listing', self.listing_df)

    print(f"Saved {len(self.sold_listing_df)} sold listings and {len(self.listing_df)} current listings to cache.")

  def _process_listing_profile_partition(self):
    self.listing_profile_partition = self.sold_listing_df.groupby(self.comparable_criteria).size().reset_index(name='population')


  def transform(self):
    self.comparable_sold_listings_result, _ = self.get_sold_listings()

    # Retrieve previous result from cache
    prev_result = self.cache.get('comparable_sold_listings_result')
    if prev_result is not None:    
      self.diff_result = compare_comparable_results(prev_result, self.comparable_sold_listings_result)
    else:
      self.diff_result = self.comparable_sold_listings_result
    
    # Cache the full current result for next time
    self.cache.set('comparable_sold_listings_result', self.comparable_sold_listings_result)

  def load(self):
    success, failed = self.update_datastore(self.diff_result)
    return success, failed
      
  def cleanup(self):
    """
    Clean up all cached data and reset instance variables.
    """
    # Clear cache entries
    self.cache.delete(self.last_run_key)
    self.cache.delete('one_year_sold_listing')
    self.cache.delete('on_listing')
    self.cache.delete('comparable_sold_listings_result')

    # Reset instance variables
    self.sold_listing_df = None
    self.listing_df = None
    self.listing_profile_partition = None
    self.comparable_sold_listings_result = None
    self.diff_result = None

    print("Cleanup completed. All cached data has been cleared and instance variables reset.")

  def full_refresh(self):
    """
    Perform a full refresh of the data, resetting to initial state and re-extracting all data.
    """
    print("Starting full refresh...")
    self.cleanup()
    self.extract(from_cache=False)
    self.transform()
    self.load()
    print("Full refresh completed.")

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
        print(f'Profile: {profile_key}, # of sold: {len(sold_partition_df)}, # of current: {len(listing_partition_df)}')
        
        filtered_distances, filtered_indices = self._compute_nearest_sold_properties(
            listing_partition_df[['lat', 'lng']].values,
            sold_partition_df[['lat', 'lng']].values
        )

        # filtered_indices are relative to sold_partition_df, so we need to map them back to the original dataframe        
        profile_results = self._postprocess_results(filtered_distances, filtered_indices, listing_partition_df, sold_partition_df)
        results.update(profile_results)

      profile_timings[profile_key] = time.time() - profile_start_time

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
        rounded_distances = [round(d, 1) for d in comparables['distance_km']]    # round to 1 decimal place
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
    print(f"Successfully updated {success} documents")
    if failed:
      print(f"Failed to update {len(failed)} documents")

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
  
  def _compute_nearest_sold_properties(self, current_coords: np.ndarray, sold_coords: np.ndarray, max_distance=1.0, max_comps=12):
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
    sorted_indices = np.argsort(D, axis=1)[:, :max_comps]
    sorted_distances = np.sort(D, axis=1)[:, :max_comps]
    mask = sorted_distances < max_distance
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
  

if __name__ == '__main__':
  datastore = Datastore(host='localhost', port=9201)
  
  processor = NearbyComparableSoldsProcessor(datastore=datastore)

  processor.extract(from_cache=True)
  
  processor.transform()

  success, failed = processor.load()

