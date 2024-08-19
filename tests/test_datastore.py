import unittest, os
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
import pandas as pd

from dotenv import load_dotenv, find_dotenv
from realestate_analytics.data.es import Datastore

class TestDatastore(unittest.TestCase):
  def setUp(self):
    _ = load_dotenv(find_dotenv())
    self.es_host = os.getenv('UAT_ES_HOST')
    self.es_port = int(os.getenv('UAT_ES_PORT'))
    try:
      self.datastore = Datastore(host=self.es_host, port=self.es_port)
    except ValueError as e:
      self.fail(f"Failed to initialize Datastore with error: {e}")

    # Set a default time range for all tests
    self.end_time = datetime.now()
    self.start_time = self.end_time - timedelta(days=3)

  def test_get_sold_listings_with_start_end_time(self):
    success, sold_listing_df = self.datastore.get_sold_listings(
      start_time=self.start_time,
      end_time=self.end_time,
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
              'city','neighbourhood',
              'provState',
              'guid'
              ])
    
    self.assertTrue(success)
    self.assertIsInstance(sold_listing_df, pd.DataFrame)
    self.assertIn('_id', sold_listing_df.columns)

    if not sold_listing_df.empty:
      # Check if lastTransition is within the specified time range
      self.assertTrue((sold_listing_df['lastTransition'] >= self.start_time).all())
      self.assertTrue((sold_listing_df['lastTransition'] <= self.end_time).all())
      
      # Additional checks to ensure data integrity
      self.assertTrue((sold_listing_df['listingStatus'] == 'SOLD').all())
      self.assertTrue(sold_listing_df['lastTransition'].notna().all())
    else:
      print("No sold listings found in the specified time range")

    required_columns = [
      'mls', 'lastTransition', 'transitions', 'lastUpdate',
      'listingType', 'searchCategoryType', 'listingStatus',
      'beds', 'bedsInt', 'baths', 'bathsInt',
      'price', 'soldPrice', 'daysOnMarket',
      'lat', 'lng', 'city', 'neighbourhood', 'provState', 'guid'
    ]
    for column in required_columns:
      self.assertIn(column, sold_listing_df.columns)

  def test_get_current_active_listings(self):
    select_fields = [
      'listingType', 'searchCategoryType', 'guid',
      'beds', 'bedsInt', 'baths', 'bathsInt',
      'lat', 'lng', 'price', 'addedOn', 'lastUpdate'
    ]

    success, all_active_listing_df = self.datastore.get_current_active_listings(
      selects=select_fields,
      prov_code='ON'
    )
    self.assertTrue(success, "Failed to retrieve active listings")
    self.assertIsInstance(all_active_listing_df, pd.DataFrame)
    self.assertFalse(all_active_listing_df.empty, "No active listings found")

    success, delta_active_listing_df = self.datastore.get_current_active_listings(
      use_script_for_last_update=True,

      addedOn_start_time = self.start_time,
      addedOn_end_time = self.end_time,
      
      updated_start_time = self.start_time,
      updated_end_time = self.end_time,

      selects=select_fields,
      prov_code='ON'
    )
    self.assertTrue(success, "Failed to retrieve active listings")
    self.assertIsInstance(delta_active_listing_df, pd.DataFrame)
    self.assertFalse(delta_active_listing_df.empty, "No time-bounded active listings found")

    # Applying date range conditions
    filtered_df = all_active_listing_df[
      ((all_active_listing_df['addedOn'] >= self.start_time) & (all_active_listing_df['addedOn'] <= self.end_time)) |
      ((all_active_listing_df['lastUpdate'] >= self.start_time) & (all_active_listing_df['lastUpdate'] <= self.end_time))      
    ]

    # Compare the results
    delta_ids = set(delta_active_listing_df['_id'])
    filtered_ids = set(filtered_df['_id'])

    # Check if all IDs in delta_active_listing_df are in filtered_df
    missing_in_filtered = delta_ids - filtered_ids
    self.assertFalse(missing_in_filtered, f"IDs in delta not in filtered: {missing_in_filtered}")

    # Check if all IDs in filtered_df are in delta_active_listing_df
    missing_in_delta = filtered_ids - delta_ids
    self.assertFalse(missing_in_delta, f"IDs in filtered not in delta: {missing_in_delta}")

    # Assert that the two dataframes have the same number of rows
    self.assertEqual(set(delta_ids), set(filtered_ids), "Mismatch in number of rows between filtered and delta_active_listing_df")

if __name__ == '__main__':
  unittest.main()