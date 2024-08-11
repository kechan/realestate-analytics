import unittest
from pathlib import Path
import os, random
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from realestate_analytics.data.caching import FileBasedCache
from realestate_analytics.data.es import Datastore
from realestate_analytics.etl.absorption_rate import AbsorptionRateProcessor

# python -m unittest test_absorption_rate.py
# python -m unittest test_absorption_rate.TestAbsorptionRate.test_es_data_consistency

class TestAbsorptionRate(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    # Set up the environment and load cached data
    _ = load_dotenv(find_dotenv())
    cache_dir = Path(os.getenv('ANALYTICS_CACHE_DIR'))
    cls.cache = FileBasedCache(cache_dir=cache_dir)

    es_host = os.getenv('ES_HOST')
    es_port = int(os.getenv('ES_PORT'))
    cls.datastore = Datastore(host=es_host, port=es_port)

    cls.processor = AbsorptionRateProcessor(
      job_id='unittest_absorption_rate',
      datastore=cls.datastore
    )

    # Load cached data
    cls.sold_listing_df = cls.cache.get('one_year_sold_listing')
    cls.listing_df = cls.cache.get('on_listing')

    print(f'# of sold listings: {cls.sold_listing_df.shape[0]}')
    print(f'# of current listings: {cls.listing_df.shape[0]}')

    # Compute absorption rates
    cls.processor.sold_listing_df = cls.sold_listing_df
    cls.processor.listing_df = cls.listing_df
    cls.processor._calculate_absorption_rates()
    cls.absorption_rates = cls.processor.absorption_rates

    print(f'# of absorption rates: {cls.absorption_rates.shape[0] if cls.absorption_rates is not None else 0}')

  @classmethod
  def tearDownClass(cls):
    cls.processor.close()

  def test_calculate_absorption_rate(self):
    # Select a random geog_id and propertyType
    sample_row = self.absorption_rates.sample(n=1).iloc[0]
    geog_id = sample_row['geog_id']
    property_type = sample_row['propertyType']

    print(f"Testing absorption rate for geog_id: {geog_id}, propertyType: {property_type}")

    # Calculate absorption rate independently
    sold_count, current_count, expected_rate = self._calculate_absorption_rate(geog_id, property_type)

    # Get the computed absorption rate
    computed_rate = sample_row['absorption_rate']

    print(f"Independently calculated rate: {expected_rate}")
    print(f"Computed rate: {computed_rate}")

    # Compare the results
    if pd.isna(expected_rate) and pd.isna(computed_rate):
      self.assertTrue(True, "Both expected and computed rates are NaN")
    else:
      self.assertAlmostEqual(expected_rate, computed_rate, places=4,
                             msg=f"Absorption rate mismatch for {geog_id}, {property_type}")

    print(f"Sold count: {sold_count}, Current count: {current_count}")


  def test_multiple_absorption_rate_calculations(self):
    for _ in range(5):
      self.test_calculate_absorption_rate()

  
  def test_all_property_type_calculation(self):
    # Select a random geog_id where propertyType is 'ALL'
    all_property_rows = self.absorption_rates[self.absorption_rates['propertyType'] == 'ALL']
    if all_property_rows.empty:
      self.fail("No 'ALL' property type rows found in absorption_rates")

    sample_row = all_property_rows.sample(n=1).iloc[0]
    geog_id = sample_row['geog_id']

    print(f"\nTesting ALL property types absorption rate for geog_id: {geog_id}")

    # Calculate absorption rate independently for ALL property types
    sold_count, current_count, expected_rate = self._calculate_absorption_rate(geog_id, 'ALL')

    # Get the computed absorption rate
    computed_rate = sample_row['absorption_rate']

    print(f"Independently calculated rate: {expected_rate}")
    print(f"Computed rate: {computed_rate}")

    # Compare the results
    if pd.isna(expected_rate) and pd.isna(computed_rate):
      self.assertTrue(True, "Both expected and computed ALL rates are NaN")
    else:
      self.assertAlmostEqual(expected_rate, computed_rate, places=4,
                             msg=f"ALL absorption rate mismatch for {geog_id}")

    print(f"ALL - Sold count: {sold_count}, Current count: {current_count}")

    # Verify that ALL indeed includes multiple property types
    property_types = self.listing_df[
      self.listing_df['guid'].apply(lambda x: geog_id in str(x).split(','))
    ]['propertyType'].unique()

    print("Property types included in ALL calculation:")
    print(property_types)

    self.assertTrue(len(property_types) > 0, f"No property types found for geog_id: {geog_id}")


  def _calculate_absorption_rate(self, geog_id, property_type):
    current_date = datetime.now()
    last_month = (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')

    # Filter sold listings for the previous month
    sold_listings = self.sold_listing_df[
      (self.sold_listing_df['guid'].apply(lambda x: geog_id in str(x).split(','))) &
      (self.sold_listing_df['lastTransition'].dt.to_period('M').astype(str) == last_month)
    ]

    # Filter current listings
    current_listings = self.listing_df[
      self.listing_df['guid'].apply(lambda x: geog_id in str(x).split(','))
    ]

    if property_type != 'ALL':
      sold_listings = sold_listings[sold_listings['propertyType'] == property_type]
      current_listings = current_listings[current_listings['propertyType'] == property_type]

    sold_count = len(sold_listings)
    current_count = len(current_listings)

    if current_count == 0:
      return sold_count, current_count, np.nan

    absorption_rate = sold_count / current_count
    return sold_count, current_count, absorption_rate

  
  def test_es_data_consistency(self):
    # Sample a few rows from absorption_rates
    sample_rows = self.absorption_rates.sample(n=5)

    for _, row in sample_rows.iterrows():
      geog_id = row['geog_id']
      property_type = row['propertyType']
      
      # Construct the document ID as used in update_mkt_trends_ts_index
      doc_id = f"{geog_id}_{property_type}"
      
      try:
        es_doc = self.datastore.search(index=self.datastore.mkt_trends_index_name, _id=doc_id)[0]
        
        # Check if the document has the expected structure
        self.assertIn('metrics', es_doc)
        self.assertIn('absorption_rate', es_doc['metrics'])
        
        # Get the most recent absorption rate from ES
        es_absorption_rates = es_doc['metrics']['absorption_rate']
        if not es_absorption_rates:
          self.fail(f"No absorption rate data found in ES for {doc_id}")
        
        latest_es_rate = es_absorption_rates[-1]

        self.assertIn('month', latest_es_rate, f"'month' field missing in absorption rate data for {doc_id}")
        self.assertRegex(latest_es_rate['month'], r'^\d{4}-\d{2}$', f"Invalid month format for {doc_id}")
        print(f"ES month: {latest_es_rate['month']}")
        
        # Compare with the rate in our computed results
        computed_rate = row['absorption_rate']
        
        print(f"Comparing rates for {doc_id}")
        print(f"ES rate: {latest_es_rate['value']}, Computed rate: {computed_rate}")
        
        if pd.isna(computed_rate) and latest_es_rate['value'] is None:
          self.assertTrue(True, f"Both ES and computed rates are NaN/None for {doc_id}")
        elif pd.notna(computed_rate) and latest_es_rate['value'] is not None:
          self.assertAlmostEqual(computed_rate, latest_es_rate['value'], places=4,
                                 msg=f"Absorption rate mismatch for {doc_id}")
        else:
          self.fail(f"Inconsistency in rate representation for {doc_id}")
        
      except Exception as e:
        self.fail(f"Error fetching or comparing data for {doc_id}: {str(e)}")


if __name__ == '__main__':
  unittest.main()