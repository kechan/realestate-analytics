import unittest
from pathlib import Path
import os, random
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

from realestate_analytics.data.caching import FileBasedCache
from realestate_analytics.data.es import Datastore
from realestate_analytics.data.bq import BigQueryDatastore
from realestate_analytics.etl.last_mth_metrics import LastMthMetricsProcessor

# python -m unittest test_last_mth_metrics.py
# python -m unittest test_last_mth_metrics.TestLastMthMetrics.???

class TestLastMthMetrics(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    # Set up the cache and load data
    _ = load_dotenv(find_dotenv())
    cache_dir = Path(os.getenv('ANALYTICS_CACHE_DIR'))
    cls.cache = FileBasedCache(cache_dir=cache_dir)

    cls.listing_df = cls.cache.get('on_current_listing')
    cls.listing_df = cls.listing_df[~cls.listing_df['is_deleted']]    # account for soft deletions
    cls.listing_df.reset_index(drop=True, inplace=True)
    print(f'# of current listings: {cls.listing_df.shape[0]}')

    es_host = os.getenv('UAT_ES_HOST')
    es_port = int(os.getenv('UAT_ES_PORT'))
    cls.datastore = Datastore(host=es_host, port=es_port)
    cls.bq_datastore = BigQueryDatastore()

    cls.last_mth_metrics_processor = LastMthMetricsProcessor(
      datastore=cls.datastore,
      bq_datastore=cls.bq_datastore
    )

    cls.last_mth_metrics_processor.extract(from_cache=True)

  @classmethod
  def tearDownClass(cls):
    cls.last_mth_metrics_processor.close()
  

  def test_compute_last_month_metrics(self):
    today = datetime.now()
    last_month = (today.replace(day=1) - pd.Timedelta(days=1))#.strftime('%Y-%m')

    # strong assumption: compute_last_month_metrics(...) must not have side effect outside the context of the instance
    self.last_mth_metrics_processor.compute_last_month_metrics()

    last_mth_metrics_results = self.last_mth_metrics_processor.last_mth_metrics_results

    print(f'listing_df.addedOn.min and .max: {self.listing_df.addedOn.min(), self.listing_df.addedOn.max()}')
    print(f'listing_df.lastUpdate.min and .max: {self.listing_df.lastUpdate.min(), self.listing_df.lastUpdate.max()}')

    self.assertIsInstance(last_mth_metrics_results, pd.DataFrame)

    expected_columns = ['geog_id', 'propertyType', 'median_price', 'new_listings_count']
    self.assertSetEqual(set(expected_columns), set(last_mth_metrics_results.columns))

    # Content checks
    self.assertFalse(last_mth_metrics_results.empty)
    self.assertTrue(all(last_mth_metrics_results['median_price'] >= 0))
    self.assertTrue(all(last_mth_metrics_results['new_listings_count'] >= 0))

    expanded_listing_df = self.listing_df.assign(geog_id=self.listing_df['guid'].str.split(',')).explode('geog_id')

    # randomly select a row and check the values
    random_row = last_mth_metrics_results.sample(n=1).iloc[0]
    geog_id, property_type, median_price, new_listings_count = random_row['geog_id'], random_row['propertyType'], random_row['median_price'], random_row['new_listings_count']

    # compute independently the expected median price and new listings count from listing_df
    if property_type != 'ALL':
      df = expanded_listing_df[(expanded_listing_df.propertyType == property_type) & (expanded_listing_df.geog_id == geog_id)]
    else:
      df = expanded_listing_df[expanded_listing_df.geog_id == geog_id]

    expected_median_price = df['price'].median()
    self.assertAlmostEqual(median_price, expected_median_price, places=2,
                           msg=f"Median price mismatch for geog_id: {geog_id}, propertyType: {property_type}")

    expected_new_listing_count = df[(df['addedOn'].dt.month == last_month.month) & (df['addedOn'].dt.year == last_month.year)].shape[0]
    self.assertEqual(new_listings_count, expected_new_listing_count,
                      msg=f"New listings count mismatch for geog_id: {geog_id}, propertyType: {property_type}")

    # Additional checks for 'ALL' property type
    all_results = last_mth_metrics_results[last_mth_metrics_results['propertyType'] == 'ALL']
    self.assertFalse(all_results.empty, "No 'ALL' property type results found")

    # Check that 'ALL' exists for each unique geog_id
    unique_geog_ids = last_mth_metrics_results['geog_id'].unique()
    all_geog_ids = all_results['geog_id'].unique()
    self.assertEqual(set(unique_geog_ids), set(all_geog_ids), 
                      "Mismatch in geog_ids between 'ALL' and specific property types")

    print(last_mth_metrics_results.describe())
    print(last_mth_metrics_results['propertyType'].value_counts())


  def test_archived_results_match_elasticsearch(self):
    archived_results = self.last_mth_metrics_processor.archiver.retrieve('last_mth_metrics_results')
    self.assertIsNotNone(archived_results, "Failed to retrieve archived results")
    self.assertIsInstance(archived_results, pd.DataFrame, "Archived results should be a DataFrame")
    self.assertFalse(archived_results.empty, "Archived results DataFrame is empty")

    print(f"Total Archived results: {len(archived_results)}")
    print(archived_results.head())

    # Randomly select 5 entries
    sample_size = min(5, len(archived_results))
    sampled_results = archived_results.sample(n=sample_size, random_state=42)
    print(f"Sampled {sample_size} entries for comparison")

    # Query ES for the corresponding data

    for _, row in sampled_results.iterrows():
      doc_id = f"{row['geog_id']}_{row['propertyType']}"
      es_doc = self.datastore.search(
        index=self.datastore.mkt_trends_index_name,
        _id=doc_id
      )
      self.assertIsNotNone(es_doc, f"Document not found in Elasticsearch for ID: {doc_id}")
            
      if es_doc:
        es_median_price = es_doc[0]['metrics']['last_mth_median_asking_price']['value']
        es_new_listings = es_doc[0]['metrics']['last_mth_new_listings']['value']
        es_month = es_doc[0]['metrics']['last_mth_median_asking_price']['month']


        print(f"\nComparing {doc_id}:")
        print(f"Month - Elasticsearch: {es_month}")
        print(f"Median Price - Archived: {row['median_price']}, Elasticsearch: {es_median_price}")
        print(f"New Listings - Archived: {row['new_listings_count']}, Elasticsearch: {es_new_listings}")

        # Check that the month is in the correct format (YYYY-MM)
        self.assertRegex(es_month, r'\d{4}-\d{2}', f"Invalid month format for {doc_id}")

        # Check median price (allowing for small floating-point differences)
        self.assertAlmostEqual(row['median_price'], es_median_price, delta=1.0,
                                msg=f"Median price mismatch for {doc_id}")
        
        # Check new listings count (exact match)
        self.assertEqual(row['new_listings_count'], es_new_listings,
                          msg=f"New listings count mismatch for {doc_id}")

                


                

    




