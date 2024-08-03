import unittest
import os, json, random
from pathlib import Path
from realestate_analytics.etl.nearby_comparable_solds import NearbyComparableSoldsProcessor
from realestate_analytics.data.caching import FileBasedCache
from realestate_analytics.data.es import Datastore
from realestate_analytics.data.bq import BigQueryDatastore

from realestate_analytics.etl.nearby_comparable_solds import scalar_calculate_distance

import pandas as pd
import numpy as np

from dotenv import load_dotenv, find_dotenv

# run to run
# python -m unittest test_nearby_comparable_solds.py
# python -m unittest test_nearby_comparable_solds.TestComparableSolds.test_es_comparable_solds

class TestComparableSolds(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
      # Set up the cache and load data
      _ = load_dotenv(find_dotenv())
      cache_dir = Path(os.getenv('ANALYTICS_CACHE_DIR'))
      es_host = os.getenv('ES_HOST')
      es_port = int(os.getenv('ES_PORT'))

      cls.cache = FileBasedCache(cache_dir=cache_dir)
      cls.datastore = Datastore(host=es_host, port=es_port)
      # cls.bq_datastore = BigQueryDatastore()
      # cls.processor = NearbyComparableSoldsProcessor(
      #     datastore = cls.datastore, 
      #     bq_datastore = cls.bq_datastore,
      #     cache_dir=cache_dir
      # )
      
      # Load cached data
      cls.listing_df = cls.cache.get('on_listing')
      cls.sold_listing_df = cls.cache.get('one_year_sold_listing')
      cls.computed_results = json.loads(cls.cache.get('comparable_sold_listings_result').replace("'", '"'))   # from last run of NearbyComparableSoldsProcessor
      
      print(f'len(listing_df): {cls.listing_df.shape[0]}')
      print(f'len(sold_listing_df): {cls.sold_listing_df.shape[0]}')
      print(f'len(computed_results): {len(cls.computed_results)}')

      # Compute the full result using the processor
      # cls.processor.extract(from_cache=True)
      # cls.processor.transform()
      # cls.full_result = cls.processor.comparable_sold_listings_result

    @classmethod
    def tearDownClass(cls):
      # cls.processor.close()
      cls.datastore.close()

    def test_sample_listing_comparable_solds(self):
      sample_listing_id = random.choice(list(self.computed_results.keys()))
      # sample_listing_id = '21910006'
      self._check_single_listing_comparable_solds(sample_listing_id)
      

    def test_multiple_listings_comparable_solds(self):
      sample_size = min(10, len(self.computed_results))  # Test up to 10 random listings
      sample_listing_ids = random.sample(list(self.computed_results.keys()), sample_size)
      
      for sample_listing_id in sample_listing_ids:
        with self.subTest(listing_id=sample_listing_id):
          self._check_single_listing_comparable_solds(sample_listing_id)


    def _check_single_listing_comparable_solds(self, sample_listing_id):
      # Choose a sample listing ID
      # sample_listing_id = random.choice(list(self.computed_results.keys()))
      
      pre_computed_result = self.computed_results[sample_listing_id]
      independent_result = self.calculate_comparable_solds(sample_listing_id)

      print('-' * 50)
      print(f'Listing ID: {sample_listing_id}')
      print(f'pre_computed_result: {pre_computed_result}')
      print(f'independent_result: {independent_result}')
      print('-' * 50)

      def group_by_distance(result):
        grouped = {}
        for lid, dist in zip(result['sold_listing_id'], result['distance_km']):
          dist = round(dist, 6)  # Round to 6 decimal places to avoid floating point issues
          if dist not in grouped:
            grouped[dist] = set()
          grouped[dist].add(lid)
        return grouped
      
      pre_grouped = group_by_distance(pre_computed_result)
      ind_grouped = group_by_distance(independent_result)

      # Ensure the number of unique distances is the same
      self.assertEqual(len(pre_grouped), len(ind_grouped), 
                      "Mismatch in number of unique distances")

      # Sort distances
      pre_distances = sorted(pre_grouped.keys())
      ind_distances = sorted(ind_grouped.keys())

      # Compare all groups except the last one
      for pre_dist, ind_dist in zip(pre_distances[:-1], ind_distances[:-1]):
        self.assertEqual(pre_dist, ind_dist, 
                        f"Distance mismatch: {pre_dist} != {ind_dist}")
        self.assertEqual(pre_grouped[pre_dist], ind_grouped[ind_dist], 
                        f"Listing set mismatch for distance {pre_dist}")

      # For the last (largest) distance, only check if the distance matches
      self.assertEqual(pre_distances[-1], ind_distances[-1], 
                      f"Largest distance mismatch: {pre_distances[-1]} != {ind_distances[-1]}")

      # Additional checks
      self.assertLessEqual(len(pre_computed_result['sold_listing_id']), 12, "More than 12 comparable solds found")
      self.assertTrue(all(d <= 1.0 for d in pre_computed_result['distance_km']), "Comparable sold found beyond 1km")

 
    def calculate_comparable_solds(self, listing_id):
      # Get the current listing details
      current_listing = self.listing_df[self.listing_df['_id'] == listing_id].iloc[0]

      # Filter sold listings by matching property profile
      matching_solds = self.sold_listing_df[
          (self.sold_listing_df['propertyType'] == current_listing['propertyType']) &
          (self.sold_listing_df['bedsInt'] == current_listing['bedsInt']) &
          (self.sold_listing_df['bathsInt'] == current_listing['bathsInt'])
      ]
        
      # Calculate distances
      distances = []
      for _, sold in matching_solds.iterrows():
        distance = scalar_calculate_distance(
          current_listing['lat'], current_listing['lng'],
          sold['lat'], sold['lng']
        )
        if distance <= 1.0:  # Only include solds within 1km
          distances.append((sold['_id'], distance))
      
      # Sort by distance and take top 12
      distances.sort(key=lambda x: x[1])
      top_12 = distances[:12]
      
      return {
        'sold_listing_id': [sold_id for sold_id, _ in top_12],
        'distance_km': [distance for _, distance in top_12]
      }
    

    def test_es_comparable_solds(self):
      sample_listing_id = random.choice(list(self.computed_results.keys()))
      # sample_listing_id = '17220308'
      
      # Get the pre-computed result for this listing
      pre_computed_result = self.computed_results[sample_listing_id]
      
      # Query the corresponding document from Elasticsearch
      es_doc = self.datastore.search(
        index=self.datastore.listing_index_name,
        _id=sample_listing_id,
        selects=['comparable_sold_listings']
      )
      
      # Check if the document was found
      if not es_doc:
        self.fail(f"Document with ID {sample_listing_id} not found in Elasticsearch index '{self.datastore.listing_index_name}'")
      
      # Check if the comparable_sold_listings field exists
      self.assertIn('comparable_sold_listings', es_doc[0], 
                    f"comparable_sold_listings not found in document {sample_listing_id}")
      
      es_result = es_doc[0]['comparable_sold_listings']
      
      # Compare the results
      self.assertEqual(set(pre_computed_result['sold_listing_id']), 
                      set(es_result['sold_listing_ids']),
                      "Mismatch in sold listing IDs")
      
      # Compare distances (allowing for small floating-point differences)
      for pre_dist, es_dist in zip(pre_computed_result['distance_km'], es_result['distances_km']):
        self.assertAlmostEqual(pre_dist, es_dist, places=1,
                              msg=f"Distance mismatch: {pre_dist} != {es_dist}")
      
      print(f"Verified Elasticsearch data for listing ID: {sample_listing_id}")
      print(f"Pre-computed result: {pre_computed_result}")
      print(f"Elasticsearch result: {es_result}")


if __name__ == '__main__':
  unittest.main()