import unittest
from pathlib import Path
import os, random
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

from realestate_analytics.data.caching import FileBasedCache
from realestate_analytics.data.es import Datastore
from realestate_analytics.etl.historic_sold_median_metrics import SoldMedianMetricsProcessor

# python -m unittest test_historic_sold_median_metrics.py
# python -m unittest test_historic_sold_median_metrics.TestMedianSoldPriceAndDOM.test_ALL_property_type_calculation

class TestMedianSoldPriceAndDOM(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    # Set up the cache and load data
    _ = load_dotenv(find_dotenv())
    cache_dir = Path(os.getenv('ANALYTICS_CACHE_DIR'))
    es_host = os.getenv('ES_HOST')
    es_port = int(os.getenv('ES_PORT'))

    cls.cache = FileBasedCache(cache_dir=cache_dir)
    cls.datastore = Datastore(host=es_host, port=es_port)
    
    # Load the sold listings data and results from cache
    cls.sold_listings_df = cls.cache.get('five_years_sold_listing')
    cls.price_series_df = cls.cache.get('five_years_price_series')
    cls.dom_series_df = cls.cache.get('five_years_dom_series')
    
    # Ensure lastTransition is datetime
    cls.sold_listings_df['lastTransition'] = pd.to_datetime(cls.sold_listings_df['lastTransition'])

  @classmethod
  def tearDownClass(cls):
    cls.datastore.close()


  def select_random_month(self, row):
    date_columns = [col for col in self.price_series_df.columns if col.startswith('20')]
    values = row[date_columns]
    
    non_nan_cols = values[values.notna()].index.tolist()
    nan_cols = values[values.isna()].index.tolist()

    if non_nan_cols and nan_cols:
      # Some columns have values and some are NaN
      return (random.choice(non_nan_cols), random.choice(nan_cols))
    elif len(non_nan_cols) >= 2:
      # Case 2: All columns have values
      return tuple(random.sample(non_nan_cols, 2))
    elif len(nan_cols) >= 2:
      # Case 3: All columns are NaN
      return tuple(random.sample(nan_cols, 2))
    else:
      # Edge case: only one column available
      only_col = non_nan_cols[0] if non_nan_cols else nan_cols[0]
      return (only_col, only_col)
    

  def test_median_price_and_dom_calculation(self):
    # 1. Pick a random geog_id/property_type where property_type is not null       
    valid_rows = self.price_series_df[self.price_series_df['propertyType'].notna()]
    random_row = valid_rows.sample(n=1).iloc[0]

    geog_id = random_row['geog_id']
    property_type = random_row['propertyType']
    geo_level = int(random_row['geo_level'])

    random_months = self.select_random_month(random_row)

    if 'guid' not in self.sold_listings_df.columns:
      self.fail("'guid' column not found in sold_listings_df")

    for random_month in random_months:
      print(f"Testing for geog_id: {geog_id}, property_type: {property_type}, month: {random_month}")

      month_start = datetime.strptime(random_month, '%Y-%m')
      month_end = (month_start.replace(day=28) + pd.Timedelta(days=4)).replace(day=1)

      filtered_df = self.sold_listings_df[
        (self.sold_listings_df['guid'].apply(lambda x: geog_id in str(x).split(','))) &
        (self.sold_listings_df['propertyType'] == property_type) &
        (self.sold_listings_df['lastTransition'] >= month_start) &
        (self.sold_listings_df['lastTransition'] < month_end)
      ]

      print(f"Number of rows in filtered_df: {len(filtered_df)}")

      independent_median_price = filtered_df['soldPrice'].median()
      independent_median_dom = filtered_df['daysOnMarket'].median()

      precomputed_median_price = self.price_series_df[
        (self.price_series_df['geog_id'] == geog_id) &
        (self.price_series_df['propertyType'] == property_type)
      ][random_month].iloc[0]

      precomputed_median_dom = self.dom_series_df[
        (self.dom_series_df['geog_id'] == geog_id) &
        (self.dom_series_df['propertyType'] == property_type)
      ][random_month].iloc[0]

      print(f"Independent median price: {independent_median_price}, Precomputed median price: {precomputed_median_price}")
      print(f"Independent median DOM: {independent_median_dom}, Precomputed median DOM: {precomputed_median_dom}")

      # Check if both are NaN or if they're almost equal
      if pd.isna(independent_median_price) and pd.isna(precomputed_median_price):
        self.assertTrue(True, "Both independent and precomputed median prices are NaN")
      else:
        self.assertAlmostEqual(independent_median_price, precomputed_median_price, places=2,
                              msg=f"Median price mismatch for {geog_id}, {property_type}, {random_month}")

      if pd.isna(independent_median_dom) and pd.isna(precomputed_median_dom):
        self.assertTrue(True, "Both independent and precomputed median DOMs are NaN")
      else:
        self.assertAlmostEqual(independent_median_dom, precomputed_median_dom, places=2,
                              msg=f"Median DOM mismatch for {geog_id}, {property_type}, {random_month}")


  def test_ALL_property_type_calculation(self):
    rows = self.price_series_df[self.price_series_df['propertyType'].isna()]
    if rows.empty:
      self.fail("No rows found with propertyType as None (ALL)")

    random_row = rows.sample(n=1).iloc[0]
    geog_id = random_row['geog_id']
    geo_level = int(random_row['geo_level'])

    random_months = self.select_random_month(random_row)

    for random_month in random_months:
      print(f"\nTesting ALL property types for geog_id: {geog_id}, month: {random_month}")

      month_start = datetime.strptime(random_month, '%Y-%m')
      month_end = (month_start.replace(day=28) + pd.Timedelta(days=4)).replace(day=1)

      # Filter sold listings for all relevant property types
      filtered_df = self.sold_listings_df[
        (self.sold_listings_df['guid'].apply(lambda x: geog_id in str(x).split(','))) &
        (self.sold_listings_df['propertyType'].isin(['CONDO', 'SEMI-DETACHED', 'TOWNHOUSE', 'DETACHED'])) &
        (self.sold_listings_df['lastTransition'] >= month_start) &
        (self.sold_listings_df['lastTransition'] < month_end)
      ]

      print(f"Number of rows in filtered_df: {len(filtered_df)}")

      independent_median_price = filtered_df['soldPrice'].median()
      independent_median_dom = filtered_df['daysOnMarket'].median()

      precomputed_median_price = self.price_series_df[
        (self.price_series_df['geog_id'] == geog_id) &
        (self.price_series_df['propertyType'].isna())
      ][random_month].iloc[0]

      precomputed_median_dom = self.dom_series_df[
        (self.dom_series_df['geog_id'] == geog_id) &
        (self.dom_series_df['propertyType'].isna())
      ][random_month].iloc[0]

      print(f"Independent median price: {independent_median_price}, Precomputed median price: {precomputed_median_price}")
      print(f"Independent median DOM: {independent_median_dom}, Precomputed median DOM: {precomputed_median_dom}")

      # Check if both are NaN or if they're almost equal
      if pd.isna(independent_median_price) and pd.isna(precomputed_median_price):
        self.assertTrue(True, "Both independent and precomputed ALL median prices are NaN")
      else:
        self.assertAlmostEqual(independent_median_price, precomputed_median_price, places=2,
                              msg=f"ALL median price mismatch for {geog_id}, month: {random_month}")

      if pd.isna(independent_median_dom) and pd.isna(precomputed_median_dom):
        self.assertTrue(True, "Both independent and precomputed ALL median DOMs are NaN")
      else:
        self.assertAlmostEqual(independent_median_dom, precomputed_median_dom, places=2,
                              msg=f"ALL median DOM mismatch for {geog_id}, month: {random_month}")      

      if len(filtered_df) > 0:
        # Additional check: Verify that ALL indeed includes multiple property types
        property_type_counts = filtered_df['propertyType'].value_counts()
        print("Property type distribution in the filtered data:")
        print(property_type_counts)

        # Instead of asserting, we'll log a warning if there's only one property type
        if len(property_type_counts) == 1:
            print(f"Warning: Only one property type ({property_type_counts.index[0]}) found for geog_id: {geog_id}, month: {random_month}")
            print("This may be normal for small neighborhoods or specific time periods, but worth noting.")
        elif len(property_type_counts) == 0:
            print(f"Warning: No property types found for geog_id: {geog_id}, month: {random_month}")
            print("This might indicate an issue with data filtering or availability for this period.")
        else:
            print(f"Found {len(property_type_counts)} different property types in the data.")

        # Optional: You can add an assertion to ensure there's at least some data
        # self.assertTrue(len(filtered_df) > 0, f"No data found for geog_id: {geog_id}, month: {random_month}")
      else:
        print("No data available for this month.")
        print(f"Precomputed median price: {precomputed_median_price}")
        print(f"Precomputed median DOM: {precomputed_median_dom}")

        self.assertTrue(pd.isna(precomputed_median_price), 
                        f"Expected NaN for precomputed median price, but got {precomputed_median_price}")
        self.assertTrue(pd.isna(precomputed_median_dom), 
                        f"Expected NaN for precomputed median DOM, but got {precomputed_median_dom}")

     
  def test_multiple_median_price_and_dom_calculation(self):
    for _ in range(5):
      self.test_median_price_and_dom_calculation()


  def test_multiple_ALL_property_type_calculation(self):
    for _ in range(5):
      self.test_ALL_property_type_calculation()


  def test_es_data_presence(self):
    # Sample a few rows from price_series_df and dom_series_df
    sample_rows = self.price_series_df.sample(n=5)

    for _, row in sample_rows.iterrows():
      geog_id = row['geog_id']
      property_type = row['propertyType'] if pd.notna(row['propertyType']) else 'ALL'
      geo_level = int(row['geo_level'])
      
      # Construct the document ID as used in update_mkt_trends_ts_index
      doc_id = f"{geog_id}_{property_type}"
      
      try:
        # Fetch the document from Elasticsearch      
        es_doc = self.datastore.es.get(index=self.datastore.mkt_trends_ts_index_name, id=doc_id)
        # Check if the document was found
        if not es_doc:
          self.fail(f"{doc_id} not found in Elasticsearch index '{self.datastore.mkt_trends_ts_index_name}'")

        es_data = es_doc['_source']
        
        # Check if the document has the expected structure
        self.assertIn('metrics', es_data)
        self.assertIn('median_price', es_data['metrics'])
        self.assertIn('median_dom', es_data['metrics'])
        
        # Check a few random months
        date_columns = [col for col in self.price_series_df.columns if col.startswith('20')]
        random_months = random.sample(date_columns, min(3, len(date_columns)))
        
        for month in random_months:
          es_price = next((item['value'] for item in es_data['metrics']['median_price'] if item['date'] == month), None)
          es_dom = next((item['value'] for item in es_data['metrics']['median_dom'] if item['date'] == month), None)
          
          df_price = row[month]
          if property_type == 'ALL':
            df_dom = self.dom_series_df[(self.dom_series_df['geog_id'] == geog_id) & 
                                        (self.dom_series_df['propertyType'].isnull())][month].iloc[0]
          else:  
            df_dom = self.dom_series_df[(self.dom_series_df['geog_id'] == geog_id) & 
                                        (self.dom_series_df['propertyType'] == property_type)][month].iloc[0]
          
          # Compare values, considering NaN as equal
          if pd.isna(df_price) and es_price is None:
            self.assertTrue(True, f"Both ES and DF price are NaN/None for {doc_id}, {month}")
          elif pd.notna(df_price) and es_price is not None:
            self.assertAlmostEqual(df_price, es_price, places=2,
                                    msg=f"Price mismatch for {doc_id}, {month}")
          
          if pd.isna(df_dom) and es_dom is None:
            self.assertTrue(True, f"Both ES and DF DOM are NaN/None for {doc_id}, {month}")
          elif pd.notna(df_dom) and es_dom is not None:
            self.assertAlmostEqual(float(df_dom), float(es_dom), places=2,
                                    msg=f"DOM mismatch for {doc_id}, {month}")
      
        print(f"Successfully verified data for {doc_id}")
      except Exception as e:
        self.fail(f"Error fetching or comparing data for {doc_id}: {str(e)}")
      

if __name__ == '__main__':
  unittest.main()