import unittest
import pandas as pd
import numpy as np
from realestate_analytics.etl.historic_sold_median_metrics import SoldMedianMetricsProcessor

# python -m unittest test_get_delta_dataframe.py

class TestGetDeltaDataframe(unittest.TestCase):
    def setUp(self):
        self.processor = SoldMedianMetricsProcessor(None)  # Pass None as we don't need a real datastore for this test

    def test_get_delta_dataframe(self):
        # Create sample previous DataFrame
        prev_df = pd.DataFrame({
            'geog_id': ['g10_abcd', 'g10_efgh', 'g20_ijkl'],
            'propertyType': ['DETACHED', 'CONDO', 'TOWNHOUSE'],
            '2023-01': [100000.0, 80000.0, np.nan],
            '2023-02': [105000.0, 82000.0, 150000.0],
            '2023-03': [110000.0, np.nan, 155000.0],
            'geo_level': [10, 10, 20]
        })

        # Create sample current DataFrame with changes
        current_df = pd.DataFrame({
            'geog_id': ['g10_abcd', 'g10_efgh', 'g20_ijkl', 'g30_mnop'],
            'propertyType': ['DETACHED', 'CONDO', 'TOWNHOUSE', 'DETACHED'],
            '2023-01': [100000.0, 80000.0, np.nan, 200000.0],
            '2023-02': [105000.0, 83000.0, 150000.0, 205000.0],  # Changed value for g10_efgh
            '2023-03': [110000.0, 85000.0, 155000.0, 210000.0],  # New value for g10_efgh
            '2023-04': [115000.0, 87000.0, 160000.0, 215000.0],  # New column
            'geo_level': [10, 10, 20, 30]
        })

        delta_df = self.processor._get_delta_dataframe(current_df, prev_df)

        # Assertions
        self.assertEqual(len(delta_df), 4)  # Should include changed and new rows
        self.assertTrue('g10_efgh' in delta_df['geog_id'].values)  # Changed row
        self.assertTrue('g30_mnop' in delta_df['geog_id'].values)  # New row
        self.assertTrue('2023-04' in delta_df.columns)  # New column should be present
        self.assertEqual(delta_df.loc[delta_df['geog_id'] == 'g10_efgh', '2023-02'].values[0], 83000.0)  # Check changed value
        self.assertTrue(pd.isna(delta_df.loc[delta_df['geog_id'] == 'g20_ijkl', '2023-01'].values[0]))  # Check NaN preservation

    def test_get_delta_dataframe_no_changes(self):
        df = pd.DataFrame({
            'geog_id': ['g10_abcd'],
            'propertyType': ['DETACHED'],
            '2023-01': [100000.0],
            'geo_level': [10]
        })

        delta_df = self.processor._get_delta_dataframe(df, df.copy())
        self.assertTrue(delta_df.empty)  # Should be empty when there are no changes

    def test_get_delta_dataframe_column_changes(self):
        prev_df = pd.DataFrame({
            'geog_id': ['g10_abcd'],
            'propertyType': ['DETACHED'],
            '2023-01': [100000.0],
            'geo_level': [10]
        })

        current_df = pd.DataFrame({
            'geog_id': ['g10_abcd'],
            'propertyType': ['DETACHED'],
            '2023-01': [100000.0],
            '2023-02': [105000.0],  # New column
            'geo_level': [10]
        })

        delta_df = self.processor._get_delta_dataframe(current_df, prev_df)
        self.assertEqual(len(delta_df), 1)  # Should include the row due to new column
        self.assertTrue('2023-02' in delta_df.columns)  # New column should be present


    def test_get_delta_dataframe_partial_change(self):
        # Create sample previous DataFrame
        prev_df = pd.DataFrame({
            'geog_id': ['g10_abcd', 'g10_efgh'],
            'propertyType': ['DETACHED', 'CONDO'],
            '2023-01': [100000.0, 80000.0],
            '2023-02': [105000.0, 82000.0],
            '2023-03': [110000.0, 84000.0],
            'geo_level': [10, 10]
        })

        # Create sample current DataFrame with changes only in the second row
        current_df = pd.DataFrame({
            'geog_id': ['g10_abcd', 'g10_efgh'],
            'propertyType': ['DETACHED', 'CONDO'],
            '2023-01': [100000.0, 80000.0],
            '2023-02': [105000.0, 82000.0],
            '2023-03': [110000.0, 85000.0],  # Changed value for g10_efgh
            'geo_level': [10, 10]
        })

        delta_df = self.processor._get_delta_dataframe(current_df, prev_df)

        # Assertions
        self.assertEqual(len(delta_df), 1)  # Should include only the changed row
        self.assertTrue('g10_efgh' in delta_df['geog_id'].values)  # Changed row
        self.assertFalse('g10_abcd' in delta_df['geog_id'].values)  # Unchanged row should not be present
        self.assertEqual(delta_df.loc[delta_df['geog_id'] == 'g10_efgh', '2023-03'].values[0], 85000.0)  # Check changed value

        
if __name__ == '__main__':
    unittest.main()
