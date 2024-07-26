from typing import List, Dict, Any

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery
from google.api_core import exceptions, retry

import pandas as pd

import logging

class BigQueryDatastore:
  def __init__(self):
    """
    A class to interact with Google BigQuery for retrieving and querying data.

    This class provides methods to execute general queries and specifically
    retrieve deleted listings from a BigQuery table.
    """
    self.logger = logging.getLogger(self.__class__.__name__)
    
    _ = load_dotenv(find_dotenv())
    self.project_id = os.getenv('BQ_PROJECT_ID')

    if not self.project_id:
      self.logger.error('BQ_PROJECT_ID is not set in .env file')
      raise ValueError('BQ_PROJECT_ID is not set in .env file')

    self.client = bigquery.Client(project=self.project_id)

    if not self.ping():
      self.logger.error('Failed to establish connection to BigQuery')

  def ping(self) -> bool:
    try:
      self.client.query('SELECT 1').result()
      return True
    except Exception as e:
      return False

    


  @retry.Retry(predicate=retry.if_exception_type(exceptions.GoogleAPICallError, exceptions.RetryError),
               initial=1, maximum=30, multiplier=2)
  def query(self, query: str, timeout=30) -> pd.DataFrame:
    try:
      query_job = self.client.query(query, timeout=timeout)
      return query_job.result().to_dataframe()
    except exceptions.TimeoutError as e:
      self.logger.error(f'Timeout error: {e}')
      raise
    except exceptions.GoogleAPICallError as e:
      self.logger.error(f'API call error: {e}')
      raise
    except Exception as e:
      self.logger.error(f'Unexpected error: {e}')
      raise

  def get_deleted_listings(self, start_time: datetime = None, end_time: datetime = None) -> pd.DataFrame:
    """
    Retrieve deleted listings within a specified time range.

    Args:
        start_time (datetime, optional): The start of the time range.
        end_time (datetime, optional): The end of the time range.

    Returns:
        pd.DataFrame: A DataFrame containing deleted listings.

    Raises:
        ValueError: If neither start_time nor end_time is provided.
        Exception: If there's an error executing the query.
    """
    if not start_time and not end_time:
      raise ValueError('At least one of start_time or end_time must be provided.')
    
    time_conditions = []
    if start_time:
      start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
      time_conditions.append(f'timestamp >= TIMESTAMP("{start_time_str}")')
    if end_time:
      end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
      time_conditions.append(f'timestamp < TIMESTAMP("{end_time_str}")')

    time_where_clause = " AND ".join(time_conditions)

    query = f"""
  SELECT timestamp, jumpId as listingId
  FROM `rlpdotca.RLP_Listings.listing` 
  WHERE {time_where_clause}
  AND indexStatus = 'deleted'
    """
    
    self.logger.debug(f'Executing BQ query: {query}')
    
    try:
      results_df = self.query(query)
      if results_df.empty:
        self.logger.warning(f'No deleted listings found between {start_time} and {end_time}')
      return results_df
    except Exception as e:
      self.logger.error(f'Failed to retrieve deleted listings: {e}')
      raise
  
  def close(self):
    if self.client:
      self.client.close()
  

  def is_valid(self) -> bool:
    """
    Perform a lightweight sanity check to ensure the BigQuery client is initialized correctly.

    Returns:
      bool: True if the client is valid, False otherwise.
    """
    try:
      # Perform a lightweight check by verifying the client has a valid project ID
      if not self.client.project or not isinstance(self.client.project, str):
        return False
      return True
    except Exception as e:
      print(f'Error validating BigQuery client: {e}')
      return False