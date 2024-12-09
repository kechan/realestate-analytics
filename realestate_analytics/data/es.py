from typing import List, Dict, Any, Tuple, Union

import os, json, time, traceback, re
from datetime import datetime, timedelta
from collections import Counter
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import pandas as pd

from .caching import FileBasedCache

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError, RequestError, TransportError
from elasticsearch.helpers import scan, bulk

from realestate_core.common.utils import load_from_pickle, save_to_pickle, join_df

import logging
from tqdm.auto import tqdm

def derive_property_type(row):
  listing_type = row.get('listingType', '').split(', ')  # Assuming listingType is a comma-separated string
  search_category_type = row.get('searchCategoryType', None)
  

  if search_category_type == 101 and any(lt in ['CONDO', 'REC'] for lt in listing_type):
    return 'CONDO'
  elif search_category_type == 104:
    return 'SEMI-DETACHED'
  elif search_category_type == 102:
    return 'TOWNHOUSE'
  elif search_category_type == 103:
    return 'DETACHED'
  else:
    return 'OTHER'
  

def format_listingType(x: List[str]):
  if len(x) > 0 and x[0] == 'listingType':
    x = x[1:]
  return ', '.join(x)


# Custom function to parse datetime with flexible format
def parse_datetime(dt_string):
  try:
    return pd.to_datetime(dt_string, format='%Y-%m-%dT%H:%M:%S.%f')
  except ValueError:
    return pd.to_datetime(dt_string, format='%Y-%m-%dT%H:%M:%S')
            
class Datastore:
  def __init__(self, host: str, port: int):
    self.logger = logging.getLogger(self.__class__.__name__)

    self.es_host = host
    self.es_port = port
    self.es = Elasticsearch([f'http://{self.es_host}:{self.es_port}/'], timeout=30, retry_on_timeout=True, max_retries=3)

    # hardcode these often used indexes
    self.sold_listing_index_name = 'rlp_archive_current'
    self.listing_index_name = 'rlp_listing_current'

    self.geo_index_name = 'rlp_content_geo_current'
    self.geo_details_en_index_name = 'rlp_geo_details_en'

    # for tracking listing so we don't lose track of sold/delisted listings
    # as of now, this is used in the context of last_mth_metrics.py
    self.listing_tracking_index_name = 'rlp_listing_tracking_current'

    # for market trends time series data
    # currently, 5yr montly median sold price and days on market by geog_id and property type
    # used in SoldMedianMetricsProcessor
    self.mkt_trends_index_name = "rlp_mkt_trends_current"

    if not self.ping():
      self.logger.error(f"Unable to connect to ES at {host}:{port}")
      # raise Exception("Unable to connect to Elasticsearch.")

    # get mappings
    try:
      self.sold_listing_index_mapping = self.get_mapping(index=self.sold_listing_index_name)
      self.listing_index_mapping = self.get_mapping(index=self.listing_index_name)
      self.geo_index_mapping = self.get_mapping(index=self.geo_index_name)
      self.geo_details_en_index_mapping = self.get_mapping(index=self.geo_details_en_index_name)    

      try:
        self.listing_tracking_index_mapping = self.get_mapping(index=self.listing_tracking_index_name)
      except NotFoundError:
        self.logger.warning(f"Index '{self.listing_tracking_index_name}' not found.")

      try:
        self.mkt_trends_index_mapping = self.get_mapping(index=self.mkt_trends_index_name)
      except NotFoundError:
        self.logger.warning(f"Index '{self.mkt_trends_index_name}' not found.")

    except ConnectionError as e:
      self.logger.error(f"Error connecting to ES at {host}:{port} mappings not retrieved.")

    # Config and instantiate cache
    self.cache = None
    _ = load_dotenv(find_dotenv())
    self.cache_dir = os.getenv('ANALYTICS_CACHE_DIR', None)
    if self.cache_dir:
      self.cache_dir = Path(self.cache_dir)
      self.cache = FileBasedCache(cache_dir=self.cache_dir)
    else:
      self.logger.warning('ANALYTICS_CACHE_DIR not set in .env file, not instantiating cache, may cause unexpected errors.')    
    
    # map from property type to the listingType and searchCategoryType encoding.
    # for used in ES querying.
    self.property_type_query_mappings = [
      # CONDO
      {"bool": {
          "must": [
              {"terms": {"listingType.keyword": ["CONDO", "REC"]}},
              {"term": {"searchCategoryType": 101}}
          ]
      }},
      # SEMI-DETACHED
      {"term": {"searchCategoryType": 104}},
      # TOWNHOUSE
      {"term": {"searchCategoryType": 102}},
      # DETACHED
      {"term": {"searchCategoryType": 103}}
    ]

    
  def ping(self) -> bool:
    return self.es.ping()
  
  def get_cluster_node_names(self) -> List[str]:
    """
    Retrieves the names of the nodes in the Elasticsearch cluster.

    Returns:
    - List[str]: A list of node names in the Elasticsearch cluster.
    """
    try:
      nodes_info = self.es.cat.nodes(format="json")
      node_names = [node['name'] for node in nodes_info]
      # print(f"Name: {node['name']}, IP: {node['ip']}, Roles: {node['node.role']}, Master: {node['master']}, Heap Percent: {node['heap.percent']}, RAM Percent: {node['ram.percent']}, CPU: {node['cpu']}, Load 1m: {node['load_1m']}, Load 5m: {node['load_5m']}, Load 15m: {node['load_15m']}")
      return node_names
    except ConnectionError:
      self.logger.error(f"Error connecting to ES at {self.es_host}:{self.es_port}")
      return None
    except Exception as e:
      self.logger.error(f"Unexpected error retrieving node names: {e}")
      return None
    
  def env(self) -> str:
    """
    Determines if the Elasticsearch cluster is running on PROD or UAT (staging).

    Returns:
    - str: 'PROD' if the cluster is in the production environment,
           'UAT' if the cluster is in the staging environment.
    """
    node_names = self.get_cluster_node_names()
    if node_names:
      for name in node_names:
        if 'prod' in name.lower():
          return 'PROD'
        elif 'uat' in name.lower():
          return 'UAT'
    return 'Unknown'

  def get_alias(self) -> Dict:
    """
    Retrieves all aliases in the Elasticsearch cluster.
    """
    return self.es.indices.get_alias("*")

  def create_listing_tracking_index(self):
    index_name = "rlp_listing_tracking_1"
    alias_name = self.listing_tracking_index_name

    index_schema = {
      "mappings": {
        "properties": {
            "price": {"type": "float"},
            "addedOn": {"type": "date"},
            "lastUpdate": {"type": "date"},
            "guid": {"type": "keyword"},
            "propertyType": {"type": "keyword"}
            }
        }
    }

    # Create the index if it doesn't already exist
    if not self.es.indices.exists(index=index_name):
      self.es.indices.create(index=index_name, body=index_schema)
      self.logger.info(f"Index '{index_name}' created successfully.")
    else:
      self.logger.info(f"Index '{index_name}' already exists.")

    # Check if the alias already exists
    if not self.es.indices.exists_alias(name=alias_name):
      self.es.indices.put_alias(index=index_name, name=alias_name)
      self.logger.info(f"Alias '{alias_name}' created for index '{index_name}'.")
    else:
      self.logger.info(f"Alias '{alias_name}' already exists.")

  def create_mkt_trends_index(self):
    index_name = "rlp_mkt_trends_1"
    alias_name = self.mkt_trends_index_name

    index_schema = {
      "mappings": {
        "properties": {
          "geog_id": {"type": "keyword"},
          "propertyType": {"type": "keyword"},
          "geo_level": {"type": "integer"},
          "metrics": {
            "properties": {
              "median_price": {
                "type": "nested",
                "properties": {
                  "month": {"type": "date", "format": "yyyy-MM"},
                  "value": {"type": "float"}
                }
              },
              "median_dom": {
                "type": "nested",
                "properties": {
                  "month": {"type": "date", "format": "yyyy-MM"},
                  "value": {"type": "float"}
                }
              },
              "absorption_rate": {
                "type": "nested",
                "properties": {
                  "month": {"type": "date", "format": "yyyy-MM"},
                  "value": {"type": "float"}
                }
              },
              "last_mth_median_asking_price": {
                "type": "nested",
                "properties": {
                  "month": {"type": "date", "format": "yyyy-MM"},
                  "value": {"type": "float"}
                }
              },
              "last_mth_new_listings": {
                "type": "nested",
                "properties": {
                  "month": {"type": "date", "format": "yyyy-MM"},
                  "value": {"type": "long"}
                }
              },
              "over_ask_percentage": {
                "type": "nested",
                "properties": {
                  "month": {"type": "date", "format": "yyyy-MM"},
                  "value": {"type": "float"}
                }
              },
              "sold_listing_count": {
                "type": "nested",
                "properties": {
                  "month": {"type": "date", "format": "yyyy-MM"},
                  "value": {"type": "integer"}
                }
              }
            }
          },
          "last_updated": {"type": "date"}
        }
      }
    }
    
    # Create the index if it doesn't already exist
    if not self.es.indices.exists(index=index_name):
      self.es.indices.create(index=index_name, body=index_schema)
      self.logger.info(f"Index '{index_name}' created successfully.")
    else:
      self.logger.info(f"Index '{index_name}' already exists.")

    # Check if the alias already exists
    if not self.es.indices.exists_alias(name=alias_name):
      self.es.indices.put_alias(index=index_name, name=alias_name)
      self.logger.info(f"Alias '{alias_name}' created for index '{index_name}'.")
    else:
      self.logger.info(f"Alias '{alias_name}' already exists.")


  def get_mapping(self, index: str) -> dict:
    """Retrieve mappings for a specific index."""
    return self.es.indices.get_mapping(index=index)
  
  def count(self, index: str) -> int:
    """Count the number of documents in a specific index."""
    return self.es.count(index=index)['count']
  
  def search(self, 
             index: str, 
             start_time: datetime = None, 
             end_time: datetime = None, 
             filters: Dict = None,
             _id: str = None,
             selects: List[str] = None,
             size=10) -> List[dict]:
    """
    Searches an Elasticsearch index for documents within a specified time range.

    Parameters:
    - index (str): The name of the Elasticsearch index to search.
    - start_time (datetime, optional): The start of the time range for the search. Defaults to None.
    - end_time (datetime, optional): The end of the time range for the search. Defaults to None.
    - filters (dict, optional): Additional key-value pairs to filter the search. Defaults to None.
    - _id (str, optional): The document ID to search for. Defaults to None.
    - selects (List[str], optional): List of fields to retrieve. If None, all fields are retrieved.
    - size (int | None, optional): The maximum number of search hits to return. Defaults to 10. If None, returns all hits.

    Returns:
    - List[dict]: A list of dictionaries, each representing a document that matches the search criteria.

    Raises:
    - ValueError: If an invalid index name is provided.

    Note: if _id is provided, all other criteria are ignored and just retrieves that doc.
    """
    self.logger.debug(f"Adhoc searching index '{index}'")
    if _id:
      try:
        return [self.es.get(index=index, id=_id, _source=selects or True)['_source']]
      except NotFoundError:
        self.logger.warning(f"Document with ID '{_id}' not found in index '{index}'.")
        return []

    if index == self.sold_listing_index_name:
      date_format = '%Y-%m-%dT%H:%M:%S'
      pertinent_time_field = 'lastTransition'
    elif index == self.listing_index_name:
      # date_format = '%Y/%m/%d'
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'addedOn'
    elif index == self.mkt_trends_index_name:
      # 2024-07-12T00:00:00Z
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'last_updated'
    elif index == self.listing_tracking_index_name:
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'addedOn'
    elif index == self.geo_index_name or index == self.geo_details_en_index_name:
      date_format = None       # TODO: decide later on date range filtering
      pertinent_time_field = None
    elif start_time is not None or end_time is not None:
      raise NotImplementedError(f"start_time or end_time provided but {index} need date format and pertinent_time_field.")
    
    query_body = self._build_query(pertinent_time_field, start_time, end_time, date_format,
                                   filters)
    self.logger.debug(f"Constructed query: {query_body}")
    if size is None:
      results = []
      for hit in scan(self.es, index=index, query=query_body, _source=selects or True):
        results.append(hit['_source'])
      return results
    elif size > 200:
      results = []
      for hit in scan(self.es, index=index, query=query_body, _source=selects or True):
        results.append(hit['_source'])
        if len(results) >= size:
          break
      return results
    else:
      response = self.es.search(index=index, body=query_body, size=size, _source=selects or True)
      return [hit['_source'] for hit in response['hits']['hits']]

  def get_sold_listings(self, start_time: datetime = None, end_time: datetime = None, 
                        date_field: str = "lastTransition", selects: List[str]=None) -> Tuple[bool, pd.DataFrame]:
    """
    Query sold listings from the past year filtered by property types:
    - CONDO, SEMI-DETACHED, TOWNHOUSE, DETACHED
    Parameters:
    - start_time (datetime, optional): The start of the time range for the search. Defaults to one year ago.
    - end_time (datetime, optional): The end of the time range for the search. Defaults to None (current time).
    - date_field (str, optional): The field to use for date filtering. Defaults to "lastTransition".
    - selects (List[str], optional): List of fields to retrieve. If None, all fields are retrieved.
    Returns:
    Tuple[bool, pd.DataFrame]: A tuple containing:
        - A boolean indicating success (True) or failure (False)
        - A DataFrame representing the sold listings.
    """
    
    self.logger.info(f"Fetching sold listings from {start_time if start_time else 'unspecified'} to {end_time if end_time else 'unspecified'}")
    start_process_time = time.time()

    try:
      must_clauses = [
        {
          "match": {"listingStatus": "SOLD"},
        },
        {
          "match": {"transactionType": "SALE"}
        },
        {
          "bool": {
            "should": self.property_type_query_mappings
          }
        }
      ]

      if start_time is None:
        start_time = datetime.now() - timedelta(days=365)  # one year ago

      if date_field == "lastTransition":
        date_format = '%Y-%m-%dT%H:%M:%S'
      elif date_field == "lastUpdate":
        date_format = '%y-%m-%d:%H:%M:%S'
      else:
        raise ValueError(f"Unsupported date_field: {date_field}")

      time_range_filter = {
        "range": {
          date_field: {
            "gte": start_time.strftime(date_format)
          }
        }
      }

      #  Add end_time to the query if provided
      if end_time:
        time_range_filter["range"][date_field]["lte"] = end_time.strftime(date_format)

      must_clauses.append(time_range_filter)
      
      query_body = {
        "query": {
          "bool": {
            "must": must_clauses
          }
        },
        "_source": selects or True  # Select all fields if no fields are specified
      }
      self.logger.debug(f"Constructed query: {query_body}")
      
      scroll_id = None
      sources = []
    
      page = self.es.search(index=self.sold_listing_index_name, body=query_body, scroll='5m', size=500)
      scroll_id = page['_scroll_id']
      total_hits = page['hits']['total']['value']
      self.logger.info(f"Total hits for sold listings: {total_hits}")

      hits = page['hits']['hits']

      while hits:
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']
    except ConnectionError as e:
      self.logger.error(f"Connection error while fetching sold listings: {str(e)}")
      return False, pd.DataFrame()
    
    except Exception as e:
      self.logger.error(f"Unexpected error in get_sold_listing: {e}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
    
    finally:
      self._safe_remove_scroll_id(scroll_id)

    if not sources:
      self.logger.info("No sold listings found")
      return False, pd.DataFrame()
    
    self.logger.info(f"Fetched {len(sources)} sold listings")

    def serialize_dict_cols(x: Any):
      return json.dumps(x) if isinstance(x, dict) else x
    
    # creating the dataframe
    try:
      sold_listings_df = pd.DataFrame(sources)
      if 'listingType' in sold_listings_df.columns:
        sold_listings_df.listingType = sold_listings_df.listingType.apply(format_listingType)

      # Add property type
      sold_listings_df['propertyType'] = sold_listings_df.apply(derive_property_type, axis=1)

      for col in ['climateInfo', 'NBHDEnEntry', 'NBHDFrEntry']:
        if col in sold_listings_df.columns:
          sold_listings_df[col] = sold_listings_df[col].apply(serialize_dict_cols)

      # format known datetime column 
      if 'lastTransition' in sold_listings_df.columns:
        sold_listings_df.lastTransition = pd.to_datetime(sold_listings_df.lastTransition)


      process_time = time.time() - start_process_time
      self.logger.info(f"Completed fetching sold listings in {process_time:.2f} seconds")
      return True, sold_listings_df
    except Exception as e:
      self.logger.error(f"Error converting sold listings to DataFrame: {e}")
      return False, pd.DataFrame()


  def get_current_active_listings(self, selects: List[str], prov_code: str=None,
                                  addedOn_start_time: datetime=None, addedOn_end_time: datetime=None,
                                  updated_start_time: datetime=None, updated_end_time: datetime=None,
                                  use_script_for_last_update: bool=False,  # TODO: should comment out later after dev
                                  active=True
                                  ):
    """
    This is used in context of nearby_comparable_solds.NearbyComparableSoldsProcessor ETL pipeline.
    Fetches current active or non-active listings based on specified filters for a NearbyComparableSoldsProcessor ETL pipeline.

    Parameters:
    - selects (List[str]): Fields to retrieve.
    - prov_code (str, optional): Province code to filter listings by.
    - addedOn_start_time (datetime, optional): Start time for filtering listings added on.
    - addedOn_end_time (datetime, optional): End time for filtering listings added on.
    - updated_start_time (datetime, optional): Start time for filtering listings last updated.
    - updated_end_time (datetime, optional): End time for filtering listings last updated.
    - use_script_for_last_update (bool, optional): If True, uses a script for filtering by last update time. Requires both updated_start_time and updated_end_time to be set.
    - active (bool, optional): If True, fetches active listings. If False, fetches non-active listings.

    Returns:
    Tuple[bool, pd.DataFrame]: A tuple containing a boolean indicating if the fetch was successful, and a DataFrame of the listings.
    """

    self.logger.info(f'Fetching current {"active" if active else "non active"} listings for province: {prov_code}')
    start_process_time = time.time()

    try:
      must_filters = [
          {"match": {"transactionType": "SALE"}},
          {"bool": {"should": self.property_type_query_mappings}}
      ]
      if active:
        must_filters.append({"match": {"listingStatus": "ACTIVE"}})
      else:
        must_filters.append({"bool": {"must_not": {"match": {"listingStatus": "ACTIVE"}}}})

      if prov_code is not None:
        must_filters.append({"match": {"provState": prov_code}})

      # Prepare time range filters
      time_filters = []

      if addedOn_start_time or addedOn_end_time:
        added_range = {}
        if addedOn_start_time:
          added_range["gte"] = addedOn_start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')
        if addedOn_end_time:
          added_range["lte"] = addedOn_end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')
        time_filters.append({"range": {"addedOn": added_range}})

      if updated_start_time or updated_end_time:
        if use_script_for_last_update:
          if updated_start_time is None or updated_end_time is None:
            raise ValueError("Both updated_start_time and updated_end_time must be provided when using script query for lastUpdate")

          script_query = {
            "script": {
              "script": {
                "source": """
                  DateTimeFormatter formatter = DateTimeFormatter.ofPattern('yy-MM-dd:HH:mm:ss');
                  LocalDateTime start = LocalDateTime.parse(params.start, formatter);
                  LocalDateTime end = LocalDateTime.parse(params.end, formatter);
                  if (doc['lastUpdate.keyword'].size() == 0) {
                    return false;
                  }
                  LocalDateTime lastUpdate = LocalDateTime.parse(doc['lastUpdate.keyword'].value, formatter);
                  return lastUpdate.isAfter(start) && lastUpdate.isBefore(end);
                """,
                "params": {
                  "start": updated_start_time.strftime('%y-%m-%d:%H:%M:%S'),
                  "end": updated_end_time.strftime('%y-%m-%d:%H:%M:%S')
                }
              }
            }
          }
          
          time_filters.append(script_query)
        else:
          updated_range = {}
          if updated_start_time:
            updated_range["gte"] = updated_start_time.strftime('%y-%m-%d:%H:%M:%S')
          if updated_end_time:
            updated_range["lte"] = updated_end_time.strftime('%y-%m-%d:%H:%M:%S')
          time_filters.append({"range": {"lastUpdate": updated_range}})

      # Add time filters to the query if any are specified
      if time_filters:
        must_filters.append({"bool": {"should": time_filters}})
      
      query_body = {
        "query": {
          "bool": {
            "must": must_filters
          }
        },
        "_source": selects or True  # Select all fields if no fields are specified
      }
      
      self.logger.debug(f"Query body: {query_body}")

      scroll_id = None
      sources = []
    
      page = self.es.search(index=self.listing_index_name, body=query_body, scroll='5m', size=500)
      scroll_id = page['_scroll_id']
      total_hits = page['hits']['total']['value']
      self.logger.info(f"Total hits for current {'active' if active else 'non active'} listings: {total_hits}")
      hits = page['hits']['hits']

      while hits:
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

      self.logger.info(f"Fetched {len(sources)} current {'active' if active else 'non active'} listings")

    except ConnectionError as e:
      self.logger.error(f"Connection error while fetching current active listings: {str(e)}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
    
    except RequestError as e:
      self.logger.error(f"Request error while fetching current active listings: {str(e)}")
      self.logger.error(f"Query body: {query_body}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
  
    except Exception as e:
      self.logger.error(f"Unexpected error in get_current_active_listings: {e}")
      self.logger.error(f"Query body: {query_body}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
    
    finally:
      self._safe_remove_scroll_id(scroll_id)

    if not sources:
      self.logger.info("No current active listings found")
      return False, pd.DataFrame()

    try:

      listings_df = pd.DataFrame(sources)
      if 'listingType' in listings_df.columns:
        listings_df.listingType = listings_df.listingType.apply(format_listingType)

      # Add property type
      listings_df['propertyType'] = listings_df.apply(derive_property_type, axis=1)

      # format known datetime column
      if 'addedOn' in listings_df.columns:
        # listings_df.addedOn = pd.to_datetime(listings_df.addedOn)
        listings_df.addedOn = listings_df.addedOn.apply(parse_datetime)
      if 'lastUpdate' in listings_df.columns:
        listings_df.lastUpdate = pd.to_datetime(listings_df.lastUpdate, format='%y-%m-%d:%H:%M:%S')

      process_time = time.time() - start_process_time
      self.logger.info(f"Completed fetching current active listings in {process_time:.2f} seconds")
      return True, listings_df
    except Exception as e:
      self.logger.error(f"Error converting current active listings to DataFrame: {e}")
      return False, pd.DataFrame()
    

  def get_listings(self, selects: List[str]=None, prov_code: str=None, 
                   addedOn_start_time: datetime=None, addedOn_end_time: datetime=None,
                   updated_start_time: datetime=None, updated_end_time: datetime=None,
                   use_script_for_last_update: bool=False  # TODO: should comment out later after dev
                   ):    
    """
    Retrieves listings from Elasticsearch based on various filters and returns them as a DataFrame.

    Parameters:
    - selects (List[str], optional): A list of fields to include in the returned documents. If None, all fields are included.
    - prov_code (str, optional): The province or state code to filter listings by.
    - addedOn_start_time (datetime, optional): The start time for filtering listings based on their added date.
    - addedOn_end_time (datetime, optional): The end time for filtering listings based on their added date.
    - updated_start_time (datetime, optional): The start time for filtering listings based on their last update date.
    - updated_end_time (datetime, optional): The end time for filtering listings based on their last update date.

    Returns:
    - Tuple[bool, pd.DataFrame]: A tuple containing:
      - A boolean indicating success (True) or failure (False)
      - A pandas DataFrame of the listings.

    Note:
    - The method applies filters for transaction type and property type by default.
    - Time filters for 'addedOn' and 'lastUpdate' are applied if corresponding start and end times are provided.
    - The method handles pagination internally using Elasticsearch's scroll API.
    """
  
    self.logger.info(f"Fetching listings for province: {prov_code}")
    start_process_time = time.time()

    must_filters = [
      {"match": {"transactionType": "SALE"}},
      {"bool": {"should": self.property_type_query_mappings}}
    ]
    if prov_code is not None:
      must_filters.append({"match": {"provState": prov_code}})

    # Prepare time range filters
    time_filters = []

    if addedOn_start_time or addedOn_end_time:
      added_range = {}
      if addedOn_start_time:
        added_range["gte"] = addedOn_start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')
      if addedOn_end_time:
        added_range["lte"] = addedOn_end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')
      time_filters.append({"range": {"addedOn": added_range}})

    if updated_start_time or updated_end_time:
      if use_script_for_last_update:
        if updated_start_time is None or updated_end_time is None:
          raise ValueError("Both updated_start_time and updated_end_time must be provided when using script query for lastUpdate")
        script_query = {
          "script": {
              "script": {
                "source": """
                  DateTimeFormatter formatter = DateTimeFormatter.ofPattern('yy-MM-dd:HH:mm:ss');
                  LocalDateTime start = LocalDateTime.parse(params.start, formatter);
                  LocalDateTime end = LocalDateTime.parse(params.end, formatter);
                  if (doc['lastUpdate.keyword'].size() == 0) {
                    return false;
                  }
                  LocalDateTime lastUpdate = LocalDateTime.parse(doc['lastUpdate.keyword'].value, formatter);
                  return lastUpdate.isAfter(start) && lastUpdate.isBefore(end);
                """,
                "params": {
                  "start": updated_start_time.strftime('%y-%m-%d:%H:%M:%S'),
                  "end": updated_end_time.strftime('%y-%m-%d:%H:%M:%S')
                }
              }
            }
        }
        time_filters.append(script_query)
      else:
        updated_range = {}
        if updated_start_time:
          updated_range["gte"] = updated_start_time.strftime('%y-%m-%d:%H:%M:%S')
        if updated_end_time:
          updated_range["lte"] = updated_end_time.strftime('%y-%m-%d:%H:%M:%S')
        time_filters.append({"range": {"lastUpdate": updated_range}})

    # Add time filters to the query if any are specified
    if time_filters:
      must_filters.append({"bool": {"should": time_filters}})

    query_body = {
      "query": {
        "bool": {
          "must": must_filters
        }
      },
      "_source": selects or True  # Select all fields if no fields are specified
    }

    self.logger.debug(f"Query body: {query_body}")

    scroll_id = None
    sources = []

    try:
      page = self.es.search(index=self.listing_index_name, body=query_body, scroll='5m', size=500)
      scroll_id = page['_scroll_id']
      total_hits = page['hits']['total']['value']
      self.logger.info(f"Total hits for listings: {total_hits}")
      hits = page['hits']['hits']
    
      while hits:
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

      self.logger.info(f"Fetched {len(sources)} current listings")
    
    except ConnectionError as e:
      self.logger.error(f"Connection error while fetching listings: {str(e)}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()
    
    except RequestError as e:
      self.logger.error(f"Request error while fetching listings: {str(e)}")
      self.logger.error(f"Query body: {query_body}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()

    except Exception as e:
      self.logger.error(f"Unexpected error in get_listings: {e}")
      self.logger.error(f"Query body: {query_body}")
      self._safe_remove_scroll_id(scroll_id)
      return False, pd.DataFrame()

    finally:
      self._safe_remove_scroll_id(scroll_id)

    if not sources:
      self.logger.info("No listings found")
      return True, pd.DataFrame()

    try:
      listings_df = pd.DataFrame(sources)
      if 'listingType' in listings_df.columns:
        listings_df.listingType = listings_df.listingType.apply(format_listingType)

      # Add property type
      listings_df['propertyType'] = listings_df.apply(derive_property_type, axis=1)

      # format known datetime column
      if 'addedOn' in listings_df.columns:
        # listings_df.addedOn = pd.to_datetime(listings_df.addedOn)
        listings_df.addedOn = listings_df.addedOn.apply(parse_datetime)
      if 'lastUpdate' in listings_df.columns:
        listings_df.lastUpdate = pd.to_datetime(listings_df.lastUpdate, format='%y-%m-%d:%H:%M:%S')
  
      process_time = time.time() - start_process_time
      self.logger.info(f"Completed fetching listings in {process_time:.2f} seconds")

      return True, listings_df
    except Exception as e:
      self.logger.error(f"Error converting listings to DataFrame: {e}")
      return False, pd.DataFrame()


  def get_geo_entry_df(self) -> pd.DataFrame:
    """
    Returns a dedup dataframe of geo_entry table rows from geo db 
    This is read from a tab delimited text dump
    """
    return self.cache.get('all_geo_entry')


  # dev workaround, need to periodically manually update the guid in sold listings cache
  def update_geo_entry_df(self, data_dir: Union[str, Path]):
    """
    Updated geo_entry table dump is read from a tab delimited text file
    This is part of the solution until guid is properly stamped to new future sold listings
    """
    data_dir = Path(data_dir)
    all_geo_entry_df = pd.read_csv(data_dir/'all_geo_entry.txt', sep='\t')  # read in new .txt dump
    self.logger.info(f"Read in {len(all_geo_entry_df)} rows from updated all_geo_entry.txt")

    # combine with previous geo_entry_df
    prev_geo_entry_df = self.cache.get('all_geo_entry')
    self.logger.info(f"Read in {len(prev_geo_entry_df)} rows from previous all_geo_entry cache") 

    all_geo_entry_df = pd.concat([prev_geo_entry_df, all_geo_entry_df], ignore_index=True)
    # drop duplicates and keep last
    all_geo_entry_df.drop_duplicates(subset=['MLS', 'CITY', 'PROV_STATE'], keep='last', inplace=True)
    all_geo_entry_df.reset_index(drop=True, inplace=True)

    self.logger.info(f"Output {len(all_geo_entry_df)} rows to updated all_geo_entry cache")

    self.cache.set(key='all_geo_entry', value=all_geo_entry_df)


  def fix_guid_in_sold_listing_cache(self):
    """
    This is a temporary fix to update guid in sold listing cache
    """
    
    for sold_listing_cache_key in ['SoldMedianMetricsProcessor/five_years_sold_listing', 'NearbyComparableSoldsProcessor/one_year_sold_listing']:

      sold_listing_df = self.cache.get(sold_listing_cache_key)
      geo_entry_df = self.cache.get('all_geo_entry')

      if sold_listing_df is None or geo_entry_df is None:
        self.logger.error("Sold listings or geo entry df not found in cache.")
        return
      
      len_b4_sold_listing = len(sold_listing_df)
      self.logger.info(f'len of {sold_listing_cache_key} before fixing guid: {len_b4_sold_listing}')

      geo_entry_df.drop_duplicates(subset=['MLS', 'CITY', 'PROV_STATE'], keep='last', inplace=True)

      # Merge the geo_entry_df with the sold_listing_df
      sold_listing_df = join_df(sold_listing_df, 
                                geo_entry_df[['MLS', 'CITY', 'PROV_STATE', 'GEOGRAPHIES']], 
                                left_on=['mls', 'city', 'provState'], 
                                right_on=['MLS', 'CITY', 'PROV_STATE'], 
                                how='left')

      # sold_listing_df['guid'] = sold_listing_df['GEOGRAPHIES']
      for index, row in sold_listing_df.iterrows():
        if pd.isna(row['guid']) or row['guid'] == '' or row['guid'] == 'None':
          sold_listing_df.at[index, 'guid'] = row['GEOGRAPHIES']

      sold_listing_df.drop(columns=['MLS', 'CITY', 'PROV_STATE', 'GEOGRAPHIES'], inplace=True)  # remove the extra columns from geo_entry
      len_after_sold_listing = len(sold_listing_df)
      self.logger.info(f'len of {sold_listing_cache_key} after fixing guid: {len_after_sold_listing}')

      assert len_b4_sold_listing == len_after_sold_listing, "Length of sold listing df changed after fixing guid"

      self.cache.set(key=sold_listing_cache_key, value=sold_listing_df)

      perc_guid_null = round(sold_listing_df.guid.isnull().sum()/len(sold_listing_df) * 100, 2)
      self.logger.info(f"% of guid null in {sold_listing_cache_key}: {perc_guid_null}%")

 
  def _update_es_sold_listings_with_guid(self):
    '''
    Fix legacy sold listings with guid (aka geog_id).
    Run this only after carefully preparing five_years_sold_listing
    '''
    sold_listing_df = self.cache.get('SoldMedianMetricsProcessor/five_years_sold_listing')
    if sold_listing_df is None or sold_listing_df.empty:
      self.logger.error("Failed to retrieve 5-year sold listing data from cache.")
      return

    # Query for documents with valid coordinates, recent transactions, and missing guids
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "range": {"lat": {"gte": -90.0, "lte": 90.0}}
            },
            {
              "range": {"lng": {"gte": -180.0, "lte": 180.0}}
            },
            {
              "range": {
                "lastTransition": {
                  "gte": "now-90d/d",   # last 90 days
                  "lte": "now"
                }
              }
            }
          ],
          "must_not": {
            "exists": {
              "field": "guid.keyword"
            }
          }
        }
      }
    }

    # Use the scan helper to efficiently retrieve all matching documents
    documents_to_update = scan(
      self.es,
      index=self.sold_listing_index_name,
      query=query,
      _source=["_id"]
    )

    def generate_updates():
      for doc in tqdm(documents_to_update, desc="Updating documents"):
        doc_id = doc["_id"]
        _df = sold_listing_df.q("_id == @doc_id")
        if not _df.empty:
          guid = _df.iloc[0].guid
          if pd.notna(guid):
            yield {
              '_op_type': 'update',
              '_index': self.sold_listing_index_name,
              '_id': doc_id,
              'doc': {
                'guid': guid
              }
            }

    # Perform bulk update
    success, failed = bulk(self.es, generate_updates(), stats_only=True, raise_on_error=False)
    self.logger.info(f"Successfully updated {success} documents with guid.")
    if failed:
      self.logger.error(f"Failed to update {failed} documents")

    return success, failed


  def _delete_computed_guid(self, index_name: str):
    sold_listing_cache_keys = ['SoldMedianMetricsProcessor/five_years_sold_listing', 'NearbyComparableSoldsProcessor/one_year_sold_listing']

    for sold_listing_cache_key in sold_listing_cache_keys:
      sold_listing_df = self.cache.get(sold_listing_cache_key)

      sold_listing_df.loc[sold_listing_df['computed_guid'].notnull(), ['guid', 'computed_guid']] = None

      self.cache.set(sold_listing_cache_key, sold_listing_df)


  def get_geos(self, selects: List[str]=None, return_df=True):
    body = {"query": {"match_all": {}}}
    scroller = scan(self.es, index=self.geo_index_name, query=body)
    
    doc_ids, sources = [], []
    for hit in scroller:
      doc_id = hit['_id']
      source = hit['_source']
      doc_ids.append(doc_id)
      sources.append(source)    
    
    if return_df:
      geo_df = pd.DataFrame(sources)
      geo_df['geog_id'] = doc_ids
        
      return geo_df
    else:
      return doc_ids, sources

  def get_geo_details(self, selects: List[str] = None, return_df=True):
    # fetch geo details docs for a province, name, or lang and return them as a dataframe

    body = {
        "query": {"match_all": {}},
        "_source": selects or True
    }
      
    scroller = scan(self.es, index=self.geo_details_en_index_name, query=body)
    
    doc_ids, sources = [], []
    for hit in scroller:
      doc_id = hit['_id']
      source = hit['_source']
      doc_ids.append(doc_id)
      sources.append(source)
      
    if return_df:
      geo_details_df = pd.DataFrame([src['data'] for src in sources])
      geo_details_df['geog_id'] = doc_ids
      
      return geo_details_df
    else:
      return doc_ids, sources
    
  def _build_query(self, 
                  pertinent_time_field: str,
                  start_time: datetime = None, 
                  end_time: datetime = None, 
                  date_format: str = '%Y-%m-%dT%H:%M:%S',
                  filters: Dict = None
                  ):
    """
    Builds a time range query for Elasticsearch based on the provided start and end times.

    Parameters:
    - pertinent_time_field (str): The field in Elasticsearch documents to query against.
    - start_time (datetime, optional): The start time for the range query.
    - end_time (datetime, optional): The end time for the range query.
    - date_format (str, optional): The strftime format to convert datetime objects to strings.
    - filters (dict, optional): Additional key-value pairs to filter the search.

    Returns:
    - dict: An Elasticsearch query dict.
    """
    
    must_clauses = []

    # Add time range query if start_time or end_time is provided
    range_query = {}
    if start_time:
      range_query['gte'] = start_time.strftime(date_format)
    if end_time:
      range_query['lte'] = end_time.strftime(date_format)

    if range_query:
      must_clauses.append({"range": {pertinent_time_field: range_query}})

    # Add filters if provided
    if filters:
      for key, value in filters.items():
        if isinstance(value, str):
          must_clauses.append({"match": {key: value}})
        else:
          must_clauses.append({"term": {key: value}})

    if must_clauses:
      return {
        "query": {
          "bool": {
            "must": must_clauses
          }
        }
      }
    else:
      return {"query": {"match_all": {}}}
    

  def _safe_remove_scroll_id(self, scroll_id: str = None):
    if scroll_id:
      try:
        self.es.clear_scroll(scroll_id=scroll_id)
      except Exception as e:
        self.logger.error(f"Error removing scroll ID: {e}")

  def close(self):
    if self.es:
      self.es.close()

  # helper to summarize update and delete failures
  def summarize_update_failures(self, update_failures):
    reasons = Counter()

    for failure in update_failures:
      error = failure['update'].get('error', {})
      reason = error.get('type', 'unknown')
      reasons[reason] += 1

    self.logger.info("ES failures summary:")
    self.logger.info("\tSummary,\t\tCount")
    self.logger.info("\t-----------------")
    for reason, count in reasons.items():
      self.logger.info(f"\t\"{reason}\", {count}")
    self.logger.info("\t-----------------")

  def summarize_delete_failures(self, delete_failures):
    reasons = Counter()

    for failure in delete_failures:
      error = failure.get('delete', {}).get('result', {})
      reasons[error] += 1

    self.logger.info("ES failures summary:")
    self.logger.info("\tSummary,\t\tCount")
    self.logger.info("\t-----------------")
    for reason, count in reasons.items():
      self.logger.info(f"\t\"{reason}\", {count}")
    self.logger.info("\t-----------------")