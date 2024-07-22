from typing import List, Dict, Any

import json
from datetime import datetime, timedelta
import pandas as pd

from .caching import FileBasedCache

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import scan, bulk

def derive_property_type(row):
  listing_type = row.get('listingType', '').split(', ')  # Assuming listingType is a comma-separated string
  search_category_type = row.get('searchCategoryType', None)
  
  # Check for CONDO
  if search_category_type == 101 and any(lt in ['CONDO', 'REC'] for lt in listing_type):
      return 'CONDO'
  # Check for SEMI-DETACHED
  elif search_category_type == 104:
      return 'SEMI-DETACHED'
  # Check for TOWNHOUSE
  elif search_category_type == 102:
      return 'TOWNHOUSE'
  # Check for DETACHED
  elif search_category_type == 103:
      return 'DETACHED'
  # Default case
  else:
      return 'OTHER'
  

def format_listingType(x: List[str]):
  if len(x) > 0 and x[0] == 'listingType':
    x = x[1:]
  return ', '.join(x)

class Datastore:
  def __init__(self, host: str, port: int):
    self.es = Elasticsearch([f'http://{host}:{port}/'], timeout=30, retry_on_timeout=True, max_retries=3)

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
    self.mkt_trends_ts_index_name = "rlp_mkt_trends_ts_current"

    if not self.ping():
      raise Exception("Unable to connect to Elasticsearch.")

    # get mappings
    self.sold_listing_index_mapping = self.get_mapping(index=self.sold_listing_index_name)
    self.listing_index_mapping = self.get_mapping(index=self.listing_index_name)
    self.geo_index_mapping = self.get_mapping(index=self.geo_index_name)
    self.geo_details_en_index_mapping = self.get_mapping(index=self.geo_details_en_index_name)

    try:
      self.listing_tracking_index_mapping = self.get_mapping(index=self.listing_tracking_index_name)
    except NotFoundError:
      print(f"Warning: Index '{self.listing_tracking_index_name}' not found.")

    try:
      self.mkt_trends_ts_index_mapping = self.get_mapping(index=self.mkt_trends_ts_index_name)
    except NotFoundError:
      print(f"Warning: Index '{self.mkt_trends_ts_index_name}' not found.")

    self.cache = FileBasedCache()
    
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
    except Exception as e:
      print(f"Error retrieving node names: {e}")
      return []
    
  def env(self) -> str:
    """
    Determines if the Elasticsearch cluster is running on PROD or UAT (staging).

    Returns:
    - str: 'PROD' if the cluster is in the production environment,
           'UAT' if the cluster is in the staging environment.
    """
    node_names = self.get_cluster_node_names()
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
      print(f"Index '{index_name}' created successfully.")
    else:
      print(f"Index '{index_name}' already exists.")

    # Check if the alias already exists
    if not self.es.indices.exists_alias(name=alias_name):
        self.es.indices.put_alias(index=index_name, name=alias_name)
        print(f"Alias '{alias_name}' created for index '{index_name}'.")
    else:
        print(f"Alias '{alias_name}' already exists.")

  def create_mkt_trends_ts_index(self):
    index_name = "rlp_mkt_trends_ts_1"
    alias_name = self.mkt_trends_ts_index_name

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
                            "date": {"type": "date", "format": "yyyy-MM"},
                            "value": {"type": "float"}
                        }
                    },
                    "median_dom": {
                        "type": "nested",
                        "properties": {
                            "date": {"type": "date", "format": "yyyy-MM"},
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
      print(f"Index '{index_name}' created successfully.")
    else:
      print(f"Index '{index_name}' already exists.")

    # Check if the alias already exists
    if not self.es.indices.exists_alias(name=alias_name):
      self.es.indices.put_alias(index=index_name, name=alias_name)
      print(f"Alias '{alias_name}' created for index '{index_name}'.")
    else:
      print(f"Alias '{alias_name}' already exists.")


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

    if _id:
      try:
        return [self.es.get(index=index, id=_id, _source=selects or True)['_source']]
      except NotFoundError:
        return []

    if index == self.sold_listing_index_name:
      date_format = '%Y-%m-%dT%H:%M:%S'
      pertinent_time_field = 'lastTransition'
    elif index == self.listing_index_name:
      # date_format = '%Y/%m/%d'
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'addedOn'
    elif index == self.mkt_trends_ts_index_name:
      # 2024-07-12T00:00:00Z
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'last_updated'
    elif index == self.listing_tracking_index_name:
      date_format = '%Y-%m-%dT%H:%M:%S.%f'
      pertinent_time_field = 'addedOn'
    elif start_time is not None or end_time is not None:
      raise NotImplementedError(f"start_time or end_time provided but {index} need date format and pertinent_time_field.")
    
    query_body = self._build_query(pertinent_time_field, start_time, end_time, date_format,
                                   filters)
    print(query_body)
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
                        date_field: str = "lastTransition", selects: List[str]=None, return_df=True):
    """
    Query sold listings from the past year filtered by property types:
    - CONDO, SEMI-DETACHED, TOWNHOUSE, DETACHED

    Parameters:
    - start_time (datetime, optional): The start of the time range for the search. Defaults to one year ago.
    - end_time (datetime, optional): The end of the time range for the search. Defaults to None (current time).
    - date_field (str, optional): The field to use for date filtering. Defaults to "lastTransition".
    - selects (List[str], optional): List of fields to retrieve. If None, all fields are retrieved.
    - return_df (bool): If True, returns a DataFrame along with the raw data. Defaults to True.

    Returns:
    DataFrame: A DataFrame representing the sold listings.
    List[dict]: A list of dictionaries where each dictionary represents a sold listing.   
  """
    
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

    page = self.es.search(index=self.sold_listing_index_name, body=query_body, scroll='5m', size=500)
    scroll_id = page['_scroll_id']
    hits = page['hits']['hits']
    sources = []

    try:
      while hits:
        # sources.extend(hit['_source'] for hit in hits)
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

    finally:
      self.es.clear_scroll(scroll_id=scroll_id)

    if return_df:

      def serialize_dict_cols(x: Any):
        return json.dumps(x) if isinstance(x, dict) else x
      
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

      return sources, sold_listings_df
    else:
      return sources


  def get_current_active_listings(self, selects: List[str], prov_code: str=None,
                                  addedOn_start_time: datetime=None, addedOn_end_time: datetime=None,
                                  updated_start_time: datetime=None, updated_end_time: datetime=None,
                                  return_df=True):
    """
    This is used in context of nearby_comparable_solds.NearbyComparableSoldsProcessor ETL pipeline.
    """
    must_filters = [
        {"match": {"listingStatus": "ACTIVE"}},
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
    
    page = self.es.search(index=self.listing_index_name, body=query_body, scroll='5m', size=500)
    scroll_id = page['_scroll_id']
    hits = page['hits']['hits']

    sources = []

    try:
      while hits:
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

    finally:
      self.es.clear_scroll(scroll_id=scroll_id)

    if return_df:

      listings_df = pd.DataFrame(sources)
      if 'listingType' in listings_df.columns:
        listings_df.listingType = listings_df.listingType.apply(format_listingType)

      # Add property type
      listings_df['propertyType'] = listings_df.apply(derive_property_type, axis=1)

      # format known datetime column
      if 'addedOn' in listings_df.columns:
        listings_df.addedOn = pd.to_datetime(listings_df.addedOn)
      if 'lastUpdate' in listings_df.columns:
        listings_df.lastUpdate = pd.to_datetime(listings_df.lastUpdate, format='%y-%m-%d:%H:%M:%S')

      return sources, listings_df
    else:
      return sources
    

  def get_listings(self, selects: List[str]=None, prov_code: str=None, 
                   addedOn_start_time: datetime=None, addedOn_end_time: datetime=None,
                   updated_start_time: datetime=None, updated_end_time: datetime=None,
                   return_df=True):    
    """
    Retrieves listings from Elasticsearch based on various filters and returns them as a list of dictionaries or a DataFrame.

    Parameters:
    - selects (List[str], optional): A list of fields to include in the returned documents. If None, all fields are included.
    - prov_code (str, optional): The province or state code to filter listings by.
    - addedOn_start_time (datetime, optional): The start time for filtering listings based on their added date.
    - addedOn_end_time (datetime, optional): The end time for filtering listings based on their added date.
    - updated_start_time (datetime, optional): The start time for filtering listings based on their last update date.
    - updated_end_time (datetime, optional): The end time for filtering listings based on their last update date.
    - return_df (bool, optional): If True, returns the results as a pandas DataFrame; otherwise, returns a list of dictionaries.

    Returns:
    - Tuple[List[dict], pd.DataFrame] if return_df is True: A tuple containing a list of the raw documents and a pandas DataFrame of the listings.
    - List[dict] if return_df is False: A list of the raw documents.

    Note:
    - The method applies filters for transaction type and property type by default.
    - Time filters for 'addedOn' and 'lastUpdate' are applied if corresponding start and end times are provided.
    - The method handles pagination internally using Elasticsearch's scroll API.
    """

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

    page = self.es.search(index=self.listing_index_name, body=query_body, scroll='5m', size=500)
    scroll_id = page['_scroll_id']
    hits = page['hits']['hits']

    sources = []

    try:
      while hits:
        for hit in hits:
          doc = hit['_source']
          doc['_id'] = hit['_id']  # incl. as primary key
          sources.append(doc)

        page = self.es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

    finally:
      self.es.clear_scroll(scroll_id=scroll_id)

    if return_df:

      listings_df = pd.DataFrame(sources)
      if 'listingType' in listings_df.columns:
        listings_df.listingType = listings_df.listingType.apply(format_listingType)

      # Add property type
      listings_df['propertyType'] = listings_df.apply(derive_property_type, axis=1)

      # format known datetime column
      if 'addedOn' in listings_df.columns:
        listings_df.addedOn = pd.to_datetime(listings_df.addedOn)
      if 'lastUpdate' in listings_df.columns:
        listings_df.lastUpdate = pd.to_datetime(listings_df.lastUpdate, format='%y-%m-%d:%H:%M:%S')

      return sources, listings_df
    else:
      return sources


  def get_geo_entry_df(self) -> pd.DataFrame:
    """
    Returns a dedup dataframe of geo_entry table rows from geo db 
    This is read from a tab delimited text dump
    """
    return self.cache.get('all_geo_entry')




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
    

