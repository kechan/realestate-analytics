from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv
from realestate_analytics.data.archive import Archiver
from realestate_analytics.data.caching import FileBasedCache
from realestate_analytics.data.geo import GeoCollection

_ = load_dotenv(find_dotenv())
cache_dir = os.getenv('ANALYTICS_CACHE_DIR', None)

if not cache_dir:
    raise ValueError("ANALYTICS_CACHE_DIR is not set in .env file")

cache_dir = Path(cache_dir)
archive_dir = cache_dir / 'archives'

archiver = Archiver(archive_dir)
cache = FileBasedCache(cache_dir)

# Initialize GeoCollection
geo_collection_path = cache_dir / 'geo_collection.dill'
geo_collection = GeoCollection.load(geo_collection_path, use_dill=True)

def get_cache():
  return cache

def get_archiver():
  return archiver

def get_geo_collection():
  return geo_collection