"""ETL-server-only dependencies for the optional administration app."""
import os
from pathlib import Path
from functools import lru_cache


@lru_cache(maxsize=1)
def get_cache():
    from dotenv import load_dotenv, find_dotenv
    from realestate_analytics.data.caching import FileBasedCache

    load_dotenv(find_dotenv())
    cache_dir = os.getenv("ANALYTICS_CACHE_DIR")
    if not cache_dir:
        raise RuntimeError("ANALYTICS_CACHE_DIR is required for the admin app")
    return FileBasedCache(Path(cache_dir))
