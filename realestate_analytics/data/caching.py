from typing import Any, Union, Optional, List

from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

import realestate_core.common.class_extensions
from realestate_core.common.class_extensions import *

class FileBasedCache:
  def __init__(self, cache_dir: Union[str, Path] = None):
    self.cache_dir = Path(cache_dir) if cache_dir else Path.home()/'tmp'/'listing_analytics_data'   # TODO: change this before deployment
    self.cache_dir.mkdir(parents=True, exist_ok=True)

  def list_keys(self) -> List[str]:
    """
    List all valid keys in the cache.
    
    Returns:
        List[str]: A list of all valid cache keys.
    """
    keys = set()
    for file in self.cache_dir.iterdir():
      if file.is_file():
        # Remove the suffix (_df or .txt) to get the key
        key = file.stem
        if file.suffix == '.txt' or key.endswith('_df'):
          keys.add(key[:-3] if key.endswith('_df') else key)
    
    # Remove any keys that only have an expiry file
    valid_keys = [key for key in keys if 
                  (self.cache_dir / f"{key}.txt").exists() or 
                  (self.cache_dir / f"{key}_df").exists()]
    
    return sorted(valid_keys)

  def _get_cache_path(self, key: str, is_df=True) -> Path:
    suffix = '_df' if is_df else '.txt'   # TODO: only pandas dataframes or text files for now
    return self.cache_dir / f'{key}{suffix}'
  
  def set(self, key: str, value: Any, expiry: Optional[timedelta] = None):
    is_df = isinstance(value, pd.DataFrame)
    cache_path = self._get_cache_path(key, is_df)

    # Store expiry information in a separate file
    expiry_path = cache_path.with_suffix('.expiry')
    if expiry:
      expiry_time = (datetime.now() + expiry).isoformat()
      expiry_path.write_text(expiry_time)
    elif expiry_path.exists():
      expiry_path.unlink()  # Remove expiry file if no expiry is set

    if is_df:
      value.to_feather(cache_path)
    else:
      if isinstance(value, datetime):
        value = value.isoformat()
      else:
        value = str(value)
      cache_path.write_text(value)

  def get(self, key: str) -> Optional[Any]:
    for is_df in [True, False]:  # Try text file first, then DataFrame
      cache_path = self._get_cache_path(key, is_df)
      if cache_path.exists():
        # Check expiry, getting something thats expired will trigger its deletion and return None
        expiry_path = cache_path.with_suffix('.expiry')
        if expiry_path.exists():
          expiry_time = datetime.fromisoformat(expiry_path.read_text())
          if datetime.now() > expiry_time:
            self.delete(key)
            return None

        if is_df:
          return pd.read_feather(cache_path)
        else:
          value = cache_path.read_text()          
          try:
            return datetime.fromisoformat(value)
          except ValueError:  # not a iso datetime
            return value
    
    return None
  
  def delete(self, key: str) -> None:
    for is_df in [True, False]:
      cache_path = self._get_cache_path(key, is_df)
      expiry_path = cache_path.with_suffix('.expiry')
      for path in [cache_path, expiry_path]:
        if path.exists():
          path.unlink()

  def clear(self) -> None:
    for cache_file in self.cache_dir.ls('*'):
      cache_file.unlink()

  # def get_last_run(self) -> Optional[datetime]:
  #   return self.get('last_run')    

  # def set_last_run(self, dt: Optional[datetime] = None) -> None:
  #   self.set('last_run', dt or datetime.now())