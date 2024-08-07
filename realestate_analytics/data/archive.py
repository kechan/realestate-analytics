from pathlib import Path
import json
from datetime import datetime

import pandas as pd
import logging

class Archiver:
  def __init__(self, archive_dir: Path):
    self.logger = logging.getLogger(self.__class__.__name__)

    self.archive_dir = Path(archive_dir)    
    self.archive_dir.mkdir(parents=True, exist_ok=True)
    self.logger.info(f"Using archive directory: {self.archive_dir}")

  def archive(self, data, name: str) -> bool:
    """
    Archive the given data under the specified name.
    
    :param data: The data to archive (must be JSON serializable or a Pandas DataFrame)
    :param name: The name to give this archive (e.g., 'daily_report' or 'dataframe')
    :return: True if archiving was successful, False otherwise
    """
    timestamp = datetime.now().strftime("%Y%m%d")
    
    if isinstance(data, pd.DataFrame):
      filename = f"{name}_{timestamp}_df"
      filepath = self.archive_dir / filename
      try:
        data.to_feather(filepath)
        self.logger.info(f"Successfully archived DataFrame {name} to {filepath}")
        return True
      except Exception as e:
        self.logger.error(f"Failed to archive DataFrame {name}: {str(e)}")
        return False
    elif isinstance(data, dict):
      filename = f"{name}_{timestamp}.json"
      filepath = self.archive_dir / filename
      try:
        filepath.write_text(json.dumps(data, indent=2))
        self.logger.info(f"Successfully archived {name} to {filepath}")
        return True
      except Exception as e:
        self.logger.error(f"Failed to archive {name}: {str(e)}")
        return False
    else:
      self.logger.error(f"Unsupported data type for archiving: {type(data)}")
      return False

  
  def retrieve(self, name: str, timestamp: str = None):
    """
    Retrieve archived data.
    
    :param name: The name of the archive to retrieve
    :param timestamp: Optional timestamp to retrieve a specific version
    :return: The retrieved data as a dictionary or DataFrame, or None if not found
    """
    if timestamp:
      json_filepath = self.archive_dir / f"{name}_{timestamp}.json"
      df_filepath = self.archive_dir / f"{name}_{timestamp}_df"
    else:
      # Get the most recent archive if no timestamp is specified
      json_files = list(self.archive_dir.glob(f"{name}_*.json"))
      df_files = list(self.archive_dir.glob(f"{name}_*_df"))
      if not json_files and not df_files:
        self.logger.warning(f"No archives found for {name}")
        return None
      json_filepath = max(json_files, key=lambda p: p.stat().st_mtime) if json_files else None
      df_filepath = max(df_files, key=lambda p: p.stat().st_mtime) if df_files else None

    if df_filepath and (not json_filepath or df_filepath.stat().st_mtime > json_filepath.stat().st_mtime):
      try:
        df = pd.read_feather(df_filepath)
        self.logger.info(f"Successfully retrieved DataFrame archive {df_filepath.name}")
        return df
      except Exception as e:
        self.logger.error(f"Failed to retrieve DataFrame archive {df_filepath.name}: {str(e)}")
        return None
    elif json_filepath:
      try:
        data = json.loads(json_filepath.read_text())
        self.logger.info(f"Successfully retrieved archive {json_filepath.name}")
        return data
      except Exception as e:
        self.logger.error(f"Failed to retrieve archive {json_filepath.name}: {str(e)}")
        return None
    else:
      self.logger.warning(f"No archives found for {name}")
      return None


  def list_archives(self, name: str = None) -> list:
    """
    List all archives or archives for a specific name.
    
    :param name: Optional name to filter archives
    :return: List of archive filenames
    """
    if name:
      json_files = self.archive_dir.glob(f"{name}_*.json")
      df_files = self.archive_dir.glob(f"{name}_*_df")
    else:
      json_files = self.archive_dir.glob("*.json")
      df_files = self.archive_dir.glob("*_df")
    
    return sorted([f.name for f in json_files] + [f.name for f in df_files])