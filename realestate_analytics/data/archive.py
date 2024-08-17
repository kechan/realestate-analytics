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
      filename = f"{name}_{timestamp}_df.txt"
      dtype_filename = f"{name}_{timestamp}_dtypes.json"
      filepath = self.archive_dir / filename
      dtype_filepath = self.archive_dir / dtype_filename
      try:
        # Save dataframe to csv with tab delimiter
        # data.to_feather(filepath)
        data.to_csv(filepath, sep='\t', index=False)

        # Save the data types
        dtypes = data.dtypes.apply(lambda x: str(x)).to_dict()
        dtype_filepath.write_text(json.dumps(dtypes, indent=2))
        
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
    elif isinstance(data, str):
      filename = f"{name}_{timestamp}.txt"
      filepath = self.archive_dir / filename
      try:
        filepath.write_text(data)
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
      df_filepath = self.archive_dir / f"{name}_{timestamp}_df.txt"
      dtype_filepath = self.archive_dir / f"{name}_{timestamp}_dtypes.json"
      txt_filepath = self.archive_dir / f"{name}_{timestamp}.txt"
    else:
      # Get the most recent archive if no timestamp is specified
      json_files = list(self.archive_dir.glob(f"{name}_*.json"))
      df_files = list(self.archive_dir.glob(f"{name}_*_df.txt"))
      txt_files = list(self.archive_dir.glob(f"{name}_*.txt"))

      if not json_files and not df_files and not txt_files:
        self.logger.warning(f"No archives found for {name}")
        return None
      
      json_filepath = max(json_files, key=lambda p: p.stat().st_mtime) if json_files else None
      df_filepath = max(df_files, key=lambda p: p.stat().st_mtime) if df_files else None
      dtype_filepath = self.archive_dir / f"{df_filepath.stem.split('_df')[0]}_dtypes.json" if df_filepath else None
      txt_filepath = max(txt_files, key=lambda p: p.stat().st_mtime) if txt_files else None

    if df_filepath and df_filepath.exists():
      try:
        # Load dtypes
        if dtype_filepath and dtype_filepath.exists():
          dtypes = json.loads(dtype_filepath.read_text())
        else:
          dtypes = None

        # df = pd.read_feather(df_filepath)
        df = pd.read_csv(df_filepath, sep='\t', dtype=dtypes)
        self.logger.info(f"Successfully retrieved DataFrame archive {df_filepath.name}")
        return df
      except Exception as e:
        self.logger.error(f"Failed to retrieve DataFrame archive {df_filepath.name}: {str(e)}")
        return None
    elif json_filepath and json_filepath.exists():
      try:
        data = json.loads(json_filepath.read_text())
        self.logger.info(f"Successfully retrieved archive {json_filepath.name}")
        return data
      except Exception as e:
        self.logger.error(f"Failed to retrieve archive {json_filepath.name}: {str(e)}")
        return None
    elif txt_filepath and txt_filepath.exists():
      try:
        data = txt_filepath.read_text()
        self.logger.info(f"Successfully retrieved archive {txt_filepath.name}")
        return data
      except Exception as e:
        self.logger.error(f"Failed to retrieve archive {txt_filepath.name}: {str(e)}")
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
      df_files = self.archive_dir.glob(f"{name}_*_df.txt")
      txt_files = [f for f in self.archive_dir.glob(f"{name}_*.txt") if not f.name.endswith('_df.txt')]
    else:
      json_files = self.archive_dir.glob("*.json")
      df_files = self.archive_dir.glob("*_df.txt")
      txt_files = [f for f in self.archive_dir.glob("*.txt") if not f.name.endswith('_df.txt')]
    
    return sorted([f.name for f in json_files] + [f.name for f in df_files] + [f.name for f in txt_files])