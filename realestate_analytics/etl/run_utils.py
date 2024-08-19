from typing import Dict, Any
from pathlib import Path
from datetime import datetime
import logging
import yaml

import pandas as pd

from .base_etl import BaseETLProcessor


# Helper methods for use in the scripts that run the ETL processes

def load_config(config_path: Path) -> Dict[str, Any]:
  with config_path.open('r') as file:
    return yaml.safe_load(file)

def get_next_job_id(csv_path: Path, job_prefix: str) -> str:
  """
  Determines the next job ID based on the last entry in the historical runs CSV file.

  This function reads a CSV file specified by `csv_path`. If the file does not exist or is empty,
  it returns a default job ID "nearby_solds_1". Otherwise, it checks the last entry in the file:
  - If the last job's `all_status` is False, indicating a failure, it returns the same job ID to retry.
  - If the last job was successful, it increments the job number by 1 and returns the new job ID.

  Parameters:
  - csv_path (Path): The path to the CSV file containing job records.

  Returns:
  - str: The next job ID to be used.
  """
  if not csv_path.exists():
    return f"{job_prefix}_1"
  
  df = pd.read_csv(csv_path)
  
  if df.empty:
    return f"{job_prefix}_1"
  
  last_job = df.iloc[-1]
  last_job_id = last_job['job_id']
  last_number = int(last_job_id.split('_')[-1])
  
  if last_job['all_status'] == False:
    return last_job_id  # Rerun the failed job
  else:
    return f"{job_prefix}_{last_number + 1}"

def update_run_csv(csv_path: Path, job_id: str, processor: BaseETLProcessor):
  standard_stages = ['extract', 'transform', 'load']

  # get all stages, incl. extra stages
  all_stages = standard_stages + processor.extra_stages

  # Check status for all stages
  # if all stages are successful, the individual stage marker files will get clean, 
  # so the individual stages are all assumed successful if 'all' is successful
  all_status = processor._was_success('all')

  stage_statuses = {
      stage: processor._was_success(stage) or all_status
      for stage in all_stages
  }

  # Create the new row data
  new_row_data = {
      'job_id': job_id,
      'all': all_status,
      'timestamp': datetime.now().isoformat()
  }
  new_row_data.update({stage: status for stage, status in stage_statuses.items()})

  new_row = pd.DataFrame([new_row_data])
  
  if csv_path.exists():
    df = pd.read_csv(csv_path)
    df = pd.concat([df, new_row], ignore_index=True)
  else:
    df = new_row
  
  df.to_csv(csv_path, index=False)

