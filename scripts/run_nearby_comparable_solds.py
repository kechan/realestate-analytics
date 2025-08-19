from typing import Dict, Any
import argparse
import logging
import yaml
import os, json, sys
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

from realestate_analytics.utils.constants import VALID_PROVINCES

from realestate_analytics.etl.run_utils import load_config, get_next_job_id, update_run_csv, is_last_run_successful

from realestate_analytics.etl.nearby_comparable_solds import NearbyComparableSoldsProcessor
from realestate_analytics.data.es import Datastore
from realestate_analytics.data.bq import BigQueryDatastore

# Default values
DEFAULT_ES_HOST = "localhost"
DEFAULT_ES_PORT = 9200
DEFAULT_LOG_LEVEL = "INFO"
JOB_ID_PREFIX = "nearby_solds"

def get_script_dir():
  return Path(__file__).resolve().parent

def rotate_log_file(log_filename: Path):
  """Rename the existing log file by appending a timestamp if it exists."""
  if log_filename.exists():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    rotated_log_filename = log_filename.with_name(f"{log_filename.stem}_{timestamp}{log_filename.suffix}")
    log_filename.rename(rotated_log_filename)
    print(f"Rotated old log file to {rotated_log_filename}")

def get_last_deletion_check_date(tracking_file: Path) -> datetime:
  if tracking_file.exists():
    with open(tracking_file, 'r') as f:
      data = json.load(f)
    return datetime.fromisoformat(data['last_check'])
  return datetime.min

def update_deletion_check_date(tracking_file: Path):
  with open(tracking_file, 'w') as f:
    json.dump({'last_check': datetime.now().isoformat()}, f)

def should_check_deletions(tracking_file: Path) -> bool:
  last_check = get_last_deletion_check_date(tracking_file)
  today = datetime.now().date()
  last_check_date = last_check.date()

  # Check if it's been 7 days since the last check
  if (today - last_check_date).days >= 7:
    return True
  
  # Check if it's the first day of the month
  if today.day == 1 and last_check_date.month != today.month:
    return True
  
  # Check if it's the last day of the month
  tomorrow = today + timedelta(days=1)
  if tomorrow.month != today.month and last_check_date.month != today.month:
    return True
  
  return False

def main():
  parser = argparse.ArgumentParser(description="Run NearbyComparableSoldsProcessor ETL")
  parser.add_argument("--config", required=True, help="Path to the YAML configuration file")
  parser.add_argument("--es_host", help="Elasticsearch host")
  parser.add_argument("--es_port", type=int, help="Elasticsearch port")
  parser.add_argument("--force_full_load", action="store_true", help="Force a full load instead of incremental")
  parser.add_argument("--prov_code", 
                      default="ON", 
                      choices=VALID_PROVINCES,
                      help="Province code (e.g., ON, BC, AB), ON is default")
  parser.add_argument("--log_level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")

  #TODO: Remove this after dev
  parser.add_argument("--sim_failure_at_pre_transform", action="store_true", help="Simulate a failure at the transform stage")
  parser.add_argument("--sim_success_at_post_load", action="store_true", help="Simulate a success at the post-load stage")

  args = parser.parse_args()

  # Load configuration
  config_path = Path(args.config).resolve()
  if not config_path.exists():
    print(f"Configuration file not found: {config_path}. Exiting.")
    sys.exit(1)

  config = load_config(config_path=config_path)

  # Use config values, command-line args, or defaults
  es_host = args.es_host or config.get('es_host') or DEFAULT_ES_HOST
  es_port = args.es_port or config.get('es_port') or DEFAULT_ES_PORT
  log_level = args.log_level or config.get('log_level') or DEFAULT_LOG_LEVEL

  # nearby_solds_run.csv keeps historical run results, which for simplicity be always in the same directory as this script
  hist_runs_csv_path = get_script_dir() / "nearby_solds_run.csv"

  #  Check if the last run was successful before proceeding
  if not is_last_run_successful(hist_runs_csv_path):
    print("="*50)
    print("PREVIOUS ETL RUN FAILED - HALTING EXECUTION")
    print(f"The last run in {hist_runs_csv_path} shows a failure.")
    print("Please investigate the failure before running again.")
    print("No new NearbyComparableSoldsProcessor job will be started.")
    print("="*50)
    sys.exit(1)

  # Set up job ID and logging
  job_id = get_next_job_id(hist_runs_csv_path, job_prefix=JOB_ID_PREFIX)
  log_filename = get_script_dir() / f"{job_id}.log"

  # Rotate old log file if it exists
  rotate_log_file(log_filename)

  logging.basicConfig(
    filename=str(log_filename),
    level=log_level,
    format='%(asctime)s [%(levelname)s] [Logger: %(name)s]: %(message)s',
    filemode='w'
  )
  # Set specific log levels for certain loggers
  # TODO: adjust this during deployment as needed
  logging.getLogger('elasticsearch').setLevel(logging.WARNING)
  logging.getLogger('Datastore').setLevel(logging.INFO)
  logging.getLogger('BigQueryDatastore').setLevel(logging.INFO)

  # Log the start of the run
  logging.info("="*50)
  logging.info(f"Starting new run for job {job_id} at {datetime.now().isoformat()}")
  logging.info(f"Province: {args.prov_code}")
  logging.info("="*50)

  # Initialize datastores
  datastore = Datastore(host=es_host, port=es_port)
  bq_datastore = BigQueryDatastore()

  # Set up the tracking file for BigQuery deletion checks and determine if we should check for deletions
  bq_deletion_check_file = get_script_dir() / "bq_deletion_check.json"
  check_bq_deletions = should_check_deletions(bq_deletion_check_file)

  if check_bq_deletions:
    update_deletion_check_date(bq_deletion_check_file)

  # Initialize and run the processor
  processor = NearbyComparableSoldsProcessor(
    job_id=job_id,
    datastore=datastore,
    bq_datastore=bq_datastore,
    check_bq_deletions=check_bq_deletions,
    prov_code=args.prov_code
  )

  if args.sim_failure_at_pre_transform:
    processor.simulate_failure_at = "transform"

  if args.sim_success_at_post_load:
    processor.simulate_success_at = "load"

  if args.force_full_load:
    processor.cleanup()

  try:
    processor.run()
  except Exception as e:
    logging.exception(f"Error during .run() for job {job_id}: {e}")

  # Update the CSV with the run results
  success = update_run_csv(csv_path=hist_runs_csv_path, job_id=job_id, processor=processor)

  # Log the end of the run
  logging.info("="*50)
  logging.info(f"Finished run for job {job_id} at {datetime.now().isoformat()}")
  logging.info("="*50)

  if not success:
    sys.exit(1)

if __name__ == "__main__":
  main()

# Should be run daily.