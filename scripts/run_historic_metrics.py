from typing import Dict, Any
import argparse
import logging
from datetime import datetime
from pathlib import Path

from realestate_analytics.etl.run_utils import load_config, get_next_job_id, update_run_csv
from realestate_analytics.etl.historic_sold_median_metrics import SoldMedianMetricsProcessor
from realestate_analytics.data.es import Datastore

# Default values
DEFAULT_ES_HOST = "localhost"
DEFAULT_ES_PORT = 9200
DEFAULT_LOG_LEVEL = "INFO"

def get_script_dir():
  return Path(__file__).resolve().parent

def rotate_log_file(log_filename: Path):
  """Rename the existing log file by appending a timestamp if it exists."""
  if log_filename.exists():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    rotated_log_filename = log_filename.with_name(f"{log_filename.stem}_{timestamp}{log_filename.suffix}")
    log_filename.rename(rotated_log_filename)
    logging.info(f"Rotated old log file to {rotated_log_filename}")

def main():
  parser = argparse.ArgumentParser(description="Run SoldMedianMetricsProcessor ETL")
  parser.add_argument("--config", required=True, help="Path to the YAML configuration file")
  parser.add_argument("--es_host", help="Elasticsearch host")
  parser.add_argument("--es_port", type=int, help="Elasticsearch port")
  parser.add_argument("--force_full_load", action="store_true", help="Force a full load instead of incremental")
  parser.add_argument("--log_level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")
  args = parser.parse_args()

  # Load configuration
  config = load_config(Path(args.config).resolve())

  # Use config values, command-line args, or defaults
  es_host = args.es_host or config.get('es_host') or DEFAULT_ES_HOST
  es_port = args.es_port or config.get('es_port') or DEFAULT_ES_PORT
  log_level = args.log_level or config.get('log_level') or DEFAULT_LOG_LEVEL

  # hist_median_metrics_run.csv keeps historical run results
  hist_runs_csv_path = get_script_dir() / "hist_median_metrics_run.csv"

  # Set up job ID and logging
  job_id = get_next_job_id(hist_runs_csv_path, job_prefix="hist_median_metrics")
  log_filename = get_script_dir() / f"{job_id}.log"

  rotate_log_file(log_filename)

  logging.basicConfig(
    filename=str(log_filename),
    level=log_level,
    format='%(asctime)s [%(levelname)s] [Logger: %(name)s]: %(message)s',
    filemode='a'
  )
  # Set specific log levels for certain loggers
  logging.getLogger('elasticsearch').setLevel(logging.WARNING)
  logging.getLogger('Datastore').setLevel(logging.INFO)

  # Log the start of the run
  logging.info("="*50)
  logging.info(f"Starting new run for job {job_id} at {datetime.now().isoformat()}")
  logging.info("="*50)

  # Initialize datastore
  datastore = Datastore(host=es_host, port=es_port)

  # Initialize and run the processor
  processor = SoldMedianMetricsProcessor(
    job_id=job_id,
    datastore=datastore
  )

  if args.force_full_load:
    processor.cleanup()

  processor.run()

  # Update the CSV with the run results
  update_run_csv(csv_path=hist_runs_csv_path, job_id=job_id, processor=processor)

  # Log the end of the run
  logging.info("="*50)
  logging.info(f"Finished run for job {job_id} at {datetime.now().isoformat()}")
  logging.info("="*50)

if __name__ == "__main__":
  main()