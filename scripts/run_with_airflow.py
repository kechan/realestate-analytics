"""
Real Estate Analytics ETL Pipeline DAG
Orchestrates the complete ETL pipeline while preserving existing logging infrastructure
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import csv
import json
from pathlib import Path
import logging

# DAG configuration
default_args = {
  'owner': 'analytics-team',
  'depends_on_past': False,
  'start_date': days_ago(1),
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 0,  # NO automatic retries - manual intervention only
  'max_active_runs': 1,  # Prevent overlapping runs
}

dag = DAG(
  'real_estate_analytics_etl',
  default_args=default_args,
  description='Real Estate Analytics ETL Pipeline with strict sequential execution',
  schedule_interval='0 2 * * *',  # Daily at 2 AM
  catchup=False,
  tags=['analytics', 'etl', 'real-estate'],
  max_active_tasks=1,  # STRICT SEQUENTIAL - only 1 task runs at a time
  max_active_runs=1,   # Only 1 DAG run at a time
)

# Configuration from Airflow Variables
SCRIPT_DIR = Variable.get("analytics_script_dir", "/home/jupyter/Analytics/realestate-analytics/scripts")
CONFIG_FILE = Variable.get("analytics_config_file", "prod_config.yaml")
PYTHON_PATH = Variable.get("analytics_python_path", "/home/jupyter/airflow_env/bin/python")

def get_bash_command(script_name, config_file=None, additional_args=""):
  """Generate standardized bash command for ETL scripts"""
  config = config_file or CONFIG_FILE
  return f"""
  cd {SCRIPT_DIR} && \
  {PYTHON_PATH} {script_name} --config {config} {additional_args}
  """

def extract_job_metadata(**context):
  import logging
  from pathlib import Path
  import csv

  task_id = context['task'].task_id
  script_mapping = {
    'run_nearby_comparable_solds': 'nearby_solds_run.csv',
    'run_historic_metrics': 'hist_median_metrics_run.csv',
    'run_last_mth_metrics': 'last_mth_metrics_run.csv',
    'run_current_mth_metrics': 'current_mth_metrics_run.csv',
    'run_absorption_rate': 'absorption_rate_run.csv',
  }

  # Map back from extractor to runner
  if task_id.startswith("extract_metadata_"):
    run_task = "run_" + task_id.replace("extract_metadata_", "")
  else:
    logging.warning(f"[{task_id}] Unrecognized naming convention")
    return

  csv_file = script_mapping.get(run_task)
  if not csv_file:
    logging.warning(f"[{task_id}] No mapping found for task ID {run_task}")
    return

  csv_path = Path(SCRIPT_DIR) / csv_file
  if not csv_path.exists():
    logging.warning(f"[{task_id}] File {csv_path} not found")
    return

  try:
    with open(csv_path, 'r') as f:
      reader = csv.DictReader(f)
      rows = list(reader)

    if not rows:
      logging.warning(f"[{task_id}] File is empty: {csv_path}")
      return

    latest_run = rows[-1]
    metadata = {
      'job_id': latest_run.get('job_id'),
      'all_status': latest_run.get('all') == 'True',
      'timestamp': latest_run.get('timestamp'),
      'stages': {
        k: v == 'True'
        for k, v in latest_run.items()
        if k not in ['job_id', 'all', 'timestamp']
      },
      'csv_file': str(csv_path)
    }

    logging.info(f"[{task_id}] Extracted metadata: {metadata}")
    context['task_instance'].xcom_push(key='job_metadata', value=metadata)
    logging.info(f"[{task_id}] XCom pushed")

    return metadata

  except Exception as e:
    logging.error(f"[{task_id}] Error reading metadata from {csv_path}: {e}")
    return


def pipeline_health_check(**context):
  """
  Comprehensive health check that validates all ETL processes
  Checks both Airflow task status and CSV file status
  """
  # Get status from all upstream tasks
  task_results = {}
  etl_tasks = [
    'run_nearby_comparable_solds',
    'run_historic_metrics', 
    'run_last_mth_metrics',
    'run_current_mth_metrics',
    'run_absorption_rate'
  ]
  
  for task_id in etl_tasks:
    try:
      # Get XCom data from extract_metadata task
      metadata = context['task_instance'].xcom_pull(
        task_ids=f"extract_metadata_{task_id.replace('run_', '')}",
        key='job_metadata'
      )
      
      if metadata:
        task_results[task_id] = {
          'airflow_success': True,
          'etl_success': metadata['all_status'],
          'job_id': metadata['job_id'],
          'timestamp': metadata['timestamp'],
          'stages': metadata['stages']
        }
      else:
        task_results[task_id] = {
          'airflow_success': False,
          'etl_success': False,
          'error': 'No metadata found'
        }
    except Exception as e:
      task_results[task_id] = {
        'airflow_success': False,
        'etl_success': False,
        'error': str(e)
      }
  
  # Generate summary
  total_tasks = len(task_results)
  successful_tasks = sum(1 for result in task_results.values() 
                        if result.get('airflow_success') and result.get('etl_success'))
  
  summary = {
    'pipeline_success': successful_tasks == total_tasks,
    'successful_tasks': successful_tasks,
    'total_tasks': total_tasks,
    'task_results': task_results,
    'timestamp': datetime.now().isoformat()
  }
  
  # Push summary to XCom
  context['task_instance'].xcom_push(key='pipeline_summary', value=summary)
  
  # Log results
  if summary['pipeline_success']:
    logging.info(f"Pipeline completed successfully: {successful_tasks}/{total_tasks} tasks passed")
  else:
    failed_tasks = [task for task, result in task_results.items() 
                   if not (result.get('airflow_success') and result.get('etl_success'))]
    logging.error(f"Pipeline failed: {failed_tasks}")
    raise Exception(f"Pipeline health check failed. Failed tasks: {', '.join(failed_tasks)}")
  
  return summary

# Task 1: Nearby Comparable Solds (no dependencies)
nearby_solds_task = BashOperator(
  task_id='run_nearby_comparable_solds',
  bash_command=get_bash_command('run_nearby_comparable_solds.py'),
  dag=dag,
)

extract_nearby_solds_metadata = PythonOperator(
  task_id='extract_metadata_nearby_comparable_solds',
  python_callable=extract_job_metadata,
  dag=dag,
)

# Task 2: Historic Metrics (runs after nearby_solds)
historic_metrics_task = BashOperator(
  task_id='run_historic_metrics', 
  bash_command=get_bash_command('run_historic_metrics.py'),
  dag=dag,
)

extract_historic_metadata = PythonOperator(
  task_id='extract_metadata_historic_metrics',
  python_callable=extract_job_metadata,
  dag=dag,
)

# Task 3: Last Month Metrics (runs after historic_metrics)
last_mth_metrics_task = BashOperator(
  task_id='run_last_mth_metrics',
  bash_command=get_bash_command('run_last_mth_metrics.py'),
  dag=dag,
)

extract_last_mth_metadata = PythonOperator(
  task_id='extract_metadata_last_mth_metrics',
  python_callable=extract_job_metadata,
  dag=dag,
)

# Task 4: Current Month Metrics (MUST run after last_mth_metrics)
current_mth_metrics_task = BashOperator(
  task_id='run_current_mth_metrics',
  bash_command=get_bash_command('run_current_mth_metrics.py'),
  dag=dag,
)

extract_current_mth_metadata = PythonOperator(
  task_id='extract_metadata_current_mth_metrics',
  python_callable=extract_job_metadata,
  dag=dag,
)

# Task 5: Absorption Rate (MUST run after nearby_solds)
absorption_rate_task = BashOperator(
  task_id='run_absorption_rate',
  bash_command=get_bash_command('run_absorption_rate.py'),
  dag=dag,
)

extract_absorption_metadata = PythonOperator(
  task_id='extract_metadata_absorption_rate',
  python_callable=extract_job_metadata,
  dag=dag,
)

# Final health check
health_check_task = PythonOperator(
  task_id='pipeline_health_check',
  python_callable=pipeline_health_check,
  trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream failures
  dag=dag,
)

# Define dependencies with metadata extraction
nearby_solds_task >> extract_nearby_solds_metadata
historic_metrics_task >> extract_historic_metadata  
last_mth_metrics_task >> extract_last_mth_metadata
current_mth_metrics_task >> extract_current_mth_metadata
absorption_rate_task >> extract_absorption_metadata

# ETL dependencies (STRICT SEQUENTIAL - exactly like your shell script)
extract_nearby_solds_metadata >> historic_metrics_task  
extract_historic_metadata >> last_mth_metrics_task
extract_last_mth_metadata >> current_mth_metrics_task
extract_current_mth_metadata >> absorption_rate_task

# All metadata extraction tasks feed into health check
extract_absorption_metadata >> health_check_task