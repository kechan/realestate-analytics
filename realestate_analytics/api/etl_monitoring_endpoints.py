from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from pathlib import Path
import re, os, logging
from datetime import datetime

from dotenv import load_dotenv, find_dotenv

from realestate_analytics.api.dependencies import get_cache

logger = logging.getLogger(__name__)

# Load environment variables
_ = load_dotenv(find_dotenv())

# Get the script directory from environment variable
SCRIPT_DIR = os.getenv('ANALYTICS_ETL_SCRIPT_DIR')
if not SCRIPT_DIR:
    raise EnvironmentError("ANALYTICS_ETL_SCRIPT_DIR is not set in the environment variables")
SCRIPT_DIR = Path(SCRIPT_DIR)
if not SCRIPT_DIR.is_dir():
    raise EnvironmentError(f"ANALYTICS_ETL_SCRIPT_DIR '{SCRIPT_DIR}' is not a valid directory")

etl_type_to_log_prefix = {
  "nearby_comparable_solds": "nearby_solds",
  "historic_metrics": "hist_median_metrics",
  "last_mth_metrics": "last_mth_metrics",
  "absorption_rate": "absorption_rate",
}

router = APIRouter()

class StageInfo(BaseModel):
    status: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    annotation: Optional[str] = None

class JobStatus(BaseModel):
    job_id: str
    overall_status: str
    stages: Dict[str, StageInfo]
    errors: List[str] = []

class JobSummary(BaseModel):
  job_id: str
  etl_type: str
  overall_status: str
  start_time: Optional[datetime] = None
  end_time: Optional[datetime] = None


@router.get("/etl-types", response_model=List[str])
async def get_valid_etl_types():
    """
    Returns a list of all valid ETL types.
    """
    return list(etl_type_to_log_prefix.keys())

@router.get("/job/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    cache = get_cache()
    log_path = SCRIPT_DIR/f"{job_id}.log"

    if not log_path.exists():
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    stages = ['Extract', 'Transform', 'Load'] # Add any additional stages here
    if job_id.startswith("last_mth_metrics"):
        stages.extend(['End_of_mth_run', 'Compute_last_month_metrics', 'Remove_deleted_listings', 'Update_mkt_trends'])
        
    job_status = JobStatus(job_id=job_id, overall_status="In Progress", stages={})
    job_status.stages = {stage: StageInfo(status="Not Completed") for stage in stages}

    # Check if the job is completed
    if cache.get(f"{job_id}_all_success") is not None:
        job_status.overall_status = "Completed"
        job_status.stages = {stage: StageInfo(status="Completed") for stage in stages}
    else:
        # Check individual stages
        for stage in stages:
            cache_key = f"{job_id}_{stage}_success"
            if cache.get(cache_key) is not None:
                job_status.stages[stage] = StageInfo(status="Completed")                           

    # Parse log file for timing information and errors
    with log_path.open('r') as log_file:
        for line in log_file:
            if "[MONITOR]" in line:
                parse_monitor_line(line, job_status)
            elif "ERROR" in line:
                job_status.errors.append(line.strip())

    # Update overall status based on errors
    if job_status.errors and job_status.overall_status != "Completed":
        job_status.overall_status = "Failed"

    return job_status




# Helper function to validate etl_type and retrieve the log prefix
def validate_etl_type_and_get_log_prefix(etl_type: Optional[str]):
    if etl_type and etl_type not in etl_type_to_log_prefix:
        raise HTTPException(status_code=400, detail=f"Invalid ETL type: {etl_type}")
    return etl_type_to_log_prefix.get(etl_type) if etl_type else None


# Helper function to filter and retrieve log files (skips rotated logs if needed)
def get_filtered_log_files(log_prefix: Optional[str], skip_rotated_logs: bool = False, search_query: Optional[str] = None):
    rotated_log_pattern = re.compile(r'.*_[0-9]{8}_[0-9]{6}$')
    
    return [
        f for f in sorted(SCRIPT_DIR.glob("*.log"), key=os.path.getmtime, reverse=True)
        if (not log_prefix or f.stem.startswith(log_prefix)) and
           (not skip_rotated_logs or not rotated_log_pattern.match(f.stem)) and
           (not search_query or search_query.lower() in f.stem.lower())
    ]

# Helper function to get job summaries from log files
async def get_job_summaries(log_files: List[Path], etl_type: Optional[str], limit: Optional[int] = None):
    jobs = []
    for log_file in log_files:
        job_id = log_file.stem
        job_status = await get_job_status(job_id)

        # Derive ETL type from job_id
        derived_etl_type = next((et for et, prefix in etl_type_to_log_prefix.items() if job_id.startswith(prefix)), None)

        # Only add job if it matches the requested ETL type (if specified)
        if not etl_type or derived_etl_type == etl_type:
            jobs.append(JobSummary(
                job_id=job_id,
                etl_type=derived_etl_type or "Unknown",
                overall_status=job_status.overall_status,
                start_time=min((stage.start_time for stage in job_status.stages.values() if stage.start_time), default=None),
                end_time=max((stage.end_time for stage in job_status.stages.values() if stage.end_time), default=None)
            ))

        # Stop when limit is reached, if applicable
        if limit and len(jobs) >= limit:
            break
    
    return jobs


@router.get("/jobs/recent", response_model=List[JobSummary])
async def get_recent_jobs(
    limit: int = Query(10, ge=1, le=100),
    etl_type: Optional[str] = None
):
    logger.info(f"Fetching recent jobs. Limit: {limit}, ETL Type: {etl_type}")
    
    log_prefix = validate_etl_type_and_get_log_prefix(etl_type)
    # Skip rotated logs in this route
    log_files = get_filtered_log_files(log_prefix, skip_rotated_logs=True)

    jobs = await get_job_summaries(log_files, etl_type, limit)
    
    logger.info(f"Returning {len(jobs)} recent job summaries")
    return jobs


@router.get("/jobs/history", response_model=List[JobSummary])
async def get_job_history(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    search_query: Optional[str] = None,
    etl_type: Optional[str] = None
):
    logger.info(f"Fetching job history. Page: {page}, Limit: {limit}, ETL Type: {etl_type}, Search Query: {search_query}")
    
    log_prefix = validate_etl_type_and_get_log_prefix(etl_type)
    # Do NOT skip rotated logs in this route
    log_files = get_filtered_log_files(log_prefix, skip_rotated_logs=False, search_query=search_query)

    # Apply pagination
    start_index = (page - 1) * limit
    relevant_files = log_files[start_index:start_index + limit]

    jobs = await get_job_summaries(relevant_files, etl_type)
    
    logger.info(f"Returning {len(jobs)} job summaries")
    return jobs


@router.get("/jobs/active", response_model=Optional[JobStatus])
async def get_active_job(etl_type: Optional[str] = None):
    logger.info(f"Fetching active job for ETL Type: {etl_type}")
    
    log_prefix = validate_etl_type_and_get_log_prefix(etl_type)
    # Skip rotated logs in this route
    log_files = get_filtered_log_files(log_prefix, skip_rotated_logs=True)

    for log_file in log_files:
        job_id = log_file.stem
        job_status = await get_job_status(job_id)
        if job_status.overall_status == "In Progress":
            return job_status
    
    return None

def parse_monitor_line(line: str, job_status: JobStatus):
    timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
    if not timestamp_match:
        return

    try:
        timestamp = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
    except ValueError:
        # Handle incorrect timestamp format
        return
    
    if "Starting" in line:
        stage = line.split("Starting")[-1].split("stage")[0].strip()
        if stage not in job_status.stages:
            job_status.stages[stage] = StageInfo(status="In Progress", annotation="Unexpected stage")

        job_status.stages[stage].start_time = timestamp
        job_status.stages[stage].status = "In Progress"
    elif "Completed" in line:
        stage = line.split("Completed")[-1].split("stage")[0].strip()
        job_status.stages[stage].end_time = timestamp
        job_status.stages[stage].status = "Completed"
        if job_status.stages[stage].start_time:
            duration = (job_status.stages[stage].end_time - job_status.stages[stage].start_time).total_seconds()
            if duration == 0.0:
                job_status.stages[stage].annotation = "Restored from cache during rerun."

            job_status.stages[stage].duration = round(duration, 2)


