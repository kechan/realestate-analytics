from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from pathlib import Path
import re, os, logging
from datetime import datetime

from dotenv import load_dotenv, find_dotenv

# from realestate_analytics.etl.nearby_comparable_solds import NearbyComparableSoldsProcessor
# from realestate_analytics.etl.historic_sold_median_metrics import SoldMedianMetricsProcessor
# from realestate_analytics.etl.last_mth_metrics import LastMthMetricsProcessor
# from realestate_analytics.etl.absorption_rate import AbsorptionRateProcessor

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

# Note: log prefix is also the job id prefix
# etl_type_to_log_prefix = {
#   "nearby_comparable_solds": "nearby_solds",
#   "historic_metrics": "hist_median_metrics",
#   "last_mth_metrics": "last_mth_metrics",
#   "absorption_rate": "absorption_rate",
# }

# etl_type_to_class_names = {
#   "nearby_comparable_solds": "NearbyComparableSoldsProcessor",
#   "historic_metrics": "SoldMedianMetricsProcessor",
#   "last_mth_metrics": "LastMthMetricsProcessor",
#   "absorption_rate": "AbsorptionRateProcessor",
# }

router = APIRouter()

class ETLJobInfo:
    # etl_type are the top level keys in _INFO
    _INFO = {
        "nearby_comparable_solds": {
            "log_prefix": "nearby_solds",
            "class_name": "NearbyComparableSoldsProcessor"
        },
        "historic_metrics": {
            "log_prefix": "hist_median_metrics",
            "class_name": "SoldMedianMetricsProcessor"
        },
        "last_mth_metrics": {
            "log_prefix": "last_mth_metrics",
            "class_name": "LastMthMetricsProcessor"
        },
        "absorption_rate": {
            "log_prefix": "absorption_rate",
            "class_name": "AbsorptionRateProcessor"
        },
        "current_mth_metrics": {
            "log_prefix": "current_mth_metrics",
            "class_name": "CurrentMthMetricsProcessor"
        }
    }

    @classmethod
    def get_log_prefix(cls, etl_type: str) -> str:
        return cls._INFO[etl_type]["log_prefix"]

    @classmethod
    def get_class_name(cls, etl_type: str) -> str:
        return cls._INFO[etl_type]["class_name"]

    @classmethod
    def get_etl_type_by_log_prefix(cls, log_prefix: str) -> str:
        for etl_type, info in cls._INFO.items():
            if info["log_prefix"] == log_prefix:
                return etl_type
        return None

    @classmethod
    def get_etl_type_by_class_name(cls, class_name: str) -> str:
        for etl_type, info in cls._INFO.items():
            if info["class_name"] == class_name:
                return etl_type
        return None

    @classmethod
    def is_valid_etl_type(cls, etl_type: str) -> bool:
        return etl_type in cls._INFO

    @classmethod
    def get_all_etl_types(cls) -> List[str]:
        return list(cls._INFO.keys())

    @classmethod
    def get_log_prefix_from_job_id(cls, job_id: str) -> str:
        for info in cls._INFO.values():
            if job_id.startswith(info["log_prefix"]):
                return info["log_prefix"]
        return None

    @classmethod
    def get_class_name_from_job_id(cls, job_id: str) -> Optional[str]:
        log_prefix = cls.get_log_prefix_from_job_id(job_id)
        if log_prefix:
            etl_type = cls.get_etl_type_by_log_prefix(log_prefix)
            return cls.get_class_name(etl_type)
        return None

    @classmethod
    def get_etl_type_from_job_id(cls, job_id: str) -> Optional[str]:
        # ETLJobInfo.get_etl_type_by_log_prefix(ETLJobInfo.get_log_prefix_from_job_id(job_id))
        log_prefix = cls.get_log_prefix_from_job_id(job_id)
        if log_prefix:
            return cls.get_etl_type_by_log_prefix(log_prefix)
        return None
        


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
    # return list(etl_type_to_log_prefix.keys())
    return ETLJobInfo.get_all_etl_types()

@router.get("/job/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    etl_class_name = ETLJobInfo.get_class_name_from_job_id(job_id)
    print(f'etl_class_name: {etl_class_name}')
    
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
    if cache.get(f"{etl_class_name}/{job_id}_all_success") is not None:
        job_status.overall_status = "Completed"
        job_status.stages = {stage: StageInfo(status="Completed") for stage in stages}
    else:
        # Check individual stages
        for stage in stages:
            cache_key = f"{etl_class_name}/{job_id}_{stage}_success"
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
    # if etl_type and etl_type not in etl_type_to_log_prefix:
    if etl_type and not ETLJobInfo.is_valid_etl_type(etl_type):
        raise HTTPException(status_code=400, detail=f"Invalid ETL type: {etl_type}")
    # return etl_type_to_log_prefix.get(etl_type) if etl_type else None
    return ETLJobInfo.get_log_prefix(etl_type) if etl_type else None


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
        # derived_etl_type = next((et for et, prefix in etl_type_to_log_prefix.items() if job_id.startswith(prefix)), None)
        derived_etl_type = ETLJobInfo.get_etl_type_from_job_id(job_id)

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


