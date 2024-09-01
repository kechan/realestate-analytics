from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, List, Optional
from pathlib import Path
import re, os
from datetime import datetime

from dotenv import load_dotenv, find_dotenv

from realestate_analytics.api.dependencies import get_cache

# Get the script directory from environment variable
SCRIPT_DIR = os.getenv('ANALYTICS_ETL_SCRIPT_DIR')
if not SCRIPT_DIR:
    raise EnvironmentError("ANALYTICS_ETL_SCRIPT_DIR is not set in the environment variables")
SCRIPT_DIR = Path(SCRIPT_DIR)
if not SCRIPT_DIR.is_dir():
    raise EnvironmentError(f"ANALYTICS_ETL_SCRIPT_DIR '{SCRIPT_DIR}' is not a valid directory")

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


