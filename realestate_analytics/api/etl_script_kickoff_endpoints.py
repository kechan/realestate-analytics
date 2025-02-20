from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
import subprocess
import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

# Example:
# curl -X POST "http://localhost:8000/op/nearby_comparable_solds?config_path=%2Fpath%2Fto%2Fyour%2Fconfig.yaml"

# Load environment variables
_ = load_dotenv(find_dotenv())

# Get the script directory from environment variable
SCRIPT_DIR = os.getenv('ANALYTICS_ETL_SCRIPT_DIR')
if not SCRIPT_DIR:
    raise EnvironmentError("ANALYTICS_ETL_SCRIPT_DIR is not set in the environment variables")
SCRIPT_DIR = Path(SCRIPT_DIR)
if not SCRIPT_DIR.is_dir():
    raise EnvironmentError(f"ANALYTICS_ETL_SCRIPT_DIR '{SCRIPT_DIR}' is not a valid directory")
    

router = APIRouter()

class BaseETLJobParams(BaseModel):
    es_host: Optional[str] = None
    es_port: Optional[int] = None
    log_level: Optional[str] = None

class FullLoadETLJobParams(BaseETLJobParams):
    force_full_load: Optional[bool] = False

def run_etl_job(script_name: str, config_path: str, params: BaseETLJobParams, force_full_load: bool = False):
    script_path = SCRIPT_DIR/f"run_{script_name}.py"
    if not script_path.exists():
        raise HTTPException(status_code=404, detail=f"ETL script {script_name} not found")

    cmd = [
        "python",
        str(script_path),
        "--config", config_path
    ]
    if params.es_host:
        cmd.extend(["--es_host", params.es_host])
    if params.es_port:
        cmd.extend(["--es_port", str(params.es_port)])
    if params.log_level:
        cmd.extend(["--log_level", params.log_level])
    if force_full_load:
        cmd.append("--force_full_load")

    print(f"Executing command: {' '.join(map(str, cmd))}")

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )

        return {"message": f"{script_name} job started", "pid": process.pid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start {script_name} job: {str(e)}")

@router.post("/nearby_comparable_solds")
async def run_nearby_comparable_solds(
    config_path: str = Query(..., description="Path to the configuration file"),
    params: Optional[FullLoadETLJobParams] = None
):
    if params is None:
        params = FullLoadETLJobParams()
    return run_etl_job("nearby_comparable_solds", config_path, params, params.force_full_load)

@router.post("/absorption_rate")
async def run_absorption_rate(
    config_path: str = Query(..., description="Path to the configuration file"),
    params: Optional[BaseETLJobParams] = None
):
    if params is None:
        params = BaseETLJobParams()
    return run_etl_job("absorption_rate", config_path, params)

@router.post("/last_mth_metrics")
async def run_last_mth_metrics(
    config_path: str = Query(..., description="Path to the configuration file"),
    params: Optional[BaseETLJobParams] = None
):
    if params is None:
        params = BaseETLJobParams()
    return run_etl_job("last_mth_metrics", config_path, params)

@router.post("/current_mth_metrics")
async def run_current_mth_metrics(
  config_path: str = Query(..., description="Path to the configuration file"),
  params: Optional[BaseETLJobParams] = None
):
  if params is None:
    params = BaseETLJobParams()
  return run_etl_job("current_mth_metrics", config_path, params)

@router.post("/historic_metrics")
async def run_historic_metrics(
    config_path: str = Query(..., description="Path to the configuration file"),
    params: Optional[FullLoadETLJobParams] = None
):
    if params is None:
        params = FullLoadETLJobParams()
    return run_etl_job("historic_metrics", config_path, params, params.force_full_load)