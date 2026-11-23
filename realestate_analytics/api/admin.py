"""Optional ETL administration app, intended for the ETL server only."""
from fastapi import FastAPI
from realestate_analytics.api.etl_script_kickoff_endpoints import router as etl_router
from realestate_analytics.api.etl_monitoring_endpoints import router as monitoring_router

app = FastAPI(title="Real Estate Analytics ETL administration")
app.include_router(etl_router, prefix="/op", tags=["operations"])
app.include_router(monitoring_router, prefix="/monitor", tags=["monitoring"])
