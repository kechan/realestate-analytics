import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from realestate_analytics.api.endpoints import historic_sold_metrics, last_mth_metrics, absorption_rate, geos
from realestate_analytics.api.etl_script_kickoff_endpoints import router as etl_router
from realestate_analytics.api.etl_monitoring_endpoints import router as monitoring_router

# uvicorn main:app --reload --log-level debug

# This should be at the top of your main FastAPI application file
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Create a logger for this module
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.debug("Application is starting up")
    yield
    # Shutdown
    logger.debug("Application is shutting down")

app = FastAPI(lifespan=lifespan)

# Include the ETL script kickoff endpoints router with the /op prefix
app.include_router(etl_router, prefix="/op", tags=["operations"])

# Include the ETL monitoring endpoints router with the /monitoring prefix
app.include_router(monitoring_router, prefix="/monitor", tags=["monitoring"])

# geo info and metrics related routers
app.include_router(historic_sold_metrics.router, prefix="/metrics/historic_sold", tags=["historic_sold"])
app.include_router(last_mth_metrics.router, prefix="/metrics", tags=["last-month"])
app.include_router(absorption_rate.router, prefix="/metrics", tags=["absorption-rate"])
app.include_router(geos.router, prefix="/geos", tags=["geos"])
# app.include_router(geos.router, prefix="/geos/search", tags=["geos"])

# uvicorn main:app --reload

@app.get("/")
async def root():
    return {"message": "Welcome to the Real Estate Analytics API"}