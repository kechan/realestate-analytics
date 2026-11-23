"""Standalone, read-only analytics serving application."""
import asyncio
import errno
import logging
import os
from pathlib import Path
from shutil import disk_usage
from tempfile import SpooledTemporaryFile
from contextlib import ExitStack, asynccontextmanager
from typing import List, Optional

from elasticsearch import AsyncElasticsearch, ElasticsearchException
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .catalog import METRIC_CATALOG, PROPERTY_TYPES
from .config import Settings
from .dependencies import get_repository
from .repository import MONTH, MissingDocument, Repository, UpstreamDataError, ESUnavailableError
from .geo_cache import combined_provenance
from .geometry_processing import NavigationGeometryProcessor
from .ims_mapping_schemas import MappingReport
from .schemas import (GeoCollectionResponse, GeoDetailFeature, GeoFeature, GeoHierarchyResponse,
                      MetricDefinition, MetricsResponse, ProvinceChildrenResponse,
                      BoundaryRequest, BoundaryCollection)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_environment()
    client = AsyncElasticsearch("https://esp.royallepage.ca:443", api_key=settings.api_key,
                                verify_certs=True, send_get_body_as="POST", timeout=10,
                                retry_on_timeout=True, max_retries=1)
    processor = None
    try:
        processor = NavigationGeometryProcessor()
        repository = Repository(client, settings, geometry_processor=processor)
        try:
            await repository.verify()
        except (ElasticsearchException, ESUnavailableError):
            if settings.geo_collection_path is None:
                raise
            logger.warning("ES unavailable at startup; geography fallback is configured, readiness remains unhealthy")
        app.state.repository = repository
        yield
    finally:
        try:
            if processor is not None:
                await asyncio.to_thread(processor.close)
        finally:
            await client.close()


app = FastAPI(title="Real Estate Analytics API", version="1.0.0", lifespan=lifespan)
origins = [x.strip() for x in os.getenv("ALLOW_ORIGINS", "http://localhost:3000").split(",") if x.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True,
                   allow_methods=["GET", "POST"], allow_headers=["*"])


@app.exception_handler(MissingDocument)
async def missing_handler(request, exc):
    return JSONResponse(status_code=404, content={"detail": "Document not found"})


@app.exception_handler(UpstreamDataError)
async def data_handler(request, exc):
    logger.warning("Malformed upstream document: %s", exc)
    return JSONResponse(status_code=502, content={"detail": "Invalid upstream data"})


@app.exception_handler(ElasticsearchException)
@app.exception_handler(ESUnavailableError)
async def transport_handler(request, exc):
    logger.warning("Elasticsearch request failed: %s", type(exc).__name__)
    return JSONResponse(status_code=503, content={"detail": "Elasticsearch unavailable"})


@app.get("/health/live")
async def live():
    return {"status": "ok"}


@app.get("/health/ready")
async def ready(repository: Repository = Depends(get_repository)):
    try:
        await repository.verify()
    except Exception:
        logger.warning("Elasticsearch readiness check failed")
        raise HTTPException(status_code=503, detail="Elasticsearch unavailable")
    return {"status": "ready"}


@app.get("/api/v1/metrics", response_model=List[MetricDefinition])
async def metric_catalog():
    return [{"name": name, "unit": values[0], "description": values[1], "value_type": values[2]}
            for name, values in METRIC_CATALOG.items()]


@app.post("/api/v1/ims-mappings/preview", response_model=MappingReport, summary="Preview IMS CSV property types",
          description="Read-only CSV computation. For multiple CSVs, concatenate their bytes in order and send their byte lengths in X-IMS-CSV-Sizes. Later rows win by IMS listing ID. No ES requests or writes to mapping files.")
async def preview_ims_csv(request: Request, repository: Repository = Depends(get_repository)):
    try:
        from .ims_csv_preview import MAX_CSV_FILES, InvalidCSV, MappingFilesUnavailable, csv_batch_preview
    except ImportError:
        raise HTTPException(status_code=503, detail="CSV preview requires the optional monitor dependencies")
    media_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if media_type not in {"text/csv", "application/octet-stream", "application/vnd.ims.csv-batch"}:
        raise HTTPException(status_code=415, detail="Send a CSV or an IMS CSV batch")
    lengths = None
    sizes = request.headers.get("x-ims-csv-sizes")
    if media_type == "application/vnd.ims.csv-batch" and sizes is None:
        raise HTTPException(status_code=422, detail="CSV batches require X-IMS-CSV-Sizes")
    if sizes is not None:
        parts = sizes.split(",")
        if (not 1 <= len(parts) <= MAX_CSV_FILES or
                any(not part.isascii() or not part.isdecimal() or len(part) > 19 or int(part) == 0 for part in parts)):
            raise HTTPException(status_code=422, detail="Send the byte lengths of 1 to 20 non-empty CSVs")
        lengths = [int(part) for part in parts]
    if not hasattr(repository, "ims_csv_lock"):
        repository.ims_csv_lock = asyncio.Lock()
    if repository.ims_csv_lock.locked():
        raise HTTPException(status_code=409, detail="Another CSV preview is running")
    async with repository.ims_csv_lock:
        # Temporary upload storage is separate from ETL caches and deleted on
        # every exit. No multipart dependency or persistent upload registry.
        upload_dir = Path.home() / "tmp"
        await asyncio.to_thread(upload_dir.mkdir, parents=True, exist_ok=True)
        spool_threshold = 8 * 1024 * 1024
        content_length = request.headers.get("content-length", "")
        expected_lengths = lengths or []
        if lengths is None and content_length.isascii() and content_length.isdecimal() and len(content_length) <= 19:
            expected_lengths = [int(content_length)]
        disk_bytes = sum(length for length in expected_lengths if length > spool_threshold)
        storage_error = "Not enough temporary disk space for the CSV preview"
        if disk_bytes and disk_bytes > (await asyncio.to_thread(disk_usage, upload_dir)).free:
            raise HTTPException(status_code=507, detail=storage_error)
        with ExitStack() as uploads:
            streams = [uploads.enter_context(SpooledTemporaryFile(
                max_size=spool_threshold, dir=upload_dir, mode="w+b"))
                for _ in range(len(lengths) if lengths else 1)]
            index = 0
            remaining = lengths[0] if lengths else 0
            try:
                async for block in request.stream():
                    if lengths is None:
                        await asyncio.to_thread(streams[0].write, block)
                        continue
                    offset = 0
                    while offset < len(block):
                        if index == len(streams):
                            raise HTTPException(status_code=422, detail="CSV byte lengths do not match the uploaded body")
                        size_to_write = min(remaining, len(block) - offset)
                        await asyncio.to_thread(streams[index].write, memoryview(block)[offset:offset + size_to_write])
                        offset += size_to_write
                        remaining -= size_to_write
                        if remaining == 0:
                            index += 1
                            if index < len(streams):
                                remaining = lengths[index]
            except OSError as exc:
                if exc.errno in {errno.ENOSPC, errno.EDQUOT}:
                    raise HTTPException(status_code=507, detail=storage_error) from exc
                raise
            if lengths is not None and index != len(streams):
                raise HTTPException(status_code=422, detail="CSV byte lengths do not match the uploaded body")
            for stream in streams:
                stream.seek(0)
            try:
                result = await asyncio.to_thread(csv_batch_preview, streams, repository.settings.analytics_cache_dir)
            except InvalidCSV as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            except MappingFilesUnavailable as exc:
                raise HTTPException(status_code=503, detail=str(exc))
            return JSONResponse(result)


@app.get("/api/v1/ims-mappings", response_model=MappingReport, summary="IMS mapping monitor", description=(
    "Group all Production IMS sold records by exact Type/SubType/Style and stored propertyType. "
    "Returns counts and conflicts, reconciled with current local mapping files using vectorized joins. "
    "Lookup consistency is not historical provenance. Read-only; no individual listings are returned."))
async def ims_mapping_monitor(repository: Repository = Depends(get_repository)):
    try:
        from .ims_mapping_monitor import mapping_report
    except ImportError:
        raise HTTPException(status_code=503, detail="IMS monitor requires its optional pandas/pyarrow dependencies")
    if not hasattr(repository, "ims_monitor_lock"):
        repository.ims_monitor_lock = asyncio.Lock()
    if repository.ims_monitor_lock.locked():
        raise HTTPException(status_code=409, detail="An IMS scan is already running in this worker")
    async with repository.ims_monitor_lock:
        return JSONResponse(await mapping_report(repository))


@app.get("/api/v1/metrics/{geog_id}", response_model=MetricsResponse, response_model_exclude_unset=True)
async def market_trends(geog_id: str, property_type: str = "ALL",
                        metrics: Optional[List[str]] = Query(None, description=(
                            "Return only the named metrics; repeat this parameter to select several. "
                            "Select current_metrics explicitly for the current snapshot. "
                            "Leave omitted to return all monthly series and the current snapshot.")),
                        start_month: Optional[str] = None, end_month: Optional[str] = None,
                        repository: Repository = Depends(get_repository)):
    property_type = property_type.upper()
    if property_type not in PROPERTY_TYPES:
        raise HTTPException(status_code=422, detail="Invalid property type")
    for value in (start_month, end_month):
        if value is not None and not MONTH.fullmatch(value):
            raise HTTPException(status_code=422, detail="Months must use YYYY-MM")
    if start_month and end_month and start_month > end_month:
        raise HTTPException(status_code=422, detail="start_month must not follow end_month")
    names = metrics if metrics is not None else list(METRIC_CATALOG)
    if not names or any(name not in METRIC_CATALOG for name in names):
        raise HTTPException(status_code=422, detail="Invalid metric selection")
    return await repository.metrics(geog_id, property_type, list(dict.fromkeys(names)),
                                    start_month, end_month)


@app.get("/api/v1/geos", response_model=GeoCollectionResponse)
async def list_geos(query: Optional[str] = None, province: Optional[str] = None,
                    level: Optional[int] = None, parent_id: Optional[str] = None,
                    include_geometry: bool = False,
                    limit: Optional[int] = Query(None, ge=1, le=200, description=(
                        "Maximum number of matching geographies to return. Leave blank to return ALL matches "
                        "after offset, with or without search filters. There is no default page-size cap.")),
                    offset: Optional[int] = Query(None, ge=0, description=(
                        "Number of matching records to skip. Leave blank to skip none. "
                        "Supplying offset, including 0, does not impose a result limit.")),
                    repository: Repository = Depends(get_repository)):
    effective_offset = offset if offset is not None else 0
    page, total = await repository.list_geographies(query, province, level, parent_id,
                                                   limit, effective_offset)
    features = [await repository.feature(g, include_geometry) for g in page]
    return {"features": features, "total": total, "limit": limit, "offset": effective_offset,
            "provenance": combined_provenance(features)}


@app.get("/api/v1/provinces/{province}/children", response_model=ProvinceChildrenResponse,
         description=("Immediate navigation areas within a province: regions and unlinked cities, "
                      "excluding their descendants. Missing parent links are not invented. "
                      "Optional boundaries are simplified at 0.0025 degrees for navigation; "
                      "use the geo detail endpoint for exact boundaries."))
async def province_children(province: str, include_geometry: bool = False,
                            repository: Repository = Depends(get_repository)):
    province = province.upper()
    if province not in {"AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"}:
        raise HTTPException(status_code=422, detail="Invalid province")
    features = await repository.province_children(province, include_geometry)
    return {"features": features, "total": len(features), "limit": None, "offset": 0,
            "provenance": combined_provenance(features),
            "geometry_tolerance": 0.0025 if include_geometry else None}


@app.get("/api/v1/geos/{geog_id}", response_model=GeoDetailFeature)
async def geo_detail(geog_id: str, include_geometry: bool = False,
                     repository: Repository = Depends(get_repository)):
    return await repository.get_geography_detail(geog_id, include_geometry)


@app.post("/api/v1/geos/boundaries", response_model=BoundaryCollection,
          description="Read-only bulk navigation polygons for explicit geo IDs. No hierarchy queries or ES writes. Detail boundaries remain exact.")
async def navigation_boundaries(selection: BoundaryRequest, repository: Repository = Depends(get_repository)):
    return {"features": await repository.navigation_boundaries(selection.geog_ids)}


@app.get("/api/v1/geos/{geog_id}/hierarchy", response_model=GeoHierarchyResponse)
async def geo_hierarchy(geog_id: str, include_geometry: bool = False,
                        repository: Repository = Depends(get_repository)):
    path, seen = [], set()
    current_id = geog_id
    while current_id:
        if current_id in seen:
            raise UpstreamDataError("Cyclic geography hierarchy")
        seen.add(current_id)
        current, _ = await repository.get_geography(current_id)
        path.append(current)
        current_id = current["parent"]
    complete = path[-1]["parent_status"] == "root"
    features = [await repository.feature(g, include_geometry) for g in reversed(path)]
    return {"features": features, "complete": complete, "provenance": combined_provenance(features)}
