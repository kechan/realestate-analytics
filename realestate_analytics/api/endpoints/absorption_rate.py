from typing import Optional

import pandas as pd

from fastapi import APIRouter, HTTPException, Query
from realestate_analytics.api.dependencies import get_archiver

router = APIRouter()

@router.get("/absorption-rate")
async def get_absorption_rate(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined.")
):
    archiver = get_archiver()
    absorption_rates = archiver.retrieve('absorption_rates')
    
    if absorption_rates is None:
        raise HTTPException(status_code=404, detail="Absorption rates not found")
    
    # Convert to DataFrame
    df = pd.DataFrame(absorption_rates)
    
    # Filter based on geog_id
    df = df[df['geog_id'] == geog_id]

    # Handle property_type filtering
    if property_type is None or property_type.upper() == 'ALL':
        # When property_type is None or 'ALL', we want the row where propertyType is 'ALL'
        df = df[df['propertyType'] == 'ALL']
    else:
        df = df[df['propertyType'] == property_type]
    
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
    
    record = df.to_dict('records')[0]
    
    if pd.isna(record['absorption_rate']):
        record['absorption_rate'] = None
    
    # Convert to list of dictionaries for JSON serialization
    month = archiver.get_latest_timestamp_in_YYYYMM('absorption_rates')
    result = {'month': month}
    result.update(record)

    return result
    
