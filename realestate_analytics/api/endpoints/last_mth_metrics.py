from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query

import pandas as pd
from realestate_analytics.api.dependencies import get_archiver

router = APIRouter()

@router.get("/last-month")
async def get_last_month_metrics(
    geog_id: str = Query(..., description="Geographic ID to filter by"),
    property_type: Optional[str] = Query(None, description="Property type to filter by. Use 'ALL' or leave empty for all property types combined.")
):
    archiver = get_archiver()
    last_mth_metrics = archiver.retrieve('last_mth_metrics_results')
    
    if last_mth_metrics is None:
        raise HTTPException(status_code=404, detail="Last month metrics not found")
    
    # Convert to DataFrame
    df = pd.DataFrame(last_mth_metrics)
    
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
    
    # Determine the month from the latest timestamp
    month = archiver.get_latest_timestamp_in_YYYYMM('last_mth_metrics_results')
    result = {'month': month}
    result.update(df.to_dict('records')[0])
        
    return result