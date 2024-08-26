import pandas as pd
from typing import Optional

def get_time_series_data(series: pd.DataFrame, geog_id: str, property_type: Optional[str]):
    # Filter the series based on geog_id and property_type
    filtered = series[series['geog_id'] == geog_id]
                      
    # Handle property_type filtering
    if property_type is None or property_type.upper() == 'ALL':
        filtered = filtered[filtered['propertyType'].isnull()]        
    else:
        filtered = filtered[filtered['propertyType'] == property_type]
        
    if filtered.empty:
        return None
    
    # Convert to list of dicts for JSON serialization
    return [
        {"month": col, "value": float(filtered[col].iloc[0])}
        for col in filtered.columns
        if col.startswith('20') and pd.notna(filtered[col].iloc[0])
    ]