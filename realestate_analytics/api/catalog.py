"""Published monthly metrics and their units."""
METRICS = {
    "median_sold_price": ("currency", "Median sold price", "number"),
    "median_dom": ("days", "Median days on market", "number"),
    "over_ask_percentage": ("percent", "Sales above asking price", "number"),
    "below_ask_percentage": ("percent", "Sales below asking price", "number"),
    "sold_listing_count": ("listings", "Sold listing count", "integer"),
    "absorption_rate": ("ratio", "Sales divided by month-end inventory", "number"),
    "months_of_inventory": ("months", "Month-end inventory divided by sales", "number"),
    "mth_end_snapshot_listing_count": ("listings", "Month-end active inventory", "integer"),
    "last_mth_new_listings": ("listings", "New listings", "integer"),
    "last_mth_median_asking_price": ("currency", "Median asking price", "number"),
    "last_mth_min_asking_price": ("currency", "Filtered minimum asking price (log-space two-standard-deviation filter)", "number"),
    "last_mth_max_asking_price": ("currency", "Filtered maximum asking price (log-space two-standard-deviation filter)", "number"),
}
METRIC_CATALOG = {
    **METRICS,
    "current_metrics": ("mixed", "Current snapshot: median asking price and new listing count", "object"),
}
PROPERTY_TYPES = {"ALL", "DETACHED", "SEMI-DETACHED", "TOWNHOUSE", "CONDO", "OTHER"}
