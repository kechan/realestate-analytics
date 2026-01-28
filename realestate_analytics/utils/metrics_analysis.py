"""
Metrics Analysis Utilities

Analytical and query functions for detecting anomalies and analyzing metrics across geographic cohorts.
Separate from metrics_viz.py which focuses on visualization.
"""

import logging
from typing import Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def detect_metric_outliers(
  datastore,
  geo_collection,
  metric_name: str,
  month: str,
  geo_level: int = 30,
  province: Optional[str] = None,
  property_type: str = "ALL",
  zscore_threshold: float = 3.0,
  percentile_threshold: float = 5.0,
  min_transactions: Optional[int] = None,
) -> pd.DataFrame:
  """
  Detect outliers across a cohort of geos for a single metric snapshot.

  Serves dual purpose:
  1. Data quality: Surface ETL bugs, missing data, calculation errors
  2. Market insights: Identify genuinely interesting market anomalies

  Args:
    datastore: Datastore instance
    geo_collection: GeoCollection instance for geo metadata
    metric_name: Metric key (e.g., "absorption_rate", "median_sold_price")
    month: Month string "YYYY-MM" (e.g., "2025-11")
    geo_level: Geographic level (10=province, 20=region, 30=city, 40/60=neighbourhood)
    province: Optional province code (e.g., "ON", "BC") to filter cohort
    property_type: Property type filter (default "ALL")
    zscore_threshold: Z-score absolute value threshold (default 3.0)
    percentile_threshold: Percentile extremes to flag (default 5.0 = top/bottom 5%)
    min_transactions: Optional minimum sold_listing_count to include geo in analysis.
      Filters out small markets where low volumes create extreme ratios. Default None (no filter).

  Returns:
    pd.DataFrame with columns:
    - geog_id: Geographic ID
    - geo_name: Human-readable name
    - province: Province code
    - value: Metric value for the month
    - z_score: Z-score relative to cohort
    - percentile: Percentile rank (0-100)
    - is_outlier_zscore: Boolean flag (|z_score| > threshold)
    - is_outlier_percentile: Boolean flag (percentile < 5 or > 95)
    - is_outlier_any: Boolean (flagged by at least one method)
    - outlier_methods: List of methods that flagged it

    Plus cohort statistics as DataFrame attributes:
    - cohort_mean, cohort_median, cohort_std
    - cohort_min, cohort_max, cohort_q1, cohort_q3
    - cohort_size (number of geos)
  """
  # Step 1: Build cohort
  logger.info(f"Building cohort: level={geo_level}, province={province}")

  cohort_geos = geo_collection.get_by_level(geo_level)
  if province:
    cohort_geos = cohort_geos.filter(lambda g: g.province == province)

  geog_ids = [g.geog_id for g in cohort_geos.all()]

  if not geog_ids:
    logger.warning(f"No geos found for level={geo_level}, province={province}")
    return pd.DataFrame()

  logger.info(f"Cohort size: {len(geog_ids)} geos")

  # Step 2: Fetch metric values
  target_month = pd.to_datetime(f"{month}-01")
  rows = []

  for geog_id in geog_ids:
    doc_id = f"{geog_id}_{property_type}"
    try:
      results = datastore.search(
        index=datastore.mkt_trends_index_name,
        _id=doc_id,
        size=1
      )

      if not results:
        logger.debug(f"No document found for {doc_id}")
        continue

      doc = results[0]

      # Extract metric value for specified month
      metrics = doc.get("metrics", {})
      series = metrics.get(metric_name, [])

      if not series:
        logger.debug(f"No {metric_name} series found for {geog_id}")
        continue

      for item in series:
        item_month = pd.to_datetime(item["month"])
        if item_month == target_month:
          row_data = {
            "geog_id": geog_id,
            "value": item["value"]
          }

          # If min_transactions specified, also fetch sold_listing_count
          if min_transactions is not None:
            sold_count_series = metrics.get("sold_listing_count", [])
            sold_count = None
            for sold_item in sold_count_series:
              if pd.to_datetime(sold_item["month"]) == target_month:
                sold_count = sold_item["value"]
                break

            if sold_count is not None:
              row_data["sold_listing_count"] = sold_count
            else:
              # No sold count data, skip this geo
              logger.debug(f"No sold_listing_count for {geog_id} {month}")
              break

          rows.append(row_data)
          break
      else:
        logger.debug(f"No data for {geog_id} {property_type} {metric_name} {month}")

    except (IndexError, ValueError, KeyError) as e:
      logger.warning(f"Error fetching data for {geog_id}: {e}")
      continue

  if not rows:
    logger.error(f"No data found for any geo in cohort for {metric_name} {month}")
    return pd.DataFrame()

  df = pd.DataFrame(rows)
  logger.info(f"Found data for {len(df)} geos out of {len(geog_ids)} in cohort")

  # Filter by minimum transactions if specified
  if min_transactions is not None:
    initial_count = len(df)
    df = df[df['sold_listing_count'] >= min_transactions].copy()
    filtered_count = initial_count - len(df)
    logger.info(f"Filtered out {filtered_count} geos with <{min_transactions} transactions ({len(df)} remaining)")

    # Drop the sold_listing_count column as it's no longer needed
    df = df.drop(columns=['sold_listing_count'])

    if df.empty:
      logger.warning(f"No geos remaining after min_transactions filter")
      return pd.DataFrame()

  # Step 3: Compute statistics

  # Calculate cohort statistics
  cohort_mean = df['value'].mean()
  cohort_median = df['value'].median()
  cohort_std = df['value'].std()
  cohort_min = df['value'].min()
  cohort_max = df['value'].max()
  cohort_q1 = df['value'].quantile(0.25)
  cohort_q3 = df['value'].quantile(0.75)
  cohort_size = len(df)

  # Z-scores (skip if cohort too small or std is 0)
  if cohort_size >= 3 and cohort_std > 0:
    df['z_score'] = (df['value'] - cohort_mean) / cohort_std
    df['is_outlier_zscore'] = df['z_score'].abs() > zscore_threshold
  else:
    logger.warning(f"Cohort too small ({cohort_size}) or std=0, skipping z-score calculation")
    df['z_score'] = np.nan
    df['is_outlier_zscore'] = False

  # Percentiles (use method='min' to handle ties - all tied values get the minimum rank)
  df['percentile'] = df['value'].rank(pct=True, method='min') * 100
  df['is_outlier_percentile'] = (
    (df['percentile'] < percentile_threshold) |
    (df['percentile'] > (100 - percentile_threshold))
  )

  # Combined flags
  df['is_outlier_any'] = df['is_outlier_zscore'] | df['is_outlier_percentile']
  df['outlier_methods'] = df.apply(
    lambda row: [
      m for m, flag in [
        ('zscore', row['is_outlier_zscore']),
        ('percentile', row['is_outlier_percentile'])
      ] if flag
    ],
    axis=1
  )

  # Step 4: Enrich with geo metadata
  geo_map = {g.geog_id: g for g in cohort_geos.all()}
  df['geo_name'] = df['geog_id'].map(lambda gid: geo_map[gid].name)
  df['province'] = df['geog_id'].map(lambda gid: geo_map[gid].province)

  # Step 5: Sort and prepare output
  # Sort by absolute z-score descending (most extreme first)
  df = df.sort_values('z_score', key=lambda x: x.abs(), ascending=False)

  # Reorder columns for better readability
  column_order = [
    'geog_id', 'geo_name', 'province', 'value',
    'z_score', 'percentile',
    'is_outlier_zscore', 'is_outlier_percentile',
    'is_outlier_any', 'outlier_methods'
  ]
  df = df[column_order]

  # Attach cohort statistics as DataFrame attributes
  df.attrs['cohort_mean'] = cohort_mean
  df.attrs['cohort_median'] = cohort_median
  df.attrs['cohort_std'] = cohort_std
  df.attrs['cohort_min'] = cohort_min
  df.attrs['cohort_max'] = cohort_max
  df.attrs['cohort_q1'] = cohort_q1
  df.attrs['cohort_q3'] = cohort_q3
  df.attrs['cohort_size'] = cohort_size
  df.attrs['min_transactions'] = min_transactions

  logger.info(
    f"Outlier detection complete: {df['is_outlier_any'].sum()} outliers found "
    f"(mean={cohort_mean:.2f}, median={cohort_median:.2f}, std={cohort_std:.2f})"
  )

  return df


def print_outlier_summary(df: pd.DataFrame, metric_name: str) -> None:
  """
  Print a formatted summary of outlier detection results.

  Args:
    df: DataFrame returned by detect_metric_outliers()
    metric_name: Name of the metric for display purposes
  """
  if df.empty:
    print("No data available.")
    return

  # Extract cohort statistics from DataFrame attributes
  cohort_mean = df.attrs.get('cohort_mean', np.nan)
  cohort_median = df.attrs.get('cohort_median', np.nan)
  cohort_std = df.attrs.get('cohort_std', np.nan)
  cohort_min = df.attrs.get('cohort_min', np.nan)
  cohort_max = df.attrs.get('cohort_max', np.nan)
  cohort_q1 = df.attrs.get('cohort_q1', np.nan)
  cohort_q3 = df.attrs.get('cohort_q3', np.nan)
  cohort_size = df.attrs.get('cohort_size', 0)
  min_transactions = df.attrs.get('min_transactions', None)

  print(f"\n{'='*80}")
  print(f"Outlier Detection Summary: {metric_name}")
  print(f"{'='*80}")

  cohort_header = f"\nCohort Statistics (n={cohort_size})"
  if min_transactions is not None:
    cohort_header += f" [filtered: min_transactions={min_transactions}]"
  cohort_header += ":"
  print(cohort_header)
  print(f"  Mean:     {cohort_mean:,.2f}")
  print(f"  Median:   {cohort_median:,.2f}")
  print(f"  Std Dev:  {cohort_std:,.2f}")
  print(f"  Min:      {cohort_min:,.2f}")
  print(f"  Q1:       {cohort_q1:,.2f}")
  print(f"  Q3:       {cohort_q3:,.2f}")
  print(f"  Max:      {cohort_max:,.2f}")

  outliers = df[df['is_outlier_any']]
  print(f"\nOutliers Found: {len(outliers)} / {cohort_size} geos")

  if not outliers.empty:
    print(f"\n{'Geo Name':<30} {'Province':<10} {'Value':>12} {'Z-Score':>10} {'%ile':>8} {'Methods':<20}")
    print("-" * 100)

    for _, row in outliers.iterrows():
      methods_str = ", ".join(row['outlier_methods'])
      z_score_str = f"{row['z_score']:.2f}" if not pd.isna(row['z_score']) else "N/A"
      print(
        f"{row['geo_name']:<30} {row['province']:<10} "
        f"{row['value']:>12,.2f} {z_score_str:>10} "
        f"{row['percentile']:>8.1f} {methods_str:<20}"
      )

  print(f"\n{'='*80}\n")
