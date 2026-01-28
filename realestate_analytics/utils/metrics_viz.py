from __future__ import annotations

from datetime import datetime, date
from typing import Iterable, Optional

import logging

import matplotlib.pyplot as plt
import pandas as pd


logger = logging.getLogger(__name__)


def fetch_mkt_trends_doc(datastore, geog_id: str, property_type: str = "ALL") -> dict:
  """
  Fetch a single mkt_trends document from ES by geog_id + property_type.

  Args:
    datastore: Datastore instance (realestate_analytics.data.es.Datastore).
    geog_id: Geog ID string (e.g., "g30_dpz89rm7").
    property_type: Property type string; default "ALL". Supported in current ETLs:
      "CONDO", "SEMI-DETACHED", "TOWNHOUSE", "DETACHED", or "ALL".

  Returns:
    dict: The ES document (source) for the given geog_id/property_type.
  """
  doc_id = f"{geog_id}_{property_type}"
  results = datastore.search(index=datastore.mkt_trends_index_name, _id=doc_id)
  if not results:
    raise ValueError(f"No mkt_trends doc found for _id={doc_id}")
  return results[0]


def plot_metric_series(
  datastore,
  geog_id: str,
  property_type: str = "ALL",
  metric_name: str = "median_sold_price",
  start_date: Optional[object] = None,
  end_date: Optional[object] = None,
  title: Optional[str] = None,
  ax: Optional[plt.Axes] = None,
) -> plt.Axes:
  """
  Plot a single metric time series from rlp_mkt_trends_current.

  Args:
    datastore: Datastore instance.
    geog_id: Geog ID string.
    property_type: Property type string; default "ALL". Supported in current ETLs:
      "CONDO", "SEMI-DETACHED", "TOWNHOUSE", "DETACHED", or "ALL".
    metric_name: Key under metrics (e.g., "median_sold_price", "median_dom",
      "absorption_rate", "months_of_inventory").
    start_date: Optional start bound. If set, show data >= start_date.
    end_date: Optional end bound. If set, show data <= end_date.
    title: Optional plot title override.
    ax: Optional matplotlib.axes.Axes to draw on.

  Returns:
    matplotlib.axes.Axes: The Axes used for plotting (new or provided).

  Date filtering:
    - If both start_date and end_date are None, plot full series.
    - If only start_date is set, plot from start_date onward.
    - If only end_date is set, plot up to end_date.
    - If both set, plot within [start_date, end_date].

  Notes:
    start_date/end_date may be datetime/date or ISO strings ("YYYY-MM" or "YYYY-MM-DD").
  """
  if ax is None:
    _, ax = plt.subplots(figsize=(12, 6))

  series_df = _metric_series_df(datastore, geog_id, property_type, metric_name)
  series_df = _filter_by_date(series_df, start_date, end_date)

  if series_df.empty:
    logger.warning("No data to plot for %s (%s, %s).", metric_name, geog_id, property_type)
    return ax

  ax.plot(series_df["month"], series_df["value"], marker="o", label=metric_name)
  ax.set_xlabel("Month")
  ax.set_ylabel(metric_name.replace("_", " ").title())
  ax.set_title(title or f"{geog_id} {property_type} - {metric_name}")
  ax.grid(True, alpha=0.3)
  ax.legend()
  plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
  return ax


def plot_multi_metrics(
  datastore,
  geog_id: str,
  property_type: str = "ALL",
  metric_names: Optional[Iterable[str]] = None,
  start_date: Optional[object] = None,
  end_date: Optional[object] = None,
  title: Optional[str] = None,
  ax: Optional[plt.Axes] = None,
) -> plt.Axes:
  """
  Plot multiple metric time series on the same axes.

  Args:
    datastore: Datastore instance.
    geog_id: Geog ID string.
    property_type: Property type string; default "ALL". Supported in current ETLs:
      "CONDO", "SEMI-DETACHED", "TOWNHOUSE", "DETACHED", or "ALL".
    metric_names: List of metric keys under metrics. If None, a sensible default set
      is used (["median_sold_price", "median_dom"]).
    start_date: Optional start bound (see plot_metric_series).
    end_date: Optional end bound (see plot_metric_series).
    title: Optional plot title override.
    ax: Optional matplotlib.axes.Axes to draw on.

  Returns:
    matplotlib.axes.Axes: The Axes used for plotting.

  Notes:
    - 1 metric: Single y-axis
    - 2 metrics: Dual y-axes (left/right) with independent scaling
    - 3 metrics: Triple y-axes (left, right, right-offset) with independent scaling
    - 4+ metrics: Single y-axis with warning (consider selecting fewer metrics)
  """
  if ax is None:
    _, ax = plt.subplots(figsize=(12, 6))

  if metric_names is None:
    metric_names = ["median_sold_price", "median_dom"]

  metric_names = list(metric_names)  # Convert to list if iterable

  # Handle different metric counts with appropriate axis strategies
  if len(metric_names) == 1:
    # Single metric - simple single axis
    series_df = _metric_series_df(datastore, geog_id, property_type, metric_names[0])
    series_df = _filter_by_date(series_df, start_date, end_date)
    if not series_df.empty:
      ax.plot(series_df["month"], series_df["value"], marker="o", label=metric_names[0])
      ax.set_ylabel(metric_names[0].replace("_", " ").title())
    else:
      logger.warning("No data for %s (%s, %s).", metric_names[0], geog_id, property_type)

    ax.set_xlabel("Month")
    ax.set_title(title or f"{geog_id} {property_type} - {metric_names[0]}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    return ax

  elif len(metric_names) == 2:
    # Dual y-axes for 2 metrics
    series_df_0 = _metric_series_df(datastore, geog_id, property_type, metric_names[0])
    series_df_0 = _filter_by_date(series_df_0, start_date, end_date)

    series_df_1 = _metric_series_df(datastore, geog_id, property_type, metric_names[1])
    series_df_1 = _filter_by_date(series_df_1, start_date, end_date)

    lines = []
    labels = []
    linestyles = ['-', '--']

    if not series_df_0.empty:
      line0, = ax.plot(series_df_0["month"], series_df_0["value"], marker="o", linestyle=linestyles[0],
                       label=metric_names[0], color='C0')
      lines.append(line0)
      labels.append(metric_names[0])
      ax.set_ylabel(metric_names[0].replace("_", " ").title(), color='C0')
      ax.tick_params(axis='y', labelcolor='C0')
    else:
      logger.warning("No data for %s (%s, %s).", metric_names[0], geog_id, property_type)

    ax2 = ax.twinx()

    if not series_df_1.empty:
      line1, = ax2.plot(series_df_1["month"], series_df_1["value"], marker="s", linestyle=linestyles[1],
                        label=metric_names[1], color='C1')
      lines.append(line1)
      labels.append(metric_names[1])
      ax2.set_ylabel(metric_names[1].replace("_", " ").title(), color='C1')
      ax2.tick_params(axis='y', labelcolor='C1')
    else:
      logger.warning("No data for %s (%s, %s).", metric_names[1], geog_id, property_type)

    ax.set_xlabel("Month")
    ax.set_title(title or f"{geog_id} {property_type} - Metrics")
    ax.grid(True, alpha=0.3)

    if lines:
      ax.legend(lines, labels, loc='best')

    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    return ax

  elif len(metric_names) == 3:
    # Triple y-axes for 3 metrics
    series_dfs = []
    for metric_name in metric_names:
      df = _metric_series_df(datastore, geog_id, property_type, metric_name)
      df = _filter_by_date(df, start_date, end_date)
      series_dfs.append(df)

    lines = []
    labels = []
    colors = ['C0', 'C1', 'C2']
    markers = ['o', 's', '^']
    linestyles = ['-', '--', ':']

    # Axis 1 (left) - First metric
    if not series_dfs[0].empty:
      line0, = ax.plot(series_dfs[0]["month"], series_dfs[0]["value"],
                       marker=markers[0], linestyle=linestyles[0], label=metric_names[0], color=colors[0])
      lines.append(line0)
      labels.append(metric_names[0])
      ax.set_ylabel(metric_names[0].replace("_", " ").title(), color=colors[0])
      ax.tick_params(axis='y', labelcolor=colors[0])
    else:
      logger.warning("No data for %s (%s, %s).", metric_names[0], geog_id, property_type)

    # Axis 2 (right) - Second metric
    ax2 = ax.twinx()
    if not series_dfs[1].empty:
      line1, = ax2.plot(series_dfs[1]["month"], series_dfs[1]["value"],
                        marker=markers[1], linestyle=linestyles[1], label=metric_names[1], color=colors[1])
      lines.append(line1)
      labels.append(metric_names[1])
      ax2.set_ylabel(metric_names[1].replace("_", " ").title(), color=colors[1])
      ax2.tick_params(axis='y', labelcolor=colors[1])
    else:
      logger.warning("No data for %s (%s, %s).", metric_names[1], geog_id, property_type)

    # Axis 3 (right, offset) - Third metric
    ax3 = ax.twinx()
    ax3.spines["right"].set_position(("axes", 1.15))
    ax3.spines["right"].set_visible(True)
    if not series_dfs[2].empty:
      line2, = ax3.plot(series_dfs[2]["month"], series_dfs[2]["value"],
                        marker=markers[2], linestyle=linestyles[2], label=metric_names[2], color=colors[2])
      lines.append(line2)
      labels.append(metric_names[2])
      ax3.set_ylabel(metric_names[2].replace("_", " ").title(), color=colors[2])
      ax3.tick_params(axis='y', labelcolor=colors[2])
    else:
      logger.warning("No data for %s (%s, %s).", metric_names[2], geog_id, property_type)

    # Adjust figure to make room for third axis
    if ax.figure:
      ax.figure.subplots_adjust(right=0.85)

    ax.set_xlabel("Month")
    ax.set_title(title or f"{geog_id} {property_type} - Metrics")
    ax.grid(True, alpha=0.3)

    if lines:
      ax.legend(lines, labels, loc='best')

    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    return ax

  else:
    # 4+ metrics - fallback to single axis with warning
    logger.warning("Plotting %d metrics on single axis. Consider selecting 2-3 metrics for better visualization.", len(metric_names))

    for metric_name in metric_names:
      series_df = _metric_series_df(datastore, geog_id, property_type, metric_name)
      series_df = _filter_by_date(series_df, start_date, end_date)
      if series_df.empty:
        logger.warning("No data for %s (%s, %s).", metric_name, geog_id, property_type)
        continue
      ax.plot(series_df["month"], series_df["value"], marker="o", label=metric_name)

    ax.set_xlabel("Month")
    ax.set_ylabel("Value")
    ax.set_title(title or f"{geog_id} {property_type} - Metrics")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    return ax


def plot_metric_across_geos(
  datastore,
  geog_ids: list,
  metric_name: str,
  property_type: str = "ALL",
  start_date: Optional[object] = None,
  end_date: Optional[object] = None,
  title: Optional[str] = None,
  geo_names: Optional[dict] = None,
  ax: Optional[plt.Axes] = None,
) -> plt.Axes:
  """
  Plot the same metric across multiple geographic areas for comparison.

  Args:
    datastore: Datastore instance.
    geog_ids: List of geog_id strings to compare.
    metric_name: Single metric key to plot (e.g., "median_sold_price", "median_dom").
    property_type: Property type string; default "ALL". Supported in current ETLs:
      "CONDO", "SEMI-DETACHED", "TOWNHOUSE", "DETACHED", or "ALL".
    start_date: Optional start bound (see plot_metric_series).
    end_date: Optional end bound (see plot_metric_series).
    title: Optional plot title override.
    geo_names: Optional dict mapping geog_id to display name (e.g., {"g30_dpz89rm7": "Toronto"}).
      If not provided, uses geog_id as label.
    ax: Optional matplotlib.axes.Axes to draw on.

  Returns:
    matplotlib.axes.Axes: The Axes used for plotting.

  Notes:
    Uses different colors and line styles for each geo. Recommended for 2-5 geos for readability.

  Example:
    >>> plot_metric_across_geos(
    ...     ds,
    ...     ["g30_dpz89rm7", "g30_c3nfkdtg", "g30_c3n55m3m"],
    ...     "median_sold_price",
    ...     geo_names={"g30_dpz89rm7": "Toronto", "g30_c3nfkdtg": "Calgary", "g30_c3n55m3m": "Vancouver"}
    ... )
  """
  if ax is None:
    _, ax = plt.subplots(figsize=(12, 6))

  # Color and line style combinations for up to 10 geos
  colors = ['C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']
  linestyles = ['-', '--', '-.', ':']
  markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p', '*', 'h']

  for idx, geog_id in enumerate(geog_ids):
    series_df = _metric_series_df(datastore, geog_id, property_type, metric_name)
    series_df = _filter_by_date(series_df, start_date, end_date)

    if series_df.empty:
      logger.warning("No data for %s (%s, %s).", metric_name, geog_id, property_type)
      continue

    # Cycle through colors, line styles, and markers
    color = colors[idx % len(colors)]
    linestyle = linestyles[idx % len(linestyles)]
    marker = markers[idx % len(markers)]

    # Use geo_names mapping if provided, otherwise use geog_id
    label = geo_names.get(geog_id, geog_id) if geo_names else geog_id

    ax.plot(series_df["month"], series_df["value"],
            marker=marker, linestyle=linestyle, color=color,
            label=label, markersize=6)

  ax.set_xlabel("Month")
  ax.set_ylabel(metric_name.replace("_", " ").title())
  ax.set_title(title or f"{metric_name.replace('_', ' ').title()} Comparison - {property_type}")
  ax.grid(True, alpha=0.3)
  ax.legend(loc='best')
  plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

  return ax


def plot_metric_across_property_types(
  datastore,
  geog_id: str,
  metric_name: str,
  property_types: Optional[list] = None,
  start_date: Optional[object] = None,
  end_date: Optional[object] = None,
  title: Optional[str] = None,
  ax: Optional[plt.Axes] = None,
) -> plt.Axes:
  """
  Plot the same metric across multiple property types for a single geographic area.

  Args:
    datastore: Datastore instance.
    geog_id: Single geog_id string.
    metric_name: Single metric key to plot (e.g., "median_sold_price", "median_dom").
    property_types: List of property type strings. If None, uses all standard types:
      ["ALL", "DETACHED", "CONDO", "TOWNHOUSE", "SEMI-DETACHED"].
    start_date: Optional start bound (see plot_metric_series).
    end_date: Optional end bound (see plot_metric_series).
    title: Optional plot title override.
    ax: Optional matplotlib.axes.Axes to draw on.

  Returns:
    matplotlib.axes.Axes: The Axes used for plotting.

  Notes:
    Uses different colors and line styles for each property type. Shows how different
    housing segments perform within the same market.

  Example:
    >>> plot_metric_across_property_types(
    ...     ds,
    ...     "g30_dpz89rm7",
    ...     "median_sold_price",
    ...     property_types=["DETACHED", "CONDO", "TOWNHOUSE"]
    ... )
  """
  if ax is None:
    _, ax = plt.subplots(figsize=(12, 6))

  if property_types is None:
    property_types = ["ALL", "DETACHED", "CONDO", "TOWNHOUSE", "SEMI-DETACHED"]

  # Color and line style combinations
  colors = ['C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']
  linestyles = ['-', '--', '-.', ':']
  markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p', '*', 'h']

  for idx, property_type in enumerate(property_types):
    series_df = _metric_series_df(datastore, geog_id, property_type, metric_name)
    series_df = _filter_by_date(series_df, start_date, end_date)

    if series_df.empty:
      logger.warning("No data for %s (%s, %s).", metric_name, geog_id, property_type)
      continue

    # Cycle through colors, line styles, and markers
    color = colors[idx % len(colors)]
    linestyle = linestyles[idx % len(linestyles)]
    marker = markers[idx % len(markers)]

    ax.plot(series_df["month"], series_df["value"],
            marker=marker, linestyle=linestyle, color=color,
            label=property_type, markersize=6)

  ax.set_xlabel("Month")
  ax.set_ylabel(metric_name.replace("_", " ").title())
  ax.set_title(title or f"{geog_id} - {metric_name.replace('_', ' ').title()} by Property Type")
  ax.grid(True, alpha=0.3)
  ax.legend(loc='best')
  plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

  return ax


def plot_absorption_vs_moi(
  datastore,
  geog_id: str,
  property_type: str = "ALL",
  start_date: Optional[object] = None,
  end_date: Optional[object] = None,
  title: Optional[str] = None,
  ax: Optional[plt.Axes] = None,
) -> plt.Axes:
  """
  Plot absorption_rate and months_of_inventory on the same axes for quick comparison.

  Args:
    datastore: Datastore instance.
    geog_id: Geog ID string.
    property_type: Property type string; default "ALL". Supported in current ETLs:
      "CONDO", "SEMI-DETACHED", "TOWNHOUSE", "DETACHED", or "ALL".
    start_date: Optional start bound (see plot_metric_series).
    end_date: Optional end bound (see plot_metric_series).
    title: Optional plot title override.
    ax: Optional matplotlib.axes.Axes to draw on.

  Returns:
    matplotlib.axes.Axes: The Axes used for plotting.
  """
  return plot_multi_metrics(
    datastore=datastore,
    geog_id=geog_id,
    property_type=property_type,
    metric_names=["absorption_rate", "months_of_inventory"],
    start_date=start_date,
    end_date=end_date,
    title=title or f"{geog_id} {property_type} - Absorption vs MOI",
    ax=ax,
  )


def plot_price_change_yoy(
  datastore,
  geog_id: str,
  property_type: str = "ALL",
  start_date: Optional[object] = None,
  end_date: Optional[object] = None,
  title: Optional[str] = None,
  ax: Optional[plt.Axes] = None,
) -> plt.Axes:
  """
  Plot year-over-year price change from median_sold_price.

  Args:
    datastore: Datastore instance.
    geog_id: Geog ID string.
    property_type: Property type string; default "ALL".
    start_date: Optional start bound (see plot_metric_series).
    end_date: Optional end bound (see plot_metric_series).
    title: Optional plot title override.
    ax: Optional matplotlib.axes.Axes to draw on.

  Returns:
    matplotlib.axes.Axes: The Axes used for plotting.

  Notes:
    - Computes (price[t] / price[t-12] - 1) * 100
    - Value expressed as percentage (5.0 = 5% YoY increase)
    - First 12 months of data excluded (no prior year comparison)
  """
  if ax is None:
    _, ax = plt.subplots(figsize=(12, 6))

  # Fetch median_sold_price series
  series_df = _metric_series_df(datastore, geog_id, property_type, "median_sold_price")

  if series_df.empty:
    logger.warning("No median_sold_price data for %s (%s).", geog_id, property_type)
    return ax

  # Ensure chronological order and create lag
  series_df = series_df.sort_values("month").reset_index(drop=True)
  series_df["prior_value"] = series_df["value"].shift(12)

  # Compute YoY percentage change: ((current / prior) - 1) * 100
  series_df["yoy_change"] = ((series_df["value"] / series_df["prior_value"]) - 1) * 100

  # Drop rows where prior is NaN (first 12 months)
  yoy_df = series_df.dropna(subset=["prior_value"]).copy()
  yoy_df = yoy_df[["month", "yoy_change"]].rename(columns={"yoy_change": "value"})

  if yoy_df.empty:
    logger.warning("Insufficient data for YoY calculation (need >12 months) for %s (%s).", geog_id, property_type)
    return ax

  # Apply date filtering
  yoy_df = _filter_by_date(yoy_df, start_date, end_date)

  if yoy_df.empty:
    logger.warning("No data in date range for YoY calculation for %s (%s).", geog_id, property_type)
    return ax

  # Plot
  ax.plot(yoy_df["month"], yoy_df["value"], marker="o", label="Price Change YoY (%)")

  # Add zero reference line
  ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.7)

  ax.set_xlabel("Month")
  ax.set_ylabel("Price Change YoY (%)")
  ax.set_title(title or f"{geog_id} {property_type} - Year-over-Year Price Change")
  ax.grid(True, alpha=0.3)
  ax.legend()
  plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

  return ax


def plot_asking_vs_sold_spread(
  datastore,
  geog_id: str,
  property_type: str = "ALL",
  start_date: Optional[object] = None,
  end_date: Optional[object] = None,
  title: Optional[str] = None,
  ax: Optional[plt.Axes] = None,
) -> plt.Axes:
  """
  Plot the spread between median asking price and median sold price.

  Args:
    datastore: Datastore instance.
    geog_id: Geog ID string.
    property_type: Property type string; default "ALL".
    start_date: Optional start bound (see plot_metric_series).
    end_date: Optional end bound (see plot_metric_series).
    title: Optional plot title override.
    ax: Optional matplotlib.axes.Axes to draw on.

  Returns:
    matplotlib.axes.Axes: The Axes used for plotting.

  Notes:
    - Computes (asking_price / sold_price - 1) * 100
    - Value expressed as percentage (5.0 = sellers asking 5% above sold prices)
    - Positive spread: sellers expecting more than market reality
    - Negative spread: sellers pricing below recent sales
    - Only months where both metrics exist are plotted
  """
  if ax is None:
    _, ax = plt.subplots(figsize=(12, 6))

  # Fetch both series
  asking_df = _metric_series_df(datastore, geog_id, property_type, "last_mth_median_asking_price")
  sold_df = _metric_series_df(datastore, geog_id, property_type, "median_sold_price")

  if asking_df.empty:
    logger.warning("No last_mth_median_asking_price data for %s (%s).", geog_id, property_type)
    return ax

  if sold_df.empty:
    logger.warning("No median_sold_price data for %s (%s).", geog_id, property_type)
    return ax

  # Merge on month to get overlapping period
  merged_df = asking_df.merge(sold_df, on="month", suffixes=("_asking", "_sold"))

  if merged_df.empty:
    logger.warning("No overlapping months between asking and sold data for %s (%s).", geog_id, property_type)
    return ax

  # Compute spread: (asking / sold - 1) * 100
  merged_df["spread"] = ((merged_df["value_asking"] / merged_df["value_sold"]) - 1) * 100

  # Prepare for filtering
  spread_df = merged_df[["month", "spread"]].rename(columns={"spread": "value"})
  spread_df = _filter_by_date(spread_df, start_date, end_date)

  if spread_df.empty:
    logger.warning("No data in date range for asking vs sold spread for %s (%s).", geog_id, property_type)
    return ax

  # Plot
  ax.plot(spread_df["month"], spread_df["value"], marker="o", label="Asking vs Sold Spread (%)")

  # Add zero reference line
  ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.7)

  ax.set_xlabel("Month")
  ax.set_ylabel("Asking vs Sold Spread (%)")
  ax.set_title(title or f"{geog_id} {property_type} - Asking vs Sold Price Spread")
  ax.grid(True, alpha=0.3)
  ax.legend()
  plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

  return ax


def _metric_series_df(datastore, geog_id: str, property_type: str, metric_name: str) -> pd.DataFrame:
  doc = fetch_mkt_trends_doc(datastore, geog_id, property_type)
  metrics = doc.get("metrics", {})
  series = metrics.get(metric_name, [])
  if not series:
    return pd.DataFrame(columns=["month", "value"])
  df = pd.DataFrame(series)
  if "month" not in df.columns:
    return pd.DataFrame(columns=["month", "value"])
  df["month"] = pd.to_datetime(df["month"], errors="coerce")
  df = df.dropna(subset=["month"]).sort_values("month").reset_index(drop=True)
  return df


def _filter_by_date(df: pd.DataFrame, start_date: Optional[object], end_date: Optional[object]) -> pd.DataFrame:
  if df.empty:
    return df
  start_dt = _coerce_date_bound(start_date, is_start=True)
  end_dt = _coerce_date_bound(end_date, is_start=False)
  if start_dt is not None:
    df = df[df["month"] >= start_dt]
  if end_dt is not None:
    df = df[df["month"] <= end_dt]
  return df.reset_index(drop=True)


def _coerce_date_bound(value: Optional[object], is_start: bool) -> Optional[pd.Timestamp]:
  if value is None:
    return None
  if isinstance(value, pd.Timestamp):
    return value
  if isinstance(value, datetime):
    return pd.Timestamp(value)
  if isinstance(value, date):
    return pd.Timestamp(value)
  if isinstance(value, str):
    if len(value) == 7:  # YYYY-MM
      ts = pd.to_datetime(f"{value}-01", errors="coerce")
      if ts is not pd.NaT and not is_start:
        ts = (ts + pd.offsets.MonthEnd(0))
      return ts
    return pd.to_datetime(value, errors="coerce")
  return None


def plot_metric_heatmap(
  datastore,
  geog_ids: list,
  metric_name: str,
  month: Optional[str] = None,
  geo_names: Optional[dict] = None,
  title: Optional[str] = None,
  cmap: str = "YlOrRd",
  fmt: Optional[str] = None,
  ax: Optional[plt.Axes] = None,
) -> plt.Axes:
  """
  Plot a heatmap of a single metric across geos (rows) and property types (columns).

  Args:
    datastore: Datastore instance.
    geog_ids: List of geog_id strings (rows of heatmap).
    metric_name: Single metric key (e.g., "median_sold_price", "median_dom", "absorption_rate").
    month: Optional month string "YYYY-MM" (e.g., "2025-11"). If None, uses most recent
      month available across all data.
    geo_names: Optional dict mapping geog_id to display name (e.g., {"g30_dpz89rm7": "Toronto"}).
    title: Optional plot title override.
    cmap: Colormap name for heatmap. Default "YlOrRd" (yellow-orange-red).
    fmt: Format string for annotations. If None, auto-selects based on metric_name:
      - median_sold_price: "$,.0f" (e.g., "$800,000")
      - absorption_rate: ".1%" (e.g., "15.2%")
      - months_of_inventory: ".1f" (e.g., "3.2")
      - others: ".1f"
    ax: Optional matplotlib.axes.Axes to draw on.

  Returns:
    matplotlib.axes.Axes: The Axes used for plotting.

  Example:
    >>> plot_metric_heatmap(
    ...     ds,
    ...     ["g30_dpz89rm7", "g30_c3nfkdtg", "g30_c3n55m3m"],
    ...     "median_sold_price",
    ...     month="2025-09",
    ...     geo_names={"g30_dpz89rm7": "Toronto", "g30_c3nfkdtg": "Calgary", "g30_c3n55m3m": "Vancouver"}
    ... )
  """
  import numpy as np

  property_types = ["DETACHED", "SEMI-DETACHED", "TOWNHOUSE", "CONDO"]

  # Build matrix: rows=geos, cols=property_types
  data_matrix = []
  most_recent_month = None

  for geog_id in geog_ids:
    row = []
    for prop_type in property_types:
      try:
        series_df = _metric_series_df(datastore, geog_id, prop_type, metric_name)
        if series_df.empty:
          row.append(np.nan)
          continue

        # Track most recent month across all data
        max_month = series_df["month"].max()
        if most_recent_month is None or max_month > most_recent_month:
          most_recent_month = max_month

        if month is not None:
          # Filter to specific month
          target_month = pd.to_datetime(f"{month}-01")
          match = series_df[series_df["month"] == target_month]
          if match.empty:
            row.append(np.nan)
          else:
            row.append(match["value"].iloc[0])
        else:
          # Will fill in later after we know most_recent_month
          row.append(series_df)  # Store df temporarily
      except Exception as e:
        logger.warning("Error fetching %s for %s/%s: %s", metric_name, geog_id, prop_type, e)
        row.append(np.nan)
    data_matrix.append(row)

  # If month was None, now extract values for most_recent_month
  if month is None and most_recent_month is not None:
    for i, row in enumerate(data_matrix):
      for j, cell in enumerate(row):
        if isinstance(cell, pd.DataFrame):
          match = cell[cell["month"] == most_recent_month]
          if match.empty:
            data_matrix[i][j] = np.nan
          else:
            data_matrix[i][j] = match["value"].iloc[0]

  # Convert to numpy array
  data_array = np.array(data_matrix, dtype=float)

  # Create figure if needed
  if ax is None:
    fig_height = max(4, len(geog_ids) * 0.6 + 2)
    _, ax = plt.subplots(figsize=(8, fig_height))

  # Create heatmap using imshow
  im = ax.imshow(data_array, cmap=cmap, aspect='auto')

  # Set ticks and labels
  ax.set_xticks(range(len(property_types)))
  ax.set_xticklabels(property_types, rotation=45, ha='right')

  row_labels = [geo_names.get(gid, gid) if geo_names else gid for gid in geog_ids]
  ax.set_yticks(range(len(geog_ids)))
  ax.set_yticklabels(row_labels)

  # Add colorbar
  cbar = ax.figure.colorbar(im, ax=ax)
  cbar.set_label(metric_name.replace("_", " ").title())

  # Auto-select format string based on metric
  if fmt is None:
    if "price" in metric_name.lower():
      fmt = "$,.0f"
    elif "rate" in metric_name.lower():
      fmt = ".1%"
    elif "inventory" in metric_name.lower():
      fmt = ".1f"
    else:
      fmt = ".1f"

  # Add annotations
  for i in range(len(geog_ids)):
    for j in range(len(property_types)):
      val = data_array[i, j]
      if np.isnan(val):
        text = "N/A"
      elif fmt == "$,.0f":
        text = f"${val:,.0f}"
      elif fmt == ".1%":
        text = f"{val:.1%}"
      else:
        text = f"{val:{fmt}}"

      # Choose text color based on background brightness
      bg_val = (val - np.nanmin(data_array)) / (np.nanmax(data_array) - np.nanmin(data_array) + 1e-9)
      text_color = "white" if bg_val > 0.6 else "black"
      if np.isnan(val):
        text_color = "gray"

      ax.text(j, i, text, ha="center", va="center", color=text_color, fontsize=9)

  # Title
  display_month = month if month else (most_recent_month.strftime("%Y-%m") if most_recent_month else "N/A")
  ax.set_title(title or f"{metric_name.replace('_', ' ').title()} - {display_month}")

  plt.tight_layout()
  return ax
