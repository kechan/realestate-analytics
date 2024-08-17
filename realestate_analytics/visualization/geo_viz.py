from typing import List, Union, Optional

import re
import numpy as np
import pandas as pd

import geopandas as gpd
from shapely.geometry import Polygon, Point
import contextily as ctx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from ..data.es import Datastore
from ..data.geo import Geo

class GeoVisualizer:
  def __init__(self, datastore: Datastore = None):
    self.datastore = datastore

  def plot(self, geog_ids: Optional[Union[str, List[str]]] = None, 
           geos: Optional[List[Geo]] = None, 
           lat: float = None,
           lng: float = None,
           figsize=(15, 15)):
    if geog_ids is None and geos is None:
      raise ValueError("Either geog_ids or geos must be provided")

    if geog_ids is not None and geos is not None:
      raise ValueError("Only one of geog_ids or geos should be provided, not both")

    if isinstance(geog_ids, str):
      geog_ids = [geog_ids]

    gdfs = []
    labels = []

    if geos is not None:
      for geo in geos:
        gdf, label = self._create_gdf_from_geo(geo)
        if gdf is not None:
          gdfs.append(gdf)
          labels.append(label)
    else:
      if self.datastore is None:
        raise ValueError("Datastore is required when using geog_ids")
      for geog_id in geog_ids:
        gdf, label = self._create_gdf_from_geog_id(geog_id)
        if gdf is not None:
          gdfs.append(gdf)
          labels.append(label)

    if not gdfs:
      print("No valid geographic data found.")
      return

    self._plot_gdfs(gdfs, labels, figsize, lat, lng)

  def _create_gdf_from_geo(self, geo: Geo):
    if geo._multipolygon is None:
      print(f"No geometry found for {geo.name}")
      return None, None

    gdf = gpd.GeoDataFrame(geometry=[geo._multipolygon], crs="EPSG:4326")
    label = f"{geo.name}, {geo.city}: {geo.geog_id}" if geo.city else geo.name
    return gdf, label

  def _create_gdf_from_geog_id(self, geog_id: str):
    geo_details = self.datastore.search(
      index=self.datastore.geo_details_en_index_name,
      _id=geog_id
    )
    if not geo_details:
      print(f"Geo details not found for {geog_id}")
      return None, None

    geo_details= geo_details[0]
    geo_basic = self.datastore.search(index=self.datastore.geo_index_name, _id=geog_id)[0]

    geo_dict = {**geo_basic, **geo_details}
    geo = Geo.from_dict(geo_dict)
    
    gdf, label = self._create_gdf_from_geo(geo)
    return gdf, label

  def _plot_gdfs(self, gdfs, labels, figsize, lat, lng):
    fig, ax = plt.subplots(figsize=figsize)
    colors = plt.cm.Set3(np.linspace(0, 1, len(gdfs)))
    gdf_all = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    gdf_all_projected = gdf_all.to_crs(epsg=3857)
    legend_handles = []

    for gdf, label, color in zip(gdfs, labels, colors):
      gdf_projected = gdf.to_crs(epsg=3857)
      gdf_projected.plot(ax=ax, color=color, alpha=0.5, label=label, edgecolor='black', linewidth=3)
      legend_handles.append(mpatches.Patch(color=color, alpha=0.3, label=label))

    ax.set_xlim(gdf_all_projected.total_bounds[[0, 2]])
    ax.set_ylim(gdf_all_projected.total_bounds[[1, 3]])

    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom='auto')

    if len(gdfs) == 1:
      ax.set_title(f"{labels[0]}")
    ax.set_axis_off()

    if len(gdfs) > 1:
      ax.legend(handles=legend_handles, loc='upper left', bbox_to_anchor=(1, 1))

    if lat is not None and lng is not None:
      point = gpd.GeoDataFrame(geometry=[Point(lng, lat)], crs="EPSG:4326")
      point_projected = point.to_crs(epsg=3857)
      point_projected.plot(ax=ax, color='red', markersize=100, marker='o', label='Point of Interest')
      legend_handles.append(plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=10, label='Point of Interest'))
      ax.legend(handles=legend_handles, loc='upper left', bbox_to_anchor=(1, 1))


    plt.tight_layout()
    plt.show()

"""
  def plot(self, geog_ids: Union[str, List[str]], figsize=(15, 15)):
    if isinstance(geog_ids, str):
      geog_ids = [geog_ids]

    gdfs = []
    labels = []

    for geog_id in geog_ids:
      geo_details_doc = self.datastore.search(
        index=self.datastore.geo_details_en_index_name,
        _id=geog_id
      )

      if geo_details_doc:
        geo_details_doc = geo_details_doc[0]
        geometry_coordinates = geo_details_doc['meta']['geometry'][0]
        geo = self.datastore.search(index=self.datastore.geo_index_name, _id=geog_id)[0]
        
        city = geo['city']
        match = re.search(r"g(\d+)_", geog_id)
        if match:
          level = match.group(1)
          level = f"level{level}En"
          neighborhood = geo[level]
        else:
          print(f"geog_id {geog_id} does not match the expected format.")
          continue

        polygon = Polygon([(point[1], point[0]) for point in geometry_coordinates])
        gdf = gpd.GeoDataFrame([1], geometry=[polygon], crs="EPSG:4326")
        gdfs.append(gdf)
        labels.append(f"{neighborhood}, {city}")
      else:
        print(f"Geo details not found for {geog_id}")

    if not gdfs:
      print("No valid geographic data found.")
      return

    fig, ax = plt.subplots(figsize=figsize)

    colors = plt.cm.Set3(np.linspace(0, 1, len(gdfs)))
    
    gdf_all = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    gdf_all_projected = gdf_all.to_crs(epsg=3857)
    
    legend_handles = []
    # Plot our polygons first
    for gdf, label, color in zip(gdfs, labels, colors):
      gdf_projected = gdf.to_crs(epsg=3857)
      gdf_projected.plot(ax=ax, color=color, alpha=0.5, label=label, edgecolor='black', linewidth=3)
      # gdf_projected.plot(ax=ax, color='blue', alpha=0.3, label=label, edgecolor='black', linewidth=2)
      legend_handles.append(mpatches.Patch(color=color, alpha=0.3, label=label))

    # Adjust plot extent to fit all geometries
    ax.set_xlim(gdf_all_projected.total_bounds[[0, 2]])
    ax.set_ylim(gdf_all_projected.total_bounds[[1, 3]])

    # Add base map after plotting polygons and setting extent
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom='auto')

    if len(gdfs) == 1:
      ax.set_title(f"{labels[0]}")

    ax.set_axis_off()
    
    # plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    if len(gdfs) > 1:
      ax.legend(handles=legend_handles, loc='upper left', bbox_to_anchor=(1, 1))

    plt.tight_layout()
    plt.show()
"""