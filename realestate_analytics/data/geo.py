from __future__ import annotations
from typing import Any, List, Dict, Optional, Tuple, Callable, Union, TypeVar
from collections import defaultdict
from collections.abc import Sequence

from dataclasses import dataclass, field
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.validation import explain_validity
import warnings

import logging, random
from pathlib import Path
import pickle, dill
from tqdm.auto import tqdm

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx

from ..data.es import Datastore


def plot_geometries(geometries, names, geog_ids, figsize=(15, 15), point_of_interest=None):
  """
  Plot one or more geometries on a single map.

  Args:
    geometries (list): List of shapely geometries to plot.
    names (list): List of names corresponding to each geometry.
    geog_ids (list): List of geog_ids corresponding to each geometry.
    figsize (tuple): Figure size for the plot. Default is (15, 15).
    point_of_interest (tuple): Optional (lat, lng) to mark on the map.

  Returns:
    None. Displays the plot.
  """
  fig, ax = plt.subplots(figsize=figsize)

  gdf_list = []
  for geom, name, geog_id in zip(geometries, names, geog_ids):
    if geom is not None:
      gdf = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:4326")
      gdf['name'] = name
      gdf['geog_id'] = geog_id
      gdf_list.append(gdf)

  if not gdf_list:
    print("No valid geometries found")
    return

  gdf_all = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs="EPSG:4326")
  gdf_projected = gdf_all.to_crs(epsg=3857)

  # Plot the geometries with different colors
  colors = plt.cm.Set3(np.linspace(0, 1, len(gdf_projected)))
  gdf_projected.plot(ax=ax, color=colors, alpha=0.5, edgecolor='black', linewidth=2)

  # Add point of interest if provided (now after main geometries but before labels and legends)
  if point_of_interest:
    lat, lng = point_of_interest
    point = gpd.GeoDataFrame(geometry=[Point(lng, lat)], crs="EPSG:4326")
    point_projected = point.to_crs(epsg=3857)
    point_projected.plot(ax=ax, color='red', markersize=100, marker='o', label='Point of Interest')

  # Add labels
  for idx, row in gdf_projected.iterrows():
    centroid = row.geometry.centroid
    ax.annotate(text=row['name'], xy=(centroid.x, centroid.y), ha='center', va='center',
                bbox=dict(facecolor='white', edgecolor='none', alpha=0.7, pad=1))

  # Set plot bounds
  ax.set_xlim(gdf_projected.total_bounds[[0, 2]])
  ax.set_ylim(gdf_projected.total_bounds[[1, 3]])

  # Add basemap
  ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom='auto')

  # Set title or legend based on number of geometries
  if len(gdf_projected) == 1:
    ax.set_title(f"{names[0]} ({geog_ids[0]})")
  else:
    legend_elements = [plt.Rectangle((0, 0), 1, 1, fc=color, alpha=0.5, ec='black') 
                       for color in colors[:len(gdf_projected)]]
    legend_labels = [f"{row['name']} ({row['geog_id']})" for _, row in gdf_projected.iterrows()]
    if point_of_interest:
      legend_elements.append(plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', 
                                        markersize=10, label='Point of Interest'))
      legend_labels.append('Point of Interest')
    ax.legend(legend_elements, legend_labels, loc='upper left', bbox_to_anchor=(1, 1), 
              title="Geos", fontsize='small')

  ax.set_axis_off()

  plt.tight_layout()
  plt.show()

@dataclass
class Geo:
  """
  Represents a geographic entity.

  Note: Geo instances should be treated as immutable. Modifying attributes
  after initialization is discouraged and may lead to inconsistencies.
  """
  geog_id: str
  name: str
  level: int
  province: str
  lat: float
  lng: float
  city: Optional[str] = None
  city_slug: Optional[str] = None
  parent_id: Optional[str] = None
  children: List['Geo'] = field(default_factory=list)
  # overlaps: List['Geo'] = field(default_factory=list)
  overlaps: GeoCollection = field(default_factory= lambda: GeoCollection())
  level_name: Optional[str] = None
  level_10_en: Optional[str] = None
  level_10_fr: Optional[str] = None
  level_20_en: Optional[str] = None
  level_20_fr: Optional[str] = None
  level_30_en: Optional[str] = None
  level_30_fr: Optional[str] = None
  level_35_en: Optional[str] = None
  level_35_fr: Optional[str] = None
  level_40_en: Optional[str] = None
  level_40_fr: Optional[str] = None
  has_en_profile: bool = False
  has_fr_profile: bool = False
  path: Optional[str] = None
  long_id: Optional[str] = None
  created: Optional[str] = None
  modified: Optional[str] = None
  uploaded: Optional[str] = None
  bounding_box: Optional[Dict[str, float]] = None
  geometry: Optional[List[List[Tuple[float, float]]]] = None
  profiles: Optional[Dict[str, str]] = None
  intro: Optional[str] = None
  manage_manually: bool = False
  name_suggest: Optional[Dict[str, Any]] = None

  logger: logging.Logger = field(init=False)

  def __post_init__(self):
    self.logger = logging.getLogger(self.__class__.__name__)
    if self.geometry:
      polygons = [Polygon((lng, lat) for lat, lng in polygon_coords) 
                  for polygon_coords in self.geometry]
      self._multipolygon = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
    else:
      self._multipolygon = None

  def __setstate__(self, state):
    # Restore the object's state from the state dictionary
    self.__dict__.update(state)
    self.logger = logging.getLogger(self.__class__.__name__)
    # Ensure overlaps field is initialized
    # if 'overlaps' not in state:
    #   self.overlaps = []    

  def __getstate__(self):
    """Custom getstate to convert overlaps to geog_id for serialization."""
    state = self.__dict__.copy()
    # Convert overlaps from List[Geo] to List[str] for serialization
    state['overlaps'] = [geo.geog_id for geo in self.overlaps]

    return state

  def add_child(self, child: 'Geo'):
    child.parent_id = self.geog_id
    self.children.append(child)

  def get_child_geo_collection(self):
    return GeoCollection.from_list(self.children)

  @property
  def simplified_multipolygons(self) -> MultiPolygon:
    if self._multipolygon is None:
      return None

    if isinstance(self._multipolygon, Polygon):
      simplified = self._multipolygon.simplify(0.01, preserve_topology=True)
      return MultiPolygon([simplified]) if simplified.geom_type == 'Polygon' else simplified
    elif isinstance(self._multipolygon, MultiPolygon):
      simplified_polys = [poly.simplify(0.01, preserve_topology=True) for poly in self._multipolygon.geoms]
      return MultiPolygon(simplified_polys)
    else:
      self.logger.warning(f"Unexpected geometry type: {type(self._multipolygon)}")
      return None
      
  @property
  def simplified_geometry(self) -> List[List[List[float]]]:
    simplified = self.simplified_multipolygons
    if simplified is None:
      return []

    result = []
    for polygon in simplified.geoms:
      coords = list(polygon.exterior.coords)
      # Convert to [lat, lng] and round
      polygon_coords = [[round(lat, 6), round(lng, 6)] for lng, lat in coords]
      result.append(polygon_coords)

    return result
  
  @property
  def simplified_geometry_lng_first(self) -> List[List[List[float]]]:
    return self.geometry_lng_first(simplified=True)

  
  def geometry_lng_first(self, simplified=False) -> Optional[List[List[List[float]]]]:
    """
    Returns the geometry with coordinates swapped from [lat, lng] to [lng, lat].
    
    Returns:
        Optional[List[List[List[float]]]]: Swapped geometry or None if no geometry exists.
    """
    if self.geometry is None:
        return None
    
    return [
        [
            [coord[1], coord[0]] for coord in polygon
        ]
        for polygon in (self.simplified_geometry if simplified else self.geometry)
    ]

  @property
  def is_neighbourhood(self) -> bool:
    return self.level == 10

  @property
  def is_city(self) -> bool:
    return self.level == 30

  @property
  def is_region(self) -> bool:
    return self.level == 40 or self.level == 35

  def contains_point(self, lat: float, lng: float) -> bool:
    if self._multipolygon is None:
      return False
    point = Point(lng, lat)
    # Check if the point is contained or on the boundary
    # contains = self._multipolygon.buffer(1e-10).contains(point) or self._multipolygon.touches(point)
    contains = self._multipolygon.contains(point) or self._multipolygon.touches(point)

    return contains

  def distance_to_point(self, lat: float, lng: float) -> float:
    point = Point(lng, lat)
    return point.distance(Point(self.lng, self.lat))

  def get_area(self) -> float:
    return self._multipolygon.area if self._multipolygon else 0

  def get_perimeter(self) -> float:
    return self._multipolygon.length if self._multipolygon else 0


  def intersect(self, other: 'Geo') -> bool:
    """Check if this Geo intersects with another Geo."""
    if not self._multipolygon or not other._multipolygon:
      return False
    return self._multipolygon.intersects(other._multipolygon)
  

  def show(self, figsize=(10, 10), point_of_interest: tuple = None):
    """
    Display the geographic area represented by this Geo object on a map.

    Args:
        figsize (tuple): Figure size for the plot. Default is (10, 10).
        point_of_interest (tuple): Optional (lat, lng) to mark on the map.

    Returns:
        None. Displays the plot.
    """
    if self._multipolygon is None:
      print(f"No geometry found for {self.name}")
      return
    
    plot_geometries(geometries=[self._multipolygon], 
                    names=[self.name], 
                    geog_ids=[self.geog_id], 
                    figsize=figsize, 
                    point_of_interest=point_of_interest)

    # fig, ax = plt.subplots(figsize=figsize)
    

    # # Create a GeoDataFrame from this Geo object
    # gdf = gpd.GeoDataFrame(geometry=[self._multipolygon], crs="EPSG:4326")
    # gdf_projected = gdf.to_crs(epsg=3857)
    
    # # Plot the geometry
    # gdf_projected.plot(ax=ax, color='blue', alpha=0.5, edgecolor='black', linewidth=2)
    
    # # Set plot bounds
    # ax.set_xlim(gdf_projected.total_bounds[[0, 2]])
    # ax.set_ylim(gdf_projected.total_bounds[[1, 3]])
    
    # # Add basemap
    # ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom='auto')
    
    # # Set title and remove axis
    # ax.set_title(f"{self.name} ({self.geog_id})")
    # ax.set_axis_off()
    
    # # Add point of interest if provided
    # if point_of_interest:
    #   lat, lng = point_of_interest
    #   point = gpd.GeoDataFrame(geometry=[Point(lng, lat)], crs="EPSG:4326")
    #   point_projected = point.to_crs(epsg=3857)
    #   point_projected.plot(ax=ax, color='red', markersize=100, marker='o', label='Point of Interest')
    #   plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    
    # plt.tight_layout()
    # plt.show()


  def sample_random_point(self):
    """
    Sample a random point uniformly within the geographic area represented by this Geo object.

    Returns:
        tuple: A tuple containing (latitude, longitude) of the sampled point.
                Returns None if the geometry is not defined.

    Note:
        This method uses rejection sampling with a convex hull approach.
        It assumes that the _multipolygon attribute is using (longitude, latitude) 
        coordinate order, which is common in GIS systems.
    """
    if self._multipolygon is None:
      self.logger.warning(f"No geometry defined for {self.name} ({self.geog_id})")
      return None

    if isinstance(self._multipolygon, MultiPolygon):
      # For multipolygons, choose a polygon based on area weights
      areas = [p.area for p in self._multipolygon.geoms]
      total_area = sum(areas)
      weights = [area / total_area for area in areas]
      chosen_poly = np.random.choice(self._multipolygon.geoms, p=weights)
    else:
      chosen_poly = self._multipolygon

    convex_hull = chosen_poly.convex_hull
    min_x, min_y, max_x, max_y = convex_hull.bounds
    
    max_attempts = 1000  # Limit the number of attempts to avoid infinite loops
    
    for _ in range(max_attempts):
      x = np.random.uniform(min_x, max_x)
      y = np.random.uniform(min_y, max_y)
      point = Point(x, y)
      
      if chosen_poly.contains(point):
        # Return as (latitude, longitude)
        return (y, x)
    
    # If we couldn't find a point after max_attempts, fall back to a centroid
    self.logger.warning(f"Could not sample random point for {self.name} ({self.geog_id}). Using centroid.")
    centroid = chosen_poly.centroid
    return (centroid.y, centroid.x)

  def __str__(self):
    # return f"{self.name} (Level {self.level})"
    attributes = vars(self)
    excluded_attributes = ["geometry", "_multipolygon"]
    attributes_str = "\n".join(f"{key}={value}" for key, value in attributes.items() if key not in excluded_attributes)
    return attributes_str

  def __repr__(self):
    return f"Geo(geog_id='{self.geog_id}', name='{self.name}', level={self.level})"


  @classmethod
  def from_geog_id(cls, geog_id: str, datastore: Datastore) -> 'Geo':
    geo_base = datastore.search(index=datastore.geo_index_name, _id=geog_id)[0]
    geo_details = datastore.search(index=datastore.geo_details_en_index_name, _id=geog_id)[0]
    geo_dict = {**geo_base, **geo_details}
    return cls.from_dict(geo_dict)
  
  @classmethod
  def from_dict(cls, data: Dict) -> 'Geo':
    geo_data = {
      'geog_id': data.get('localLogicId'),
      'name': (data.get(f'level{data["level"]}En') or 
                data.get('city') or 
                data.get('name') or 
                "Unknown"),
      'level': data['level'],
      'province': data['province'],
      'lat': data['lat'],
      'lng': data['lng'],
      'city': data.get('city'),
      'city_slug': data.get('citySlug'),
      'level_name': data.get('levelName'),
      'level_10_en': data.get('level10En'),
      'level_10_fr': data.get('level10Fr'),
      'level_20_en': data.get('level20En'),
      'level_20_fr': data.get('level20Fr'),
      'level_30_en': data.get('level30En'),
      'level_30_fr': data.get('level30Fr'),
      'level_35_en': data.get('level35En'),
      'level_35_fr': data.get('level35Fr'),
      'level_40_en': data.get('level40En'),
      'level_40_fr': data.get('level40Fr'),
      'has_en_profile': data.get('hasEnProfile', False),
      'has_fr_profile': data.get('hasFrProfile', False),
      'path': data.get('path'),
      'long_id': data.get('longId'),
      'created': data.get('created'),
      'modified': data.get('modified'),
      'uploaded': data.get('uploaded'),
      'manage_manually': data.get('manageManually', False),
      'name_suggest': data.get('name_suggest'),
    }

    if 'data' in data:
      geo_data.update({
        'bounding_box': data['data'].get('bounding_box'),
        'geometry': data.get('meta', {}).get('geometry'),
        'profiles': data['data'].get('profiles'),
        'intro': data['data'].get('intro'),
        'parent_id': data['data'].get('parent'),
      })

    return cls(**geo_data)

  @classmethod
  def save_objects(cls, geo_list: List['Geo'], file_path: str, use_dill: bool = False):
    path = Path(file_path)
    # Ensure the correct extension is used
    if path.suffix != cls._get_file_extension(use_dill):
      path = path.with_suffix(cls._get_file_extension(use_dill))
    
    path.parent.mkdir(parents=True, exist_ok=True)
    
    cls._serializer = dill if use_dill else pickle
    
    with path.open('wb') as f:
        cls._serializer.dump(geo_list, f)
    
    print(f"Saved {len(geo_list)} Geo objects to {path}")

  @classmethod
  def load_objects(cls, file_path: str, use_dill: bool = False) -> List['Geo']:
    path = Path(file_path)
    # Check if the file exists with the correct extension
    if not path.exists():
      alternate_path = path.with_suffix(cls._get_file_extension(use_dill))
      if alternate_path.exists():
        path = alternate_path
      else:
        raise FileNotFoundError(f"File not found: {path} or {alternate_path}")
    
    cls._serializer = dill if use_dill else pickle
    
    with path.open('rb') as f:
      geo_list = cls._serializer.load(f)
    
    print(f"Loaded {len(geo_list)} Geo objects from {path}")
    return geo_list
  
  @staticmethod
  def _get_file_extension(use_dill: bool) -> str:
    return '.dill' if use_dill else '.pkl'
  



class GeoCollection:
  logger = logging.getLogger(__name__)

  def __init__(self):
    self.logger = logging.getLogger(self.__class__.__name__)

    self._geos: List[Geo] = []
    self._index_by_id: Dict[str, Geo] = {}
    self._index_by_level: Dict[int, List[Geo]] = defaultdict(list)

  def add(self, geo: Geo) -> None:
    self._geos.append(geo)
    self._index_by_id[geo.geog_id] = geo
    self._index_by_level[geo.level].append(geo)

  def append(self, geo: Geo) -> None:
    self.add(geo)

  def get_by_id(self, geog_id: str) -> Optional[Geo]:
    return self._index_by_id.get(geog_id)

  def get_by_level(self, level: int) -> 'GeoCollection':
    level_geos = self._index_by_level[level]
    return GeoCollection.from_list(level_geos)

  def all(self) -> List[Geo]:
    return self._geos

  def filter(self, predicate) -> 'GeoCollection':
    return GeoCollection.from_list([geo for geo in self._geos if predicate(geo)])

  def find_containing(self, lat: float, lng: float) -> 'GeoCollection':
    """
    Finds and returns a list of Geo objects that contain the given latitude and longitude.

    Parameters:
    - lat (float): The latitude of the point.
    - lng (float): The longitude of the point.

    Returns:
    List[Geo]: A list of Geo objects that contain the specified point.
    """
    # return [geo for geo in self._geos if geo.contains_point(lat, lng)]
    return self.filter(lambda geo: geo.contains_point(lat, lng))

  def sample(self) -> Optional[Geo]:
    if not self._geos:
      return None
    return random.choice(self._geos)

 
  def search(self, query: str, level: Optional[int] = None) -> 'GeoCollection':
    """
    Search for Geo objects by name, optionally filtered by level.

    Args:
      query (str): The search query string.
      level (Optional[int]): The geographic level to filter by (if provided).

    Returns:
      GeoCollection: A new GeoCollection containing Geo objects matching the search criteria.
    """
    query = query.lower()
    matching_geos = [
      geo for geo in self._geos
      if query in geo.name.lower() and (level is None or geo.level == level)
    ]
    return GeoCollection.from_list(matching_geos)

  def show(self, figsize=(10, 10), point_of_interest: tuple = None, max_geos=3):
    """
    show a max of max_geos geos on a map
    """
    geometries = []
    names = []
    geog_ids = []
    for geo in self._geos[:max_geos]:
      if geo._multipolygon is not None:
        geometries.append(geo._multipolygon)
        names.append(geo.name)
        geog_ids.append(geo.geog_id)
    
    plot_geometries(geometries, names, geog_ids, 
                    figsize=figsize, point_of_interest=point_of_interest)

  def __len__(self) -> int:
    return len(self._geos)

  def __iter__(self):
    return iter(self._geos)

  def __getitem__(self, index):
    if isinstance(index, int):
      return self._geos[index]
    elif isinstance(index, str):
      return self.get_by_id(index)
    else:
      raise TypeError("Index must be an integer or string")

  @classmethod
  def from_list(cls, geo_list: List[Geo]) -> 'GeoCollection':
    collection = cls()
    for geo in geo_list:
      collection.add(geo)
    return collection
  
  @classmethod
  def from_datastore(cls, datastore: Datastore) -> 'GeoCollection':

    geo_list = []
    fail_list = []
    for geo_base in datastore.search(index=datastore.geo_index_name, selects=['localLogicId', 'longId', 'path'], size=None):
      geog_id = geo_base['localLogicId']

      try:
        geo = Geo.from_geog_id(geog_id=geog_id, datastore=datastore)
        geo_list.append(geo)
      except Exception as e:
        cls.logger.warning(f"Error creating Geo object for {geog_id}: {e}")
        fail_list.append(geog_id)

    cls.logger.warning(f"Failed to create Geo objects for {len(fail_list)} geos")

    # try again 2nd time for failed geos
    for geog_id in fail_list:
      try:
        geo = Geo.from_geog_id(geog_id=geog_id, datastore=datastore)
        geo_list.append(geo)
      except Exception as e:
        cls.logger.error(f"Error creating Geo object for {geog_id}: {e}")
      
    return cls.from_list(geo_list)


  def save(self, file_path: str, use_dill: bool = False) -> None:
    Geo.save_objects(self._geos, file_path, use_dill)

  @classmethod
  def load(cls, file_path: str, use_dill: bool = False) -> 'GeoCollection':
    geo_list = Geo.load_objects(file_path, use_dill)
    collection = cls.from_list(geo_list)

    collection.restore_all_overlaps()  # overlaps geos are serialized as List[str] using the geog_id

    return collection


  def assign_guids_to_listing_df(self, df: pd.DataFrame, filter: Callable = None, cutoff_timestamp: pd.Timestamp = None):
    """
    Compute, construct and assign guid for df
    """
    assert 'lat' in df.columns and 'lng' in df.columns, "Latitude and longitude columns are required in the DataFrame"
    
    self.logger.info(f'# of geos in collection: {len(self)}')

    def construct_guid(lat: float, lng: float) -> str:
      levels = [40, 35, 30, 20, 10]
      found_geos = []
      searched_levels = set()

      def search_geo(level, parent_geo=None):
        """Search for the geo at the current level."""
        if level in searched_levels:
          return None
        geos = parent_geo.children if parent_geo else self.get_by_level(level)
        searched_levels.add(level)
        for geo in geos:
          if geo.contains_point(lat, lng):
            return geo
        return None

      # Start from the coarsest level (40) and work down to the finest (10)
      parent_geo = None
      for level in levels:
        geo = search_geo(level, parent_geo)
        if geo:
          if geo not in found_geos:
            found_geos.append(geo)
            # Check for overlaps at this level
            for overlap_geo in geo.overlaps:
              if overlap_geo.contains_point(lat, lng) and overlap_geo not in found_geos:
                found_geos.append(overlap_geo)
          parent_geo = geo
        else:
          parent_geo = None

      # Construct the GUID string
      guid = ','.join([geo.geog_id for geo in reversed(found_geos)])
      return guid if guid else None
    
    # just fix here those 'None' a string with None, they are really None.
    df['guid'] = df['guid'].replace({'None': None})
    
    # Apply the filter to the DataFrame only if filter is not None
    df_filtered = df[filter(df)] if filter is not None else df

    total_rows = len(df_filtered)
    for i, row in tqdm(df_filtered.iterrows(), total=total_rows):
      # if (row['guid'] is None or row['computed_guid'] is not None) and (cutoff_timestamp is None or row['lastTransition'] < cutoff_timestamp):
      if row['guid'] is None and (cutoff_timestamp is None or row['lastTransition'] < cutoff_timestamp):
        lat, lng = row['lat'], row['lng']

        # old_guid = row['guid']  # Fetch the old GUID for comparison    
        guid = construct_guid(lat, lng)
        
        df.loc[i, 'computed_guid'] = None if guid == '' else guid
        df.loc[i, 'guid'] = None if guid == '' else guid

  def restore_all_overlaps(self):
    """Restore overlaps for all geos after deserialization."""
    for geo in self._geos:
      # geo.overlaps = [self.get_by_id(geog_id) for geog_id in geo.overlaps if geog_id in self._index_by_id]
      overlap_ids = geo.overlaps  # this is currently a list of geog_ids as it comes out of deserialization
      geo.overlaps = GeoCollection.from_list([
        self[geog_id] 
        for geog_id in overlap_ids 
        if geog_id in self._index_by_id
      ])

  def fix_parent_ids(self):
    could_not_finds = []
    fixed_count = 0

    for geo in self._geos:
      if geo.parent_id is None:
        possible_parent_levels = [level for level in [20, 30, 35, 40] if level > geo.level]
        for parent_level in possible_parent_levels:
          parent_id = self._find_parent(geo, parent_level)
          if parent_id:
            print(f'Found parent_id {parent_id} for {geo.geog_id} (name: {geo.name}, level: {geo.level})')
            geo.parent_id = parent_id
            fixed_count += 1
            break
        else:
          print(f'Could not find parent for {geo.geog_id} (name: {geo.name}, level: {geo.level})')
          could_not_finds.append(geo.geog_id)

    return fixed_count, could_not_finds

  def _find_parent(self, geo, parent_level):
    parent_name = getattr(geo, f'level_{parent_level}_en')
    if parent_name:
      exact_match = next((g for g in self.get_by_level(parent_level) if g.name == parent_name), None)
      if exact_match:
        return exact_match.geog_id

    child_geometry = self._fix_invalid_geometry(geo._multipolygon)
    if child_geometry is None:
      warnings.warn(f"Invalid child geometry for {geo.geog_id}")
      return None

    for potential_parent in self.get_by_level(parent_level):
      parent_geometry = self._fix_invalid_geometry(potential_parent._multipolygon)
      if parent_geometry is None:
        warnings.warn(f"Invalid parent geometry for {potential_parent.geog_id}")
        continue
      
      if self._check_containment(parent_geometry, child_geometry):
        return potential_parent.geog_id
    
    return None
  
  def _fix_invalid_geometry(self, geom):
    if geom is None or geom.is_valid:
      return geom
    
    fixed_geom = geom.buffer(0)
    if not fixed_geom.is_valid:
      warnings.warn(f"Failed to fix invalid geometry: {explain_validity(geom)}")
      return None
    return fixed_geom
  
  def _check_containment(self, parent_geometry, child_geometry, area_threshold=0.99):
    if parent_geometry is None or child_geometry is None:
      return False
    try:
      intersection_area = parent_geometry.intersection(child_geometry).area
      child_area = child_geometry.area
      return intersection_area / child_area >= area_threshold
    except Exception as e:
      warnings.warn(f"Error in check_containment: {str(e)}")
      return False

  def populate_children(self):

    # Dictionary to store children for each geo
    children_dict = {geo.geog_id: [] for geo in self._geos}

    # Populate children_dict
    for geo in self._geos:
      if geo.parent_id and geo.parent_id in children_dict:
        children_dict[geo.parent_id].append(geo)

    # Assign children to each geo
    for geo_id, children in children_dict.items():
      # geo_dict[geo_id].children = children
      self._index_by_id[geo_id].children = children

    # Count total geos with children
    # geos_with_children = sum(1 for geo in geo_dict.values() if geo.children)
    n = self.filter(lambda g: len(g.children) > 0)

    print(f"Populated children for {n} Geo objects")

  def compute_overlaps(self):
    """Compute overlaps for each Geo object in the collection based on their level."""
    for level, geos in self._index_by_level.items():
      print(f"Computing overlaps for level {level} with {len(geos)} geos.")
      
      for i, geo in enumerate(geos):
        # Skip if the geometry is None
        if geo._multipolygon is None:
          print(f"Geometry is None for {geo.geog_id}. Skipping.")
          continue

        # Ensure the geometry is valid
        if not geo._multipolygon.is_valid:
          print(f"Invalid geometry detected in {geo.geog_id}. Attempting to fix.")
          geo._multipolygon = geo._multipolygon.buffer(0)
          if geo._multipolygon is None or not geo._multipolygon.is_valid:
            print(f"Failed to fix invalid geometry for {geo.geog_id}. Skipping.")
            continue

        for other_geo in geos[i+1:]:
          # Skip if the geometry is None
          if other_geo._multipolygon is None:
            print(f"Geometry is None for {other_geo.geog_id}. Skipping.")
            continue

          # Ensure the other geometry is valid
          if not other_geo._multipolygon.is_valid:
            print(f"Invalid geometry detected in {other_geo.geog_id}. Attempting to fix.")
            other_geo._multipolygon = other_geo._multipolygon.buffer(0)
            if other_geo._multipolygon is None or not other_geo._multipolygon.is_valid:
              print(f"Failed to fix invalid geometry for {other_geo.geog_id}. Skipping.")
              continue

          # Check for overlaps
          if geo.intersect(other_geo):
            geo.overlaps.append(other_geo)
            other_geo.overlaps.append(geo)
                    
