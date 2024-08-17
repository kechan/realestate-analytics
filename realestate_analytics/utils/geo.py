from typing import List, Tuple, Union
from pathlib import Path

import pandas as pd

from shapely.geometry import Point, Polygon, MultiPolygon
from tqdm.auto import tqdm

from ..data.geo import GeoCollection

def is_point_in_polygon(lat: float, lng: float, geometry_coordinates: list) -> bool:
    """
    Check if a given point (lat, lng) is inside the polygon or multipolygon defined by geometry_coordinates.
    
    :param lat: Latitude of the point
    :param lng: Longitude of the point
    :param geometry_coordinates: List of coordinate pairs defining the polygon or list of lists of coordinate pairs for multipolygons
    :return: True if the point is inside the polygon or any polygon in the multipolygon, False otherwise
    """
    point = Point(lng, lat)  # Note: shapely uses (x, y) order, which is (longitude, latitude)

    # Check if it's a single polygon (list of tuples) or a multipolygon (list of list of tuples)
    if all(isinstance(coord, tuple) for coord in geometry_coordinates):
        # Single polygon
        polygon = Polygon(geometry_coordinates)
        return polygon.contains(point)
    elif all(isinstance(polygon, list) for polygon in geometry_coordinates):
        # Multipolygon
        multipolygon = MultiPolygon([Polygon(polygon) for polygon in geometry_coordinates])
        return multipolygon.contains(point)
    else:
        raise ValueError("Invalid geometry_coordinates structure")
    

def assign_guids_to_listing_df_total_bruteforce(df: pd.DataFrame, geo_collection_path: Union[str, Path]):
    assert 'lat' in df.columns and 'lng' in df.columns, "Latitude and longitude columns are required in the DataFrame"

    geo_collection_path = Path(geo_collection_path)
    geo_collection = GeoCollection.load(geo_collection_path, use_dill=True)

    if 'guid' not in df.columns:
        df['guid'] = None

    total_rows = len(df)

    for i, row in tqdm(df.iterrows(), total=total_rows):
        lat, lng = row['lat'], row['lng']
        geog_ids = ','.join([g.geog_id for g in geo_collection.find_containing(lat, lng)])
        
        guid = ','.join(geog_ids) if geog_ids != '' else None
        df.loc[i, 'guid'] = guid 

def assign_guids_to_listing_df_brute_force_per_level(df: pd.DataFrame, geo_collection_path: Union[str, Path]):
    assert 'lat' in df.columns and 'lng' in df.columns, "Latitude and longitude columns are required in the DataFrame"

    geo_collection_path = Path(geo_collection_path)
    geo_collection = GeoCollection.load(geo_collection_path, use_dill=True)

    if 'guid' not in df.columns:
        df['guid'] = None

    levels = [10, 20, 30, 35, 40]
    geog_ids = []
    for i, row in tqdm(df.iterrows(), total=len(df)):
        lat, lng = row['lat'], row['lng']
        for level in levels:
            geos = geo_collection.get_by_level(level)
            for geo in geos:
                if geo.contains_point(lat, lng):
                    geog_ids.append(geo.geog_id)
                    break

        guid = ','.join(geog_ids)
        df.loc[i, 'guid'] = guid if guid != '' else None

        geog_ids.clear()
        

def assign_guids_to_listing_df_parent_id_chasing(df: pd.DataFrame, geo_collection_path: Union[str, Path]):
    """
    Bottom up approach, relies on parent_id integrity

    Assigns GUIDs to each row in a DataFrame based on the geographical location (latitude and longitude) of the listings.
    The GUIDs are constructed by searching through a hierarchical geo-collection from broader to more specific geographical areas (levels 10 to 40).
    For each listing, the function searches the geo-collection for the smallest geographical area that contains the listing's coordinates.
    Once found, the geographical ID (geog_id) of the area and its parent areas are concatenated to form the GUID.

    The search strategy involves iterating through predefined levels (10, 20, 30, 35, 40) in the geo-collection.
    For each level, it checks if the listing's coordinates are contained within any geographical area at that level.
    If a containing area is found, the search stops for that listing, and the process of chasing up the hierarchy of parent IDs begins to construct the GUID.

    Parameters:
    - df (pd.DataFrame): The DataFrame containing the listings. Must include 'lat' and 'lng' columns for latitude and longitude, respectively.
    - geo_collection_path (Union[str, Path]): The file path to the geo-collection object, which contains the hierarchical geographical areas.

    Returns:
    - None: The function modifies the DataFrame in place by adding a 'guid' column with the constructed GUIDs.
    """
    assert 'lat' in df.columns and 'lng' in df.columns, "Latitude and longitude columns are required in the DataFrame"
    
    geo_collection_path = Path(geo_collection_path)
    geo_collection = GeoCollection.load(geo_collection_path, use_dill=True)

    if 'guid' not in df.columns:
        df['guid'] = None

    total_rows = len(df)

    for i, row in tqdm(df.iterrows(), total=total_rows):
        lat, lng = row['lat'], row['lng']
        # search from level 10 to 40
        geog_ids = []
        found = False
        for level in [10, 20, 30, 35, 40]:
            geos = geo_collection.get_by_level(level)
            for geo in geos:
                if geo.contains_point(lat, lng):
                    geog_ids.append(geo.geog_id)
                    found = True
                    break

                if found:
                    break

        # follow the parent_id until None
        while geo.parent_id:
            geo = geo_collection.get_by_id(geo.parent_id)
            geog_ids.append(geo.geog_id)

        guid = ','.join(geog_ids)
        df.loc[i, 'guid'] = guid if guid != '' else None


def assign_guids_to_listing_df_top_down_recursively(df: pd.DataFrame, geo_collection_path: Union[str, Path]):
    assert False, "this method has a bug. Debug before using"

    assert 'lat' in df.columns and 'lng' in df.columns, "Latitude and longitude columns are required in the DataFrame"
    
    geo_collection_path = Path(geo_collection_path)
    geo_collection = GeoCollection.load(geo_collection_path, use_dill=True)

    if 'guid' not in df.columns:
        df['guid'] = None


    total_rows = len(df)

    for i, row in tqdm(df.iterrows(), total=total_rows):
        lat, lng = row['lat'], row['lng']
        
        geog_ids = []
        
        def search_geo(level, geo=None):
            """Recursively search for the geo at the current level and its children."""
            nonlocal geog_ids
            
            geos = geo_collection.get_by_level(level) if geo is None else geo_collection.get_children(geo.geog_id)
            for geo in geos:
                if geo.contains_point(lat, lng):
                    geog_ids.append(geo.geog_id)
                    if level > 10:  # if not at the finest level, go to the next finer level
                        next_level = level - 10 if level > 30 else level - 5
                        search_geo(next_level, geo)
                    return True  # found the geo, exit the loop
            return False

        # Start from the coarsest level (40)
        search_geo(40)
        
        # Reverse the geog_ids list to match the required order from most granular to coarsest
        geog_ids.reverse()
        
        # Update the DataFrame with the constructed guid
        guid = ','.join(geog_ids)
        df.loc[i, 'guid'] = guid if guid != '' else None

def assign_guids_to_listing_df_top_down_nonrecursive(df: pd.DataFrame, geo_collection: Union[str, Path, GeoCollection], cutoff_timestamp: pd.Timestamp = None):
    assert 'lat' in df.columns and 'lng' in df.columns, "Latitude and longitude columns are required in the DataFrame"
    
    if isinstance(geo_collection, (str, Path)):
        geo_collection_path = Path(geo_collection_path)
        geo_collection = GeoCollection.load(geo_collection_path, use_dill=True)

    print(f'# of geos in collection: {len(geo_collection.geos)}')

    def construct_guid(lat: float, lng: float, geo_collection: GeoCollection) -> str:
        levels = [40, 35, 30, 20, 10]
        found_geos = []
        searched_levels = set()

        def search_geo(level, parent_geo=None):
            """Search for the geo at the current level."""
            if level in searched_levels:
                return None
            geos = parent_geo.children if parent_geo else geo_collection.get_by_level(level)
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
                if not found_geos or found_geos[-1] != geo:
                    found_geos.append(geo)
                parent_geo = geo
            else:
                parent_geo = None

        # Construct the GUID string
        guid = ','.join([geo.geog_id for geo in reversed(found_geos)])
        return guid if guid else None

    if 'guid' not in df.columns:
        df['guid'] = None

    total_rows = len(df)
    for i, row in tqdm(df.iterrows(), total=total_rows):
        if row['guid'] is None and (cutoff_timestamp is None or row['lastTransition'] < cutoff_timestamp):
            lat, lng = row['lat'], row['lng']

            # old_guid = row['guid']  # Fetch the old GUID for comparison    
            guid = construct_guid(lat, lng, geo_collection)
            
            df.loc[i, 'computed_guid'] = None if guid == '' else guid
            df.loc[i, 'guid'] = None if guid == '' else guid
        

def validate_guid(new_guid, brute_force_guid):
    def extract_geos(guid):
        """Extract geog_ids and group them by level."""
        geos_by_level = {}
        if guid:
            geos = guid.split(',')
            for geo in geos:
                match = re.match(r'g(\d+)_\w+', geo)
                if match:
                    level = int(match.group(1))
                    if level not in geos_by_level:
                        geos_by_level[level] = set()
                    geos_by_level[level].add(geo)
        return geos_by_level

    new_geos = extract_geos(new_guid)
    brute_force_geos = extract_geos(brute_force_guid)
    
    # Check if every geog_id in new_guid exists in the corresponding brute_force level
    for level, new_geo_set in new_geos.items():
        if level in brute_force_geos:
            if not new_geo_set.issubset(brute_force_geos[level]):
                return False
        else:
            return False
    
    return True
  