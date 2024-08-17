'''
  def build_loc_info_geog_id_map(self):
    """
    Builds a mapping of location information keys to geographic identifiers (geog_ids) at various levels.

    This method iterates over unique location information extracted from the sold listings dataframe,
    querying for geographic identifiers (geog_ids) for each location. It constructs a dictionary where
    each key is a location information key (constructed from province/state, city, and neighbourhood names),
    and the value is another dictionary mapping 'geog_id_{level}' to the corresponding geographic identifier.

    The process involves:
    - Extracting unique location information from the sold listings dataframe.
    - For each unique location, constructing a location key.
    - Querying geographic identifiers for each location at various levels of granularity.
    - Handling any exceptions during the querying process and breaking the loop in case of errors.
    - Constructing the final mapping of location keys to their respective geographic identifiers.

    Returns:
        dict: A dictionary where each key is a location information key and the value is another dictionary
              mapping 'geog_id_{level}' to geographic identifiers. The levels indicate the granularity of the
              geographic area, with lower numbers being more specific.

    Example of returned dictionary structure:
    {
      'ON_Toronto_Casa_Loma': {
        'geog_id_10': 'g10_dpz82zw0',
        'geog_id_20': 'g20_dpz83mm2',
        'geog_id_30': 'g30_dpz89rm7'
      },
      'BC_Vancouver_Kitsilano': {
        'geog_id_30': 'g30_dpz89abc'
      }
    }
    """
    loc_infos = self.get_unique_loc_infos()
    hash = {}
    for _, loc_info in tqdm(loc_infos.iterrows()):
      loc_info = loc_info.to_dict()

      try:
        results = self.get_geog_ids(loc_info)
      except Exception as e:
        print(f"Error while processing {loc_info}: {e}")
        raise e   # debug further. #TODO: log errors in the future.

      loc_info_key = self.build_loc_info_key(loc_info)
    
      hash[loc_info_key] = {f"geog_id_{result['level']}": result['geog_id'] for result in results}

    return hash

  def build_geog_id_level_df(self):
    """
    build a dataframe with loc_info_key (as defined in build_loc_info_key) and 
    geog_id_10, geog_id_20, geog_id_30 as columns. The geog_ids will be used as gropuby keys
    when calculating median metrics for sold listings.

    Returns:
        pandas.DataFrame: A dataframe containing location information keys and geog_ids at different levels.

        E.g. 
              loc_info_key	                geog_id_30	  geog_id_10	  geog_id_20
            0	ON_Brockville_Windsor_Heights	g30_drfjtrnn	NaN	          NaN
            1	NS_Dartmouth_None	            NaN	          NaN	          NaN
            2	ON_Ottawa_WESTBORO	          g30_f241etq5	g10_f244hr3n	NaN
    """
    df = pd.DataFrame(self.build_loc_info_geog_id_map()).T
    df = df.reset_index().rename(columns={'index': 'loc_info_key'})
    self._geog_ids_df = df
    return df
  
  def get_unique_loc_infos(self):
    """
    Extracts unique location information from the sold listings dataframe.

    This method selects the 'provState', 'city', and 'neighbourhood' columns from the
    sold_listing_df dataframe, and then drops any duplicate rows to ensure that each
    combination of province/state, city, and neighbourhood is unique.

    Returns:
        pandas.DataFrame: A dataframe containing unique combinations of province/state,
                          city, and neighbourhood from the sold listings.
    """
    return self.sold_listing_df[['provState', 'city', 'neighbourhood']].drop_duplicates()

  def get_geog_ids(self, loc_info: Dict[str, str]) -> List[Dict]:
    """
    Retrieve geog identifiers (geog_ids) for a given location at various levels of granularity.

    This function queries geo_df (derived from rlp_content_geo_current) to find matching geog_ids for a location,
    starting from the most specific level (neighborhood) and moving to broader levels (city).

    Parameters:
    loc_info (Dict[str, str]): A dictionary containing location information with keys:
      - 'provState': The province or state code (e.g., 'ON' for Ontario)
      - 'city': The city name
      - 'neighbourhood': The neighborhood name

    geo_df (pd.DataFrame): A pandas DataFrame containing geographic data with columns:
      - 'level': The geo level (10 for most granular, 20, 30 for broader coarser levels)
      - 'level{level}En': The name of the location at each level (e.g., 'level10En' for neighborhood name)
      - 'geog_id': The geographic identifier
      - 'province': The province or state code
      - 'city': The city name

    Returns:
    List[Dict[str, str]]: A list of dictionaries, each containing:
      - 'geog_id': The geographic identifier
      - 'level': The level of the geog_id (as a string: '10', '20', or '30')

    The list is ordered from most specific to most general geographic level found.

    Example:
    >>> loc_info = {'provState': 'ON', 'city': 'Toronto', 'neighbourhood': 'Casa Loma'}
    >>> geog_ids = get_geog_ids(loc_info, geo_df)
    >>> print(geog_ids)
    [{'geog_id': 'g10_dpz82zw0', 'level': '10'}, 
    {'geog_id': 'g20_dpz83mm2', 'level': '20'}, 
    {'geog_id': 'g30_dpz89rm7', 'level': '30'}]
    """

    def query_geo_df(level: int, value: str):
      # return self.geo_df.q(f"level == {level} and level{level}En == '{value}' and city == '{city}' and province == '{provState}'")
      mask = (
        (self.geo_df['level'] == level) &
        (self.geo_df[f'level{level}En'].fillna('').str.strip() == value) &
        (self.geo_df['city'].fillna('').str.strip() == city) &
        (self.geo_df['province'].fillna('').str.strip() == provState)
      )
      return self.geo_df[mask]
    
    def cleanup_string(s):
      if s is None:
        return ''
      return s.strip() #.replace("'", "''") # no need for escaping since we arent using .q or .query numexpr 
    
    provState = cleanup_string(loc_info.get('provState'))
    city = cleanup_string(loc_info.get('city'))
    neighbourhood = loc_info.get('neighbourhood')
    if neighbourhood:
      neighbourhood = neighbourhood.title()
      neighbourhood = cleanup_string(neighbourhood)
    
    geog_ids = []
    
    # Start with the most granular level (10) and stop as soon as we reach a level with a match
    for level in [10, 20, 30]:
      first_result = query_geo_df(level, neighbourhood)      
      if not first_result.empty:
        geog_ids.append({
          'geog_id': first_result.iloc[0].geog_id,
          'level': str(level)
        })
        break

    # Every parents (up the level) should be present in first_result
    for upper_level in range(level+10, 40, 10):
      value = first_result.iloc[0][f'level{upper_level}En']
      result = query_geo_df(upper_level, value)
      if not result.empty:
        geog_ids.append({
          'geog_id': result.iloc[0].geog_id,
          'level': str(upper_level)
        })

    # If no geog_ids were found, just try to find a city-level match
    if not geog_ids:
      # city_result = self.geo_df.q(f"level == 30 and level30En == '{city}' and province == '{provState}'")
      city_result = query_geo_df(30, city)
      if not city_result.empty:
        geog_ids.append({
          'geog_id': city_result.iloc[0].geog_id,
          'level': '30'
        })
    
    return geog_ids
  
  def build_loc_info_key(self, loc_info: Dict[str, str]) -> str:
    """
    Constructs a key from location information for consistent identification.

    This method takes a dictionary containing location information, specifically the province/state,
    city, and neighbourhood, and constructs a unique key by concatenating these values. Spaces in the
    neighbourhood name are replaced with underscores to ensure the key is a valid identifier.

    Parameters:
        loc_info (Dict[str, str]): A dictionary containing the location information with keys 'provState',
                                  'city', and 'neighbourhood'.

    Returns:
        str: A unique location key constructed from the provided location information, formatted as
            'provState_city_neighbourhood'. If the neighbourhood is not provided, it is omitted from the key.

    Example:
        >>> loc_info = {'provState': 'ON', 'city': 'Toronto', 'neighbourhood': 'Casa Loma'}
        >>> key = build_loc_info_key(loc_info)
        >>> print(key)
        'ON_Toronto_Casa_Loma'
    """
    
    provState = loc_info['provState']
    city = loc_info['city']
    neighbourhood = loc_info['neighbourhood']
    if neighbourhood:
      neighbourhood = neighbourhood.replace(' ', '_')

    return f"{provState}_{city}_{neighbourhood}"
  
  def old_add_geog_ids_to_sold_listings(self):
    """
    # def create_loc_info_key(row):
    #   return self.build_loc_info_key(row[['provState', 'city', 'neighbourhood']].to_dict())
    
    # self.sold_listing_df['loc_info_key'] = self.sold_listing_df.apply(create_loc_info_key, axis=1)

    # if self._geog_ids_df is None:
    #   self.build_geog_id_level_df()

    # self.sold_listing_df = join_df(self.sold_listing_df, 
    #                               self._geog_ids_df, 
    #                               left_on='loc_info_key', 
    #                               right_on='loc_info_key', 
    #                               how='left')
    # self.sold_listing_df.drop(columns=['loc_info_key'], inplace=True)
    """
    pass

  def workaround(self):
    # WORKAROUND: to fill in missing geog_ids with only ['CITY', 'PROV_STATE'] as the FKs
    missing_mask = self.sold_listing_df[['geog_id_10', 'geog_id_20', 'geog_id_30', 'geog_id_40']].isnull().all(axis=1)
    if not missing_mask.any(): return

    def count_geog_ids(geographies):
      if pd.isna(geographies) or not isinstance(geographies, str):
        return 0
      return len(geographies.split(','))
    
    self.geo_entry_df['geog_id_count'] = self.geo_entry_df['GEOGRAPHIES'].apply(count_geog_ids)  # add geog_id count

    # Create a reduced version of geo_entry_df with unique CITY and PROV_STATE
    unique_geo_entry = (self.geo_entry_df
                        .sort_values('geog_id_count', ascending=False)
                        .groupby(['CITY', 'PROV_STATE'])
                        .first()
                        .reset_index()[['CITY', 'PROV_STATE', 'GEOGRAPHIES']])


    # Perform the join without MLS
    filled_df = join_df(
      self.sold_listing_df.loc[missing_mask, ['city', 'provState']],  # only CITY and PROV_STATE are needed for the join
      unique_geo_entry,
      left_on=['city', 'provState'],
      right_on=['CITY', 'PROV_STATE'],
      how='left'
    )

    geog_data = filled_df['GEOGRAPHIES'].apply(parse_geog_ids)
    for level in ['10', '20', '30', '40']:
      self.sold_listing_df.loc[missing_mask, f'geog_id_{level}'] = geog_data.apply(lambda x: x.get(f'geog_id_{level}'))

      
      
def update_mkt_trends_ts_index(self):
    """
    Sample doc from the mkt_trends_ts index:
    {'geog_id': 'g30_dpz89rm7',
      'propertyType': 'SEMI-DETACHED',
      'geo_level': 30,
      'metrics': {'median_price': [{'date': '2023-01', 'value': 1035000.0},
        {'date': '2023-10', 'value': 1192000.0},
        {'date': '2023-11', 'value': 1037500.0},
        {'date': '2023-12', 'value': 976000.0},
        {'date': '2024-01', 'value': 1075000.0},
        {'date': '2024-02', 'value': 1201054.0},
        {'date': '2024-03', 'value': 1175333.0},
        {'date': '2024-04', 'value': 1159444.0},
        {'date': '2024-06', 'value': 915000.0}],
      'median_dom': [{'date': '2023-01', 'value': 3},
        {'date': '2023-10', 'value': 9},
        {'date': '2023-11', 'value': 15},
        {'date': '2023-12', 'value': 33},
        {'date': '2024-01', 'value': 9},
        {'date': '2024-02', 'value': 8},
        {'date': '2024-03', 'value': 8},
        {'date': '2024-04', 'value': 8},
        {'date': '2024-06', 'value': 49}],
      'last_mth_median_asking_price': {'date': '2024-06', 'value': 1150000.0},
      'last_mth_new_listings': {'date': '2024-06', 'value': 48}},
      'last_updated': '2024-07-24T21:33:28.443159'}
    """
    def generate_actions():
        # Merge price and DOM series on geog_id, propertyType, and geo_level
        merged_df = pd.merge(self.diff_price_series, self.diff_dom_series, 
                             on=['geog_id', 'propertyType', 'geo_level'], 
                             suffixes=('_price', '_dom'))
        
        for _, row in merged_df.iterrows():
            doc = {
                "geog_id": row['geog_id'],
                "propertyType": 'ALL' if row['propertyType'] is None else row['propertyType'],
                "geo_level": int(row['geo_level']),
                "metrics": {
                    "median_price": [],
                    "median_dom": []
                },
                "last_updated": datetime.now().isoformat()
            }

            for col in merged_df.columns:
                if col.startswith('20') and col.endswith('_price'):
                    date = col.split('_')[0]
                    value = row[col]
                    if pd.notna(value):
                        doc["metrics"]["median_price"].append({
                            "date": date,
                            "value": float(value)
                        })
                elif col.startswith('20') and col.endswith('_dom'):
                    date = col.split('_')[0]
                    value = row[col]
                    if pd.notna(value):
                        doc["metrics"]["median_dom"].append({
                            "date": date,
                            "value": int(value)
                        })

            property_type_id = "ALL" if row['propertyType'] is None else row['propertyType']            

            # Composite _id using geog_id and propertyType
            composite_id = f"{row['geog_id']}_{property_type_id}"

            yield {
              "_op_type": "update",
              "_index": self.datastore.mkt_trends_index_name,
              "_id": composite_id,
              "doc": doc,
              "doc_as_upsert": True
            }

    # Perform bulk insert
    success, failed = bulk(self.datastore.es, generate_actions(), raise_on_error=False, raise_on_exception=False)

    self.logger.info(f"Successfully updated {success} documents")
    if failed: self.logger.error(f"Failed to update {len(failed)} documents")

    return success, failed

# Function to compute metrics for a given geog level
def compute_metrics(df, date_mask, geo_level):
  # Group by geog_id, property type, and month, then calculate metrics
  grouped = df[date_mask].groupby([f'geog_id_{geo_level}', 'propertyType', pd.Grouper(key='lastTransition', freq='M')])
  metrics = grouped.agg({
      'soldPrice': 'median',
      'daysOnMarket': 'median',
      'sold_over_ask': lambda x: (x.sum() / len(x)) * 100,  # Percentage over ask
      'sold_below_ask': lambda x: (x.sum() / len(x)) * 100  # Percentage below ask
  }).reset_index()

  # Calculate metrics for all property types combined
  grouped_all = df[date_mask].groupby([f'geog_id_{geo_level}', pd.Grouper(key='lastTransition', freq='M')])
  metrics_all = grouped_all.agg({
    'soldPrice': 'median',
    'daysOnMarket': 'median',
    'sold_over_ask': lambda x: (x.sum() / len(x)) * 100,  # Percentage over ask
    'sold_below_ask': lambda x: (x.sum() / len(x)) * 100  # Percentage below ask
  }).reset_index()
  # Add a 'propertyType' column with None value for the combined metrics
  metrics_all['propertyType'] = None

  # Concatenate the property-specific and combined metrics
  metrics_combined = pd.concat([metrics, metrics_all], ignore_index=True)
  
  # Pivot the data to create time series
  price_series = metrics_combined.pivot(index=[f'geog_id_{geo_level}', 'propertyType'], 
                                columns='lastTransition', 
                                values='soldPrice')
  dom_series = metrics_combined.pivot(index=[f'geog_id_{geo_level}', 'propertyType'], 
                              columns='lastTransition', 
                              values='daysOnMarket')
  over_ask_series = metrics_combined.pivot(index=[f'geog_id_{geo_level}', 'propertyType'], 
                                columns='lastTransition', 
                                values='sold_over_ask')
  below_ask_series = metrics_combined.pivot(index=[f'geog_id_{geo_level}', 'propertyType'],
                                columns='lastTransition',
                                values='sold_below_ask')
  
  # Rename columns to YYYY-MM format
  for series in [price_series, dom_series, over_ask_series, below_ask_series]:
    series.columns = series.columns.strftime('%Y-%m')

    # Remove the 'lastTransition' label from the column index
    series.columns.name = None

    # Reset index to make geog_id and propertyType regular columns
    # Rename the geog_id_<N> uniformly to geog_id
    series.reset_index(inplace=True)
    series.rename(columns={f'geog_id_{geo_level}': 'geog_id'}, inplace=True)

    # Replace NaN with None in the propertyType 
    series['propertyType'] = series['propertyType'].where(series['propertyType'].notna(), None)

  # price_series.columns = price_series.columns.strftime('%Y-%m')
  # dom_series.columns = dom_series.columns.strftime('%Y-%m')

  # # Remove the 'lastTransition' label from the column index
  # price_series.columns.name = None
  # dom_series.columns.name = None
  
  # # Reset index to make geog_id and propertyType regular columns
  # # Rename the geog_id_<N> uniformly to geog_id
  # price_series = price_series.reset_index().rename(columns={f'geog_id_{geo_level}': 'geog_id'})
  # dom_series = dom_series.reset_index().rename(columns={f'geog_id_{geo_level}': 'geog_id'})

  # # Replace NaN with None in the propertyType 
  # price_series['propertyType'] = price_series['propertyType'].where(price_series['propertyType'].notna(), None)
  # dom_series['propertyType'] = dom_series['propertyType'].where(dom_series['propertyType'].notna(), None)

  return price_series, dom_series, over_ask_series, below_ask_series



'''
