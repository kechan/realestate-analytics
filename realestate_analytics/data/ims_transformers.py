from __future__ import annotations
from typing import List

import pandas as pd


def derive_property_type(row):
  listing_type = row.get('listingType', '').split(', ')  # Assuming listingType is a comma-separated string
  search_category_type = row.get('searchCategoryType', None)


  if search_category_type == 101 and any(lt in ['CONDO', 'REC'] for lt in listing_type):
    return 'CONDO'
  elif search_category_type == 104:
    return 'SEMI-DETACHED'
  elif search_category_type == 102:
    return 'TOWNHOUSE'
  elif search_category_type == 103:
    return 'DETACHED'
  else:
    return 'OTHER'


def format_listingType(x: List[str]):
  if len(x) > 0 and x[0] == 'listingType':
    x = x[1:]
  return ', '.join(x)


# Custom function to parse datetime with flexible format
def parse_datetime(dt_string):
  try:
    return pd.to_datetime(dt_string, format='%Y-%m-%dT%H:%M:%S.%f')
  except ValueError:
    return pd.to_datetime(dt_string, format='%Y-%m-%dT%H:%M:%S')


def validate_ims_columns(ims_df: pd.DataFrame) -> tuple[bool, list]:
  """
  Validate that IMS dataframe has all required columns for mapping.

  Args:
    ims_df: DataFrame with IMS column names

  Returns:
    Tuple of (is_valid, missing_columns)

  Example:
    >>> ims_df = pd.DataFrame({'WrittenDate': [...], 'SoldPrice': [...]})
    >>> is_valid, missing = validate_ims_columns(ims_df)
    >>> if not is_valid:
    ...     print(f"Missing columns: {missing}")
  """

  required_cols = [
    'WrittenDate',
    'SoldPrice',
    'ListPrice',
    'Latitude',
    'Longitude',
    'DaysOnMarket',
    'MLSNumber',
    'MLSID',
    'City',
    'Community',
    'Province',
    'Bedrooms',
    'Baths',
    # Property type fields needed for derivation
    'Type',
    'SubType',
    'Style',
  ]

  missing = [col for col in required_cols if col not in ims_df.columns]

  return len(missing) == 0, missing


def rename_ims_to_canonical(ims_df: pd.DataFrame,
                            keep_bonus_cols: bool = False) -> pd.DataFrame:
  """
  Rename IMS dataset columns to canonical sold listing column names.

  This function handles the column name transformation from IMS format to
  the standardized format used in sold_listing_df. It also fixes dtype
  mismatches to match legacy expectations.

  Args:
    ims_df: DataFrame with IMS column names (modified in-place for memory efficiency)
    keep_bonus_cols: If True, keep additional IMS columns not in canonical format.
                    If False, drop them after creating canonical columns (recommended for large datasets).

  Returns:
    DataFrame with canonical column names and correct dtypes

  Canonical columns expected:
    lng, provState, city, transitions, daysOnMarket, bedsInt, baths, mls,
    neighbourhood, price, soldPrice, listingStatus, guid, lastTransition,
    beds, lat, bathsInt, _id, lastUpdate,
    listingType, searchCategoryType, propertyType

  Notes:
    - _id is constructed as {province}-{mlsid}-{mlsnumber}-{year} (lowercase, 4 parts)
    - guid field is populated during bulk import via MultiPassGUIDResolver
    - listingStatus hardcoded as "SOLD" for all IMS records
    - transitions and lastUpdate set as None (not used downstream)
    - beds/baths are string versions of bedsInt/bathsInt (legacy compatibility)
    - daysOnMarket converted to float64 (allows averaging and NaN values)
    - listingType set as None (legacy field, not in IMS)
    - searchCategoryType set as None (NOT used in IMS ETL logic)
    - propertyType should already exist in input (populated during bulk import via ims_mapper)
    - For memory efficiency with large datasets, this function modifies the input DataFrame
      and returns a view with only canonical columns when keep_bonus_cols=False

  Example:
    >>> ims_df = pd.DataFrame({
    ...     'WrittenDate': ['2024-01-15'],
    ...     'SoldPrice': [500000],
    ...     'ListPrice': [495000],
    ...     'MLSNumber': ['12345'],
    ...     'MLSID': [1],
    ...     'Province': ['ON']
    ... })
    >>> canonical_df = rename_ims_to_canonical(ims_df, keep_bonus_cols=False)
    >>> print(canonical_df['_id'].iloc[0])  # 'on-1-12345-2024'
  """

  # Step 0: Preserve ES _id if already present; otherwise construct from IMS fields
  if '_id' not in ims_df.columns:
    if all(col in ims_df.columns for col in ['Province', 'MLSID', 'MLSNumber', 'WrittenDate']):
      # Extract year from WrittenDate (YYYY-MM-DD format)
      written_date_str = ims_df['WrittenDate'].astype(str)
      year = written_date_str.str[:4]  # Extract YYYY

      ims_df['_id'] = (
        ims_df['Province'].str.lower() + '-' +
        ims_df['MLSID'].astype(str).str.lower() + '-' +
        ims_df['MLSNumber'].astype(str).str.lower() + '-' +
        year.fillna('unknown')
      )
    else:
      ims_df['_id'] = None

  # Step 1: Direct column renames (IMS name → canonical name)
  # Using inplace=True to modify the DataFrame directly without creating a copy
  rename_mapping = {
    'WrittenDate': 'lastTransition',
    'SoldPrice': 'soldPrice',
    'ListPrice': 'price',
    'Latitude': 'lat',
    'Longitude': 'lng',
    'DaysOnMarket': 'daysOnMarket',
    'MLSNumber': 'mls',
    'City': 'city',
    'Community': 'neighbourhood',
    'Province': 'provState',
    'Bedrooms': 'bedsInt',  # IMS Bedrooms are numeric → bedsInt
    'Baths': 'bathsInt',    # IMS Baths are numeric → bathsInt
  }

  ims_df.rename(columns=rename_mapping, inplace=True)

  # Step 2: Create beds and baths as string versions of bedsInt/bathsInt
  # These match legacy format (object dtype) but IMS doesn't have "1+1" format
  # Not used in ETL logic, but kept for schema consistency with legacy
  if 'bedsInt' in ims_df.columns:
    ims_df['beds'] = ims_df['bedsInt'].astype(str)
  if 'bathsInt' in ims_df.columns:
    ims_df['baths'] = ims_df['bathsInt'].astype(str)

  # Step 2.5: Fix dtype mismatches
  # daysOnMarket must be float64 (not int64) to allow averaging and NaN values
  if 'daysOnMarket' in ims_df.columns:
    ims_df['daysOnMarket'] = ims_df['daysOnMarket'].astype(float)

  # Step 3: Add missing fields with default values

  # listingStatus - all IMS records are sold
  ims_df['listingStatus'] = 'SOLD'

  # guid - preserve from ES (populated during bulk import via MultiPassGUIDResolver)
  # Only set to None if missing from input
  if 'guid' not in ims_df.columns:
    ims_df['guid'] = None

  # transitions - not used downstream, set as None
  if 'transitions' not in ims_df.columns:
    ims_df['transitions'] = None

  # lastUpdate - not critical, set as None
  if 'lastUpdate' not in ims_df.columns:
    ims_df['lastUpdate'] = None

  # Step 5: Property type fields
  # propertyType should already exist in ims_df (populated during bulk import via ims_mapper)
  # If not present, it will be None (but this indicates missing preprocessing)
  ims_df['listingType'] = None  # Legacy field, not directly in IMS
  ims_df['searchCategoryType'] = None  # Legacy field, NOT USED in IMS ETL logic

  # Note: searchCategoryType is only used in legacy derive_property_type() function.
  # For IMS, propertyType is already populated in ES index during bulk import.
  # searchCategoryType can remain None and is NOT referenced in any IMS ETL logic.

  # Step 6: Drop bonus/extra IMS columns to save memory
  # This is the ONLY step that creates a new DataFrame - selecting columns returns a view/copy
  if not keep_bonus_cols:
    # Define canonical columns we want to keep
    canonical_cols = [
      '_id', 'mls', 'lastTransition', 'soldPrice', 'price', 'daysOnMarket',
      'lat', 'lng', 'city', 'neighbourhood', 'provState',
      'beds', 'bedsInt', 'baths', 'bathsInt',
      'listingStatus', 'guid', 'computed_guid', 'transitions', 'lastUpdate',
      'listingType', 'searchCategoryType', 'propertyType'
    ]

    # Drop non-canonical columns in-place to avoid returning a view/slice
    cols_to_drop = [col for col in ims_df.columns if col not in canonical_cols]
    if cols_to_drop:
      ims_df.drop(columns=cols_to_drop, inplace=True)
    return ims_df

  return ims_df
