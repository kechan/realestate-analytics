from pathlib import Path
from tqdm.auto import tqdm
import pandas as pd
import logging
from datetime import datetime


class IMSPropertyTypeMapper:
  def __init__(self, cached_mapping_path: Path = None):
    self.logger = logging.getLogger(self.__class__.__name__)
    self.cached_mapping_path = cached_mapping_path  # Store for later use in save_mapping()

    # Load cache if provided
    if cached_mapping_path and cached_mapping_path.exists():
      self.cached_mapping_df = pd.read_feather(cached_mapping_path)
      self.logger.info(f"Loaded {len(self.cached_mapping_df)} cached mappings from {cached_mapping_path.name}")
    else:
      self.cached_mapping_df = None


    # Our target categories - can be expanded beyond the original 4
    self.primary_categories = ['CONDO', 'SEMI-DETACHED', 'TOWNHOUSE', 'DETACHED']
    self.additional_categories = ['MOBILE_MINI', 'LAND_VACANT', 'COMMERCIAL', 'OTHER']
    self.all_categories = self.primary_categories + self.additional_categories
    
    # Initialize mapping rules - we'll build these incrementally
    self.type_rules = {}
    self.subtype_rules = {}
    self.style_rules = {}
    self.combination_lookup = {}
    
    # Keywords that indicate specific property types
    # Note: Provincial exceptions (like AB duplex) are handled in es.py, not here
    self.keyword_patterns = {
      'CONDO': [
        'condo', 'apartment', 'unit', 'co-op', 'cooperative', 'comm element',
        'bachelor', 'studio', 'multi-level',
        # '5-8 units', '9-12 units', 'over 12 units',
        'leasehold', 'stacked townhse', 'apt'
      ],
      'SEMI-DETACHED': [
        'semi', 'half duplex', '1/2 duplex', 'semi detached', 'semi-detached',
        'over-under', 'side by side'
        # Note: 'duplex' goes to OTHER (AB exception handled in es.py)
      ],
      'TOWNHOUSE': [
        'townhouse', 'row', 'row/townhouse', 'town house', 'condo townhouse',
        'link'  # Link homes are typically townhouse style
      ],
      'DETACHED': [
        'detached', 'single family', 'house', 'bungalow', 'bungaloft',
        'cottage', '2-storey', 'storey', 
        'rancher', 'backsplit', 'sidesplit'
      ],
      'MOBILE_MINI': [
        'mobile', 'mini', 'mobile / mini', 
        # 'manufactured'
      ],
      'LAND_VACANT': [
        'land only', 'vacant land'
      ],
      'COMMERCIAL': [
        'store', 'office', 'commercial', 'store w/apt/office',
        '5-8 units', '9-12 units', 'over 12 units'    # moved here from CONDO Per Jon's email july 25th 2025
      ],
      'OTHER': [
        'locker', 'parking space',  # Storage/parking
        'triplex', 'fourplex', '4 plex', 'multiplex',  # Most plexes go to OTHER
        'duplex', 'full duplex',  # Duplex goes to OTHER (except AB, handled in es.py)
        'modular home', 'manufactured', 'modular',
      ]
    }

    # Large multi-unit buildings that should be condos (not plexes)
    self.large_multi_unit_patterns = [
      'multifamily', 'multi-family', 'multi-residential', 'multi-6-9 unit'
    ]

  def load_dataframe(self, df):
    """Load a pandas DataFrame with IMS data"""
    required_cols = ['Type', 'SubType', 'Style']
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
      raise ValueError(f"Missing required columns: {missing_cols}")

    self.df = df
    self.logger.info(f"Loaded {len(self.df)} records from DataFrame")
    self.logger.info(f"Available columns: {list(self.df.columns)}")

  def get_unique_combinations(self):
    """Get all unique Type/SubType/Style combinations"""
    combinations = self.df[['Type', 'SubType', 'Style']].drop_duplicates()
    combinations['combination_key'] = combinations.apply(
      lambda x: f"{x['Type']} | {x['SubType']} | {x['Style']}", axis=1
    )
    return combinations.sort_values('combination_key')

  def analyze_combinations_batch(self, start_idx=0, batch_size=20):
    """Analyze a batch of combinations for manual review"""
    combinations = self.get_unique_combinations()
    total = len(combinations)

    print(f"\n=== BATCH {start_idx//batch_size + 1}: Combinations {start_idx+1} to {min(start_idx+batch_size, total)} of {total} ===\n")

    batch = combinations.iloc[start_idx:start_idx+batch_size]

    for idx, row in batch.iterrows():
      type_val = row['Type'] if pd.notna(row['Type']) else 'N/A'
      subtype_val = row['SubType'] if pd.notna(row['SubType']) else 'N/A'
      style_val = row['Style'] if pd.notna(row['Style']) else 'N/A'

      # Get count of this combination in the data
      mask = (self.df['Type'] == row['Type']) & (self.df['SubType'] == row['SubType']) & (self.df['Style'] == row['Style'])
      count = len(self.df[mask])

      suggested_mapping = self.suggest_mapping(type_val, subtype_val, style_val)

      print(f"{idx+1:3d}. Type: {type_val:<20} | SubType: {subtype_val:<25} | Style: {style_val:<15} | Count: {count:4d} | Suggested: {suggested_mapping}")

    print(f"\nNext batch: analyze_combinations_batch({start_idx+batch_size}, {batch_size})")
    print(f"Remaining: {max(0, total - start_idx - batch_size)} combinations")

    return batch

  def suggest_mapping(self, type_val, subtype_val, style_val):
    """
    Suggest a mapping based on keyword patterns.

    Note: Provincial business rules (like AB duplex → SEMI-DETACHED) are NOT handled here.
    Those exceptions are applied in es.py during the lookup phase.
    This method returns the base classification for Type/SubType/Style combinations.
    """
    # Clean the values
    type_clean = str(type_val).lower() if pd.notna(type_val) else ""
    subtype_clean = str(subtype_val).lower() if pd.notna(subtype_val) else ""
    style_clean = str(style_val).lower() if pd.notna(style_val) else ""

    combined_text = f"{type_clean} {subtype_clean} {style_clean}"

    # PRIORITY 0: Handle specific Type exact matches (highest priority)
    if type_clean in ['16-unit', '5-8 units', '9-12 units', 'over 12 units']:
      return "COMMERCIAL"
    
    # PRIORITY 0.1: Mobile Home - Strongest Signal (Type OR Style)
    # [NEW] Added style tokens here to catch "Detached/Mobile" cases
    mobile_tokens = ['mobile', 'mobile home', 'manufactured', 'double wide', 'single wide', 'park model', 'mini home']
    if 'mobile home' in type_clean or type_clean == 'mobile':
        return "MOBILE_MINI"
    if any(token in style_clean for token in mobile_tokens):
        return "MOBILE_MINI"

    # PRIORITY 0.5: Handle all plex cases first
    plex_patterns = ['triplex', 'fourplex', '4 plex', 'multiplex',
                     '4plex', '5plex', '6plex', '7plex', '8plex', 
                    'duplex', 'dixplex', 'huitplex', 'cinqplex'
                     ]

    # Check for non-duplex plexes → OTHER (highest priority)
    for plex in plex_patterns:
      if plex in combined_text:
        return "OTHER"

    # Handle duplex → OTHER (base classification, AB exception in es.py)
    if (
      '6plex' in combined_text or '8plex' in combined_text or '12plex' in combined_text or
      'uplex' in combined_text or 
      ' plex' in combined_text or 
      'iplex' in combined_text or 
      '-plex' in combined_text or 
      'doublex' in combined_text or
      'quintplex' in combined_text or 'quadrex' in combined_text or 'fourplex' in combined_text or
      "fiveplex" in combined_text or "sixplex" in combined_text or 
      "sevenplex" in combined_text or "eightplex" in combined_text or 
      "tenplex" in combined_text or 
      "12 Apartments" in combined_text
      ):
      return "OTHER"

    
    
    # PRIORITY 1: Direct Type matches (definitive - skip scoring)

    # Handle multi-family Type patterns → OTHER
    if 'multifamily' in type_clean or 'multi-family' in type_clean or 'multi-residential' in type_clean:
      return "OTHER"
    
    if 'vacant land' in type_clean or 'land only' in type_clean or 'lots/acreage' in type_clean:
      return "LAND_VACANT"
    
    # This catches "Live/Work", "Warehouse Conversion", etc.
    comm_style_tokens = ['live/work', 'warehouse', 'industrial', 'retail', 'office']
    if any(token in style_clean for token in comm_style_tokens) or any(token in subtype_clean for token in comm_style_tokens):
        return "COMMERCIAL"
    
    if 'twnhouse' in type_clean or 'townhouse' in type_clean or 'row house' in type_clean or 'row unit' in type_clean or 'attached' in type_clean:
      return "TOWNHOUSE"
    
    if 'apartment' in type_clean or 'condo' in type_clean or 'co-op' in type_clean or 'cooperative' in type_clean:
      return "CONDO"         
    
    if 'modular' in type_clean or 'parking space' in type_clean or 'locker' in type_clean or 'other' in type_clean:
      return "OTHER"
    
    if 'multi family' in type_clean:
      return "OTHER"
    
    if 'commercial' in type_clean or 'office' in type_clean:
      return "COMMERCIAL"
    

    # PRIORTY 2: Direct Style matches (definitive - skip scoring)
    # if 'semi detached' in style_clean or 'semi-detached' in style_clean or 'semi - detached' in style_clean:
    if 'semi' in style_clean:
      return "SEMI-DETACHED"
    if 'attached' in style_clean or 'townhouse' in style_clean or 'row house' in style_clean or 'row unit' in style_clean:
      return "TOWNHOUSE"
    if 'stacked' in style_clean:
      return "TOWNHOUSE"
    if 'vacant' in style_clean or 'land' in style_clean:
      return "LAND_VACANT"
    if 'apartment' in style_clean or 'condo' in style_clean or 'condominium' in style_clean:
      return "CONDO"
    if 'modular' in style_clean or 'manufactured' in style_clean or 'floating' in style_clean:
      return "OTHER"

    # PRIORITY 3: Direct SubType matches (definitive - skip scoring)
    # Check longer patterns first to avoid substring conflicts
    if 'mobile home' in subtype_clean or 'mobile' in subtype_clean:
      return "MOBILE_MINI"
    # if 'semi detached' in subtype_clean or 'semi-detached' in subtype_clean or 'semi - detached' in subtype_clean:
    if 'semi' in subtype_clean:
      return "SEMI-DETACHED"
    if 'townhouse' in subtype_clean or 'attached' in subtype_clean or 'row house' in subtype_clean or 'row unit' in subtype_clean:
      return "TOWNHOUSE"
    if 'detached' in subtype_clean:
      return "DETACHED"
    if 'condo' in subtype_clean or 'condominium' in subtype_clean or 'apartment' in subtype_clean:
      return "CONDO"
    
    if 'modular' in subtype_clean or 'manufactured' in subtype_clean or 'other' in subtype_clean:
      return "OTHER"

   
    # PRIORITY 4: Regular keyword scoring for all other cases
    # print("using scoring")
    scores = {}
    for category, keywords in self.keyword_patterns.items():
      type_matches = sum(2 for keyword in keywords if keyword.lower() in type_clean)  # Weight=2 for Type
      subtype_matches = sum(1.5 for keyword in keywords if keyword.lower() in subtype_clean)  # Weight=1.5 for SubType
      style_matches = sum(1 for keyword in keywords if keyword.lower() in style_clean)  # Weight=1 for Style

      total_score = type_matches + subtype_matches + style_matches
      if total_score > 0:
        scores[category] = total_score

    # Special handling for large multi-unit buildings (should be CONDO)
    large_multi_score = sum(2 for pattern in self.large_multi_unit_patterns
                           if pattern.lower() in combined_text)
    if large_multi_score > 0:
      scores['CONDO'] = scores.get('CONDO', 0) + large_multi_score

    # Guard: Residential listings should not be classified as COMMERCIAL
    if type_clean == "residential":
      scores.pop("COMMERCIAL", None)

    # Special handling for ambiguous cases
    # "End unit" without clear ownership type indicator should be unclear
    if 'end unit' in combined_text and subtype_clean in ['other', 'n/r'] and not any(keyword in combined_text for keyword in ['apartment', 'condo', 'townhouse', 'row']):
      # return "UNCLEAR"
      # print("calling _suggest_mapping_refined")
      return self._suggest_mapping_refined(type_val, subtype_val, style_val)

    if not scores:
      # return "UNCLEAR"
      # print("Calling _suggest_mapping_refined")
      return self._suggest_mapping_refined(type_val, subtype_val, style_val)
    
    return max(scores.keys(), key=lambda k: scores[k])   # Return the category with highest score

  def create_mapping_rule(self, type_pattern=None, subtype_pattern=None, 
                         style_pattern=None, target_category=None):
    """Create a mapping rule"""
    rule = {
      'type_pattern': type_pattern,
      'subtype_pattern': subtype_pattern, 
      'style_pattern': style_pattern,
      'target_category': target_category
    }
    return rule

  def apply_mapping_rules(self, df_subset=None):
    """Apply all mapping rules to classify property types"""
    if df_subset is None:
      df_subset = self.df.copy()
    
    df_subset['predicted_category'] = 'UNCLASSIFIED'
    
    # Apply rules in order of specificity
    # TODO: Implement rule application logic once we define the rules
    
  def get_statistics(self):
    """Get current statistics of the data"""
    self.logger.info("=== Current IMS Data Statistics ===\n")

    self.logger.info("Type distribution:")
    self.logger.info(f"\n{self.df['Type'].value_counts().head(10)}")
    self.logger.info(f"Total Type values: {self.df['Type'].nunique()}\n")

    self.logger.info("SubType distribution:")
    self.logger.info(f"\n{self.df['SubType'].value_counts().head(10)}")
    self.logger.info(f"Total SubType values: {self.df['SubType'].nunique()}\n")

    self.logger.info("Style distribution:")
    self.logger.info(f"\n{self.df['Style'].value_counts().head(10)}")
    self.logger.info(f"Total Style values: {self.df['Style'].nunique()}\n")

    self.logger.info(f"Total unique Type/SubType/Style combinations: {len(self.get_unique_combinations())}")

  
  def generate_full_mapping_dataframe(self):
    """
    Generate a complete DataFrame with all combinations and their mappings.

    Uses optimized cache-first strategy with vectorized pandas operations:
    1. Get unique combinations with counts (vectorized groupby)
    2. Use merge to identify cached vs. new combinations
    3. Only loop through NEW combinations to call suggest_mapping()
    4. Preserve ALL cached combinations (even if not in current data)
    5. Build final result from list of dicts (memory efficient)

    Returns:
      DataFrame with columns: Type, SubType, Style, Count, Inferred
      Sorted by Count (descending)

    Note: Province is NOT included in the output. Provincial business rules
    (like AB duplex) are handled separately in es.py during lookup.

    Cache behavior: The cache NEVER shrinks - all previously cached combinations
    are preserved even if they don't appear in the current dataset.
    """
    # Get counts for each combination using vectorized groupby
    combinations_with_counts = self.df.groupby(['Type', 'SubType', 'Style'], dropna=False).size().reset_index(name='Count')

    # Handle NaN/None values consistently (convert to 'N/A' for matching)
    combinations_with_counts['Type'] = combinations_with_counts['Type'].fillna('N/A')
    combinations_with_counts['SubType'] = combinations_with_counts['SubType'].fillna('N/A')
    combinations_with_counts['Style'] = combinations_with_counts['Style'].fillna('N/A')

    # Start with ALL cached combinations (preserve everything from cache)
    if self.cached_mapping_df is not None and not self.cached_mapping_df.empty:
      # Merge current data with cache to update counts for existing combinations
      merged = combinations_with_counts.merge(
        self.cached_mapping_df[['Type', 'SubType', 'Style', 'Inferred']],
        on=['Type', 'SubType', 'Style'],
        how='outer',  # OUTER join to preserve cache + add new
        indicator=True
      )

      # Update Count for combinations that exist in current data
      # For cache-only combinations (not in current data), set Count to 0
      merged['Count'] = merged['Count'].fillna(0).astype(int)
    else:
      # No cache - mark all as new
      merged = combinations_with_counts
      merged['Inferred'] = None
      merged['_merge'] = 'left_only'

    # Split into cached and new using boolean masks (no copies)
    is_cached = merged['_merge'].isin(['both', 'right_only'])  # right_only = cache-only combinations
    is_new = merged['_merge'] == 'left_only'

    cached_count = is_cached.sum()
    new_count = is_new.sum()

    # Build mappings list from cache hits (vectorized - no loop)
    if cached_count > 0:
      cached_rows = merged[is_cached]
      mappings = cached_rows[['Type', 'SubType', 'Style', 'Count', 'Inferred']].to_dict('records')
    else:
      mappings = []

    # Process only NEW combinations (loop only when necessary)
    if new_count > 0:
      new_rows = merged[is_new]
      for _, row in tqdm(new_rows.iterrows(), total=new_count, desc="Processing new combinations"):
        suggested_mapping = self.suggest_mapping(row['Type'], row['SubType'], row['Style'])

        mappings.append({
          'Type': row['Type'],
          'SubType': row['SubType'],
          'Style': row['Style'],
          'Count': row['Count'],
          'Inferred': suggested_mapping
        })

    # Report cache performance
    total_in_current_data = len(combinations_with_counts)
    cached_in_current = (merged['_merge'] == 'both').sum()
    cache_only = (merged['_merge'] == 'right_only').sum()

    self.logger.info(f"Cache hits (in current data): {cached_in_current:,} ({cached_in_current/total_in_current_data*100:.1f}%)")
    self.logger.info(f"New combinations: {new_count:,} ({new_count/total_in_current_data*100:.1f}%)")
    if cache_only > 0:
      self.logger.info(f"Preserved from cache (not in current data): {cache_only:,}")

    # Create final DataFrame from list and update cache
    new_mappings_df = pd.DataFrame(mappings).sort_values(['Count'], ascending=False).reset_index(drop=True)

    # Replace old cache with updated one (memory efficient - just reassign reference)
    del self.cached_mapping_df  # Explicitly delete old reference
    self.cached_mapping_df = new_mappings_df

    # Log UNCLEAR mappings to CSV for manual review
    self._log_unclear_mappings()

    return self.cached_mapping_df

  def _log_unclear_mappings(self):
    """
    Log UNCLEAR property type mappings to a CSV file for manual review.

    This method is called automatically at the end of generate_full_mapping_dataframe().
    It extracts all UNCLEAR mappings from the cache and logs them to unclear_mappings.csv
    in the cache directory. The CSV file maintains unique combinations (no duplicates).

    For each UNCLEAR combination, it attempts to extract City and Province from a sample
    record in self.df to help prioritize manual review.

    CSV Structure:
    - Columns: Type, SubType, Style, Count, City, Province
    - Uniqueness: (Type, SubType, Style) combinations are unique
    - Location: {cache_dir}/unclear_mappings.csv

    Note: If City or Province columns don't exist in self.df, 'N/A' is used as placeholder.
    """
    # Check if we have cached mappings to log
    if self.cached_mapping_df is None or self.cached_mapping_df.empty:
      return

    # Extract UNCLEAR mappings
    unclear_mask = self.cached_mapping_df['Inferred'] == 'UNCLEAR'
    unclear_mappings = self.cached_mapping_df[unclear_mask].copy()

    if unclear_mappings.empty:
      self.logger.info("No UNCLEAR mappings to log")
      return

    self.logger.info(f"Found {len(unclear_mappings)} UNCLEAR mappings to log")

    # Add City and Province columns by looking up sample records from self.df
    if hasattr(self, 'df') and self.df is not None and not self.df.empty:
      unclear_mappings['City'] = 'N/A'
      unclear_mappings['Province'] = 'N/A'

      for idx, row in unclear_mappings.iterrows():
        # Find a sample record with this combination
        mask = (
          (self.df['Type'].fillna('N/A') == row['Type']) &
          (self.df['SubType'].fillna('N/A') == row['SubType']) &
          (self.df['Style'].fillna('N/A') == row['Style'])
        )

        matching_records = self.df[mask]
        if not matching_records.empty:
          sample_record = matching_records.iloc[0]

          # Extract City if available
          if 'City' in sample_record.index and pd.notna(sample_record['City']):
            unclear_mappings.at[idx, 'City'] = sample_record['City']

          # Extract Province if available
          if 'Province' in sample_record.index and pd.notna(sample_record['Province']):
            unclear_mappings.at[idx, 'Province'] = sample_record['Province']
    else:
      # No data available - use N/A placeholders
      unclear_mappings['City'] = 'N/A'
      unclear_mappings['Province'] = 'N/A'

    # Determine CSV file path
    if self.cached_mapping_path is None:
      self.logger.warning("No cache path set - cannot log UNCLEAR mappings to CSV")
      return

    csv_path = self.cached_mapping_path.parent / 'unclear_mappings.csv'

    # Load existing CSV if it exists and merge with new UNCLEAR items
    if csv_path.exists():
      try:
        existing_unclear = pd.read_csv(csv_path)
        self.logger.info(f"Loaded {len(existing_unclear)} existing UNCLEAR mappings from {csv_path.name}")

        # Merge and deduplicate based on (Type, SubType, Style)
        # Keep the row with higher Count (prefer new data)
        combined = pd.concat([existing_unclear, unclear_mappings[['Type', 'SubType', 'Style', 'Count', 'City', 'Province']]], ignore_index=True)

        # Sort by Count descending and drop duplicates, keeping first (highest count)
        combined = combined.sort_values('Count', ascending=False)
        combined = combined.drop_duplicates(subset=['Type', 'SubType', 'Style'], keep='first')

        # Sort by Count descending for final output
        combined = combined.sort_values('Count', ascending=False).reset_index(drop=True)

        self.logger.info(f"After deduplication: {len(combined)} unique UNCLEAR mappings")

      except Exception as e:
        self.logger.error(f"Error loading existing unclear_mappings.csv: {e}. Creating new file.")
        combined = unclear_mappings[['Type', 'SubType', 'Style', 'Count', 'City', 'Province']].sort_values('Count', ascending=False)
    else:
      # No existing file - use new data
      combined = unclear_mappings[['Type', 'SubType', 'Style', 'Count', 'City', 'Province']].sort_values('Count', ascending=False)
      self.logger.info(f"Creating new unclear_mappings.csv with {len(combined)} entries")

    # Save to CSV
    try:
      combined.to_csv(csv_path, index=False)
      self.logger.info(f"Logged {len(combined)} UNCLEAR mappings to {csv_path}")
    except Exception as e:
      self.logger.error(f"Error saving unclear_mappings.csv: {e}")

  def apply_property_types(self, df: pd.DataFrame = None):
    """
    Apply property type mappings to a DataFrame in-place.

    Uses vectorized pandas operations for memory efficiency.
    Adds a new 'propertyType' column to the DataFrame with the inferred property types.

    Parameters:
    - df: Optional DataFrame to apply mappings to. If None, uses self.df (the loaded data)

    Raises:
    - ValueError: If no mappings are available or required columns are missing
    """
    # Determine which DataFrame to use
    if df is None:
      if not hasattr(self, 'df') or self.df is None:
        raise ValueError("No DataFrame available. Either pass df parameter or call load_dataframe() first.")
      df = self.df

    # Check that mappings have been generated
    if self.cached_mapping_df is None or self.cached_mapping_df.empty:
      raise ValueError("No mappings available. Call generate_full_mapping_dataframe() first.")

    # Validate required columns exist
    required_cols = ['Type', 'SubType', 'Style']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
      raise ValueError(f"DataFrame missing required columns: {missing_cols}")

    self.logger.info(f"Applying property type mappings to {len(df):,} records...")

    # Create a lookup dictionary from cached mappings (vectorized approach)
    # Use 3-tuple (Type, SubType, Style) -> propertyType
    lookup_dict = self.cached_mapping_df.set_index(['Type', 'SubType', 'Style'])['Inferred'].to_dict()

    # Handle NaN/None values in df by filling with 'N/A' temporarily for lookup
    # We'll do this on-the-fly without modifying the original columns
    # Create a temporary tuple column for vectorized lookup
    df['_temp_key'] = list(zip(
      df['Type'].fillna('N/A'),
      df['SubType'].fillna('N/A'),
      df['Style'].fillna('N/A')
    ))

    # Vectorized lookup using map (memory efficient)
    df['propertyType'] = df['_temp_key'].map(lookup_dict)

    # Drop temporary column
    df.drop(columns=['_temp_key'], inplace=True)

    # Count how many were successfully mapped
    mapped_count = df['propertyType'].notna().sum()
    unmapped_count = df['propertyType'].isna().sum()
    unclear_count = (df['propertyType'] == 'UNCLEAR').sum()

    self.logger.info(f"Mapping results:")
    self.logger.info(f"  Mapped: {mapped_count:,} ({mapped_count/len(df)*100:.1f}%)")
    self.logger.info(f"  Unmapped (not in cache): {unmapped_count:,} ({unmapped_count/len(df)*100:.1f}%)")
    self.logger.info(f"  UNCLEAR: {unclear_count:,} ({unclear_count/len(df)*100:.1f}%)")

  def refresh_unclear_mappings(self, raise_on_regression: bool = True) -> dict:
    """
    Re-evaluate UNCLEAR mappings using current suggest_mapping() logic.

    This method is used in devops workflow after improving suggest_mapping() code.
    It protects existing correct mappings from regression - if any non-UNCLEAR
    mapping changes, it raises severe warning/error.

    IMPORTANT: This method works directly on the cached tuples (Type, SubType, Style).
    You do NOT need to call load_dataframe() before this - just initialize with
    the cached_mapping_path.

    Workflow:
    1. Developer improves suggest_mapping() logic
    2. Run this method to re-evaluate UNCLEAR items only (no IMS data needed)
    3. Detect regressions (changes to previously correct mappings)
    4. Update cache with improved mappings
    5. Call save_mapping() to persist (will check for regressions)

    Parameters:
    - raise_on_regression: If True, raises RuntimeError on regression (default: True)
                          If False, only logs warning

    Returns:
    - dict with:
        'total_unclear_before': Number of UNCLEAR items before refresh
        'updated_count': Number of UNCLEAR → new value
        'still_unclear': Number still UNCLEAR after refresh
        'regressions': DataFrame of regressed tuples (None if no regressions)
        'has_regressions': Boolean flag for easy checking
        'updated_mappings': DataFrame showing UNCLEAR → new value changes (None if no updates)
                           Columns: Type, SubType, Style, Old_Value, New_Value

    Raises:
    - RuntimeError: If regression detected and raise_on_regression=True
    - ValueError: If cached_mapping_df not available

    Usage (in Jupyter notebook):
        mapper = IMSPropertyTypeMapper(cached_mapping_path=cache_path)
        result = mapper.refresh_unclear_mappings()

        # Review what changed
        if result['updated_count'] > 0:
            print(f"\n{result['updated_count']} UNCLEAR items resolved:")
            display(result['updated_mappings'])

        # If satisfied with changes and no regressions, save
        if not result['has_regressions']:
            mapper.save_mapping()
    """
    # Validate cache exists
    if self.cached_mapping_df is None or self.cached_mapping_df.empty:
      raise ValueError("No cached mappings available. Load or generate mappings first.")

    self.logger.info("Starting UNCLEAR mappings refresh...")
    self.logger.info(f"Total mappings in cache: {len(self.cached_mapping_df)}")

    # Step 1: Identify UNCLEAR and non-UNCLEAR rows (boolean indexing - no copy)
    unclear_mask = self.cached_mapping_df['Inferred'] == 'UNCLEAR'
    total_unclear = unclear_mask.sum()

    self.logger.info(f"UNCLEAR mappings to re-evaluate: {total_unclear}")
    self.logger.info(f"Non-UNCLEAR mappings (protected): {(~unclear_mask).sum()}")

    # Step 2: REGRESSION CHECK FIRST - Re-run on non-UNCLEAR tuples (cache still untouched)
    self.logger.info("Running regression check on non-UNCLEAR mappings...")
    non_unclear_tuples = self.cached_mapping_df[~unclear_mask][['Type', 'SubType', 'Style']].values
    old_values = self.cached_mapping_df[~unclear_mask]['Inferred'].values

    new_values = [
      self.suggest_mapping(row[0], row[1], row[2])
      for row in tqdm(non_unclear_tuples, desc="Checking regressions")
    ]

    # Detect regressions (vectorized comparison)
    regressions_mask = old_values != new_values

    # Step 3: Handle regressions - BAIL IMMEDIATELY if detected
    regressions_df = None
    if regressions_mask.any():
      # Create DataFrame of violating tuples
      regressions_df = pd.DataFrame({
        'Type': non_unclear_tuples[regressions_mask, 0],
        'SubType': non_unclear_tuples[regressions_mask, 1],
        'Style': non_unclear_tuples[regressions_mask, 2],
        'Old_Value': old_values[regressions_mask],
        'New_Value': [new_values[i] for i in range(len(new_values)) if regressions_mask[i]]
      })

      # Severe warning via logger (works in Jupyter if logger configured)
      self.logger.error("=" * 80)
      self.logger.error(f"REGRESSION DETECTED: {len(regressions_df)} mappings changed!")
      self.logger.error("⚠️  SEVERE WARNING: Code changes caused REGRESSIONS! ⚠️")
      self.logger.error("=" * 80)
      self.logger.error(f"\n{len(regressions_df)} previously correct mappings have changed:\n")
      self.logger.error(f"\n{regressions_df.to_string()}\n")
      self.logger.error("=" * 80)
      self.logger.error("ACTION REQUIRED:")
      self.logger.error("  1. Review the changes above")
      self.logger.error("  2. If unintended: git restore suggest_mapping() and redo changes")
      self.logger.error("  3. If intended: Update these mappings manually in cache")
      self.logger.error("=" * 80)
      self.logger.error("⚠️  CACHE NOT MODIFIED - bailing out before any changes")

      # Return immediately with regression info - DO NOT touch cache
      result = {
        'total_unclear_before': int(total_unclear),
        'updated_count': 0,
        'still_unclear': int(total_unclear),
        'regressions': regressions_df,
        'has_regressions': True
      }

      if raise_on_regression:
        raise RuntimeError(f"Regression detected: {len(regressions_df)} mappings changed. Cache not modified.")

      return result

    else:
      self.logger.info("✅ No regressions detected - all non-UNCLEAR mappings unchanged")

    # Step 4: Extract UNCLEAR tuples and re-evaluate (only if no regressions)
    self.logger.info("Re-running suggest_mapping() on UNCLEAR tuples...")
    unclear_tuples = self.cached_mapping_df[unclear_mask][['Type', 'SubType', 'Style']].values

    new_mappings = [
      self.suggest_mapping(row[0], row[1], row[2])
      for row in tqdm(unclear_tuples, desc="Re-evaluating UNCLEAR")
    ]

    # Step 5: Update cache in-place (only UNCLEAR rows, only after regression check passed)
    self.logger.info("Updating cache with new mappings...")
    self.cached_mapping_df.loc[unclear_mask, 'Inferred'] = new_mappings

    # Step 6: Create detailed change report for developer review
    # Identify which UNCLEAR items got resolved (UNCLEAR → something else)
    resolved_mask = unclear_mask & (self.cached_mapping_df['Inferred'] != 'UNCLEAR')

    updated_mappings_df = None
    if resolved_mask.any():
      resolved_tuples = self.cached_mapping_df[resolved_mask][['Type', 'SubType', 'Style']].values
      resolved_new_values = self.cached_mapping_df[resolved_mask]['Inferred'].values

      updated_mappings_df = pd.DataFrame({
        'Type': resolved_tuples[:, 0],
        'SubType': resolved_tuples[:, 1],
        'Style': resolved_tuples[:, 2],
        'Old_Value': 'UNCLEAR',
        'New_Value': resolved_new_values
      })

      self.logger.info(f"\nResolved UNCLEAR mappings ({len(updated_mappings_df)}):")
      self.logger.info(f"\n{updated_mappings_df.to_string()}\n")

    # Calculate statistics
    updated_count = resolved_mask.sum()
    still_unclear = (unclear_mask & (self.cached_mapping_df['Inferred'] == 'UNCLEAR')).sum()

    # Step 7: Return comprehensive results for developer review
    result = {
      'total_unclear_before': int(total_unclear),
      'updated_count': int(updated_count),
      'still_unclear': int(still_unclear),
      'regressions': regressions_df,
      'has_regressions': False,
      'updated_mappings': updated_mappings_df
    }

    self.logger.info(f"Refresh complete:")
    self.logger.info(f"  Total UNCLEAR before: {result['total_unclear_before']}")
    self.logger.info(f"  Updated (UNCLEAR → new): {result['updated_count']}")
    self.logger.info(f"  Still UNCLEAR: {result['still_unclear']}")

    return result

  def apply_corrected_mappings(self, corrected_mappings: dict, raise_on_regression: bool = True) -> dict:
    """
    Apply bug fixes to cached mappings after improving suggest_mapping() logic.

    This method is used when you discover that suggest_mapping() had a bug that caused
    incorrect classifications in the cache (e.g., Mobile Home → DETACHED should be MOBILE_MINI).
    After fixing the bug in suggest_mapping(), use this method to systematically update
    the affected cache entries.

    IMPORTANT: This method works directly on the cached tuples (Type, SubType, Style).
    You do NOT need to call load_dataframe() before this - just initialize with
    the cached_mapping_path.

    Workflow:
    1. Fix bug in suggest_mapping() code
    2. Identify which cache entries need correction
    3. Run this method with corrected mappings (validates changes are safe)
    4. Call save_mapping() to persist (in separate cell after manual review)

    Parameters:
    - corrected_mappings: Dict mapping (Type, SubType, Style) -> expected_propertyType
                         Example: {('Mobile Home', 'Detached', 'N/R'): 'MOBILE_MINI'}
    - raise_on_regression: If True, raises RuntimeError on regression (default: True)

    Returns:
    - dict with:
        'total_corrected': Number of mappings to be corrected
        'validation_passed': Boolean (did suggest_mapping produce expected values?)
        'validation_failures': DataFrame showing mismatches (None if all passed)
                              Columns: Type, SubType, Style, Expected, Actual
        'regressions': DataFrame of regressed tuples (None if no regressions)
        'has_regressions': Boolean flag
        'corrected_mappings_df': DataFrame showing old → new values (None if validation failed)
                                Columns: Type, SubType, Style, Old_Value, New_Value

    Raises:
    - RuntimeError: If regression detected and raise_on_regression=True
    - ValueError: If cached_mapping_df not available or corrected_mappings empty

    Usage (in Jupyter notebook):
        # After fixing bug in suggest_mapping()
        corrected = {
            ('Mobile Home', 'Detached', 'N/R'): 'MOBILE_MINI',
            ('Mobile Home', 'Semi-Detached', 'N/R'): 'MOBILE_MINI',
        }

        mapper = IMSPropertyTypeMapper(cached_mapping_path=cache_path)
        result = mapper.apply_corrected_mappings(corrected, raise_on_regression=True)

        # Review validation results
        if not result['validation_passed']:
            print("⚠️ Validation failed!")
            display(result['validation_failures'])
        elif result['total_corrected'] > 0:
            print(f"\n{result['total_corrected']} mappings corrected:")
            display(result['corrected_mappings_df'])

        # In separate cell: save if satisfied
        if result['validation_passed'] and not result['has_regressions']:
            mapper.save_mapping()
    """
    # Validate inputs
    if self.cached_mapping_df is None or self.cached_mapping_df.empty:
      raise ValueError("No cached mappings available. Load or generate mappings first.")

    if not corrected_mappings:
      raise ValueError("corrected_mappings cannot be empty")

    self.logger.info("Starting apply_corrected_mappings...")
    self.logger.info(f"Total mappings in cache: {len(self.cached_mapping_df)}")
    self.logger.info(f"Mappings to correct: {len(corrected_mappings)}")

    # Step 1: Create mask for rows to be corrected
    corrected_tuples_set = set(corrected_mappings.keys())

    def is_corrected_tuple(row):
      return (row['Type'], row['SubType'], row['Style']) in corrected_tuples_set

    corrected_mask = self.cached_mapping_df.apply(is_corrected_tuple, axis=1)

    self.logger.info(f"Corrected mappings found in cache: {corrected_mask.sum()}")
    self.logger.info(f"Protected mappings (non-corrected): {(~corrected_mask).sum()}")

    # Step 2: REGRESSION CHECK FIRST - Re-run on non-corrected tuples (cache still untouched)
    self.logger.info("Running regression check on non-corrected mappings...")
    non_corrected_tuples = self.cached_mapping_df[~corrected_mask][['Type', 'SubType', 'Style']].values
    old_values = self.cached_mapping_df[~corrected_mask]['Inferred'].values

    new_values = [
      self.suggest_mapping(row[0], row[1], row[2])
      for row in tqdm(non_corrected_tuples, desc="Checking regressions")
    ]

    # Detect regressions (vectorized comparison)
    regressions_mask = old_values != new_values

    # Step 3: Handle regressions - BAIL IMMEDIATELY if detected
    regressions_df = None
    if regressions_mask.any():
      # Create DataFrame of violating tuples
      regressions_df = pd.DataFrame({
        'Type': non_corrected_tuples[regressions_mask, 0],
        'SubType': non_corrected_tuples[regressions_mask, 1],
        'Style': non_corrected_tuples[regressions_mask, 2],
        'Old_Value': old_values[regressions_mask],
        'New_Value': [new_values[i] for i in range(len(new_values)) if regressions_mask[i]]
      })

      # Severe warning via logger
      self.logger.error("=" * 80)
      self.logger.error(f"REGRESSION DETECTED: {len(regressions_df)} mappings changed!")
      self.logger.error("⚠️  SEVERE WARNING: Code changes caused REGRESSIONS! ⚠️")
      self.logger.error("=" * 80)
      self.logger.error(f"\n{len(regressions_df)} non-corrected mappings have changed:\n")
      self.logger.error(f"\n{regressions_df.to_string()}\n")
      self.logger.error("=" * 80)
      self.logger.error("ACTION REQUIRED:")
      self.logger.error("  1. Review the changes above")
      self.logger.error("  2. If unintended: git restore suggest_mapping() and redo changes")
      self.logger.error("  3. If intended: Add these to corrected_mappings")
      self.logger.error("=" * 80)
      self.logger.error("⚠️  CACHE NOT MODIFIED - bailing out before any changes")

      # Return immediately with regression info - DO NOT touch cache
      result = {
        'total_corrected': len(corrected_mappings),
        'validation_passed': False,
        'validation_failures': None,
        'regressions': regressions_df,
        'has_regressions': True,
        'corrected_mappings_df': None
      }

      if raise_on_regression:
        raise RuntimeError(f"Regression detected: {len(regressions_df)} mappings changed. Cache not modified.")

      return result

    else:
      self.logger.info("✅ No regressions detected - all non-corrected mappings unchanged")

    # Step 4: Validate that suggest_mapping() produces expected values for corrected tuples
    self.logger.info("Validating corrected mappings...")
    validation_failures = []

    for (type_val, subtype_val, style_val), expected_value in tqdm(
      corrected_mappings.items(), desc="Validating corrections"
    ):
      actual_value = self.suggest_mapping(type_val, subtype_val, style_val)
      if actual_value != expected_value:
        validation_failures.append({
          'Type': type_val,
          'SubType': subtype_val,
          'Style': style_val,
          'Expected': expected_value,
          'Actual': actual_value
        })

    validation_failures_df = None
    if validation_failures:
      validation_failures_df = pd.DataFrame(validation_failures)

      self.logger.error("=" * 80)
      self.logger.error(f"VALIDATION FAILED: {len(validation_failures)} mappings don't match expected!")
      self.logger.error("⚠️  suggest_mapping() did not produce expected values! ⚠️")
      self.logger.error("=" * 80)
      self.logger.error(f"\n{validation_failures_df.to_string()}\n")
      self.logger.error("=" * 80)
      self.logger.error("ACTION REQUIRED:")
      self.logger.error("  1. Review your suggest_mapping() fix - it's not working as intended")
      self.logger.error("  2. Fix the logic and try again")
      self.logger.error("=" * 80)
      self.logger.error("⚠️  CACHE NOT MODIFIED - bailing out before any changes")

      # Return with validation failure - DO NOT touch cache
      result = {
        'total_corrected': len(corrected_mappings),
        'validation_passed': False,
        'validation_failures': validation_failures_df,
        'regressions': regressions_df,
        'has_regressions': False,
        'corrected_mappings_df': None
      }

      return result

    else:
      self.logger.info("✅ Validation passed - all corrections produce expected values")

    # Step 5: Update cache with corrected mappings (only if validation passed and no regressions)
    self.logger.info("Updating cache with corrected mappings...")

    # Collect old values for reporting
    corrected_changes = []
    for (type_val, subtype_val, style_val), new_value in corrected_mappings.items():
      # Find this tuple in cache
      mask = (
        (self.cached_mapping_df['Type'] == type_val) &
        (self.cached_mapping_df['SubType'] == subtype_val) &
        (self.cached_mapping_df['Style'] == style_val)
      )

      if mask.any():
        old_value = self.cached_mapping_df.loc[mask, 'Inferred'].iloc[0]

        # Update cache
        self.cached_mapping_df.loc[mask, 'Inferred'] = new_value

        corrected_changes.append({
          'Type': type_val,
          'SubType': subtype_val,
          'Style': style_val,
          'Old_Value': old_value,
          'New_Value': new_value
        })

    corrected_mappings_df = pd.DataFrame(corrected_changes) if corrected_changes else None

    if corrected_mappings_df is not None:
      self.logger.info(f"\nCorrected mappings ({len(corrected_mappings_df)}):")
      self.logger.info(f"\n{corrected_mappings_df.to_string()}\n")

    # Step 6: Return comprehensive results
    result = {
      'total_corrected': len(corrected_mappings),
      'validation_passed': True,
      'validation_failures': None,
      'regressions': regressions_df,
      'has_regressions': False,
      'corrected_mappings_df': corrected_mappings_df
    }

    self.logger.info(f"Apply corrected mappings complete:")
    self.logger.info(f"  Total corrected: {result['total_corrected']}")
    self.logger.info(f"  Validation: {'PASSED' if result['validation_passed'] else 'FAILED'}")
    self.logger.info(f"  Regressions: {'NONE' if not result['has_regressions'] else 'DETECTED'}")

    return result

  def save_mapping(self, output_path: Path = None):
    """
    Save the property type mappings to a feather file.

    Uses self.cached_mapping_df which is populated by generate_full_mapping_dataframe().
    Backs up the existing cache file (if it exists) by appending .YYYYMMDD to the filename,
    then writes the new mappings to the cache path.

    Parameters:
    - output_path: Optional custom path to save to. If None, uses self.cached_mapping_path

    Returns:
    - Path to the saved mapping file

    Raises:
    - ValueError: If generate_full_mapping_dataframe() hasn't been called yet
    """
    # Check that mappings have been generated
    if self.cached_mapping_df is None or self.cached_mapping_df.empty:
      raise ValueError("No mappings to save. Call generate_full_mapping_dataframe() first.")

    # Determine output path
    if output_path is None:
      if self.cached_mapping_path is None:
        raise ValueError("No output path specified and no cached_mapping_path was set during initialization")
      output_path = self.cached_mapping_path
    else:
      output_path = Path(output_path)

    # Backup existing cache if it exists
    if output_path.exists():
      timestamp = datetime.now().strftime('%Y%m%d')
      backup_path = output_path.parent / f"{output_path.name}.{timestamp}"

      # If backup already exists today, append a counter
      counter = 1
      while backup_path.exists():
        backup_path = output_path.parent / f"{output_path.name}.{timestamp}.{counter}"
        counter += 1

      self.logger.info(f"Backing up existing cache to: {backup_path}")
      output_path.rename(backup_path)

    # Save to feather format
    self.logger.info(f"Saving {len(self.cached_mapping_df)} mappings to: {output_path}")
    self.cached_mapping_df.to_feather(output_path)
    self.logger.info(f"Successfully saved mapping cache")

    return output_path

  def _suggest_mapping_refined(self, Type, SubType, Style):
    # normalize inputs
    t  = "" if Type  is None else str(Type).strip().lower()
    st = "" if SubType is None else str(SubType).strip().lower()
    sy = "" if Style   is None else str(Style).strip().lower()

    text = f"{t} {st} {sy}"

    # -----------------------------------------------------------
    # 0) "See Remarks" is too ambiguous -> UNCLEAR
    # -----------------------------------------------------------
    if "see remarks" in st:
      return "UNCLEAR"

    # -----------------------------------------------------------
    # 1) LAND_VACANT  (clear "land only" cases)
    # -----------------------------------------------------------
    if t == "lots/acreage":
      return "LAND_VACANT"

    if st in {"vacant lot/land", "land"}:
      return "LAND_VACANT"

    # special-case: entire Type "modular home" goes to OTHER here
    if t == "modular home":
      return "OTHER"

    # -----------------------------------------------------------
    # 2) TOWNHOUSE  (attached low-rise residential)
    # -----------------------------------------------------------
    townhouse_types = {"residential", "other", "split-level"}

    # explicit Multi Family triple from your mapping
    if (t, st, sy) == ("multi family", "n/r", "attached, multi level"):
      return "TOWNHOUSE"

    if t in townhouse_types:
      # attached units in subtype or style
      if "attached" in st or "attached" in sy:
        return "TOWNHOUSE"

      # townhome keyword in style
      if "townhome" in sy:
        return "TOWNHOUSE"

      # end-unit townhouse style
      if "end unit" in sy:
        return "TOWNHOUSE"

      # explicit stacked townhouse triples from your mapping
      if (t, st, sy) in {
        ("residential", "one level", "stacked"),
        ("residential", "other", "stacked"),
      }:
        return "TOWNHOUSE"

      # explicit multi/split townhouse triple
      if (t, st, sy) == ("residential", "recreational", "multi/split"):
        return "TOWNHOUSE"

    # -----------------------------------------------------------
    # 3) DETACHED  (freehold / stand-alone houses)
    #     Only when we have positive detached signals.
    # -----------------------------------------------------------
    detached_types = {
      "residential",
      "other",
      "resort property",
      "pre-fab modular",
      "split-level",
      "maison de chambre",
    }

    if t in detached_types:
      # Maison De Chambre: always detached in your mapping
      if t == "maison de chambre":
        return "DETACHED"

      # strong: "Carriage" anywhere in Type or SubType
      if "carriage" in t or "carriage" in st:
        return "DETACHED"

      # recreational residential: usually cottage-style detached,
      # but with a few known non-detached exceptions that we treat as UNCLEAR
      if t == "residential" and st == "recreational":
        if sy in {"park model", "float home", "hunt camp", "bi-level"}:
          return "UNCLEAR"
        return "DETACHED"

      # structural detached subtypes
      if st == "residential" or (t == "residential" and st == "modular home"):
        return "DETACHED"

      # common detached indicators (house-like structure/style)
      common_detached_keywords = [
        # split / ranch family
        "ranch", "raised ranch", "back split", "side split",
        "split level", "split-level", "split entry",

        # bungalow-ish / bi-level
        "bi-level", "blevl", "bng", "bngr", "bung", "bungr",

        # recreational house-like structures
        "a-frame", "a frame", "chalet", "cabin", "camp",
        "log home", "log", "villa",
        "patio home", "acreage", "summer home",

        # style cues from DETACHED mapping
        "west coast", "westcoast", "contemporary",
        "cape cod", "cbovr", "character",

        # entry-style cues often used in detached listings
        "main lev ent", "grd lev ent",
      ]

      if any(kw in text for kw in common_detached_keywords):
        return "DETACHED"

      # additional detached styles that we trust only for residential-ish types
      residential_style_keywords = [
        # split-level shorthands
        "sp2s", "sp3l", "sp4l", "spml",
        "split", "split 3 level", "split 4 level",
        # storey / level indicators
        "two", "two level", "one level", "multi level",
        # fractional storeys
        "1+1/2", "1+3/4", "2+1/2",
        # loft & arts-and-crafts
        "loft",
        "arts and crafts", "arts  and  crafts",
      ]

      if t in {"residential", "resort property", "pre-fab modular", "split-level"}:
        if any(kw in sy for kw in residential_style_keywords):
          return "DETACHED"

    # -----------------------------------------------------------
    # 4) OTHER  (things we explicitly don't care about in ETL)
    #     - multiplex / income properties
    #     - farms / hobby farms
    #     - float homes, park models, hunt camps
    #     - timeshares, tourism/seniors/shared ownership
    #     - mixed commercial-res, parking, B&B
    # -----------------------------------------------------------
    other_keywords = [
      # multi-unit / plex buildings
      "plex", "logements", "logis", "dwellings", "housing",
      "rooms", "appartments", "appts", "commerces",
      "multi family", "multi-family", "multi-plex", "multiplez", "quadrex",

      # farms / rural / agri
      "farm", "farm/ranch", "hobby farm",

      # float / seasonal / mini
      "float home", "floating home",
      # park model handled via rec-exception above (UNCLEAR)

      # mixed commercial / special tenure
      "commercail", "commercial", "mixed use",
      "timeshare", "timeshares", "tourisme residence", "tourism residence",
      "seniors residence", "shared owner",

      # parking / stall
      "prk. stall", "prk stall",

      # small b&b operations
      "b and b", "b  and  b",

      # province-coded / non-core types
      "alberta",
    ]

    if any(kw in t for kw in other_keywords) or \
      any(kw in st for kw in other_keywords) or \
      any(kw in sy for kw in other_keywords):
      return "OTHER"

    # numeric / code-like Types (16, 24, 32, 6, 7, 20x, etc.)
    if any(ch.isdigit() for ch in t):
      if t not in {"residential", "other", "multi family", "n/r", "lots/acreage"}:
        return "OTHER"

    # explicit plex-like type words without 'plex' substring (e.g. "six")
    if t in {"six"}:
      return "OTHER"

    # very generic residential/other junk you bucketed as OTHER
    if t in {"residential", "other"} and st == "n/r" and sy in {"n/r", "other", "0"}:
      return "OTHER"

    # Type = 'other' with weak styles like one/two level -> OTHER
    if t == "other" and st == "n/r" and sy in {"one level", "two level"}:
      return "OTHER"

    # Residential / Other / Other or N/R -> OTHER
    if t == "residential" and st == "other" and sy in {"other", "n/r"}:
      return "OTHER"

    if t == "n/r":
      return "OTHER"

    # -----------------------------------------------------------
    # 5) FALLBACK
    # -----------------------------------------------------------
    return "UNCLEAR"
