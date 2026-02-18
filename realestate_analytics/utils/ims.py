from __future__ import annotations
from typing import List, Dict, Optional, Union
from pathlib import Path
from tqdm.auto import tqdm
import pandas as pd
import logging
import unicodedata
from datetime import datetime
import json

from .ims_geocoder import GeocoderCAClient
from .ims_oda_geocoder import OdaOfflineGeocoder
from .ims_locallogic import LocalLogicAPIClient

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

    # Load manual override lookup table if available
    self.override_lookup_path = None
    self.override_lookup_df = None

    if cached_mapping_path:
      # Derive override table path from main cache path
      self.override_lookup_path = cached_mapping_path.parent / 'ims_property_type_lookup_df'

      if self.override_lookup_path.exists():
        try:
          self.override_lookup_df = pd.read_feather(self.override_lookup_path)

          # Validate columns
          required_cols = ['Type', 'SubType', 'Style', 'propertyType']
          missing_cols = [col for col in required_cols if col not in self.override_lookup_df.columns]

          if missing_cols:
            self.logger.error(f"Override lookup table missing columns: {missing_cols}")
            self.override_lookup_df = None
          else:
            self.logger.info(f"Loaded {len(self.override_lookup_df)} manual overrides from {self.override_lookup_path.name}")
        except Exception as e:
          self.logger.warning(f"Failed to load override lookup table: {e}")
          self.override_lookup_df = None
      else:
        self.logger.debug(f"No override lookup table found at {self.override_lookup_path}")

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
        'over-under', 'side by side',
        'link'  # Link homes - minimal connection, more similar to semi than townhouse (mgmt approved 2026-01-28)
        # Note: 'duplex' goes to OTHER (AB exception handled in es.py)
      ],
      'TOWNHOUSE': [
        'townhouse', 'row', 'row/townhouse', 'town house', 'condo townhouse'
        # 'link' removed 2026-01-28 - now classified as SEMI-DETACHED (minimal connection vs row housing)
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

    # PRIORITY 0.45: Duplex + Semi patterns → SEMI-DETACHED
    # Management approved 2026-01-28: Reclassify duplex with semi indicators as SEMI-DETACHED
    # Exclude: apartment/condo ownership types and multi-family (keep as OTHER/CONDO)
    if 'apartment' not in type_clean and 'condo' not in type_clean and not ('multi' in type_clean and 'family' in type_clean):
      # Pattern 1: Type=Duplex → SEMI-DETACHED
      # Management approved 2026-01-29: Trust Type field above SubType/Style (Type is the higher order bit)
      # All Type=Duplex → SEMI-DETACHED (except apartment/condo/multi-family already excluded above)
      if type_clean == 'duplex':
        return 'SEMI-DETACHED'

      # Pattern 2: Type contains "semi" AND "duplex"
      # Matches: Semi Detached half Duplex / ... / Duplex (2 listings)
      if 'semi' in type_clean and 'duplex' in type_clean:
        return 'SEMI-DETACHED'

      # Pattern 3: Style contains both "duplex" AND "semi"
      # Matches: Residential/Other / N/R / Semi Detached, Duplex (112 listings)
      if ('duplex' in style_clean or '1/2 duplex' in style_clean or 'half duplex' in style_clean) and 'semi' in style_clean:
        return 'SEMI-DETACHED'

      # Pattern 4: Half Duplex or 1/2 Duplex → SEMI-DETACHED
      # Matches: BC/AB "Half Duplex" SubType (~15,000 listings)
      if 'half duplex' in combined_text or '1/2 duplex' in combined_text:
        return 'SEMI-DETACHED'

      # Pattern 5: SubType contains "Duplex" (when Type is Residential/single-family) → SEMI-DETACHED
      # Catches: ON "Residential/2 Storey, Duplex", "Residential/Duplex", etc.
      # Exclude multi-family types: Multi-residential, Multi-family, Multifamily (those are investment properties)
      if 'duplex' in subtype_clean and 'multi' not in type_clean:
        return 'SEMI-DETACHED'

    # PRIORITY 0.5: Handle all plex cases first
    # BUT exclude half duplex and 1/2 duplex - those are semi-detached (management approved 2026-01-28)
    if 'half duplex' not in combined_text and '1/2 duplex' not in combined_text:
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

    # PRIORITY 1.5: Strong SubType signal - Semi Detached (before Style "attached" check)
    # This ensures SubType="Semi Detached" is classified correctly regardless of Style
    # containing "Attached-side By Side" etc. Added 2025-01 to fix Calgary SEMI-DETACHED drop.
    if 'semi detached' in subtype_clean or 'semi-detached' in subtype_clean:
      return "SEMI-DETACHED"

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
    # Keep only strong TOWNHOUSE signals (no ambiguity)
    if 'townhouse' in subtype_clean or 'row house' in subtype_clean or 'row unit' in subtype_clean:
      return "TOWNHOUSE"

    # Handle SubType=Attached based on Style field
    # SubType=Attached is ambiguous - use Style to distinguish SEMI-DETACHED from TOWNHOUSE
    if 'attached' in subtype_clean:
      # Override: Penthouse is always CONDO
      if 'penthouse' in style_clean:
        return "CONDO"

      # If Style indicates row/townhouse → TOWNHOUSE
      if 'row' in style_clean or 'townhouse' in style_clean or 'twnhouse' in style_clean:
        return "TOWNHOUSE"

      # If Style indicates standalone dwelling → SEMI-DETACHED
      standalone_styles = ['bungalow', 'storey', 'story', 'bi-level', 'split', 'raised', 'tri-level']
      if any(style_word in style_clean for style_word in standalone_styles):
        return "SEMI-DETACHED"

      # Default: preserve old behavior for ambiguous cases → TOWNHOUSE
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
    # Track new combinations separately for logging
    new_mappings_only = []

    if new_count > 0:
      new_rows = merged[is_new]
      for _, row in tqdm(new_rows.iterrows(), total=new_count, desc="Processing new combinations"):
        suggested_mapping = self.suggest_mapping(row['Type'], row['SubType'], row['Style'])

        new_mapping = {
          'Type': row['Type'],
          'SubType': row['SubType'],
          'Style': row['Style'],
          'Count': row['Count'],
          'Inferred': suggested_mapping
        }

        mappings.append(new_mapping)
        new_mappings_only.append(new_mapping)  # Track for separate logging

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

    # Split new combinations into UNCLEAR and RESOLVED for separate logging
    new_unclear_combos = [m for m in new_mappings_only if m['Inferred'] == 'UNCLEAR']
    new_resolved_combos = [m for m in new_mappings_only if m['Inferred'] != 'UNCLEAR']

    # Log UNCLEAR mappings to CSV for manual review
    self._log_unclear_mappings(new_unclear_combos=new_unclear_combos)

    # Log newly-resolved mappings to CSV for audit and validation
    # Skip if no initial cache (all combinations would be "new", causing massive slowdown)
    if cached_count > 0:
      self._log_new_resolved_mappings(new_resolved_combos=new_resolved_combos)
    else:
      self.logger.info("Skipping resolved mappings log (no initial cache - all combinations are new)")

    return self.cached_mapping_df

  def _log_unclear_mappings(self, new_unclear_combos: List[Dict] = None):
    """
    Log UNCLEAR property type mappings to a CSV file for manual review.

    This method is called automatically at the end of generate_full_mapping_dataframe().
    It extracts all UNCLEAR mappings from the cache and logs them to unclear_mappings.csv
    in the cache directory. The CSV file maintains unique combinations (no duplicates).

    For each UNCLEAR combination, it attempts to extract City and Province from a sample
    record in self.df to help prioritize manual review.

    Args:
        new_unclear_combos: Optional list of dicts with newly-classified UNCLEAR combinations.
                           If provided, only these will be logged (more efficient).
                           If None, extracts all UNCLEAR from cache (legacy behavior).

    CSV Structure:
    - Columns: Type, SubType, Style, Count, City, Province
    - Uniqueness: (Type, SubType, Style) combinations are unique
    - Location: {cache_dir}/unclear_mappings.csv

    Note: If City or Province columns don't exist in self.df, 'N/A' is used as placeholder.
    """
    # If new_unclear_combos provided, use those (more efficient)
    if new_unclear_combos is not None:
      if not new_unclear_combos:
        self.logger.info("No new UNCLEAR mappings to log")
        return

      # Convert list of dicts to DataFrame
      unclear_mappings = pd.DataFrame(new_unclear_combos)
      self.logger.info(f"Found {len(unclear_mappings)} new UNCLEAR mappings to log")
    else:
      # Legacy behavior: extract from cache
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

    # Filter out combinations that are already handled by override lookup table
    # These have been manually reviewed and don't need to appear in unclear_mappings.csv
    if self.override_lookup_df is not None and not self.override_lookup_df.empty:
      # Reset index to ensure alignment with merge result (merge creates RangeIndex)
      unclear_mappings_reset = unclear_mappings.reset_index(drop=True)

      # Use vectorized merge to identify which unclear mappings exist in override table
      override_check = unclear_mappings_reset[['Type', 'SubType', 'Style']].merge(
        self.override_lookup_df[['Type', 'SubType', 'Style']],
        on=['Type', 'SubType', 'Style'],
        how='left',
        indicator=True
      )

      # Keep only rows NOT in override table (indicator='left_only')
      is_not_overridden = override_check['_merge'] == 'left_only'

      before_filter = len(unclear_mappings_reset)
      unclear_mappings = unclear_mappings_reset[is_not_overridden].copy()

      filtered_count = before_filter - len(unclear_mappings)
      if filtered_count > 0:
        self.logger.info(f"Filtered {filtered_count} UNCLEAR items already in override table")

      # Early exit if all UNCLEAR items are already handled by overrides
      if unclear_mappings.empty:
        self.logger.info("All UNCLEAR mappings are handled by override table - nothing to log")
        return

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

        # Merge and aggregate based on (Type, SubType, Style)
        combined = pd.concat(
          [existing_unclear, unclear_mappings[['Type', 'SubType', 'Style', 'Count', 'City', 'Province']]],
          ignore_index=True
        )

        # Ensure Count is numeric for aggregation
        combined['Count'] = pd.to_numeric(combined['Count'], errors='coerce').fillna(0)

        def _first_non_null(series: pd.Series) -> str:
          non_null = series.dropna()
          if not non_null.empty:
            return str(non_null.iloc[0])
          return 'N/A'

        combined = combined.groupby(['Type', 'SubType', 'Style'], as_index=False).agg({
          'Count': 'sum',
          'City': _first_non_null,
          'Province': _first_non_null
        })

        # Sort by Count descending for final output
        combined = combined.sort_values('Count', ascending=False).reset_index(drop=True)

        self.logger.info(f"After aggregation: {len(combined)} unique UNCLEAR mappings")

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

  def _log_new_resolved_mappings(self, new_resolved_combos: List[Dict] = None):
    """
    Log newly-resolved property type mappings to CSV for audit and validation.

    This method is called automatically at the end of generate_full_mapping_dataframe().
    It logs new combinations (not previously in cache) that resolved to a property type
    (DETACHED, TOWNHOUSE, etc.) - excluding UNCLEAR.

    Args:
        new_resolved_combos: List of dicts with newly-classified resolved combinations.
                            Each dict contains: Type, SubType, Style, Count, Inferred.
                            If None or empty, no logging occurs.

    CSV Structure:
    - Columns: Type, SubType, Style, Count, Inferred, City, Province
    - Uniqueness: (Type, SubType, Style, Inferred) combinations are unique
    - Location: {cache_dir}/new_resolved_mappings.csv

    Behavior:
    - Appends to existing CSV (never overwrites)
    - Aggregates counts for duplicate combinations
    - Filters out combinations in override lookup table
    - Each combo appears only once when first encountered

    Note: If City or Province columns don't exist in self.df, 'N/A' is used as placeholder.
    """
    # Early exit if no new resolved combos
    if not new_resolved_combos:
      self.logger.info("No new resolved mappings to log")
      return

    # Convert list of dicts to DataFrame
    resolved_mappings = pd.DataFrame(new_resolved_combos)
    self.logger.info(f"Found {len(resolved_mappings)} new resolved mappings to log")

    # Filter out combinations that are already handled by override lookup table
    # These have been manually reviewed and don't need to appear in new_resolved_mappings.csv
    if self.override_lookup_df is not None and not self.override_lookup_df.empty:
      # Reset index to ensure alignment with merge result
      resolved_mappings_reset = resolved_mappings.reset_index(drop=True)

      # Use vectorized merge to identify which resolved mappings exist in override table
      override_check = resolved_mappings_reset[['Type', 'SubType', 'Style']].merge(
        self.override_lookup_df[['Type', 'SubType', 'Style']],
        on=['Type', 'SubType', 'Style'],
        how='left',
        indicator=True
      )

      # Keep only rows NOT in override table (indicator='left_only')
      is_not_overridden = override_check['_merge'] == 'left_only'

      before_filter = len(resolved_mappings_reset)
      resolved_mappings = resolved_mappings_reset[is_not_overridden].copy()

      filtered_count = before_filter - len(resolved_mappings)
      if filtered_count > 0:
        self.logger.info(f"Filtered {filtered_count} resolved items already in override table")

      # Early exit if all resolved items are already handled by overrides
      if resolved_mappings.empty:
        self.logger.info("All resolved mappings are handled by override table - nothing to log")
        return

    # Add City and Province columns by looking up sample records from self.df
    if hasattr(self, 'df') and self.df is not None and not self.df.empty:
      resolved_mappings['City'] = 'N/A'
      resolved_mappings['Province'] = 'N/A'

      for idx, row in resolved_mappings.iterrows():
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
            resolved_mappings.at[idx, 'City'] = sample_record['City']

          # Extract Province if available
          if 'Province' in sample_record.index and pd.notna(sample_record['Province']):
            resolved_mappings.at[idx, 'Province'] = sample_record['Province']
    else:
      # No data available - use N/A placeholders
      resolved_mappings['City'] = 'N/A'
      resolved_mappings['Province'] = 'N/A'

    # Determine CSV file path
    if self.cached_mapping_path is None:
      self.logger.warning("No cache path set - cannot log resolved mappings to CSV")
      return

    csv_path = self.cached_mapping_path.parent / 'new_resolved_mappings.csv'

    # Load existing CSV if it exists and merge with new resolved items
    if csv_path.exists():
      try:
        existing_resolved = pd.read_csv(csv_path)
        self.logger.info(f"Loaded {len(existing_resolved)} existing resolved mappings from {csv_path.name}")

        # Merge and aggregate based on (Type, SubType, Style, Inferred)
        combined = pd.concat(
          [existing_resolved, resolved_mappings[['Type', 'SubType', 'Style', 'Count', 'Inferred', 'City', 'Province']]],
          ignore_index=True
        )

        # Ensure Count is numeric for aggregation
        combined['Count'] = pd.to_numeric(combined['Count'], errors='coerce').fillna(0)

        def _first_non_null(series: pd.Series) -> str:
          non_null = series.dropna()
          if not non_null.empty:
            return str(non_null.iloc[0])
          return 'N/A'

        combined = combined.groupby(['Type', 'SubType', 'Style', 'Inferred'], as_index=False).agg({
          'Count': 'sum',
          'City': _first_non_null,
          'Province': _first_non_null
        })

        # Sort by Count descending for final output
        combined = combined.sort_values('Count', ascending=False).reset_index(drop=True)

        self.logger.info(f"After aggregation: {len(combined)} unique resolved mappings")

      except Exception as e:
        self.logger.error(f"Error loading existing new_resolved_mappings.csv: {e}. Creating new file.")
        combined = resolved_mappings[['Type', 'SubType', 'Style', 'Count', 'Inferred', 'City', 'Province']].sort_values('Count', ascending=False)
    else:
      # No existing file - use new data
      combined = resolved_mappings[['Type', 'SubType', 'Style', 'Count', 'Inferred', 'City', 'Province']].sort_values('Count', ascending=False)
      self.logger.info(f"Creating new new_resolved_mappings.csv with {len(combined)} entries")

    # Save to CSV
    try:
      combined.to_csv(csv_path, index=False)
      self.logger.info(f"Logged {len(combined)} resolved mappings to {csv_path}")
    except Exception as e:
      self.logger.error(f"Error saving new_resolved_mappings.csv: {e}")

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

    # Apply manual override lookup table if available
    # This runs AFTER the main cache, allowing overrides to supersede base mappings
    override_count = 0  # Initialize for logging later

    if self.override_lookup_df is not None and not self.override_lookup_df.empty:
      self.logger.info("Applying manual override lookup table...")

      # Create override lookup dictionary (same pattern as main lookup)
      override_dict = self.override_lookup_df.set_index(['Type', 'SubType', 'Style'])['propertyType'].to_dict()

      # Apply overrides using vectorized map (only overrides non-None values)
      override_values = df['_temp_key'].map(override_dict)

      # Count how many overrides will be applied
      override_mask = override_values.notna()
      override_count = override_mask.sum()

      if override_count > 0:
        # Apply overrides (replaces existing propertyType values where override exists)
        df.loc[override_mask, 'propertyType'] = override_values[override_mask]
        self.logger.info(f"Applied {override_count:,} manual overrides ({override_count/len(df)*100:.1f}%)")
      else:
        self.logger.info("No overrides matched current data")

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
    if self.override_lookup_df is not None and override_count > 0:
      self.logger.info(f"  Manual overrides applied: {override_count:,}")

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


class MultiPassGUIDResolver:
  """
  Multi-pass GUID resolution for IMS sold listings.

  Implements multiple resolution strategies:
  - City name matching (g30 only) ✅
  - Polygon containment (all levels) ✅
  - LocalLogic API (all levels) ✅
  - Geocoder coordinate correction (automatic for invalid coords) ✅

  The geocoder is used transparently by polygon and API methods to fix
  invalid/missing coordinates before attempted GUID resolution.
  """

  def __init__(
    self,
    geo_collection: 'GeoCollection',
    checkpoint_dir: Union[str, Path],
    checkpoint_batch_size: int = 100,
    use_geocoder: bool = True,
    geocoder_min_interval_sec: float = 3.0,
    logger: Optional[logging.Logger] = None
  ):
    """
    Initialize MultiPassGUIDResolver.

    Args:
        geo_collection: GeoCollection instance with geographic data
        checkpoint_dir: Directory for checkpoint files (REQUIRED, str or Path)
        checkpoint_batch_size: Listings per batch for checkpointing (default: 100)
        use_geocoder: Whether to use geocoder for coordinate correction (default: True)
        geocoder_min_interval_sec: Minimum seconds between geocoder requests (default: 3.0)
        logger: Optional logger instance
    """
    self.logger = logger or logging.getLogger(self.__class__.__name__)
    self.geo_collection = geo_collection

    # Checkpoint configuration
    self.checkpoint_dir = Path(checkpoint_dir)
    self.checkpoint_batch_size = checkpoint_batch_size

    # Create checkpoint directory if doesn't exist
    self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Verify checkpoint directory is writable by attempting to create a test file
    try:
      test_file = self.checkpoint_dir / '.write_test'
      test_file.touch()
      test_file.unlink()
    except (OSError, PermissionError) as e:
      raise ValueError(f"Checkpoint directory not writable: {checkpoint_dir}") from e

    # Checkpoint file paths
    self.metadata_file = self.checkpoint_dir / 'metadata.json'
    self.city_results_file = self.checkpoint_dir / 'city_results.json'
    self.polygon_results_file = self.checkpoint_dir / 'polygon_results.json'
    self.geocoder_cache_file = self.checkpoint_dir / 'geocoder_cache.json'
    self.api_results_file = self.checkpoint_dir / 'locallogic_api_results.json'

    # Build city name lookup indices
    self.city_en_index = {}  # (normalized_city, province) → g30_geog_id
    self.city_fr_index = {}  # (normalized_city, province) → g30_geog_id

    self._build_city_indices()

    # Initialize geocoder client for fixing invalid coordinates
    self.geocoder_client = None
    self.geocoder_cache = {}  # doc_id → (lat, lng)

    if use_geocoder:
      try:
        self.geocoder_client = GeocoderCAClient(
          min_interval_sec=geocoder_min_interval_sec,
          logger=self.logger
        )
        self.logger.info(f"Geocoder.ca client initialized (min_interval={geocoder_min_interval_sec}s)")
      except Exception as e:
        self.logger.warning(f"Failed to initialize geocoder client: {e}")
        self.logger.warning("Coordinate correction via geocoder will be disabled")

    # Initialize LocalLogic API client (reused across multiple resolve_by_locallogic_api calls)
    self.locallogic_client = None
    try:
      self.locallogic_client = LocalLogicAPIClient(logger=self.logger)
      self.logger.info("LocalLogic API client initialized")
    except RuntimeError as e:
      self.logger.warning(f"LocalLogic API not available: {e}")
      self.logger.warning("LocalLogic API resolution will be disabled")

    # Statistics tracking
    self.stats = {
      'city_name_success': 0,
      'city_name_fsa_fallback': 0,
      'geocoder_corrections': 0,
      # More stats will be added for other methods
    }

  # ========== Checkpoint Management Methods ==========

  def _init_metadata(self, total_listings: int):
    """Initialize metadata for new checkpoint run."""
    metadata = {
      'start_datetime': datetime.now().isoformat(),
      'total_listings': total_listings,
      'batch_size': self.checkpoint_batch_size,
      'status': 'in_progress',
      'city_completed': False,
      'polygon_last_batch': -1,
      'api_last_batch': -1
    }
    self._save_metadata(metadata)

  def _save_metadata(self, metadata: Dict):
    """Save metadata to checkpoint file."""
    with open(self.metadata_file, 'w') as f:
      json.dump(metadata, f, indent=2)

  def _load_metadata(self) -> Optional[Dict]:
    """Load metadata from checkpoint file if exists."""
    if self.metadata_file.exists():
      try:
        with open(self.metadata_file, 'r') as f:
          return json.load(f)
      except (json.JSONDecodeError, IOError) as e:
        self.logger.warning(f"Failed to load metadata: {e}")
        return None
    return None

  def _update_metadata(self, **updates):
    """Update specific metadata fields."""
    metadata = self._load_metadata()
    if metadata:
      metadata.update(updates)
      self._save_metadata(metadata)

  def _check_stale_checkpoint(self, metadata: Dict):
    """
    Check if checkpoint is stale (older than 7 days).

    Raises RuntimeError if checkpoint is older than 7 days.
    Devops can override by manually removing 'start_datetime' field.
    """
    # If start_datetime field absent, skip check (manual override)
    if 'start_datetime' not in metadata:
      self.logger.info("Checkpoint age check skipped (start_datetime field removed)")
      return

    start_datetime_str = metadata['start_datetime']

    try:
      start_datetime = datetime.fromisoformat(start_datetime_str)
      now = datetime.now()
      age = now - start_datetime
      age_days = age.total_seconds() / 86400

      self.logger.info(f"Checkpoint age: {age_days:.1f} days (started {start_datetime_str})")

      if age_days > 7:
        raise RuntimeError(
          f"Checkpoint is stale (started {start_datetime_str}, {age_days:.1f} days ago). "
          f"This job may be stuck. Please investigate and manually remove 'start_datetime' "
          f"from {self.metadata_file} to override this check."
        )
    except ValueError as e:
      self.logger.warning(f"Failed to parse start_datetime '{start_datetime_str}': {e}")

  def _append_results(self, filepath: Path, batch_results: List[Dict]):
    """Append batch results to checkpoint file."""
    # Load existing results if file exists
    if filepath.exists():
      try:
        with open(filepath, 'r') as f:
          all_results = json.load(f)
      except (json.JSONDecodeError, IOError):
        all_results = []
    else:
      all_results = []

    # Append new results
    all_results.extend(batch_results)

    # Save back
    with open(filepath, 'w') as f:
      json.dump(all_results, f)

  def _load_results(self, filepath: Path) -> List[Dict]:
    """Load results from checkpoint file."""
    if filepath.exists():
      try:
        with open(filepath, 'r') as f:
          return json.load(f)
      except (json.JSONDecodeError, IOError) as e:
        self.logger.warning(f"Failed to load results from {filepath}: {e}")
        return []
    return []

  def _save_results(self, filepath: Path, results: List[Dict]):
    """Save results to checkpoint file (overwrite)."""
    with open(filepath, 'w') as f:
      json.dump(results, f)

  def _save_geocoder_cache(self):
    """Save entire geocoder cache to checkpoint file."""
    # Convert to JSON-serializable format
    cache_data = {
      doc_id: [lat, lng] if lat is not None and lng is not None else None
      for doc_id, (lat, lng) in self.geocoder_cache.items()
    }
    with open(self.geocoder_cache_file, 'w') as f:
      json.dump(cache_data, f)

  def _load_geocoder_cache(self):
    """Load geocoder cache from checkpoint file."""
    if self.geocoder_cache_file.exists():
      try:
        with open(self.geocoder_cache_file, 'r') as f:
          cache_data = json.load(f)
        # Convert back to tuple format
        self.geocoder_cache = {
          doc_id: tuple(coords) if coords is not None else (None, None)
          for doc_id, coords in cache_data.items()
        }
        self.logger.info(f"Loaded {len(self.geocoder_cache)} entries from geocoder cache")
      except (json.JSONDecodeError, IOError) as e:
        self.logger.warning(f"Failed to load geocoder cache: {e}")
        self.geocoder_cache = {}

  def _cleanup_checkpoints(self):
    """Delete all checkpoint files on successful completion."""
    for filepath in [self.metadata_file, self.city_results_file,
                     self.polygon_results_file, self.geocoder_cache_file,
                     self.api_results_file]:
      if filepath.exists():
        filepath.unlink()
    self.logger.info(f"Cleaned up checkpoint files in {self.checkpoint_dir}")

  # ========== End Checkpoint Management Methods ==========

  def _is_valid_canada_lat_lng(self, lat: float, lng: float) -> bool:
    """
    Validate coordinates are within Canada bounds.

    Uses same bounds as LocalLogicAPIClient for consistency.

    Args:
        lat: Latitude
        lng: Longitude

    Returns:
        True if coordinates are valid and within Canada bounds
    """
    # Canada geographic bounds
    MIN_LAT = 41.0   # Windsor, ON ~42°
    MAX_LAT = 84.0   # Ellesmere Island ~83°
    MIN_LNG = -141.1 # Yukon border
    MAX_LNG = -52.0  # Eastern Newfoundland

    if pd.isna(lat) or pd.isna(lng):
      return False

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
      return False

    return (MIN_LAT <= lat <= MAX_LAT and MIN_LNG <= lng <= MAX_LNG)

  def _get_valid_coords(self, listing: Dict) -> tuple:
    """
    Get valid coordinates for a listing, using geocoder to fix invalid coords.

    Args:
        listing: Listing dict with IMS field names (Latitude, Longitude, Address, City, Province, PostalCode, etc.)

    Returns:
        Tuple (lat, lng) with valid coordinates, or (None, None) if unable to resolve
    """
    lat = listing.get('Latitude')
    lng = listing.get('Longitude')

    # If coordinates are already valid, return them
    if self._is_valid_canada_lat_lng(lat, lng):
      return (lat, lng)

    # If geocoder not available, return None
    if self.geocoder_client is None:
      return (None, None)

    # Check cache first (to avoid duplicate API calls for same listing)
    doc_id = self._construct_doc_id(listing)
    if doc_id in self.geocoder_cache:
      cached_coords = self.geocoder_cache[doc_id]
      self.logger.debug(f"Using cached geocoded coords for {doc_id}")
      return cached_coords

    # Call geocoder API to fix coordinates
    self.logger.debug(f"Geocoding {doc_id} - invalid coords: ({lat}, {lng})")

    geocoded = self.geocoder_client.geocode_address(
      address=listing.get('Address'),
      city=listing.get('City'),
      province=listing.get('Province'),
      postal_code=listing.get('PostalCode')
    )

    if geocoded:
      corrected_lat = geocoded['lat']
      corrected_lng = geocoded['lng']
      self.geocoder_cache[doc_id] = (corrected_lat, corrected_lng)
      self.stats['geocoder_corrections'] += 1
      self.logger.info(f"Geocoded {doc_id}: ({lat}, {lng}) → ({corrected_lat}, {corrected_lng})")
      return (corrected_lat, corrected_lng)
    else:
      # Cache the failure to avoid retrying
      self.geocoder_cache[doc_id] = (None, None)
      return (None, None)

  def _build_city_indices(self):
    """Build city name lookup dictionaries from GeoCollection"""
    self.logger.info("Building city name indices...")

    g30_geos = self.geo_collection.get_by_level(30)

    for geo in g30_geos.all():
      province = geo.province

      # Index English name
      if geo.level_30_en:
        key = (self._normalize_city(geo.level_30_en), province)
        self.city_en_index[key] = geo.geog_id

      # Index French name
      if geo.level_30_fr:
        key = (self._normalize_city(geo.level_30_fr), province)
        self.city_fr_index[key] = geo.geog_id

      # Also index geo.city for additional coverage
      if geo.city:
        key = (self._normalize_city(geo.city), province)
        self.city_en_index[key] = geo.geog_id

    self.logger.info(f"City indices built: EN={len(self.city_en_index)}, FR={len(self.city_fr_index)}")

  def _normalize_city(self, name: str) -> str:
    """Normalize city name for matching (lowercase, trim, strip accents)"""
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return normalized.lower().strip()

  def _lookup_city_g30(self, city_name: str, province: str) -> Optional[str]:
    """
    Internal helper: Find g30 geog_id by city name.

    Checks both English and French indices.
    Returns None if not found.
    """
    if not city_name or not province:
      return None

    normalized = self._normalize_city(city_name)
    province_norm = province.upper()
    key = (normalized, province_norm)

    # Try English index first
    if key in self.city_en_index:
      return self.city_en_index[key]

    # Try French index
    if key in self.city_fr_index:
      return self.city_fr_index[key]

    return None

  def _construct_doc_id(self, listing: Dict) -> str:
    """
    Construct the ES document _id to align with bulk_import_ims_sold_listings.
    Format: {province}-{mlsid}-{mlsnumber}-{year}
    """
    written_date_raw = listing.get('WrittenDate', '')
    written_date = None
    year = None
    if pd.notna(written_date_raw):
      written_date_str = str(written_date_raw)
      if 'T' in written_date_str:
        written_date = written_date_str.split('T')[0]
      else:
        written_date = written_date_str
      try:
        year = written_date.split('-')[0] if written_date else None
      except (IndexError, AttributeError):
        year = None

    return (
      f"{str(listing.get('Province', '')).lower()}-"   # province
      f"{str(listing.get('MLSID', '')).lower()}-"      # board id
      f"{str(listing.get('MLSNumber', '')).lower()}-"  # listing number
      f"{year if year else 'unknown'}"                 # year
    )

  def resolve_by_city_name(self, listings_df: pd.DataFrame, show_progress: bool = True) -> List[Dict]:
    """
    City name matching (g30 level only).

    Fast in-memory lookup using bilingual city name matching.

    Args:
        listings_df: DataFrame of listings with IMS field names:
            - 'City': City name (from IMS 'City' field)
            - 'FSACity': FSA-based city name (fallback, optional)
            - 'Province': Province code (e.g., 'ON', 'BC')
            - 'MLSNumber': MLS listing number (primary key)
            - 'MLSID': MLS board id (used for _id construction)
            - 'WrittenDate': transaction date (used for _id year)

    Returns:
        List of result dicts with keys:
            - '_id': document id {province}-{mlsid}-{mlsnumber}-{year}
            - 'guid': resolved g30 geog_id (single entry) or None
            - 'source': 'city_name', 'fsa_city', or None
    """
    self.logger.info(f"Starting City Name Matching for {len(listings_df)} listings")

    results = []
    success_count = 0
    fsa_fallback_count = 0

    for _, listing in tqdm(listings_df.iterrows(), total=len(listings_df), desc="City Name Matching", disable=not show_progress):
      city = listing.get('City')          # IMS field name
      fsa_city = listing.get('FSACity')   # IMS field name (fallback)
      province = listing.get('Province')  # IMS field name

      g30_geog_id = None
      source = None

      # Try primary city name
      if city and province:
        g30_geog_id = self._lookup_city_g30(city, province)
        if g30_geog_id:
          source = 'city_name'
          success_count += 1

      # Try FSACity fallback if primary failed
      if not g30_geog_id and fsa_city and province:
        g30_geog_id = self._lookup_city_g30(fsa_city, province)
        if g30_geog_id:
          source = 'fsa_city'
          success_count += 1
          fsa_fallback_count += 1

      doc_id = self._construct_doc_id(listing)

      results.append({
        '_id': doc_id,
        'guid': g30_geog_id,
        'source': source
      })

    total = len(listings_df)
    self.logger.info(f"City Name Matching complete: {success_count}/{total} matched ({success_count/total*100:.1f}%)")
    if fsa_fallback_count > 0:
      self.logger.info(f"  FSA fallback used: {fsa_fallback_count} times")

    return results

  def resolve_by_polygon(self, listings_df: pd.DataFrame, show_progress: bool = True) -> List[Dict]:
    """
    Polygon containment using GeoCollection.compute_guid for each listing.

    Automatically uses geocoder to fix invalid coordinates before polygon checks.
    Geocoded coordinates are cached to avoid duplicate API calls.

    Args:
        listings_df: DataFrame of listings with IMS field names:
            - 'Latitude', 'Longitude': Coordinates (will be geocoded if invalid)
            - 'Address', 'City', 'Province', 'PostalCode': Used for geocoding if needed
            - 'MLSID', 'MLSNumber', 'WrittenDate': used for _id construction
    Returns:
        List of result dicts with keys:
            - '_id': document id {province}-{mlsid}-{mlsnumber}-{year}
            - 'guid': comma-separated geog_ids (all levels) or None
            - 'source': 'polygon' when guid is found, else None
    """
    total = len(listings_df)
    self.logger.info(f"Starting Polygon Containment for {total} listings")
    if total == 0:
      return []

    results = []
    success_count = 0

    for _, listing in tqdm(listings_df.iterrows(), total=total, desc="Polygon Containment", disable=not show_progress):
      # Get valid coordinates (uses geocoder to fix invalid coords if needed)
      lat, lng = self._get_valid_coords(listing)

      guid = None
      if lat is not None and lng is not None:
        try:
          guid = self.geo_collection.compute_guid(lat, lng)
        except Exception as e:
          # Log full diagnostic information about the problematic listing
          self.logger.error("=" * 80)
          self.logger.error("CRITICAL: Polygon containment failed for listing")
          self.logger.error("=" * 80)
          self.logger.error(f"Exception type: {type(e).__name__}")
          self.logger.error(f"Exception message: {e}")
          self.logger.error(f"\nListing coordinates:")
          self.logger.error(f"  Latitude: {lat}")
          self.logger.error(f"  Longitude: {lng}")
          self.logger.error(f"\nListing details:")
          for key, value in listing.items():
            self.logger.error(f"  {key}: {value}")
          self.logger.error("=" * 80)
          # Re-raise the exception to halt processing
          raise

      if guid:
        success_count += 1

      doc_id = self._construct_doc_id(listing)
      results.append({
        '_id': doc_id,
        'guid': guid,
        'source': 'polygon' if guid else None
      })

    self.logger.info(f"Polygon Containment complete: {success_count}/{total} matched ({success_count/total*100:.1f}%)")
    return results

  def resolve_by_locallogic_api(self, listings_df: pd.DataFrame, show_progress: bool = True) -> List[Dict]:
    """
    LocalLogic API resolution (all levels: g10, g20, g30, g35, g40).

    Uses LocalLogic API to query geographies by lat/lng coordinates.
    Automatically uses geocoder to fix invalid coordinates before API calls.
    Geocoded coordinates are cached to avoid duplicate API calls.
    Gracefully skips if credentials not provided.

    Args:
        listings_df: DataFrame of listings with IMS field names:
            - 'Latitude', 'Longitude': Coordinates (will be geocoded if invalid)
            - 'Address', 'City', 'Province', 'PostalCode': Used for geocoding if needed
            - 'MLSID', 'MLSNumber', 'WrittenDate': used for _id construction

    Returns:
        List of result dicts with keys:
            - '_id': document id {province}-{mlsid}-{mlsnumber}-{year}
            - 'guid': comma-separated geog_ids (all levels) or None
            - 'source': 'locallogic_api' when guid is found, else None
    """
    total = len(listings_df)
    self.logger.info(f"Starting LocalLogic API Resolution for {total} listings")

    # Check if LocalLogic client is available
    if self.locallogic_client is None:
      self.logger.warning("LocalLogic API client not available, skipping API resolution")
      return [{'_id': self._construct_doc_id(row), 'guid': None, 'source': None}
              for _, row in listings_df.iterrows()]

    if total == 0:
      return []

    results = []
    success_count = 0

    for _, listing in tqdm(listings_df.iterrows(), total=total, desc="LocalLogic API", disable=not show_progress):
      # Get valid coordinates (uses geocoder to fix invalid coords if needed)
      lat, lng = self._get_valid_coords(listing)

      guid = None

      if lat is not None and lng is not None:
        # Call LocalLogic API
        geog_dict = self.locallogic_client.get_geographies(lat, lng)

        if geog_dict:
          # Build comma-separated GUID ordered from finest to coarsest
          # Order: g40, g35, g30, g20, g10 (reverse of hierarchy)
          guid_parts = []
          for level in ['g40', 'g35', 'g30', 'g20', 'g10']:
            if level in geog_dict:
              guid_parts.append(geog_dict[level])

          guid = ','.join(guid_parts) if guid_parts else None

      if guid:
        success_count += 1

      doc_id = self._construct_doc_id(listing)
      results.append({
        '_id': doc_id,
        'guid': guid,
        'source': 'locallogic_api' if guid else None
      })

    self.logger.info(f"LocalLogic API complete: {success_count}/{total} matched ({success_count/total*100:.1f}%)")
    self.logger.info(f"Total API calls made: {self.locallogic_client.total_api_calls}")

    return results

  def resolve(self, listings_df: pd.DataFrame, show_progress: bool = True) -> pd.DataFrame:
    """
    Main method to resolve GUIDs for all listings using multi-pass strategy with checkpointing.

    Strategy:
    1. City name matching (fast, g30 only)
    2. Polygon containment (batched with checkpoints, all levels, with geocoder fallback)
    3. LocalLogic API (batched with checkpoints, for failed polygon OR geocoded coords, all levels)
    4. Merge all GUIDs found for each listing into comprehensive result
    5. Add 'guid' column to DataFrame inplace
    6. Update 'Latitude' and 'Longitude' columns with geocoded coordinates if available

    The method modifies the input DataFrame inplace by:
    - Adding a 'guid' column with resolved geographic identifiers
    - Updating 'Latitude' and 'Longitude' columns with corrected coordinates

    Checkpointing:
    - Automatically resumes from last checkpoint if interrupted
    - Raises RuntimeError if checkpoint is >7 days old (stale job detection)
    - Cleans up checkpoint files on successful completion

    Args:
        listings_df: DataFrame with IMS field names (modified inplace)
        show_progress: Show progress bars

    Returns:
        Same DataFrame with 'guid' column added and coordinates corrected
    """
    total = len(listings_df)
    self.logger.info(f"Starting multi-pass GUID resolution for {total} listings")

    # Step 1: Load checkpoint metadata (if exists)
    metadata = self._load_metadata()

    if metadata:
      # Check if checkpoint is stale (raises RuntimeError if >7 days old)
      self._check_stale_checkpoint(metadata)

      self.logger.info(f"Resuming from checkpoint: city_completed={metadata['city_completed']}, "
                       f"polygon_last_batch={metadata['polygon_last_batch']}, "
                       f"api_last_batch={metadata['api_last_batch']}")

      # Load geocoder cache
      self._load_geocoder_cache()

      # Validate checkpoint is for same dataset size
      if metadata['total_listings'] != total:
        self.logger.warning(f"Checkpoint total_listings mismatch ({metadata['total_listings']} vs {total}), starting fresh")
        self._cleanup_checkpoints()
        metadata = None

    if not metadata:
      self._init_metadata(total)
      metadata = self._load_metadata()

    # Step 2: Pass 1 - City name matching (fast, no batching)
    if metadata['city_completed']:
      self.logger.info("Pass 1/3: City name matching (loading from checkpoint)...")
      city_results = self._load_results(self.city_results_file)
    else:
      self.logger.info("Pass 1/3: City name matching...")
      city_results = self.resolve_by_city_name(listings_df, show_progress=show_progress)
      self._save_results(self.city_results_file, city_results)
      self._update_metadata(city_completed=True)

    city_df = pd.DataFrame(city_results)

    # Step 3: Pass 2 - Polygon containment (batched)
    self.logger.info("Pass 2/3: Polygon containment...")
    last_polygon_batch = metadata['polygon_last_batch']
    polygon_results = self._load_results(self.polygon_results_file)

    # Calculate starting point
    start_batch_idx = last_polygon_batch + 1
    start_idx = start_batch_idx * self.checkpoint_batch_size

    if start_idx < total:
      self.logger.info(f"Resuming polygon from batch {start_batch_idx} (listing {start_idx}/{total})")

      # Calculate total batches for progress tracking
      total_batches = (total + self.checkpoint_batch_size - 1) // self.checkpoint_batch_size

      # Process remaining batches
      for batch_start in range(start_idx, total, self.checkpoint_batch_size):
        batch_idx = batch_start // self.checkpoint_batch_size
        batch_end = min(batch_start + self.checkpoint_batch_size, total)
        batch_df = listings_df.iloc[batch_start:batch_end]

        # Log batch progress
        current_batch = batch_idx + 1  # Convert to 1-based numbering
        self.logger.info(f"Processing batch {current_batch}/{total_batches} ({len(batch_df)} listings)")

        # Call existing resolve_by_polygon method
        batch_results = self.resolve_by_polygon(batch_df, show_progress=show_progress)

        # Save checkpoint
        self._append_results(self.polygon_results_file, batch_results)
        self._save_geocoder_cache()  # Save at same time
        self._update_metadata(polygon_last_batch=batch_idx)

        polygon_results.extend(batch_results)

      # Log completion
      success_count = sum(1 for r in polygon_results if r.get('guid'))
      self.logger.info(f"Polygon Containment complete: {success_count}/{total} matched ({success_count/total*100:.1f}%)")
    else:
      self.logger.info(f"Polygon already completed (all {total} listings)")

    # Deduplicate polygon results by '_id' (keeps last entry for duplicates)
    # This handles cases where batch size changed and listings were reprocessed
    if polygon_results:
      original_count = len(polygon_results)
      seen_ids = {}
      for result in polygon_results:
        doc_id = result['_id']
        seen_ids[doc_id] = result  # Overwrites duplicates with latest
      polygon_results = list(seen_ids.values())

      if len(polygon_results) < original_count:
        self.logger.warning(f"Deduplicated polygon results: {original_count} → {len(polygon_results)} (removed {original_count - len(polygon_results)} duplicates)")

    polygon_df = pd.DataFrame(polygon_results)

    # Step 4: Pass 3 - LocalLogic API (batched, conditional)
    api_df = None
    if self.locallogic_client is not None:
      # Determine which listings need API (failed polygon OR geocoded coords)
      failed_mask = polygon_df['guid'].isna()
      geocoded_mask = polygon_df['_id'].isin(self.geocoder_cache.keys())
      needs_api_mask = failed_mask | geocoded_mask
      needs_api_count = needs_api_mask.sum()

      if needs_api_count > 0:
        self.logger.info(f"Pass 3/3: LocalLogic API for {needs_api_count} listings")
        self.logger.info(f"  - Polygon failures: {failed_mask.sum()}")
        self.logger.info(f"  - Geocoded coords: {geocoded_mask.sum()}")

        # Get indices of listings needing API
        api_indices = polygon_df[needs_api_mask].index.tolist()
        api_listings_df = listings_df.iloc[api_indices].copy()

        # Process in batches
        last_api_batch = metadata['api_last_batch']
        api_results = self._load_results(self.api_results_file)

        start_api_batch_idx = last_api_batch + 1
        start_api_idx = start_api_batch_idx * self.checkpoint_batch_size
        api_total = len(api_listings_df)

        if start_api_idx < api_total:
          self.logger.info(f"Resuming API from batch {start_api_batch_idx} (listing {start_api_idx}/{api_total})")

          # Process remaining batches
          for batch_start in range(start_api_idx, api_total, self.checkpoint_batch_size):
            batch_idx = batch_start // self.checkpoint_batch_size
            batch_end = min(batch_start + self.checkpoint_batch_size, api_total)
            batch_df = api_listings_df.iloc[batch_start:batch_end]

            # Call existing resolve_by_locallogic_api method
            batch_results = self.resolve_by_locallogic_api(batch_df, show_progress=show_progress)

            # Save checkpoint
            self._append_results(self.api_results_file, batch_results)
            self._save_geocoder_cache()  # Save in case geocoding happened
            self._update_metadata(api_last_batch=batch_idx)

            api_results.extend(batch_results)

          # Log completion
          success_count = sum(1 for r in api_results if r.get('guid'))
          self.logger.info(f"LocalLogic API complete: {success_count}/{api_total} matched ({success_count/api_total*100:.1f}%)")
        else:
          self.logger.info(f"LocalLogic API already completed (all {api_total} listings)")

        # Deduplicate API results by '_id' (keeps last entry for duplicates)
        # This handles cases where batch size changed and listings were reprocessed
        if api_results:
          original_count = len(api_results)
          seen_ids = {}
          for result in api_results:
            doc_id = result['_id']
            seen_ids[doc_id] = result  # Overwrites duplicates with latest
          api_results = list(seen_ids.values())

          if len(api_results) < original_count:
            self.logger.warning(f"Deduplicated API results: {original_count} → {len(api_results)} (removed {original_count - len(api_results)} duplicates)")

        api_df = pd.DataFrame(api_results)
      else:
        self.logger.info("Pass 3/3: Skipping LocalLogic API - all listings resolved by polygon")
    else:
      self.logger.info("Pass 3/3: Skipping LocalLogic API - client not available")

    # Step 5: Merge results
    self.logger.info("Merging results from all passes...")
    merged_df = self._merge_resolution_results(city_df, polygon_df, api_df)

    # Add guid column to original DataFrame
    guid_map = merged_df.set_index('_id')['guid'].to_dict()
    listings_df['guid'] = listings_df.apply(
      lambda row: guid_map.get(self._construct_doc_id(row)),
      axis=1
    )

    # Propagate corrected coordinates from geocoder cache
    if self.geocoder_cache:
      self._update_corrected_coordinates(listings_df)

    # Step 6: Mark as completed and cleanup
    self._update_metadata(status='completed')
    self._cleanup_checkpoints()

    # Log final statistics
    resolved_count = listings_df['guid'].notna().sum()
    self.logger.info(f"Resolution complete: {resolved_count}/{total} listings resolved ({resolved_count/total*100:.1f}%)")
    self.logger.info(f"Geocoder corrections: {self.stats.get('geocoder_corrections', 0)}")

    return listings_df

  def _merge_resolution_results(self, city_df: pd.DataFrame, polygon_df: pd.DataFrame, api_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge GUID results from all resolution methods.

    Strategy: Combine all unique geog_ids from all sources, deduplicate, and order by level.

    Args:
        city_df: Results from city name matching (g30 only)
        polygon_df: Results from polygon containment (all levels)
        api_df: Results from LocalLogic API (all levels, may be None)

    Returns:
        DataFrame with columns: _id, guid (merged and deduplicated)
    """
    # Start with all doc IDs from polygon (which has all listings)
    merged = polygon_df[['_id']].copy()

    # Merge city results
    merged = merged.merge(
      city_df[['_id', 'guid']].rename(columns={'guid': 'city_guid'}),
      on='_id',
      how='left'
    )

    # Merge polygon results
    merged = merged.merge(
      polygon_df[['_id', 'guid']].rename(columns={'guid': 'polygon_guid'}),
      on='_id',
      how='left'
    )

    # Merge API results if available
    if api_df is not None and len(api_df) > 0:
      merged = merged.merge(
        api_df[['_id', 'guid']].rename(columns={'guid': 'api_guid'}),
        on='_id',
        how='left'
      )
    else:
      merged['api_guid'] = None

    # Merge GUIDs from all sources (vectorized)
    merged['guid'] = merged.apply(
      lambda row: self._combine_guids(row['city_guid'], row['polygon_guid'], row['api_guid']),
      axis=1
    )

    return merged[['_id', 'guid']]

  def _combine_guids(self, city_guid, polygon_guid, api_guid) -> Optional[str]:
    """
    Combine GUIDs from all sources into a single comprehensive GUID.

    Collects all unique geog_ids from all sources, deduplicates, and orders by level.

    Args:
        city_guid: GUID from city name matching (g30 only)
        polygon_guid: GUID from polygon containment (all levels)
        api_guid: GUID from LocalLogic API (all levels)

    Returns:
        Comma-separated GUID with all unique geog_ids, ordered by level (g40→g10)
    """
    all_geog_ids = set()

    # Collect geog_ids from all sources
    for guid_str in [city_guid, polygon_guid, api_guid]:
      if pd.notna(guid_str) and guid_str:
        all_geog_ids.update(guid_str.split(','))

    if not all_geog_ids:
      return None

    # Sort by level (g40, g35, g30, g20, g10)
    level_order = {'g40': 0, 'g35': 1, 'g30': 2, 'g20': 3, 'g10': 4}

    def sort_key(geog_id):
      prefix = geog_id[:3]
      return level_order.get(prefix, 99)

    sorted_ids = sorted(all_geog_ids, key=sort_key)

    return ','.join(sorted_ids)

  def _update_corrected_coordinates(self, listings_df: pd.DataFrame):
    """
    Update DataFrame with corrected coordinates from geocoder cache.

    Propagates geocoded coordinates back to the original DataFrame inplace.
    Only updates rows where coordinates were originally invalid/missing and
    geocoder successfully resolved them.

    This is a vectorized operation using pandas merge for efficiency.

    Args:
        listings_df: DataFrame to update (modified inplace)
    """
    if not self.geocoder_cache:
      return

    # Build DataFrame from geocoder cache
    corrections = []
    for doc_id, (lat, lng) in self.geocoder_cache.items():
      if lat is not None and lng is not None:
        corrections.append({'_doc_id': doc_id, 'corrected_lat': lat, 'corrected_lng': lng})

    if not corrections:
      return

    corrections_df = pd.DataFrame(corrections)

    # Add temporary _doc_id column (vectorized)
    listings_df['_doc_id'] = listings_df.apply(lambda row: self._construct_doc_id(row), axis=1)

    # Merge corrections using a temporary variable (to avoid breaking the reference to original DataFrame)
    merged_df = listings_df.merge(corrections_df, on='_doc_id', how='left')

    # Update Latitude and Longitude where corrections exist (vectorized)
    has_correction = merged_df['corrected_lat'].notna()
    correction_count = has_correction.sum()

    if correction_count > 0:
      # Update the ORIGINAL DataFrame using values from the merged DataFrame
      listings_df.loc[has_correction, 'Latitude'] = merged_df.loc[has_correction, 'corrected_lat'].values
      listings_df.loc[has_correction, 'Longitude'] = merged_df.loc[has_correction, 'corrected_lng'].values

      self.logger.info(f"Updated {correction_count} listings with corrected coordinates from geocoder")

    # Drop temporary _doc_id column from original DataFrame
    listings_df.drop(columns=['_doc_id'], inplace=True, errors='ignore')
