#!/usr/bin/env python3
"""
One-off script to backfill historical mth_end_snapshot_listing_count to ES
This populates ES documents with historical month-end current listing snapshots

Usage:
    python backfill_mth_end_snapshots.py
    python backfill_mth_end_snapshots.py --host localhost --port 9200
    python backfill_mth_end_snapshots.py --dry-run
"""

import argparse
from elasticsearch.helpers import bulk
import pandas as pd
import random
import string
from realestate_analytics.etl.absorption_rate import AbsorptionRateProcessor
from realestate_analytics.data.es import Datastore

def parse_args():
  parser = argparse.ArgumentParser(
    description='Backfill historical month-end snapshot listing counts to Elasticsearch',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  %(prog)s                           # Use defaults (localhost:9201)
  %(prog)s --host prod.es.com        # Use production host
  %(prog)s --port 9200               # Use different port
  %(prog)s --dry-run                 # Preview what would be updated
    """
  )
  
  parser.add_argument('--host', 
                      default='localhost', 
                      help='Elasticsearch host (default: localhost)')
  
  parser.add_argument('--port', 
                      type=int, 
                      default=9201, 
                      help='Elasticsearch port (default: 9201)')
  
  parser.add_argument('--dry-run', 
                      action='store_true', 
                      help='Show what would be updated without making changes')
  
  return parser.parse_args()

def main():
  args = parse_args()
  
  # Generate random alphanumeric job_id
  job_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
  print(f"Using job_id: {job_id}")
  print(f"Connecting to Elasticsearch at {args.host}:{args.port}")
  
  # Setup datastore and processor
  datastore = Datastore(host=args.host, port=args.port)
  processor = AbsorptionRateProcessor(
    job_id=job_id,
    datastore=datastore
  )
  
  try:
    processor.extract(from_cache=True)
  except Exception as e:
    print(f"❌ Failed to load cached data: {e}")
    return 1
  
  if processor.current_counts_ts_df is None or processor.current_counts_ts_df.empty:
    print("❌ No current counts data found in cache")
    return 1
  
  # Step 1: Add 'ALL' property type aggregation to current_counts_ts_df
  all_aggregation = processor.current_counts_ts_df.groupby(['geog_id', 'year-month'])['current_count'].sum().reset_index()
  all_aggregation['propertyType'] = 'ALL'
  current_counts_df = pd.concat([processor.current_counts_ts_df, all_aggregation], ignore_index=True)
  
  if args.dry_run:
    print("🔍 DRY RUN - would update these combinations:")
    for (geog_id, propertyType), group in current_counts_df.groupby(['geog_id', 'propertyType']):
      print(f"  {geog_id}_{propertyType}: {len(group)} months")
    print(f"\nTotal: {current_counts_df.groupby(['geog_id', 'propertyType']).ngroups} combinations")
    return 0
  
  print(f"Total combinations to update: {current_counts_df.groupby(['geog_id', 'propertyType']).ngroups}")
  print(f"Date range: {current_counts_df['year-month'].min()} to {current_counts_df['year-month'].max()}")
  
  # Step 2: Generate ES update operations
  def generate_snapshot_updates():
    # Group by geog_id and propertyType to create time series for each combination
    for (geog_id, propertyType), group in current_counts_df.groupby(['geog_id', 'propertyType']):
      # Create time series array
      time_series = []
      for _, row in group.iterrows():
        time_series.append({
          "month": row['year-month'].strftime('%Y-%m'),
          "value": int(row['current_count'])
        })
      
      # Sort by month
      time_series.sort(key=lambda x: x['month'])
      
      # Create composite_id
      property_type = propertyType if propertyType != 'ALL' else None
      composite_id = f"{geog_id}_{property_type or 'ALL'}"
      
      yield {
        "_op_type": "update",
        "_index": processor.datastore.mkt_trends_index_name,
        "_id": composite_id,
        "script": {
          "source": """
          if (ctx._source.metrics == null) {
            ctx._source.metrics = new HashMap();
          }
          ctx._source.metrics.mth_end_snapshot_listing_count = params.time_series;
          """,
          "params": {
            "time_series": time_series
          }
        }
      }
  
  # Step 3: Execute bulk update
  print("Starting ES bulk update...")
  try:
    success, failed = bulk(processor.datastore.es, generate_snapshot_updates(), raise_on_error=False, raise_on_exception=False)
    print(f"✅ Successfully updated {success} documents")
    if failed:
      print(f"❌ Failed to update {len(failed)} documents")
      for failure in failed[:5]:  # Show first 5 failures
        print(f"Failed: {failure}")
    
    print("\n🎯 Historical month-end snapshot backfill complete!")
    return 0 if len(failed) == 0 else 1
    
  except Exception as e:
    print(f"❌ Bulk update failed: {e}")
    return 1

if __name__ == "__main__":
  exit(main())