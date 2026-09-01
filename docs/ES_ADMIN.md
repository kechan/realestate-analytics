# Elasticsearch Admin — Local Dev Reference

## Disk Full: Bulk Update Fails with `cluster_block_exception`

**Symptom:** During bulk update you see `0/N successful (0.00%)` with errors like:
```
cluster_block_exception: index [rlp_ims_sold_listing_1] blocked by:
[TOO_MANY_REQUESTS/12/disk usage exceeded flood-stage watermark,
index has read-only-allow-delete block]
```

**Root cause:** Local ES disk hit the flood-stage watermark (default 95%), causing ES to put all indices into read-only mode. Deleted documents from previous bulk updates accumulate and waste disk space (segment bloat).

**Fix — run in this order:**

```bash
# 1. Check disk usage and deleted doc counts
curl -s 'localhost:9200/_cat/allocation?v&h=node,disk.indices,disk.used,disk.avail,disk.total,disk.percent'
curl -s 'localhost:9200/_cat/indices?v&h=index,docs.count,docs.deleted,store.size' | grep -v "       0"

# 2. Raise flood-stage watermark to give room to operate
curl -X PUT 'localhost:9200/_cluster/settings' -H 'Content-Type: application/json' -d '{
  "persistent": {
    "cluster.routing.allocation.disk.watermark.low": "95%",
    "cluster.routing.allocation.disk.watermark.high": "97%",
    "cluster.routing.allocation.disk.watermark.flood_stage": "99%"
  }
}'

# 3. Remove read-only blocks from all indices  ← bulk updates work again after this
curl -X PUT 'localhost:9200/_all/_settings' -H 'Content-Type: application/json' -d '{
  "index.blocks.read_only_allow_delete": null
}'

# 4. Forcemerge to reclaim disk space (expunge deleted docs)
# Run in background — rlp_ims_sold_listing_1 first (smaller, faster)
curl -X POST 'localhost:9200/rlp_ims_sold_listing_1/_forcemerge?only_expunge_deletes=true' &
curl -X POST 'localhost:9200/rlp_mkt_trends_current/_forcemerge?only_expunge_deletes=true' &
# Note: the current market-trends backing index has ~629K deleted docs — leave overnight if needed

# 5. After disk usage drops, restore watermark to defaults
curl -X PUT 'localhost:9200/_cluster/settings' -H 'Content-Type: application/json' -d '{
  "persistent": {
    "cluster.routing.allocation.disk.watermark.low": "85%",
    "cluster.routing.allocation.disk.watermark.high": "90%",
    "cluster.routing.allocation.disk.watermark.flood_stage": "95%"
  }
}'
```

**After step 3, bulk updates will work again immediately** — no need to wait for forcemerge to complete.

**Key terms:** flood-stage watermark, read-only-allow-delete block, segment bloat, forcemerge, deleted documents

## Adding a New Field to `MKT_TRENDS_INDEX_MAPPING` — Must Also Be Patched onto Existing Indices

**Root cause of the pattern:** `create_mkt_trends_index()` (`realestate_analytics/data/es.py`) only applies
`MKT_TRENDS_INDEX_MAPPING` (`realestate_analytics/data/es_index_schemas.py`) when the index doesn't exist yet
(`if not self.es.indices.exists(...): create(...)`). Editing the schema file has **no effect on an already-existing
index** — there is no code path that ever calls `put_mapping` for an update. If a new metrics field is added to a
processor's ES write logic but the index it writes to already exists (true for local/UAT/PROD's long-lived
`rlp_mkt_trends_*` indices), the field only gets defined implicitly the first time a script-based update touches it
— and Elasticsearch's dynamic mapping infers a plain `object` type for an array of `{month, value}` docs, never
`nested` (which is only ever set explicitly, never inferred). This silently produces a field whose mapping is
inconsistent with its siblings (e.g. `last_mth_median_asking_price`, which *is* `nested`), degrading nested-query
support for that one field.

**2026-09-01 incident:** `last_mth_min_asking_price` / `last_mth_max_asking_price` were added to
`LastMthMetricsProcessor` without a matching entry in `MKT_TRENDS_INDEX_MAPPING`. Local ES (used for dev testing)
already shows this exact drift — both fields exist there as implicit `object`, not `nested`. Caught before PROD's
index behind the `rlp_mkt_trends_current` alias ever wrote these fields for the first time (PROD's monthly
`end_of_mth_run()` had not yet run against this code), so PROD was patched directly via
`put_mapping` — safe specifically because the fields had **no prior mapping on PROD**; this only works for adding a
net-new field, not for correcting one that already has data (that requires a reindex). Local ES's existing
`object`-typed data was left as-is (dev-only, low stakes).

**Fix — for any new nested metrics field going forward, after adding it to `MKT_TRENDS_INDEX_MAPPING`:**

Before the mutating mapping update, confirm the alias resolves to exactly one backing index:

```bash
. ~/.rlp_es_env && curl -s -H "Authorization: ApiKey $RLP_LISTING_ES_API_KEY" \
  "https://esp.royallepage.ca:443/_alias/rlp_mkt_trends_current" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); assert len(d)==1, f'expected one backing index, got {list(d)}'; print(next(iter(d)))"
```

Then apply the mapping through the stable alias:

```bash
. ~/.rlp_es_env && curl -s -X PUT -H "Authorization: ApiKey $RLP_LISTING_ES_API_KEY" -H "Content-Type: application/json" \
  "https://esp.royallepage.ca:443/rlp_mkt_trends_current/_mapping" \
  -d '{
  "properties": {
    "metrics": {
      "properties": {
        "<new_field_name>": {
          "type": "nested",
          "properties": {
            "month": {"type": "date", "format": "yyyy-MM"},
            "value": {"type": "float"}
          }
        }
      }
    }
  }
}'
```

Run this **before** the first ETL write to the new field, using each environment's
`rlp_mkt_trends_current` alias. Verify the field mapping without assuming a particular backing-index version:

```bash
curl -s -H "Authorization: ApiKey $RLP_LISTING_ES_API_KEY" "https://esp.royallepage.ca:443/rlp_mkt_trends_current/_mapping" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); assert len(d)==1, f'expected one backing index, got {list(d)}'; m=next(iter(d.values())); print(m['mappings']['properties']['metrics']['properties']['<new_field_name>'])"
```

**Key terms:** dynamic mapping, nested vs object type, put_mapping, index-creation-only schema application
