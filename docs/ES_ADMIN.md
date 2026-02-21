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
curl -X POST 'localhost:9200/rlp_mkt_trends_1/_forcemerge?only_expunge_deletes=true' &
# Note: rlp_mkt_trends_1 has ~629K deleted docs — leave overnight if needed

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
