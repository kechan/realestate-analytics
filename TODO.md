- **[HIGH PRIORITY] `geo_collection.dill` must be updated whenever we add support for a new geog_id level/tier, or `ProvinceGeogIdValidator` will silently drop 100% of that level's data as "cross-province contamination."** `ProvinceGeogIdValidator` (`realestate_analytics/utils/validators.py`) builds its `valid_geog_ids` allow-list from `geo_collection.dill` (loaded/generated via `Datastore._ensure_geo_collection()` in `es.py`, cache-first — it does not re-derive itself automatically). The file is an accurate snapshot of the geo taxonomy as of whenever it was last generated; it isn't wrong or broken, it just needs to be regenerated as a deliberate step any time we start supporting a geog_id level it doesn't yet cover. A geog_id from a level the file has never been updated to include gets treated identically to a genuine wrong-province geog_id and filtered out.

  **Confirmed 2026-08-25** (while testing the `AbsorptionRateProcessor` live-listing-count fetch against local ES + the current local `geo_collection.dill`): filtering 96,352 live ON active listings dropped 119,075 of 366,967 exploded `(listing, geog_id)` rows (32.45%) as "contamination." Verified these are not real contamination — every dropped row's `provState` was `ON`, and every sampled city/neighbourhood was a genuine Ontario place. Cause: the `g37`-level geog_id tier isn't covered by that `geo_collection.dill` at all (0 of ~9,859 geos), so every listing referencing a `g37` geog_id has that specific `(listing, g37-geog_id)` tuple dropped — plus a smaller number of `g10`/`g20`/`g40` geog_ids not yet covered either.

  **Impact:** any geog_id level we start tracking without first updating `geo_collection.dill` to include it will have its `current_count`/metrics silently come out as ~zero everywhere `ProvinceGeogIdValidator` is used (`LastMthMetricsProcessor`, `SoldMedianMetricsProcessor`, and `AbsorptionRateProcessor`'s month-end snapshot), with no error — just quietly wrong numbers for that level, for every province.

  **Action needed:** (1) whenever a new geog_id level/tier is added to what we support or track, regenerate/update `geo_collection.dill` to include it as part of that work, not as an afterthought; (2) audit whether `g37` (and any other currently-uncovered levels) are actually meant to be tracked in `rlp_mkt_trends_current` today — if not tracked, the drops are currently harmless as-is.

- **Enhance `IMSPropertyTypeMapper` to use auxiliary attributes (sqft, price) as secondary signals** for ambiguous tuples where the 3-tuple alone is insufficient. Currently the mapper decides solely on `(Type, SubType, Style)` + Province, which causes systematic misclassification for a known pattern:

  **Example — `Residential / N/R / 3-storey` in Ontario:**
  - Classified as `DETACHED` by current mapper (SubType is `N/R`, no signal to distinguish)
  - In practice, 90% of these are **freehold townhouses** (~1,500 sqft, $700K–$1.1M) — confirmed in Cornell, Markham where 215/647 DETACHED listings (33%) are this combo
  - The remaining 10% are genuine large detached homes (sqft > 2,500, $2.7M–$7.7M) in Toronto/Oakville/Halton Hills
  - A sqft threshold (e.g. ≤ 2,000 → TOWNHOUSE, > 2,000 → DETACHED) would correctly split these two populations

  **Proposed approach:**
  - Extend mapper to accept optional `sqft` and/or `price` as secondary signals for flagged ambiguous tuples
  - Define a small set of "ambiguous combos" that trigger the secondary-signal path
  - This is a significant architectural change to the mapper (currently stateless per-tuple) — requires careful design to avoid adding noise to well-classified tuples

  **Impact:** Fixes DETACHED median price inflation in new Cornell-style communities across GTA (Cornell, Stouffville, Milton, Brampton, etc.) where freehold 3-storey townhomes are common.

- **Refactor `es.py`**: file is too long, making it hard to reason about the full pipeline. Natural split points:
  - Extract IMS bulk import/update methods (`bulk_import_ims_sold_listings`, `bulk_update_ims_sold_listings`) into a dedicated `ims_ingest.py` module — these are the largest and most complex methods
  - Extract index/schema management (index creation, alias management, schema definitions) into a separate module
  - Query/search operations are another natural grouping
- **Add regex pattern to `suggest_mapping()`** in `ims.py` to catch multi-unit Type values in `N Units`, `N-plex`, `Nplex` formats (e.g. `6 Units`, `6-plex`, `Sixplex`) and map them to `COMMERCIAL` — currently only range formats like `5-8 units` are handled, causing some multi-unit types to fall through to wrong classifications
- **Cleanup IMS canonical schema drift**: `computed_guid` appears in IMS canonical column lists (`ims_transformers.py`, `get_ims_sold_listings()` empty-result schema) even though IMS import/read paths only populate and use `guid`. This is not a current runtime bug, but it is legacy non-IMS baggage and should eventually be either removed from the IMS canonical schema or initialized explicitly to `None` for consistency.

- Add override entries for low-prevalence IMS mapper mismatches (quadrex, residential mixed use, residential ranch + townhome, duplex storey conflict) in `ims_property_type_lookup_df` if we choose to use the tree model instead of `suggest_mapping()`.
- Decide whether to align ML mapper to cache overrides or updated `suggest_mapping` duplex rules before switching to ML-based mapping.
- ML mapping experiment (not for prod yet): use `IMSPropertyTypeEnsembleModel.evaluate_dataframe(..., min_conf=...)` on new IMS dataframes. For tuples not in the cache, stamp model predictions into ES; log `(Type, SubType, Style, pred, p_pred)` for review and prioritize by lowest `p_pred`. Any `pred == "UNCLEAR"` should be logged as unclear for manual review. Corrections should go to `ims_property_type_lookup_df`; retrain the model on cache + overrides if that table grows too large.
- Starter snippet (offline, ensemble only):
  ```python
  from realestate_analytics.utils.ims_ml import IMSPropertyTypeEnsembleModel, extract_features
  import numpy as np

  model = IMSPropertyTypeEnsembleModel()
  model.fit(mapping_df, label_col="Inferred")

  Xv = model.lr.vec.transform([
    extract_features(row["Type"], row["SubType"], row["Style"])
    for _, row in new_tuples_df.iterrows()
  ])
  lr_probs = model.lr.model.predict_proba(Xv)
  tree_probs = model.tree.model.predict_proba(Xv.toarray())
  classes = model.lr.model.classes_
  avg_probs = (lr_probs + tree_probs) / 2.0
  pred = classes[avg_probs.argmax(axis=1)]
  p_pred = avg_probs.max(axis=1)

  review_df = new_tuples_df.assign(pred=pred, p_pred=p_pred).sort_values("p_pred")
  ```

- **Investigate stale `last_mth_median_asking_price`/`mth_end_snapshot_listing_count` for specific geog_id/propertyType combos in `rlp_mkt_trends_current`.** Most docs update monthly as expected (July 2026 was current everywhere else checked), but some CONDO-specific docs are lagging by multiple months, with count and price sometimes stale by *different* amounts within the same doc — suggesting `LastMthMetricsProcessor` (price) and `AbsorptionRateProcessor` (count) are independently failing/skipping for these specific geog_id/propertyType combos, not just running late together.

  **Confirmed examples (found 2026-08-21, via `realestate-content-transformer`'s v1 city-page rewrite spot-checks):**
  - `g30_f80t7mzs_CONDO` (Fredericton, NB): count **and** price both last updated `2026-04` (3 months stale vs. the `2026-07` current month everywhere else for this geog_id — `ALL`/`DETACHED` are both current).
  - `g30_f84q8h7q_CONDO` (Beaubassin East, NB): count last updated `2026-04`, but price last updated `2026-01` — **6 months stale**, and notably staler than its own count field within the same doc.

  **Impact:** Downstream consumers that only validate *sample size* (e.g. `realestate-content-transformer`'s `get_market_trend_metrics()`, which suppresses a median if its listing count is `< 5`) have no way to detect this — a stale price with a healthy listing count passes that check silently. Concretely, Fredericton's city page currently publishes a condo median of $529,900 sourced from April data, presented as if current. This was flagged as an accepted risk when `realestate-content-transformer`'s v1 was scoped (stale-data detection was explicitly out of scope), but a 6-month gap is worse than what was originally envisioned ("last month's value" staleness), so it's worth understanding the actual failure mode rather than continuing to just accept it.

  **Suggested next step:** Check whether these specific geog_id/propertyType combos have low/zero listing volume for CONDO in a given month (which could cause the processor to skip writing an update rather than write a null/zero), vs. a genuine processing failure/exception being swallowed. If it's the former, that's arguably expected behavior and the real fix belongs in the *consumer* (flag staleness, not just sample size); if it's the latter, that's a real bug in `LastMthMetricsProcessor`/`AbsorptionRateProcessor` worth fixing directly.
