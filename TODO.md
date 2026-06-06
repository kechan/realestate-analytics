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
