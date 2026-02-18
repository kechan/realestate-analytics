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
