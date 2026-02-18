from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path
from typing import Dict, Iterable, Tuple

import numpy as np
import pandas as pd
import joblib
from sklearn.calibration import CalibratedClassifierCV
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression


def _tokens(text: str) -> Iterable[str]:
  normalized = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
  return [tok for tok in normalized.split() if tok]


def _bigrams(tokens: Iterable[str]) -> Iterable[str]:
  tokens = list(tokens)
  return [f"{tokens[idx]}_{tokens[idx + 1]}" for idx in range(len(tokens) - 1)]


def extract_features(type_val: str, subtype_val: str, style_val: str) -> Dict[str, float]:
  t = "" if type_val is None else str(type_val).strip().lower()
  st = "" if subtype_val is None else str(subtype_val).strip().lower()
  sy = "" if style_val is None else str(style_val).strip().lower()
  combined = f"{t} {st} {sy}"

  feats: Dict[str, float] = {}

  def add(name: str, val: float = 1.0) -> None:
    feats[name] = feats.get(name, 0.0) + val

  exact_type = {
    "16-unit",
    "5-8 units",
    "9-12 units",
    "over 12 units",
    "duplex",
    "mobile",
    "other",
    "split-level",
    "resort property",
    "pre-fab modular",
    "maison de chambre",
    "multi family",
    "lots/acreage",
    "modular home",
    "n/r",
    "six",
  }
  exact_subtype = {
    "vacant lot/land",
    "land",
    "recreational",
    "residential",
    "modular home",
    "n/r",
    "other",
    "semi detached",
    "semi-detached",
  }
  exact_style = {
    "attached, multi level",
    "stacked",
    "multi/split",
    "park model",
    "float home",
    "hunt camp",
    "bi-level",
    "n/r",
    "other",
    "0",
    "one level",
    "two level",
  }

  add(f"type_eq:{t}", 1.0 if t in exact_type else 0.0)
  add(f"subtype_eq:{st}", 1.0 if st in exact_subtype else 0.0)
  add(f"style_eq:{sy}", 1.0 if sy in exact_style else 0.0)

  triple_matches = {
    ("multi family", "n/r", "attached, multi level"),
    ("residential", "one level", "stacked"),
    ("residential", "other", "stacked"),
    ("residential", "recreational", "multi/split"),
  }
  add(
    f"triple_eq:{t}|{st}|{sy}",
    1.0 if (t, st, sy) in triple_matches else 0.0,
  )

  add("subtype_has:see_remarks", 1.0 if "see remarks" in st else 0.0)
  add("type_not_residential", 1.0 if t and t != "residential" else 0.0)
  add("residential_subtype_nr", 1.0 if t == "residential" and st == "n/r" else 0.0)

  mobile_tokens = [
    "mobile",
    "mobile home",
    "manufactured",
    "double wide",
    "single wide",
    "park model",
    "mini home",
  ]
  for tok in mobile_tokens:
    add(f"type_has:{tok}", 1.0 if tok in t else 0.0)
    add(f"subtype_has:{tok}", 1.0 if tok in st else 0.0)
    add(f"style_has:{tok}", 1.0 if tok in sy else 0.0)

  add("type_has_duplex", 1.0 if "duplex" in t else 0.0)
  add("subtype_has_duplex", 1.0 if "duplex" in st else 0.0)
  add("style_has_duplex", 1.0 if "duplex" in sy else 0.0)
  add("type_has_half_duplex", 1.0 if "half duplex" in t or "1/2 duplex" in t else 0.0)
  add(
    "subtype_has_half_duplex",
    1.0 if "half duplex" in st or "1/2 duplex" in st else 0.0,
  )
  add("style_has_half_duplex", 1.0 if "half duplex" in sy or "1/2 duplex" in sy else 0.0)
  add(
    "half_duplex_any",
    1.0 if "half duplex" in combined or "1/2 duplex" in combined else 0.0,
  )

  add("type_has_apartment", 1.0 if "apartment" in t else 0.0)
  add("type_has_condo", 1.0 if "condo" in t else 0.0)
  add("type_has_multi_family", 1.0 if "multi" in t and "family" in t else 0.0)
  add(
    "duplex_allowed_type",
    1.0
    if "duplex" in t
    and "apartment" not in t
    and "condo" not in t
    and not ("multi" in t and "family" in t)
    else 0.0,
  )
  add(
    "duplex_blocked_type",
    1.0
    if "duplex" in t and ("apartment" in t or "condo" in t or ("multi" in t and "family" in t))
    else 0.0,
  )

  add("duplex_semi_type", 1.0 if "duplex" in t and "semi" in t else 0.0)
  add(
    "duplex_semi_style",
    1.0
    if ("duplex" in sy or "1/2 duplex" in sy or "half duplex" in sy) and "semi" in sy
    else 0.0,
  )
  add(
    "duplex_subtype_non_multi",
    1.0 if "duplex" in st and not ("multi" in t and "family" in t) else 0.0,
  )

  plex_patterns = [
    "triplex",
    "fourplex",
    "4 plex",
    "multiplex",
    "4plex",
    "5plex",
    "6plex",
    "7plex",
    "8plex",
    "duplex",
    "dixplex",
    "huitplex",
    "cinqplex",
    "uplex",
    " plex",
    "iplex",
    "-plex",
    "doublex",
    "quintplex",
    "quadrex",
    "fiveplex",
    "sixplex",
    "sevenplex",
    "eightplex",
    "tenplex",
    "12 apartments",
  ]
  for pattern in plex_patterns:
    add(f"combined_has:{pattern}", 1.0 if pattern in combined else 0.0)

  type_tokens = [
    "multifamily",
    "multi-family",
    "multi-residential",
    "vacant land",
    "land only",
    "lots/acreage",
    "duplex",
    "twnhouse",
    "townhouse",
    "row house",
    "row unit",
    "attached",
    "apartment",
    "condo",
    "co-op",
    "cooperative",
    "modular",
    "parking space",
    "locker",
    "other",
    "multi family",
    "commercial",
    "office",
  ]
  for tok in type_tokens:
    add(f"type_has:{tok}", 1.0 if tok in t else 0.0)

  comm_style_tokens = ["live/work", "warehouse", "industrial", "retail", "office"]
  for tok in comm_style_tokens:
    add(f"style_has:{tok}", 1.0 if tok in sy else 0.0)
    add(f"subtype_has:{tok}", 1.0 if tok in st else 0.0)

  subtype_tokens = [
    "semi detached",
    "semi-detached",
    "semi",
    "townhouse",
    "attached",
    "row house",
    "row unit",
    "detached",
    "condo",
    "condominium",
    "apartment",
    "modular",
    "manufactured",
    "other",
    "mobile",
  ]
  for tok in subtype_tokens:
    add(f"subtype_has:{tok}", 1.0 if tok in st else 0.0)

  style_tokens = [
    "semi",
    "attached",
    "townhouse",
    "row house",
    "row unit",
    "twnhouse",
    "stacked",
    "vacant",
    "land",
    "apartment",
    "condo",
    "condominium",
    "modular",
    "manufactured",
    "floating",
  ]
  for tok in style_tokens:
    add(f"style_has:{tok}", 1.0 if tok in sy else 0.0)

  add("style_has:townhome", 1.0 if "townhome" in sy else 0.0)
  add("style_has:end_unit", 1.0 if "end unit" in sy else 0.0)
  add("style_has:penthouse", 1.0 if "penthouse" in sy else 0.0)
  add("style_has:row", 1.0 if "row" in sy else 0.0)
  add("subtype_has:attached", 1.0 if "attached" in st else 0.0)
  add("style_has:attached", 1.0 if "attached" in sy else 0.0)
  add("attached_subtype", 1.0 if "attached" in st else 0.0)
  add("attached_subtype_penthouse", 1.0 if "attached" in st and "penthouse" in sy else 0.0)
  add(
    "attached_subtype_row_townhouse_style",
    1.0
    if "attached" in st
    and ("row" in sy or "townhouse" in sy or "twnhouse" in sy)
    else 0.0,
  )

  standalone_styles = [
    "bungalow",
    "storey",
    "story",
    "bi-level",
    "split",
    "raised",
    "tri-level",
  ]
  for kw in standalone_styles:
    add(f"style_has_standalone:{kw}", 1.0 if kw in sy else 0.0)
  add(
    "attached_subtype_standalone_style",
    1.0 if "attached" in st and any(kw in sy for kw in standalone_styles) else 0.0,
  )
  add(
    "attached_subtype_no_row_townhouse_style",
    1.0
    if "attached" in st and not ("row" in sy or "townhouse" in sy or "twnhouse" in sy)
    else 0.0,
  )

  known_style_markers = (
    mobile_tokens
    + comm_style_tokens
    + style_tokens
    + standalone_styles
    + ["duplex", "plex", "1/2 duplex", "half duplex"]
  )
  style_unknown = not any(marker in sy for marker in known_style_markers)
  add("style_unknown", 1.0 if style_unknown else 0.0)
  add("subtype_nr_style_unknown", 1.0 if st == "n/r" and style_unknown else 0.0)
  add(
    "residential_nr_style_unknown",
    1.0 if t == "residential" and st == "n/r" and style_unknown else 0.0,
  )
  add("residential_nr_style_other", 1.0 if t == "residential" and st == "n/r" and sy == "other" else 0.0)
  add(
    "residential_nr_style_nr_other_0",
    1.0 if t == "residential" and st == "n/r" and sy in {"n/r", "other", "0"} else 0.0,
  )

  add("type_has:carriage", 1.0 if "carriage" in t else 0.0)
  add("subtype_has:carriage", 1.0 if "carriage" in st else 0.0)

  common_detached_keywords = [
    "ranch",
    "raised ranch",
    "back split",
    "side split",
    "split level",
    "split-level",
    "split entry",
    "bi-level",
    "blevl",
    "bng",
    "bngr",
    "bung",
    "bungr",
    "a-frame",
    "a frame",
    "chalet",
    "cabin",
    "camp",
    "log home",
    "log",
    "villa",
    "patio home",
    "acreage",
    "summer home",
    "west coast",
    "westcoast",
    "contemporary",
    "cape cod",
    "cbovr",
    "character",
    "main lev ent",
    "grd lev ent",
  ]
  for kw in common_detached_keywords:
    add(f"combined_has:{kw}", 1.0 if kw in combined else 0.0)

  detached_type_tokens = [
    "residential",
    "other",
    "resort property",
    "pre-fab modular",
    "split-level",
    "maison de chambre",
  ]
  add("type_is_detached_group", 1.0 if t in detached_type_tokens else 0.0)
  add("type_is_maison_de_chambre", 1.0 if t == "maison de chambre" else 0.0)
  add("subtype_is_recreational", 1.0 if st == "recreational" else 0.0)
  add("subtype_is_residential", 1.0 if st == "residential" else 0.0)
  add("residential_modular_subtype", 1.0 if t == "residential" and st == "modular home" else 0.0)
  recreational_exceptions = {"park model", "float home", "hunt camp", "bi-level"}
  add(
    "recreational_style_exception",
    1.0 if t == "residential" and st == "recreational" and sy in recreational_exceptions else 0.0,
  )

  residential_style_keywords = [
    "sp2s",
    "sp3l",
    "sp4l",
    "spml",
    "split",
    "split 3 level",
    "split 4 level",
    "two",
    "two level",
    "one level",
    "multi level",
    "1+1/2",
    "1+3/4",
    "2+1/2",
    "loft",
    "arts and crafts",
    "arts  and  crafts",
  ]
  for kw in residential_style_keywords:
    add(f"style_has:{kw}", 1.0 if kw in sy else 0.0)

  other_keywords = [
    "plex",
    "logements",
    "logis",
    "dwellings",
    "housing",
    "rooms",
    "appartments",
    "appts",
    "commerces",
    "multi family",
    "multi-family",
    "multi-plex",
    "multiplez",
    "quadrex",
    "farm",
    "farm/ranch",
    "hobby farm",
    "float home",
    "floating home",
    "commercail",
    "commercial",
    "mixed use",
    "timeshare",
    "timeshares",
    "tourisme residence",
    "tourism residence",
    "seniors residence",
    "shared owner",
    "prk. stall",
    "prk stall",
    "b and b",
    "b  and  b",
    "alberta",
  ]
  for kw in other_keywords:
    add(f"type_has:{kw}", 1.0 if kw in t else 0.0)
    add(f"subtype_has:{kw}", 1.0 if kw in st else 0.0)
    add(f"style_has:{kw}", 1.0 if kw in sy else 0.0)

  add("type_has_digit", 1.0 if any(ch.isdigit() for ch in t) else 0.0)
  add(
    "type_digit_non_whitelist",
    1.0
    if any(ch.isdigit() for ch in t)
    and t not in {"residential", "other", "multi family", "n/r", "lots/acreage"}
    else 0.0,
  )

  add(
    "t_res_other_sy_n_r",
    1.0 if t == "residential" and st == "other" and sy in {"other", "n/r"} else 0.0,
  )
  add(
    "t_other_nr_sy_level",
    1.0 if t == "other" and st == "n/r" and sy in {"one level", "two level"} else 0.0,
  )
  add(
    "t_res_other_sy_othernr",
    1.0 if t == "residential" and st == "other" and sy in {"other", "n/r"} else 0.0,
  )
  add(
    "t_st_nr_sy_nr_other_0",
    1.0
    if t in {"residential", "other"} and st == "n/r" and sy in {"n/r", "other", "0"}
    else 0.0,
  )
  add("t_eq_nr", 1.0 if t == "n/r" else 0.0)
  add("triple_res_nr_nr", 1.0 if t == "residential" and st == "n/r" and sy == "n/r" else 0.0)
  add(
    "triple_res_nr_other",
    1.0 if t == "residential" and st == "n/r" and sy == "other" else 0.0,
  )
  add(
    "t_res_or_other_nr_other_0",
    1.0
    if t in {"residential", "other"} and st == "n/r" and sy in {"n/r", "other", "0"}
    else 0.0,
  )
  add("t_other_nr_level", 1.0 if t == "other" and st == "n/r" and sy in {"one level", "two level"} else 0.0)

  keyword_patterns = {
    "CONDO": [
      "condo",
      "apartment",
      "unit",
      "co-op",
      "cooperative",
      "comm element",
      "bachelor",
      "studio",
      "multi-level",
      "leasehold",
      "stacked townhse",
      "apt",
    ],
    "SEMI-DETACHED": [
      "semi",
      "half duplex",
      "1/2 duplex",
      "semi detached",
      "semi-detached",
      "over-under",
      "side by side",
    ],
    "TOWNHOUSE": [
      "townhouse",
      "row",
      "row/townhouse",
      "town house",
      "condo townhouse",
      "link",
    ],
    "DETACHED": [
      "detached",
      "single family",
      "house",
      "bungalow",
      "bungaloft",
      "cottage",
      "2-storey",
      "storey",
      "rancher",
      "backsplit",
      "sidesplit",
    ],
    "MOBILE_MINI": [
      "mobile",
      "mini",
      "mobile / mini",
    ],
    "LAND_VACANT": [
      "land only",
      "vacant land",
    ],
    "COMMERCIAL": [
      "store",
      "office",
      "commercial",
      "store w/apt/office",
      "5-8 units",
      "9-12 units",
      "over 12 units",
    ],
    "OTHER": [
      "locker",
      "parking space",
      "triplex",
      "fourplex",
      "4 plex",
      "multiplex",
      "duplex",
      "full duplex",
      "modular home",
      "manufactured",
      "modular",
    ],
  }
  for cat, keywords in keyword_patterns.items():
    t_count = sum(1 for kw in keywords if kw in t)
    st_count = sum(1 for kw in keywords if kw in st)
    sy_count = sum(1 for kw in keywords if kw in sy)
    add(f"{cat}_kw_type_count", float(t_count))
    add(f"{cat}_kw_subtype_count", float(st_count))
    add(f"{cat}_kw_style_count", float(sy_count))
    add(f"{cat}_kw_total", float(t_count + st_count + sy_count))

  large_multi_unit_patterns = [
    "multifamily",
    "multi-family",
    "multi-residential",
    "multi-6-9 unit",
  ]
  add(
    "combined_has_large_multi",
    1.0 if any(p in combined for p in large_multi_unit_patterns) else 0.0,
  )

  add(
    "ambiguous_end_unit",
    1.0
    if "end unit" in combined
    and st in {"other", "n/r"}
    and not any(k in combined for k in ["apartment", "condo", "townhouse", "row"])
    else 0.0,
  )

  for tok in _tokens(t):
    if tok != "residential":
      add(f"type_tok:{tok}")
  for tok in _tokens(st):
    add(f"subtype_tok:{tok}")
  for tok in _tokens(sy):
    add(f"style_tok:{tok}")

  for bg in _bigrams(_tokens(t)):
    if "residential" not in bg:
      add(f"type_bg:{bg}")
  for bg in _bigrams(_tokens(st)):
    add(f"subtype_bg:{bg}")
  for bg in _bigrams(_tokens(sy)):
    add(f"style_bg:{bg}")

  return feats


@dataclass
class IMSPropertyTypeModel:
  class_weight: str | None = "balanced"
  max_iter: int = 2000
  cv: int = 3

  vec: DictVectorizer | None = None
  model: CalibratedClassifierCV | None = None

  def fit(self, df: pd.DataFrame, label_col: str = "Inferred") -> "IMSPropertyTypeModel":
    if label_col not in df.columns:
      raise ValueError(f"Missing label column: {label_col}")

    train_df = df[df[label_col] != "UNCLEAR"].reset_index(drop=True)
    features = [
      extract_features(row["Type"], row["SubType"], row["Style"])
      for _, row in train_df.iterrows()
    ]
    labels = train_df[label_col].values

    self.vec = DictVectorizer(sparse=True)
    Xv = self.vec.fit_transform(features)

    base = LogisticRegression(
      max_iter=self.max_iter,
      multi_class="multinomial",
      class_weight=self.class_weight,
    )
    self.model = CalibratedClassifierCV(base, method="sigmoid", cv=self.cv)
    self.model.fit(Xv, labels)
    return self

  def predict_with_uncertainty(
    self, type_val: str, subtype_val: str, style_val: str, min_conf: float = 0.5
  ) -> Tuple[str, Dict[str, float]]:
    if self.vec is None or self.model is None:
      raise RuntimeError("Model not fitted. Call fit(...) first.")

    X = self.vec.transform([extract_features(type_val, subtype_val, style_val)])
    probs = self.model.predict_proba(X)[0]
    classes = self.model.classes_
    prob_dict = dict(zip(classes, probs))

    max_prob = max(prob_dict.values())
    if max_prob < min_conf:
      return "UNCLEAR", prob_dict

    label = classes[int(np.argmax(probs))]
    return label, prob_dict

  def save(self, path: Path | str) -> None:
    joblib.dump(self, path)

  @staticmethod
  def load(path: Path | str) -> "IMSPropertyTypeModel":
    return joblib.load(path)


@dataclass
class IMSPropertyTypeTreeModel:
  max_iter: int = 300
  max_depth: int | None = None
  learning_rate: float = 0.1
  l2_regularization: float = 0.0
  cv: int = 3
  calibrate: bool = True

  vec: DictVectorizer | None = None
  model: object | None = None

  def fit(
    self,
    df: pd.DataFrame,
    label_col: str = "Inferred",
    sample_weight: np.ndarray | None = None,
  ) -> "IMSPropertyTypeTreeModel":
    if label_col not in df.columns:
      raise ValueError(f"Missing label column: {label_col}")

    train_df = df[df[label_col] != "UNCLEAR"].reset_index(drop=True)
    features = [
      extract_features(row["Type"], row["SubType"], row["Style"])
      for _, row in train_df.iterrows()
    ]
    labels = train_df[label_col].values

    if sample_weight is not None and len(sample_weight) != len(train_df):
      raise ValueError("sample_weight must match training row count.")

    self.vec = DictVectorizer(sparse=True)
    Xv = self.vec.fit_transform(features)

    base = HistGradientBoostingClassifier(
      max_iter=self.max_iter,
      max_depth=self.max_depth,
      learning_rate=self.learning_rate,
      l2_regularization=self.l2_regularization,
    )
    if self.calibrate:
      self.model = CalibratedClassifierCV(base, method="sigmoid", cv=self.cv)
      self.model.fit(Xv.toarray(), labels, sample_weight=sample_weight)
    else:
      self.model = base
      self.model.fit(Xv.toarray(), labels, sample_weight=sample_weight)
    return self

  def predict_with_uncertainty(
    self, type_val: str, subtype_val: str, style_val: str, min_conf: float = 0.5
  ) -> Tuple[str, Dict[str, float]]:
    if self.vec is None or self.model is None:
      raise RuntimeError("Model not fitted. Call fit(...) first.")

    X = self.vec.transform([extract_features(type_val, subtype_val, style_val)])
    probs = self.model.predict_proba(X.toarray())[0]
    classes = self.model.classes_
    prob_dict = dict(zip(classes, probs))

    max_prob = max(prob_dict.values())
    if max_prob < min_conf:
      return "UNCLEAR", prob_dict

    label = classes[int(np.argmax(probs))]
    return label, prob_dict

  def save(self, path: Path | str) -> None:
    joblib.dump(self, path)

  @staticmethod
  def load(path: Path | str) -> "IMSPropertyTypeTreeModel":
    return joblib.load(path)


@dataclass
class IMSPropertyTypeEnsembleModel:
  lr: IMSPropertyTypeModel | None = None
  tree: IMSPropertyTypeTreeModel | None = None

  def fit(self, df: pd.DataFrame, label_col: str = "Inferred") -> "IMSPropertyTypeEnsembleModel":
    self.lr = IMSPropertyTypeModel()
    self.tree = IMSPropertyTypeTreeModel()
    # Tuning hints (not defaults):
    # self.lr = IMSPropertyTypeModel(max_iter=10000)
    # self.tree = IMSPropertyTypeTreeModel(max_iter=80, max_depth=6, learning_rate=0.1)
    self.lr.fit(df, label_col=label_col)
    self.tree.fit(df, label_col=label_col)
    return self

  def predict_with_uncertainty(
    self, type_val: str, subtype_val: str, style_val: str, min_conf: float = 0.5
  ) -> Tuple[str, Dict[str, float]]:
    if self.lr is None or self.tree is None:
      raise RuntimeError("Model not fitted. Call fit(...) first.")

    X = self.lr.vec.transform([extract_features(type_val, subtype_val, style_val)])

    lr_probs = self.lr.model.predict_proba(X)[0]
    lr_classes = self.lr.model.classes_

    tree_probs = self.tree.model.predict_proba(X.toarray())[0]
    tree_classes = self.tree.model.classes_

    if list(lr_classes) != list(tree_classes):
      raise ValueError("Class order mismatch between LR and tree models.")

    avg_probs = (lr_probs + tree_probs) / 2.0
    prob_dict = dict(zip(lr_classes, avg_probs))

    max_prob = max(prob_dict.values())
    if max_prob < min_conf:
      return "UNCLEAR", prob_dict

    label = lr_classes[int(np.argmax(avg_probs))]
    return label, prob_dict

  def predict_proba(self, Xv) -> np.ndarray:
    if self.lr is None or self.tree is None:
      raise RuntimeError("Model not fitted. Call fit(...) first.")

    lr_probs = self.lr.model.predict_proba(Xv)
    tree_probs = self.tree.model.predict_proba(Xv.toarray())
    return (lr_probs + tree_probs) / 2.0

  def predict(self, Xv) -> np.ndarray:
    avg_probs = self.predict_proba(Xv)
    classes = self.lr.model.classes_
    return classes[avg_probs.argmax(axis=1)]

  def evaluate_dataframe(
    self,
    df: pd.DataFrame,
    label_col: str = "Inferred",
    min_conf: float = 0.0,
  ) -> Tuple[float, pd.DataFrame]:
    if self.lr is None or self.tree is None:
      raise RuntimeError("Model not fitted. Call fit(...) first.")
    if label_col not in df.columns:
      raise ValueError(f"Missing label column: {label_col}")

    Xv = self.lr.vec.transform([
      extract_features(row["Type"], row["SubType"], row["Style"])
      for _, row in df.iterrows()
    ])
    avg_probs = self.predict_proba(Xv)
    classes = self.lr.model.classes_
    pred = classes[avg_probs.argmax(axis=1)]
    p_pred = avg_probs.max(axis=1)

    if min_conf > 0.0:
      pred = np.where(p_pred < min_conf, "UNCLEAR", pred)

    accuracy = (pred == df[label_col].values).mean()
    result_df = df.assign(pred=pred, p_pred=p_pred)
    mismatch = result_df[result_df["pred"] != df[label_col]]
    return accuracy, mismatch

  def save(self, path: Path | str) -> None:
    joblib.dump(self, path)

  @staticmethod
  def load(path: Path | str) -> "IMSPropertyTypeEnsembleModel":
    return joblib.load(path)
