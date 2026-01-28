from typing import Optional, Dict, Tuple, List, Union
from pathlib import Path
from dataclasses import dataclass
import pandas as pd
import re


@dataclass(frozen=True)
class _Agg:
  sum_lat: float
  sum_lng: float
  n: int

  def add(self, lat: float, lng: float) -> "_Agg":
    return _Agg(self.sum_lat + lat, self.sum_lng + lng, self.n + 1)

  def mean(self) -> Tuple[float, float]:
    return (self.sum_lat / self.n, self.sum_lng / self.n)


class OdaOfflineGeocoder:
  """
  Offline, deterministic geocoder backed by Statistics Canada ODA CSVs (one file per province).

  Resolution order:
  1) postal_code centroid (if postal_code provided and known)
  2) address-point match by (province, city, normalized_street_key, street_no)
     - if exact street_no exists: use it
     - else interpolate between nearest street_nos on same street within configured limits

  Notes:
  - Builds lookups on-the-fly per instance (no simplified CSV caching).
  - Uses streaming aggregation (means) to keep memory footprint down.
    If you later want medians, you’ll need a heavier aggregation strategy.
  - Assumes IMS address format for non-geocoded listings ends with street number:
      "Abbeydale Cir 334"
  """

  _SUFFIX_NO_RE = re.compile(r"^(?P<street>.+?)\s+(?P<no>\d+)\s*$")

  def __init__(
    self,
    oda_dir: Union[str, Path],
    *,
    min_points_for_street: int = 4,
    max_interpolate_house_no_gap: int = 30,
  ):
    self.oda_dir = Path(oda_dir).expanduser().resolve()
    self.min_points_for_street = int(min_points_for_street)
    self.max_interpolate_house_no_gap = int(max_interpolate_house_no_gap)

    # Aggregated stats (streaming mean) to reduce memory:
    # postal_code -> _Agg
    self._pc_agg: Dict[str, _Agg] = {}
    # (prov, city_key, street_key, street_no) -> _Agg
    self._addr_agg: Dict[Tuple[str, str, str, int], _Agg] = {}

    # Built after ingest:
    # postal_code -> (lat, lng)
    self._pc_centroid: Dict[str, Tuple[float, float]] = {}
    # (prov, city_key, street_key) -> sorted list of (street_no, lat, lng, n_points)
    self._street_index: Dict[Tuple[str, str, str], List[Tuple[int, float, float, int]]] = {}

    self._ingest_all_provinces()
    self._finalize_indexes()

  def get_coord(
    self,
    *,
    province: str,
    city: str,
    address: str,
    postal_code: Optional[str] = None,
  ) -> Optional[Tuple[float, float]]:
    prov = self._norm_province(province)
    city_key = self._norm_city(city)

    # 1) Postal centroid
    if postal_code:
      pc = self._normalize_postal_code(postal_code)
      if pc:
        hit = self._pc_centroid.get(pc)
        if hit is not None:
          return hit

    # 2) Street + street_no path (with interpolation)
    parsed = self._parse_ims_suffix_street_no(address)
    if parsed is None:
      return None

    street_key, street_no = parsed
    seg_key = (prov, city_key, street_key)

    points = self._street_index.get(seg_key)
    if not points:
      return None

    # Fast exact match via binary search over sorted street_no
    exact = self._find_exact(points, street_no)
    if exact is not None:
      return (exact[1], exact[2])  # lat, lng

    # Interpolate / snap between nearest neighbors
    lower, upper = self._find_bracketing(points, street_no)
    if lower is None and upper is None:
      return None

    # If only one side exists, snap to nearest (still mark as "interpolated" externally if you track method)
    if lower is None:
      # upper exists
      if abs(upper[0] - street_no) > self.max_interpolate_house_no_gap:
        return None
      return (upper[1], upper[2])

    if upper is None:
      # lower exists
      if abs(street_no - lower[0]) > self.max_interpolate_house_no_gap:
        return None
      return (lower[1], lower[2])

    # Both sides exist: enforce local gap constraint
    gap = upper[0] - lower[0]
    if gap <= 0:
      return None
    if gap > self.max_interpolate_house_no_gap:
      return None

    # Linear interpolation in house-number space
    # (Good enough for neighbourhood-level analytics; street geometry is handled implicitly by nearby points.)
    t = (street_no - lower[0]) / gap
    lat = lower[1] + t * (upper[1] - lower[1])
    lng = lower[2] + t * (upper[2] - lower[2])
    return (lat, lng)

  # ---------------------------
  # Ingest + index build
  # ---------------------------

  def _ingest_all_provinces(self) -> None:
    # Expect files like ODA_ON_v1.csv, ODA_AB_v1.csv, etc.
    csv_paths = sorted(self.oda_dir.glob("ODA_*_v1.csv"))

    usecols = [
      "latitude",
      "longitude",
      "postal_code",
      "street_no",
      "street",
      "city",
      "city_pcs",
      "csdname",
      "str_name_pcs",
      "str_type_pcs",
      "str_dir_pcs",
    ]

    # Chunked read keeps peak memory down while still staying straightforward.
    for csv_path in csv_paths:
      prov = self._province_from_filename(csv_path.name)

      for chunk in pd.read_csv(
        csv_path,
        usecols=usecols,
        dtype={
          "postal_code": "string",
          "street_no": "string",
          "street": "string",
          "city": "string",
          "city_pcs": "string",
          "csdname": "string",
          "str_name_pcs": "string",
          "str_type_pcs": "string",
          "str_dir_pcs": "string",
        },
        chunksize=400_000,
      ):
        # Ensure numeric coords early and drop invalid
        chunk["latitude"] = pd.to_numeric(chunk["latitude"], errors="coerce")
        chunk["longitude"] = pd.to_numeric(chunk["longitude"], errors="coerce")
        chunk = chunk[chunk["latitude"].notna() & chunk["longitude"].notna()]

        # Province constant per file
        chunk["_prov"] = prov

        # City key: city_pcs -> city -> csdname (your observation: city often NaN while city_pcs present)
        city_series = chunk["city_pcs"].fillna(chunk["city"]).fillna(chunk["csdname"])
        chunk["_city_key"] = city_series.map(self._norm_city)

        # Street key: prefer PCS components; fall back to "street"
        chunk["_street_key"] = self._build_street_key_series(chunk)

        # Street no
        sno = pd.to_numeric(chunk["street_no"], errors="coerce")

        # Keep only values that are effectively integers (tolerance handles 165.0 stored as float cleanly)
        sno_int_like = sno.where(sno.isna() | ((sno - sno.round()).abs() < 1e-9))

        chunk["_street_no"] = sno_int_like.round().astype("Int64")

        # --- Postal aggregation (centroid) ---
        pc_norm = chunk["postal_code"].map(self._normalize_postal_code)
        pc_mask = pc_norm.notna()
        if pc_mask.any():
          pc_vals = pc_norm[pc_mask]
          lat_vals = chunk.loc[pc_mask, "latitude"]
          lng_vals = chunk.loc[pc_mask, "longitude"]

          for pc, lat, lng in zip(pc_vals, lat_vals, lng_vals):
            agg = self._pc_agg.get(pc)
            if agg is None:
              self._pc_agg[pc] = _Agg(float(lat), float(lng), 1)
            else:
              self._pc_agg[pc] = agg.add(float(lat), float(lng))

        # --- Address-point aggregation ---
        addr_mask = (
          (chunk["_city_key"] != "")
          & (chunk["_street_key"] != "")
          & chunk["_street_no"].notna()
        )
        if addr_mask.any():
          sub = chunk.loc[addr_mask, ["_prov", "_city_key", "_street_key", "_street_no", "latitude", "longitude"]]

          for prov2, city_key, street_key, street_no, lat, lng in zip(
            sub["_prov"], sub["_city_key"], sub["_street_key"], sub["_street_no"], sub["latitude"], sub["longitude"]
          ):
            key = (prov2, city_key, street_key, int(street_no))
            agg = self._addr_agg.get(key)
            if agg is None:
              self._addr_agg[key] = _Agg(float(lat), float(lng), 1)
            else:
              self._addr_agg[key] = agg.add(float(lat), float(lng))

  def _finalize_indexes(self) -> None:
    # Finalize postal centroids
    for pc, agg in self._pc_agg.items():
      self._pc_centroid[pc] = agg.mean()

    # Build street index: (prov, city_key, street_key) -> list sorted by street_no
    tmp: Dict[Tuple[str, str, str], List[Tuple[int, float, float, int]]] = {}

    for (prov, city_key, street_key, street_no), agg in self._addr_agg.items():
      if agg.n < 1:
        continue
      lat, lng = agg.mean()
      seg_key = (prov, city_key, street_key)
      tmp.setdefault(seg_key, []).append((street_no, lat, lng, agg.n))

    # Filter + sort
    for seg_key, pts in tmp.items():
      # Require some density before enabling interpolation on this street
      total_pts = sum(p[3] for p in pts)
      if total_pts < self.min_points_for_street:
        continue
      pts.sort(key=lambda x: x[0])
      self._street_index[seg_key] = pts

  # ---------------------------
  # Parsing + normalization
  # ---------------------------

  def _parse_ims_suffix_street_no(self, address: str) -> Optional[Tuple[str, int]]:
    """
    IMS (for missing lat/lng) typically looks like: "Abbeydale Cir 334"
    We parse suffix integer as street_no, remaining as street string.
    Returns (street_key, street_no).
    """
    if address is None:
      return None
    s = str(address).strip()
    m = self._SUFFIX_NO_RE.match(s)
    if not m:
      return None

    street_raw = m.group("street")
    no_raw = m.group("no")
    try:
      street_no = int(no_raw)
    except ValueError:
      return None

    street_key = self._norm_street(street_raw)
    if not street_key:
      return None
    return (street_key, street_no)

  def _build_street_key_series(self, df: pd.DataFrame) -> pd.Series:
    # If PCS components exist, they’re typically standardized.
    name = df["str_name_pcs"].fillna("")
    stype = df["str_type_pcs"].fillna("")
    sdir = df["str_dir_pcs"].fillna("")

    pcs = (name + " " + stype + " " + sdir).map(self._norm_street)
    fallback = df["street"].fillna("").map(self._norm_street)

    # Prefer pcs when it has something meaningful
    use_pcs = pcs != ""
    return pcs.where(use_pcs, fallback)

  def _norm_province(self, prov: str) -> str:
    prov = str(prov).strip().upper()
    return prov

  def _norm_city(self, city: str) -> str:
    if city is None or pd.isna(city):
      return ""
    s = str(city).strip().upper()
    s = re.sub(r"[^A-Z0-9\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

  def _norm_street(self, street: str) -> str:
    if street is None or pd.isna(street):
      return ""
    s = str(street).strip().upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Minimal, deterministic canonicalization for common street types.
    # (Keep short and conservative; expand later if needed.)
    repl = {
      "STREET": "ST",
      "AVENUE": "AVE",
      "ROAD": "RD",
      "DRIVE": "DR",
      "CIRCLE": "CIR",
      "COURT": "CT",
      "CRESCENT": "CRES",
      "BOULEVARD": "BLVD",
      "LANE": "LN",
      "TERRACE": "TER",
      "PLACE": "PL",
    }
    parts = s.split(" ")
    out_parts = []
    for p in parts:
      out_parts.append(repl.get(p, p))
    return " ".join(out_parts).strip()

  def _normalize_postal_code(self, pc: str) -> Optional[str]:
    if pc is None or pd.isna(pc):
      return None
    pc = str(pc).strip().upper().replace(" ", "")
    if len(pc) != 6:
      return None
    if not pc[0].isalpha():
      return None
    if not (pc[1].isdigit() and pc[3].isdigit() and pc[5].isdigit()):
      return None
    if not (pc[2].isalpha() and pc[4].isalpha()):
      return None
    return pc

  def _province_from_filename(self, name: str) -> str:
    # "ODA_ON_v1.csv" -> "ON"
    # "ODA_NT_v1.csv" -> "NT"
    parts = name.split("_")
    return parts[1].upper()

  # ---------------------------
  # Search helpers
  # ---------------------------

  def _find_exact(
    self,
    pts: List[Tuple[int, float, float, int]],
    target_no: int,
  ) -> Optional[Tuple[int, float, float, int]]:
    lo = 0
    hi = len(pts) - 1
    while lo <= hi:
      mid = (lo + hi) // 2
      no = pts[mid][0]
      if no == target_no:
        return pts[mid]
      if no < target_no:
        lo = mid + 1
      else:
        hi = mid - 1
    return None

  def _find_bracketing(
    self,
    pts: List[Tuple[int, float, float, int]],
    target_no: int,
  ) -> Tuple[Optional[Tuple[int, float, float, int]], Optional[Tuple[int, float, float, int]]]:
    # Returns (lower, upper) nearest by street_no
    lo = 0
    hi = len(pts)
    while lo < hi:
      mid = (lo + hi) // 2
      if pts[mid][0] < target_no:
        lo = mid + 1
      else:
        hi = mid

    upper = pts[lo] if lo < len(pts) else None
    lower = pts[lo - 1] if lo - 1 >= 0 else None
    return (lower, upper)

