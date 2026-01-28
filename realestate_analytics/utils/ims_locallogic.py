"""
LocalLogic API Client

Handles OAuth2 authentication, rate limiting, and coordinate validation
for querying LocalLogic geographies API.
"""
from typing import Optional, Dict
import logging
import threading
import requests
import os
import time
import pandas as pd
from dotenv import load_dotenv, find_dotenv


class LocalLogicAPIClient:
  """
  Client for querying LocalLogic API with OAuth2 and rate limiting.

  Handles:
  - OAuth2 bearer token acquisition and caching
  - Token expiry management
  - Rate limiting (250ms between requests)
  - Coordinate validation for Canada bounds
  """

  TOKEN_URL = "https://api.locallogic.co/oauth/token"
  API_URL = "https://api.locallogic.co/v3/geographies"

  # Canada geographic bounds (from Java implementation)
  MIN_LAT = 41.0   # Windsor, ON ~42°
  MAX_LAT = 84.0   # Ellesmere Island ~83°
  MIN_LNG = -141.1 # Yukon border
  MAX_LNG = -52.0  # Eastern Newfoundland

  def __init__(self, logger: Optional[logging.Logger] = None):
    """
    Initialize LocalLogic API client.

    Loads credentials from environment variables:
    - LOCALLOGIC_CLIENT_ID: OAuth2 client ID
    - LOCALLOGIC_CLIENT_SECRET: OAuth2 client secret

    Args:
        logger: Optional logger instance

    Raises:
        RuntimeError: If credentials are not found in environment
    """
    self.logger = logger or logging.getLogger(self.__class__.__name__)

    # Load .env file
    load_dotenv(find_dotenv())

    # Get credentials from environment
    self.client_id = os.getenv('LOCALLOGIC_CLIENT_ID')
    self.client_secret = os.getenv('LOCALLOGIC_CLIENT_SECRET')

    if not self.client_id or not self.client_secret:
      raise RuntimeError(
        "LocalLogic credentials not found in environment. "
        "Please set LOCALLOGIC_CLIENT_ID and LOCALLOGIC_CLIENT_SECRET in .env file"
      )

    # OAuth2 token state
    self.access_token = None
    self.expiry_time = 0  # epoch time (seconds) when token expires
    self.token_lock = threading.Lock()

    # Rate limiting state
    self.min_interval_ms = 250  # 250ms between calls
    self.last_call_time = 0
    self.rate_limit_lock = threading.Lock()

    # Statistics
    self.total_api_calls = 0

  def _is_valid_canada_lat_lng(self, lat: float, lng: float) -> bool:
    """
    Validate coordinates are within Canada bounds.

    Args:
        lat: Latitude
        lng: Longitude

    Returns:
        True if coordinates are valid and within Canada bounds
    """
    if pd.isna(lat) or pd.isna(lng):
      return False

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
      return False

    return (self.MIN_LAT <= lat <= self.MAX_LAT and
            self.MIN_LNG <= lng <= self.MAX_LNG)

  def _ensure_token(self):
    """
    Check token validity and refresh if needed.

    Thread-safe token refresh using lock.
    """
    now = time.time()

    if self.access_token is None or now >= self.expiry_time:
      with self.token_lock:
        # Double-check after acquiring lock
        if self.access_token is None or now >= self.expiry_time:
          self._fetch_access_token()

  def _fetch_access_token(self):
    """
    Fetch new access token from LocalLogic OAuth endpoint.

    Updates self.access_token and self.expiry_time.
    """
    self.logger.info("Fetching new LocalLogic access token...")

    url = f"{self.TOKEN_URL}?grant_type=client_credentials&client_id={self.client_id}&client_secret={self.client_secret}"

    try:
      response = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
      response.raise_for_status()

      data = response.json()

      if "access_token" not in data:
        raise RuntimeError(f"Token not found in response: {data}")

      self.access_token = data["access_token"]
      expires_in = data.get("expires_in", 3600)  # Default 1 hour
      self.expiry_time = time.time() + expires_in

      self.logger.info(f"Access token acquired, expires in {expires_in}s")

    except requests.exceptions.RequestException as e:
      self.logger.error(f"Failed to fetch access token: {e}")
      raise RuntimeError(f"LocalLogic token fetch failed: {e}")

  def _rate_limit_wait(self):
    """
    Wait to respect rate limiting (250ms between calls).

    Thread-safe rate limiting using lock.
    """
    with self.rate_limit_lock:
      now = time.time() * 1000  # Convert to milliseconds
      elapsed = now - self.last_call_time

      if elapsed < self.min_interval_ms:
        wait_ms = self.min_interval_ms - elapsed
        time.sleep(wait_ms / 1000.0)

      self.last_call_time = time.time() * 1000

  def get_geographies(self, lat: float, lng: float) -> Optional[Dict[str, str]]:
    """
    Query LocalLogic API for all geog_ids at given coordinates.

    Args:
        lat: Latitude
        lng: Longitude

    Returns:
        Dict with keys 'g10', 'g20', 'g30', 'g35', 'g40' and values as geog_ids,
        or None if API call fails or coordinates invalid

    Example: {'g40': 'g40_abc', 'g30': 'g30_def', 'g20': 'g20_ghi', 'g10': 'g10_jkl'}
    """
    # Validate coordinates
    if not self._is_valid_canada_lat_lng(lat, lng):
      return None

    # Ensure token is valid
    self._ensure_token()

    # Rate limit wait
    self._rate_limit_wait()

    # Make API call
    url = f"{self.API_URL}?lat={lat}&lng={lng}"

    try:
      response = requests.get(
        url,
        headers={
          "Authorization": f"Bearer {self.access_token}",
          "Accept": "application/json"
        },
        timeout=10
      )

      self.total_api_calls += 1

      if response.status_code != 200:
        self.logger.warning(f"LocalLogic API error: {response.status_code} - {response.text}")
        return None

      data = response.json()

      # Parse response: data.geographies is a dict with geog_ids as keys
      geographies = data.get("data", {}).get("geographies", {})

      if not geographies:
        return None

      # Extract geog_ids by level prefix
      result = {}
      for geog_id in geographies.keys():
        if geog_id.startswith("g10_"):
          result["g10"] = geog_id
        elif geog_id.startswith("g20_"):
          result["g20"] = geog_id
        elif geog_id.startswith("g30_"):
          result["g30"] = geog_id
        elif geog_id.startswith("g35_"):
          result["g35"] = geog_id
        elif geog_id.startswith("g40_"):
          result["g40"] = geog_id

      return result if result else None

    except requests.exceptions.RequestException as e:
      self.logger.error(f"LocalLogic API request failed: {e}")
      return None
