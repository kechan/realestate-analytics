from typing import Optional, Dict
import logging
import threading
import time
import random
import requests
import pandas as pd


class GeocoderCAClient:
  """
  Client for Geocoder.ca API with rate limiting and daily quota management.

  Handles:
  - Forward geocoding (address → lat/lng)
  - Rate limiting (1 call per second)
  - Daily quota tracking (2,500 requests/day)
  - Thread-safe request management
  """

  API_URL = "https://geocoder.ca"

  # Canada geographic bounds (from realestate_analytics.utils.ims_locallogic.LocalLogicAPIClient)
  MIN_LAT = 41.0   # Windsor, ON ~42°
  MAX_LAT = 84.0   # Ellesmere Island ~83°
  MIN_LNG = -141.1 # Yukon border
  MAX_LNG = -52.0  # Eastern Newfoundland

  def __init__(self, daily_limit: int = 2500, min_interval_sec: float = 3.0, max_retries: int = 2, logger: Optional[logging.Logger] = None):
    """
    Initialize Geocoder.ca API client.

    Args:
        daily_limit: Maximum requests per day (default: 2500)
        min_interval_sec: Minimum seconds between requests (default: 3.0)
        max_retries: Maximum retry attempts for transient errors (default: 2)
        logger: Optional logger instance
    """
    self.logger = logger or logging.getLogger(self.__class__.__name__)
    self.daily_limit = daily_limit
    self.min_interval_ms = min_interval_sec * 1000  # Convert to milliseconds
    self.max_retries = max_retries

    # Rate limiting state
    self.last_call_time = 0
    self.rate_limit_lock = threading.Lock()

    # Daily quota tracking
    self.daily_call_count = 0
    self.quota_reset_time = time.time() + 86400  # Reset after 24 hours
    self.quota_lock = threading.Lock()

    # Statistics
    self.total_api_calls = 0
    self.successful_calls = 0
    self.failed_calls = 0

  def _check_daily_quota(self) -> bool:
    """
    Check if daily quota allows another request.

    Resets counter if 24 hours have passed.

    Returns:
        True if quota available, False if limit reached
    """
    with self.quota_lock:
      now = time.time()

      # Reset counter if 24 hours passed
      if now >= self.quota_reset_time:
        self.daily_call_count = 0
        self.quota_reset_time = now + 86400
        self.logger.info("Daily quota counter reset")

      # Check if quota available
      if self.daily_call_count >= self.daily_limit:
        self.logger.warning(f"Daily quota limit reached ({self.daily_limit} requests/day)")
        return False

      return True

  def _increment_daily_count(self):
    """Increment daily call counter (thread-safe)."""
    with self.quota_lock:
      self.daily_call_count += 1

  def _rate_limit_wait(self):
    """
    Wait to respect rate limiting (1 call per second by default).

    Thread-safe rate limiting using lock.
    """
    with self.rate_limit_lock:
      now = time.time() * 1000  # Convert to milliseconds
      elapsed = now - self.last_call_time

      if elapsed < self.min_interval_ms:
        wait_ms = self.min_interval_ms - elapsed
        # Add random jitter (0.1-0.5s) to appear more human-like
        jitter = random.uniform(0.1, 0.5)
        time.sleep((wait_ms / 1000.0) + jitter)
        # Update last_call_time to be min_interval + jitter after the last call
        # This ensures varied spacing to avoid bot detection
        self.last_call_time = self.last_call_time + self.min_interval_ms + (jitter * 1000)
      else:
        # Enough time has passed, use current time
        self.last_call_time = now

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

  def geocode_address(self, address: str = None, city: str = None, province: str = None, postal_code: str = None) -> Optional[Dict[str, float]]:
    """
    Forward geocoding: Convert address to coordinates.

    Args:
        address: Street address (optional)
        city: City name (optional, helps with disambiguation)
        province: Province code (optional, e.g., "ON", "BC")
        postal_code: Postal code (optional, uses separate 'postal=' parameter)

    Returns:
        Dict with 'lat' and 'lng' keys, or None if geocoding fails
        Example: {'lat': 43.651070, 'lng': -79.347015}
    """
    # Check daily quota
    if not self._check_daily_quota():
      return None

    # Safety check: Convert NaN values to None
    if pd.notna(address):
      address = str(address) if address else None
    else:
      address = None

    if pd.notna(city):
      city = str(city) if city else None
    else:
      city = None

    if pd.notna(province):
      province = str(province) if province else None
    else:
      province = None

    if pd.notna(postal_code):
      postal_code = str(postal_code) if postal_code else None
    else:
      postal_code = None

    # If postal code provided, use it as primary lookup method
    if postal_code:
      # Clean postal code (remove spaces)
      clean_postal = postal_code.replace(" ", "").upper()
      url = f"{self.API_URL}/?postal={clean_postal}&json=1"
      locate_param = clean_postal  # For logging
    else:
      # Build address string from components
      address_parts = []
      if address:
        address_parts.append(address)
      if city:
        address_parts.append(city)
      if province:
        address_parts.append(province)

      if not address_parts:
        self.logger.warning("No address components provided")
        return None

      locate_param = ", ".join(address_parts)
      url = f"{self.API_URL}/?locate={requests.utils.quote(locate_param)}&json=1"

    # Retry loop for transient errors
    for attempt in range(self.max_retries + 1):
      try:
        # Rate limiting
        self._rate_limit_wait()

        headers = {
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)

        self.total_api_calls += 1
        self._increment_daily_count()

        # Check for transient server errors (500, 502, 503)
        if response.status_code in [500, 502, 503]:
          if attempt < self.max_retries:
            backoff_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            self.logger.warning(f"Geocoder.ca transient error {response.status_code}, retrying in {backoff_time}s (attempt {attempt + 1}/{self.max_retries})")
            time.sleep(backoff_time)
            continue  # Retry
          else:
            self.logger.warning(f"Geocoder.ca API error: {response.status_code} (max retries exhausted)")
            self.failed_calls += 1
            return None

        # Check for client errors (403, 404, 400) - don't retry
        if response.status_code != 200:
          self.logger.warning(f"Geocoder.ca API error: {response.status_code}")
          self.failed_calls += 1
          return None

        data = response.json()

        # Check for error in response
        if 'error' in data:
          self.logger.debug(f"Geocoder.ca returned error: {data.get('error', {}).get('description', 'Unknown error')}")
          self.failed_calls += 1
          return None

        # Extract coordinates
        lat = data.get('latt')
        lng = data.get('longt')

        if lat is None or lng is None:
          self.logger.debug(f"No coordinates in response for: {locate_param}")
          self.failed_calls += 1
          return None

        # Convert to float
        try:
          lat = float(lat)
          lng = float(lng)
        except (ValueError, TypeError):
          self.logger.warning(f"Invalid coordinate format: lat={lat}, lng={lng}")
          self.failed_calls += 1
          return None

        # Validate coordinates are in Canada
        if not self._is_valid_canada_lat_lng(lat, lng):
          self.logger.debug(f"Coordinates outside Canada bounds: ({lat}, {lng})")
          self.failed_calls += 1
          return None

        self.successful_calls += 1
        return {'lat': lat, 'lng': lng}

      except requests.exceptions.RequestException as e:
        if attempt < self.max_retries:
          backoff_time = 2 ** attempt
          self.logger.warning(f"Geocoder.ca request exception: {e}, retrying in {backoff_time}s (attempt {attempt + 1}/{self.max_retries})")
          time.sleep(backoff_time)
          continue  # Retry
        else:
          self.logger.error(f"Geocoder.ca API request failed: {e} (max retries exhausted)")
          self.failed_calls += 1
          return None
      except (ValueError, KeyError) as e:
        self.logger.error(f"Failed to parse Geocoder.ca response: {e}")
        self.failed_calls += 1
        return None

    # If we exit the loop without returning, all retries failed
    return None

  def get_stats(self) -> Dict:
    """
    Get current statistics.

    Returns:
        Dict with call counts and quota information
    """
    with self.quota_lock:
      hours_until_reset = (self.quota_reset_time - time.time()) / 3600

      return {
        'total_calls': self.total_api_calls,
        'successful': self.successful_calls,
        'failed': self.failed_calls,
        'success_rate': f"{self.successful_calls/self.total_api_calls*100:.1f}%" if self.total_api_calls > 0 else "N/A",
        'daily_count': self.daily_call_count,
        'daily_limit': self.daily_limit,
        'quota_remaining': self.daily_limit - self.daily_call_count,
        'hours_until_reset': f"{hours_until_reset:.1f}h"
      }


