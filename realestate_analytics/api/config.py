"""Configuration for the standalone serving API."""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    api_key: str
    geo_index: str = "rlp_content_geo_4"
    geo_details_index: str = "rlp_geo_details_en"
    metrics_index: str = "rlp_mkt_trends_current"
    origins: tuple = ("http://localhost:3000",)
    analytics_cache_dir: Optional[Path] = None
    ims_sold_index: str = "rlp_ims_sold_listing_current"

    @property
    def geo_collection_path(self):
        return self.analytics_cache_dir / "geo_collection.dill" if self.analytics_cache_dir else None

    @classmethod
    def from_environment(cls):
        load_dotenv(Path.home() / ".env", override=False)
        load_dotenv(Path.home() / ".rlp_es_env", override=True)
        key = os.environ.get("RLP_LISTING_ES_API_KEY", "").strip()
        if not key:
            raise RuntimeError("RLP_LISTING_ES_API_KEY is required")
        origins = tuple(x.strip() for x in os.getenv(
            "ALLOW_ORIGINS", "http://localhost:3000").split(",") if x.strip())
        cache_dir = os.environ.get("ANALYTICS_CACHE_DIR", "").strip()
        return cls(api_key=key,
                   geo_index=os.getenv("API_GEO_INDEX", "rlp_content_geo_4"),
                   geo_details_index=os.getenv("API_GEO_DETAILS_INDEX", "rlp_geo_details_en"),
                   origins=origins,
                   analytics_cache_dir=Path(cache_dir).expanduser() if cache_dir else None)
