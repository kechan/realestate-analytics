import logging
from pathlib import Path
from typing import Set

from ..data.geo import GeoCollection


class ProvinceGeogIdValidator:
    """
    Validates that geog_ids belong to the correct province.
    Prevents cross-province contamination in ETL metrics.

    Example usage:
        validator = ProvinceGeogIdValidator(archive_dir='/path/to/archive', prov_code='ON')

        # Check if a geog_id is valid for the province
        is_valid = validator.is_valid('g30_dpz8qpvk')

        # Get all valid geog_ids for filtering
        valid_ids = validator.valid_geog_ids

        # Use with pandas dataframe
        filtered_df = df[df['geog_id'].isin(validator.valid_geog_ids)]
    """

    def __init__(self, archive_dir: Path, prov_code: str):
        """
        Initialize the validator for a specific province.

        Args:
            archive_dir: Path to archive directory containing geo_collection.dill
            prov_code: Two-letter province code (e.g., 'ON', 'BC', 'AB')

        Note:
            If geo_collection.dill cannot be loaded, the validator enters bypass mode
            where all geog_ids are considered valid (no validation occurs).
            This prevents ETL failures due to missing optional enhancement file.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.prov_code = prov_code.upper()
        self.bypass_mode = False  # Track if validation is active

        # Try to load GeoCollection
        geo_collection_path = Path(archive_dir) / 'geo_collection.dill'

        try:
            if not geo_collection_path.exists():
                raise FileNotFoundError(f"GeoCollection not found at {geo_collection_path}")

            self.logger.info(f"Loading GeoCollection from {geo_collection_path}")
            geo_collection = GeoCollection.load(geo_collection_path, use_dill=True)

            # Build valid geog_id set for this province
            self.valid_geog_ids: Set[str] = {
                geo.geog_id for geo in geo_collection
                if geo.province == self.prov_code
            }

            self.logger.info(
                f"Initialized validator for {self.prov_code} with "
                f"{len(self.valid_geog_ids)} valid geog_ids"
            )

        except (FileNotFoundError, Exception) as e:
            # Graceful degradation: bypass validation
            self.bypass_mode = True
            self.valid_geog_ids = None
            self.logger.warning(
                f"[VALIDATION BYPASS] Failed to load GeoCollection for province {self.prov_code}: {e}. "
                f"Cross-province geog_id validation will be DISABLED. "
                f"ETL will continue without contamination checks."
            )

    def is_valid(self, geog_id: str) -> bool:
        """
        Check if a single geog_id is valid for this province.

        Args:
            geog_id: The geog_id to validate (e.g., 'g30_dpz8qpvk')

        Returns:
            True if the geog_id belongs to this province, False otherwise.
            Always returns True if validator is in bypass mode.
        """
        if self.bypass_mode:
            return True  # Accept all geog_ids when in bypass mode
        return geog_id in self.valid_geog_ids
