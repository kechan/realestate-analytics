"""Native, bounded processing of navigation polygons; no ETL dependencies."""
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import shapely
from shapely.geometry import mapping, shape

NAVIGATION_TOLERANCE = 0.0025
NATIVE_WORKERS = 2


def simplify_geometry(geometry):
    """Scalar reference and fallback, preserving invalid or empty input."""
    original = shape(geometry)
    if original.is_valid:
        simplified = original.simplify(NAVIGATION_TOLERANCE, preserve_topology=True)
        if not simplified.is_empty and simplified.is_valid:
            return mapping(simplified)
    return geometry


class NavigationGeometryProcessor:
    """One shared native pool per serving process, owned by the app lifespan.

    Input coordinates have already passed Repository's strict validation. NumPy
    must not be used to coerce unvalidated strings or booleans into coordinates.
    """

    def __init__(self):
        # ThreadPoolExecutor starts threads only when the first work is submitted.
        self._executor = ThreadPoolExecutor(
            max_workers=NATIVE_WORKERS, thread_name_prefix="api-geometry")
        self._condition = threading.Condition()
        self._inflight = 0
        self._closed = False

    def close(self):
        """Reject new calls, drain active calls, then join the native threads."""
        with self._condition:
            self._closed = True
            while self._inflight:
                self._condition.wait()
        self._executor.shutdown(wait=True)

    def simplify(self, geometries):
        with self._condition:
            if self._closed:
                raise RuntimeError("Navigation geometry processor is closed")
            self._inflight += 1
        try:
            return self._simplify(geometries)
        finally:
            with self._condition:
                self._inflight -= 1
                self._condition.notify_all()

    def _simplify(self, geometries):
        result = list(geometries)
        selected, rings, polygon_counts, geometry_counts, singles = [], [], [], [], []
        for index, geometry in enumerate(geometries):
            if not geometry:
                continue
            kind = geometry.get("type")
            if kind == "Polygon":
                polygons = [geometry.get("coordinates")]
            elif kind == "MultiPolygon":
                polygons = geometry.get("coordinates")
            else:
                polygons = None
            if not polygons or any(
                not polygon or any(not ring or len(ring) < 4 for ring in polygon)
                for polygon in polygons
            ):
                result[index] = simplify_geometry(geometry)
                continue
            selected.append(index)
            singles.append(kind == "Polygon")
            geometry_counts.append(len(polygons))
            for polygon in polygons:
                polygon_counts.append(len(polygon))
                rings.extend(polygon)
        if not selected:
            return result

        # Keep all three nesting levels, including holes within each polygon.
        arrays = [np.asarray(ring, dtype=np.float64) for ring in rings]
        coordinates = np.concatenate(arrays)
        ring_offsets = np.r_[0, np.cumsum([len(array) for array in arrays])]
        polygon_offsets = np.r_[0, np.cumsum(polygon_counts)]
        geometry_offsets = np.r_[0, np.cumsum(geometry_counts)]
        geometries = shapely.from_ragged_array(
            shapely.GeometryType.MULTIPOLYGON, coordinates,
            (ring_offsets, polygon_offsets, geometry_offsets))
        singles = np.asarray(singles, dtype=bool)
        geometries[singles] = shapely.get_geometry(geometries[singles], 0)

        indices = np.arange(len(selected))
        futures = [self._executor.submit(self._work, indices[i::NATIVE_WORKERS], geometries)
                   for i in range(min(NATIVE_WORKERS, len(selected)))]
        groups, error = [], None
        # Drain both tasks even if one raises: a failed request cannot leave its
        # sibling native work outstanding or poison subsequent requests.
        for future in futures:
            try:
                groups.append(future.result())
            except Exception as exc:
                error = error or exc
        if error is not None:
            raise error
        for group in groups:
            for index, geometry in group:
                result[selected[index]] = geometry
        return result

    def _work(self, indices, geometries):
        selected = indices[shapely.is_valid(geometries[indices])]
        simplified = shapely.simplify(
            geometries[selected], NAVIGATION_TOLERANCE, preserve_topology=True)
        good = shapely.is_valid(simplified) & ~shapely.is_empty(simplified)
        outputs = []
        for index, text in zip(selected[good], shapely.to_geojson(simplified[good])):
            geometry = json.loads(text)
            # Preserve mapping()'s tuple container contract as well as exact
            # coordinate values. HTTP JSON remains the same nested arrays.
            if geometry["type"] == "Polygon":
                geometry["coordinates"] = tuple(
                    tuple(map(tuple, ring)) for ring in geometry["coordinates"])
            else:
                geometry["coordinates"] = tuple(
                    tuple(tuple(map(tuple, ring)) for ring in polygon)
                    for polygon in geometry["coordinates"])
            outputs.append((int(index), geometry))
        return outputs
