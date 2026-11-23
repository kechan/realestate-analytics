"""Read-only adapter around the production mapper, treated as a third-party API.

All classification, cache-first inference and manual-override decisions remain
in IMSPropertyTypeMapper. This module neither copies those rules nor constructs
a Datastore or calls an ES-writing import routine.
"""
import hashlib
import marshal

from realestate_analytics.utils.ims import IMSPropertyTypeMapper
from .ims_provincial_rules import apply_provincial_exception


class ReadOnlyIMSMapper(IMSPropertyTypeMapper):
    """Keep the mapper's computations; suppress only persistent side effects."""

    def _log_unclear_mappings(self, *args, **kwargs):
        # generate_full_mapping_dataframe calls this audit-file writer.
        return None

    def _log_new_resolved_mappings(self, *args, **kwargs):
        # This is another audit-file writer, not a classification step.
        return None

    def save_mapping(self, *args, **kwargs):
        raise PermissionError("CSV preview must never save production mapping files")


def map_groups(frame, cache_dir):
    mapper = ReadOnlyIMSMapper(cache_dir / "ims_property_type_mapping_v4_df")
    if mapper.cached_mapping_df is None or mapper.override_lookup_df is None:
        raise ValueError("Both current production mapping dataframes are required")
    original_base = mapper.cached_mapping_df.copy()
    # Constructor has finished loading its inputs. Remove persistent destinations
    # as a second guard, in addition to the overridden audit/save methods.
    mapper.cached_mapping_path = None
    mapper.override_lookup_path = None
    mapper.load_dataframe(frame.copy())
    # Reuse the real importer methods. Only unseen tuples reach its inference
    # loop; CSV rows have already been grouped, avoiding per-listing mapping work.
    mapper.generate_full_mapping_dataframe()
    mapper.apply_property_types()
    return mapper, original_base


def mapper_version():
    # Fingerprint executed method code, not a vendored algorithm or source file
    # that might have changed on disk without this worker being restarted.
    return hashlib.sha256(b"".join([
        marshal.dumps(IMSPropertyTypeMapper.__init__.__code__),
        marshal.dumps(IMSPropertyTypeMapper.suggest_mapping.__code__),
        marshal.dumps(IMSPropertyTypeMapper._suggest_mapping_refined.__code__),
        marshal.dumps(IMSPropertyTypeMapper.generate_full_mapping_dataframe.__code__),
        marshal.dumps(IMSPropertyTypeMapper.apply_property_types.__code__),
    ])).hexdigest()


# The ONLY copied classification rule lives in ims_provincial_rules.py and is
# re-exported above. Its source, justification, drift test and refactor TODO are
# documented there and in root TODO.md / AGENTS.md. Everything else above reuses
# the actual production mapper, including its manual-override implementation.
