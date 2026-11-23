"""The sole user-approved business-rule duplication in the serving API.

DELIBERATE, TEMPORARY DUPLICATION: production embeds this AB exception in
Datastore.bulk_import_ims_sold_listings() and bulk_update_ims_sold_listings() in
realestate_analytics/data/es.py. There is no pure helper to import. Never invoke
those ES writers simply to obtain this rule. Main-project files are immutable
for this secondary tool; the user explicitly permits mirroring ONLY this rule.

When either production provincial block changes, update this mirror too. The
CSV preview and ES lineage monitor share it, and a regression test compares it
with both live production blocks. See root TODO.md and AGENTS.md for the future
refactor and debugging guidance. All other mapping/override logic is imported.
"""

AB_DUPLEX_SUBTYPES = ("Duplex", "Full Duplex", "Half Duplex", "Duplex Side By Side")
AB_DUPLEX_PROPERTY_TYPE = "SEMI-DETACHED"


def provincial_mask(province, subtype):
    # Keep exact case/spacing semantics from the production post-trimming block.
    return province.eq("AB") & subtype.isin(AB_DUPLEX_SUBTYPES)


def apply_provincial_exception(frame):
    """Apply the documented exception only after the real mapper finishes."""
    mask = provincial_mask(frame["Province"], frame["SubType"])
    frame.loc[mask, "propertyType"] = AB_DUPLEX_PROPERTY_TYPE
    return mask
