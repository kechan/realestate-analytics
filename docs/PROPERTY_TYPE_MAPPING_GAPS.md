# `searchCategoryType` Reliability Gaps — Active-Listing Property Type Mapping

**Status:** confirmed, scoped at the province/board level (2026-10-17). Not yet root-caused upstream
(whichever system assigns `searchCategoryType` per board) and no fix designed yet. This doc is the reference
for that follow-up; see [TODO.md](../TODO.md).

## Summary

`Datastore.property_type_query_mappings` (`realestate_analytics/data/es.py`) is the allow-list that
`get_listings()` and `get_current_active_listings()` use to decide which active listings even enter the
ETL pipeline for `LastMthMetricsProcessor`, `AbsorptionRateProcessor`, and `CurrentMthMetricsProcessor`:

```python
self.property_type_query_mappings = [
  # CONDO
  {"bool": {"must": [
      {"terms": {"listingType.keyword": ["CONDO", "REC"]}},
      {"term": {"searchCategoryType": 101}}
  ]}},
  {"term": {"searchCategoryType": 104}},  # SEMI-DETACHED
  {"term": {"searchCategoryType": 102}},  # TOWNHOUSE
  {"term": {"searchCategoryType": 103}},  # DETACHED
]
```

This is a `should`-clause inside a `must` filter — any listing whose `searchCategoryType` isn't 101/102/103/104
is **excluded from extraction entirely**, before `derive_property_type()` (`ims_transformers.py`) ever runs.
`derive_property_type()`'s `else: return 'OTHER'` branch is therefore effectively dead code for all
current-listing-derived metrics — nothing downstream of this filter can ever land in `OTHER`. (`OTHER` *is*
reachable for sold/historic metrics, which go through the separate IMS text-based `IMSPropertyTypeMapper`
pipeline — a different code path from the one described here.)

This was written assuming `searchCategoryType` is a reliably-populated field across all boards. It isn't.

## Confirmed scope (2026-10-17 investigation, via live PROD ES queries)

Checked `rlp_listing_current`, `transactionType: SALE` only (matching what `get_listings()` actually extracts),
bucketed by `searchCategoryType`.

### Per-province rollup

| Province | Total SALE listings | Mapped % (101/102/103/104) | `999` % | `SEMI` (104) count |
|---|---|---|---|---|
| ON | 70,622 | 78.1% | 0.9% | 515 |
| AB | 29,080 | 75.4% | 0.0% | 1,116 |
| BC | 49,864 | 68.1% | 9.0% | 78 |
| QC | 8,878 | 61.3% | 2.4% | 264 |
| SK | 6,681 | 61.9% | 0.0% | 90 |
| MB | 1,243 | 60.3% | 0.8% | 0 |
| NL | 3,239 | 55.7% | 7.7% | 54 |
| NS | 9,960 | 50.5% | 6.1% | 224 |
| PE | 2,943 | 40.8% | 6.0% | 76 |
| **NB** | 7,073 | **16.1%** | **56.1%** | 3 |

ON and AB's major urban boards populate `searchCategoryType` reliably. Every other province degrades to some
degree; **NB is the worst by far** — the majority of its entire active-listing book sits in `999`, not a
regional pocket of it.

Separately (not yet investigated further): `SEMI-DETACHED` (104) is near-zero in BC/SK/MB/NL/PE relative to
ON/AB. Likely those boards/regions simply don't use that code for anything — a distinct, probably
lower-priority quirk from the `999` problem below.

Also noted in passing: a handful of junk `provState` values exist (`colombie-britannique`,
`nouvelle-ecosse`, `nouveau-brunswick`, `Ab`, `autreother` — French/typo variants of real codes), 1–129 docs
each. Minor, separate data-quality issue, not pursued further.

### BC city-level breakdown — the problem is regional/board-specific, not province-wide

Unlike NB, BC's province-wide 68.1%/9.0% numbers mask a sharp geographic split. Per-city breakdown
(`provState: BC`, `transactionType: SALE`, bucketed by `city.keyword`):

**Mainland/interior BC — normal:** Vancouver (82.9% mapped), Surrey (86.4%), Richmond (85.5%), Burnaby
(87.2%), Kelowna (80.4%), Langley (84.9%), Coquitlam (92.0%), Abbotsford (83.9%), Kamloops (73.9%), Vernon
(61.3%), Prince George (50.0%), Cranbrook (62.2%) — DETACHED/TOWNHOUSE well represented in all of these.

**Vancouver Island — broken, uniformly:** every single Vancouver Island city checked shows `DETACHED`
(103) = 0, `TOWNHOUSE` (102) = 0, `SEMI` (104) = 0, and a large `999` bucket:

| City | Total | Mapped % | `999` | DET | TH | SEMI | CONDO |
|---|---|---|---|---|---|---|---|
| Victoria | 758 | 55.1% | 188 | 0 | 0 | 0 | 418 |
| Nanaimo | 877 | 14.4% | 469 | 2 | 0 | 0 | 124 |
| Langford | 615 | 36.1% | 286 | 0 | 0 | 0 | 222 |
| Saanich | 586 | 22.2% | 391 | 0 | 0 | 0 | 130 |
| Campbell River | 315 | 11.4% | 173 | 1 | 0 | 0 | 35 |
| Duncan | 312 | 7.1% | 207 | 0 | 0 | 0 | 22 |
| Port Alberni | 291 | **1.0%** | 175 | 0 | 0 | 0 | 3 |
| Sooke | 279 | 4.7% | 155 | 0 | 0 | 0 | 13 |
| Parksville | 206 | 16.0% | 129 | 0 | 0 | 0 | 33 |
| Qualicum Beach | 172 | 4.7% | 117 | 0 | 0 | 0 | 8 |
| Ladysmith | 160 | 3.1% | 100 | 0 | 0 | 0 | 5 |
| Colwood | 144 | 17.4% | 88 | 0 | 0 | 0 | 25 |
| Salt Spring | 144 | **0.0%** | 104 | 0 | 0 | 0 | 0 |
| Esquimalt | 143 | 46.9% | 49 | 0 | 0 | 0 | 67 |
| Central Saanich | 142 | 23.9% | 68 | 0 | 0 | 0 | 34 |
| Courtenay | 298 | 23.8% | 152 | 4 | 1 | 0 | 66 |

Likely cause: whichever board(s) serve Vancouver Island (VIREB for most of the island, VREB for the south
island/Victoria — MLS numbers sampled, e.g. `1049220`, are consistent with VREB) vs. REBGV/FVREB/OMREB etc.
on the mainland. Not yet confirmed which board(s) exactly, or where in the feed the `searchCategoryType`
assignment actually happens (outside this codebase — RLP's own ingestion from the board, not something
`realestate_analytics` controls).

**Effect on `mkt_trends`:** for every Vancouver Island geog_id, the `ALL` property-type aggregate is not
really "all" — it's close to CONDO-only, because CONDO (101) is the one code that survives. The
DETACHED/SEMI/TOWNHOUSE breakdown is effectively dead, and `ALL`'s median asking price / absorption rate /
months-of-inventory are silently skewed toward condo pricing/turnover rather than reflecting the whole
market.

### What `999` actually contains — not a clean category, a true gap

Unlike `401`–`405` (COMMERCIAL_BUSINESS/INDUSTRIAL/OFFICE/RETAIL/OTHER-commercial), `501`/`502`
(VACANT_LAND), `201`/`204` (DUPLEX/FOURPLEX investment) — all of which map cleanly to real, legitimately
out-of-residential-scope categories, confirmed consistent in both Victoria and Toronto samples — `999` is
not a coherent category. Sampled Victoria `999` listings (`listingType: RES` mostly, `propertyType: null`
in nearly all cases):

- `#14-1810B Store St` — 1bd/3ba? unit-numbered address → likely condo
- `717 Robleda Cres` — 5bd/5ba, $4.88M → likely detached estate
- `2639 Prior St` — 2bd/1ba, $799K → likely a house
- `204-588 Pandora Ave` — unit-numbered → likely condo
- one `COMMERCIAL`/`LEASE` listing with `propertyType: OTHER` explicitly set

So `999` is a genuine "board didn't classify this" sentinel catching a real mix of residential property
types, not a legitimate separate bucket that's merely out of scope. This is the part worth fixing — unlike
401-502/201/204, which should stay excluded from residential `mkt_trends` by design.

Toronto's much smaller `999` bucket (45 SALE docs, 0.5%) looks similar in kind (mixed RES/CONDO/commercial)
but trivial in scale — which is likely why this was never noticed via ON/AB data alone.

## Why this matters

- `ALL`/`DETACHED`/`TOWNHOUSE`/`SEMI-DETACHED` medians, counts, absorption rate, and months-of-inventory for
  any geog_id dominated by an affected board are silently wrong — not missing (which would be visible),
  but computed from a biased subset and presented as if representative.
- This is worse for NB (province-wide, 56% excluded) than for BC (regional, Vancouver Island only).
- Nothing in the existing pipeline surfaces this — there's no alert or halt-gate condition for "most of this
  geog's listings got excluded by property-type filtering." It only surfaces via manual inspection like this.

## Not yet done

- Root-cause which board(s) feed Vancouver Island and NB, and whether `searchCategoryType` assignment is
  fixable at the RLP ingestion layer (outside this repo) vs. something `realestate_analytics` needs to work
  around.
- City-level breakdown for NB/PE/NS (only done for BC so far) — unclear whether NB's problem is uniform
  province-wide or concentrated in specific boards/regions the way BC's is.
- No remediation designed yet. Candidate directions (unevaluated):
  - Route unmapped-but-residential (`999`, `listingType: RES`) listings into `OTHER` instead of dropping
    them, so `ALL` at least includes them even without a DETACHED/TOWNHOUSE/CONDO/SEMI split.
  - Heuristic reclassification of `999 + RES` listings using `address1` (unit-number prefix → CONDO-like),
    `bedsInt`/`bathsInt`/`price`, similar in spirit to the IMS sold-listing mapper's secondary-signal
    approach proposed in TODO.md — unproven for this data, would need validation.
  - Push upstream: fix `searchCategoryType` assignment at the source for the affected boards, if that's
    within RLP's control.

## How to reproduce / extend this investigation

Raw PROD ES queries used (no code changes needed, read-only):

```bash
. ~/.rlp_es_env   # exports RLP_LISTING_ES_API_KEY

# Per-province rollup
curl -s -X POST "https://esp.royallepage.ca:443/rlp_listing_current/_search" \
  -H "Authorization: ApiKey $RLP_LISTING_ES_API_KEY" -H "Content-Type: application/json" \
  -d '{
    "size": 0,
    "query": {"match": {"transactionType": "SALE"}},
    "aggs": {"by_prov": {"terms": {"field": "provState.keyword", "size": 20},
      "aggs": {
        "mapped":  {"filter": {"terms": {"searchCategoryType": [101,102,103,104]}}},
        "sct_999": {"filter": {"term":  {"searchCategoryType": 999}}}
      }}}
  }'

# Per-city breakdown within a province (swap provState value)
curl -s -X POST "https://esp.royallepage.ca:443/rlp_listing_current/_search" \
  -H "Authorization: ApiKey $RLP_LISTING_ES_API_KEY" -H "Content-Type: application/json" \
  -d '{
    "size": 0,
    "query": {"bool": {"must": [{"match": {"provState": "BC"}}, {"match": {"transactionType": "SALE"}}]}},
    "aggs": {"by_city": {"terms": {"field": "city.keyword", "size": 60, "order": {"_count": "desc"}},
      "aggs": {
        "mapped":    {"filter": {"terms": {"searchCategoryType": [101,102,103,104]}}},
        "sct_999":   {"filter": {"term":  {"searchCategoryType": 999}}},
        "detached":  {"filter": {"term":  {"searchCategoryType": 103}}},
        "townhouse": {"filter": {"term":  {"searchCategoryType": 102}}},
        "semi":      {"filter": {"term":  {"searchCategoryType": 104}}},
        "condo":     {"filter": {"term":  {"searchCategoryType": 101}}}
      }}}
  }'
```

See `.claude/skills/realestate-es-query/references/listing_schema.md` for the full `searchCategoryType`
encoding table (101-104 residential, 201-204 investment, 401-405 commercial, 501-502 land).
