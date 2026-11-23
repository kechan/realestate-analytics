# Market Explorer

A standalone React/TypeScript frontend for the read-only FastAPI serving API.
Canada is the home view, with province/territory browsing and a permanent GTA
shortcut to the original regional map. Search and ancestry support Canadian locations.
All geography and market data come through `/api/v1`. The browser never reads
Elasticsearch credentials, ETL files, or the GeoCollection snapshot directly.

## Install locally

Run commands from this `front-end` directory. Node tooling is separate from the
Python virtual environment. Dependencies and the optional Node runtime remain
inside this project; no global npm packages or shared Python changes are needed.

```sh
npm install --prefix .tooling --no-save --package-lock=false node@24.22.2
export PATH="$PWD/.tooling/node_modules/.bin:$PATH"
node --version
npm install
```

The export selects Node 24 only in this terminal. It does not replace global Node
or affect already-running apps. `.tooling/` and `node_modules/` are ignored by git.
Commit `package-lock.json` for reproducible dependency resolution.

After setup, **every npm script automatically uses the project's local Node 24**
through `scripts/tool.mjs`, even from a fresh terminal whose default Node is 23.
A separately selected Node 24 also works when `.tooling/` is absent.

The initial Node-23-compatible Vitest 3 pin was replaced with patched Vitest 5.1.0
after npm reported test-tool vulnerabilities. Node 24 supports the patched test
runner without changing the Python environment. Keep Vite 7.3.7 and the React
plugin 5.2.0 pinned; there is no need for a forced build-tool upgrade.

## Run

Keep the serving FastAPI app running separately, normally on port 8013:

```sh
npm run dev
```

Open `http://127.0.0.1:5173`. The port is strict: another application on that port
causes an error rather than silently moving the frontend. Stop with Ctrl-C.
No frontend script starts, restarts, or stops FastAPI or other projects.

Vite proxies `/api/v1` to `http://127.0.0.1:8013`, avoiding cross-origin setup in
development. Copy `.env.example` to `.env.local` only to change configuration:

| Setting | Purpose |
| --- | --- |
| `API_PROXY_TARGET` | FastAPI origin, used only by Vite's dev server. |
| `VITE_API_BASE_URL` | Browser-facing prefix; defaults to `/api/v1`. |
| `VITE_MAP_TILE_URL` | Basemap tile template; defaults to OpenStreetMap. |
| `VITE_MAP_ATTRIBUTION` | Attribution required by the selected tile provider. |

All `VITE_` values are public browser configuration. Never put ES API keys or
other secrets here. ES host/key configuration belongs exclusively to FastAPI.
Restart Vite after changing environment configuration.

## Features and data semantics

- Canada overview with all 13 provinces/territories, accessible map selection,
  province → region/city → child-area drill-down in both the map and sliding
  Areas pane, and a debounced province-wide search covering all geography levels,
  including regions such as Estrie. Search results show the area type; clearing
  the search hides results and makes no directory request. The
  initial national view requests no geography documents, geometry or market-data
  documents (the small metric catalog can still load).
  Province navigation loads child metadata first, then navigation boundaries;
  it does not fabricate national or provincial metric aggregates or geography IDs.
- Shareable browse routes: `/` for Canada, `/?province=BC` for British Columbia,
  and `/?geo=g40_dpz3tpuh` for the original GTA landing. Existing geography deep
  links still work. The brand returns to Canada; the header's GTA shortcut is
  always available. Root geographies return to province browsing where known.
- Region/city/district/neighbourhood navigation, source ancestry, Canada-wide
  debounced search, browser history, and copyable URL selections.
- Polygon and MultiPolygon maps, including holes, with hover/click previews,
  mobile tap previews, explicit fit and zoom controls, and keyboard-equivalent
  navigation in the Areas list. GeoJSON coordinates are not manually swapped.
- Drag a preview by its title/grip to reposition it within the map. Focus that
  handle and use arrow keys (Shift for larger steps) for keyboard movement.
  Interacting keeps the preview open until close, Escape, or navigation; moving
  it does not fetch data again. A newly opened preview starts in its usual corner.
- All twelve monthly metrics and the separate current snapshot, six property
  types, independent reporting-month labels, All/3Y/1Y views, and data/CSV export.
- Monthly gaps remain missing; zeroes remain zero. Absorption is displayed as a
  percentage but CSV preserves the API's raw ratio. Asking-price bounds are
  filtered values, not literal extrema. No change percentages are invented.
- Available area profiles, source provenance, and metric explanations are kept
  in contextual disclosures. Incomplete ancestry is shown, not fabricated.
- Request cancellation and shared query ownership prevent stale results from
  overwriting a different selection. Metrics are fetched once per geography and
  property type; changing chart metric/range does not fetch them again.
- Browser-memory freshness: geography five minutes, metrics one minute, catalog
  for the browser session, inactive query eviction after five minutes. No data
  is persisted to localStorage, IndexedDB, or a service worker. Refresh explicitly
  invalidates active data; window focus and reconnect do not trigger refetches.
- Child names load before polygon outlines. The backend can take several seconds
  to assemble larger geometry collections; navigation and main-area data remain
  independently usable. Missing metrics and tile failures do not blank the app.

GTA requests are scoped to its level-30 children. Search requests use explicit
20-result pages without geometry. No UI flow requests an unfiltered download of
all Canadian geographies. Hover previews are fetched on demand, not for every
city at startup.

Province maps and sidebars share `/api/v1/provinces/{province}/children?include_geometry=false`, which
returns regions and cities without a resolved geo parent, excluding descendants
of those areas. Provinces are navigation containers, not invented geo IDs. Gaps
in published parent links mean some cities remain directly under a province;
overlapping regions can also exist in the source. No geographic containment is
fabricated. Areas missing geometry remain selectable in the sidebar. The map sends only those returned IDs to the read-only `POST /api/v1/geos/boundaries` route; it does not repeat hierarchy discovery. Requests larger than 2,000 IDs are split without truncation. The API bounds ES boundary reads to three concurrent batches of 100. Client joins use IDs, retain current metadata and provenance, and treat polygons as atomic values. Polygon layers are memoized so hover changes only update affected highlights.

Only a nonblank province search requests `/geos?province=…&query=…&limit=30&offset=…`,
without a level restriction. The existing backend still retrieves matching base
metadata before paginating filtered searches. Province lists describe geographic
markets, not verified metric availability.
City-marker clustering and an ES-level filtered-pagination redesign are deferred.

The national overview uses Natural Earth 1:50m Admin 1 outlines projected to a
Canada-oriented Lambert conformal conic SVG. Provincial views use OpenStreetMap
with a Natural Earth context outline and child polygons from the API. The new
province endpoint simplifies child polygons at 0.0025 degrees with topology
preservation to reduce navigation payloads; geo detail still returns exact
published polygons. Natural Earth data is public domain:
https://www.naturalearthdata.com/downloads/50m-cultural-vectors/50m-admin-1-states-provinces/

`src/domain/canadaMap.ts` and `provinceBoundaries.ts` record the source checksum. To regenerate them
without adding packages, download the source GeoJSON outside the repository,
then run `node scripts/build-canada-map.mjs /path/to/source.geojson` from this
directory. The generated map loads separately from the local map/chart modules.

## Verify and build

```sh
npm run typecheck
npm test
npm run build
npm audit
```

The tests cover numeric/period semantics, URL restoration, API contracts, shared
requests, search races, partial failures, navigation, and accessible data tables.
Map and chart rendering additionally require real-browser QA; unit tests alone
do not prove visual correctness. The committed tests use deterministic fixtures;
the application has no fixture-data or hardcoded-market-value mode.

The build produces static files in `dist/`. In production, serve them through a
web server and proxy `/api/v1` to FastAPI on the same origin. Vite's dev proxy does
not run in a static deployment or `npm run preview`. If using an absolute API
origin, configure the corresponding allowed frontend origin on the API.

OpenStreetMap tiles load externally, with visible attribution and normal browser
cache headers. There is no offline/bulk tile prefetch. The basemap can be replaced
through configuration without changing map components.

## IMS property type mapping

Open <http://127.0.0.1:5173/ims-property-type-mapping>. Use **Choose Files** or
drag one or more CSVs into the drop zone. Files are initially sorted by
last-modified time, oldest first. Check the order with the drag handles or
arrows before selecting **Compute property types**. After a manual reorder,
new files are added at the bottom. The last occurrence
of a listing wins, both across files and within a file. Selecting or dropping
files does not upload automatically. Batch previews require Type, SubType,
Style, Province, MLSID, MLSNumber, and WrittenDate in every file. Limits are 20
files, 5 million input rows combined, and 100,000 unique normalized tuples.
Uploads use temporary disk space as needed. Errors reject the preview instead
of silently truncating it.

The backend reuses the production `IMSPropertyTypeMapper` directly through a
read-only adapter. Existing base mappings win; its real methods infer unseen
tuples and apply manual overrides. Save and audit-file hooks are disabled.
Tuple values are trimmed like the importer; existing propertyType CSV values
are ignored. No Production query runs. Temporary upload storage is deleted.
The API deduplicates before grouping by province, MLS board, MLS number, and
sale year. It reports input rows, duplicates removed, and rows without a usable
listing ID that were kept separately. A single legacy four-column CSV can still
be previewed by row occurrence.

CSV results show the filename for a single file or the file count for a batch.
They are distinct from stored classifications loaded from Production ES. Source
controls retain each completed report without fetching. Failed computations
keep the previous completed report visible. Search, sorting, filters, resizing,
and 50-row display pages operate on the complete grouped result locally.

Use **Export CSV** beside the pagination controls to save the full CSV preview.
One file contains the table, summary counts, original run date, and mapping
versions. Keep the exported file unchanged for **Import report**, which restores
the saved result locally without the original IMS CSVs or a new computation.

**Group by Province** starts off. Turning it on shows province-specific counts
and conflicts from the same compute. CSV exports with a province breakdown
include both sets of rows. Filter the **View** column to **Combined** or
**By Province** before summing counts in a spreadsheet. Older exports open
with the toggle disabled.

Production queries remain available for occasional verification. Every **Query
Production ES** or **Refresh from ES** action warns about CPU/disk cost and
requires confirmation; Cancel sends no request. The full source scan uses
sequential slices of one ES snapshot. The display page size does not limit its
coverage. There are no automatic queries on mount, focus, or reconnection.

The table includes tuple fields, property type, count, base mapping, manual
override, source, and check status. Expand a source cell for weighted counts or
the explanation for provenance limits. The CSV path uses the real mapper's
current code, not a copied algorithm. Restart FastAPI after mapper source
changes. The only user-approved copied business rule is the AB provincial
exception, because production embeds it in ES-writing methods. See its code
comments in `api/ims_provincial_rules.py`, root TODO.md, and AGENTS.md for drift
checks and the future helper refactor. Production ETL source remains untouched.

The tool stays read-only: no import into ES, mapping edits, saves, ETL cache
updates, or sighting-log writes. Both local mapping files are required via
`ANALYTICS_CACHE_DIR`. No new npm packages or shared-environment changes are
needed.

## Design

The approved direction is a true-white workspace over a cool-gray background,
dark crimson interaction accents, a geographic navigation rail, one useful map,
a compact market summary, and one selectable history chart. Mobile replaces the
rail with a focus-managed Areas drawer and stacks the content vertically.

Intentional differences from the illustrative mockups:

- Real API boundaries and monthly points replace generated map shapes and lines.
- Current metrics are labelled **Current snapshot**, with the source document's
  update date when available. The API does not attach a reporting-month field
  to that object, so the frontend does not invent one.
- Metric definitions replace the mockup's illustrative-history caption.
- Child level labels clarify mixed hierarchy levels; source diagnostics sit
  inside the expandable profile instead of consuming the main workspace.

Map and chart modules load lazily. ECharts' SVG renderer is registered explicitly
with only the required chart/components; map objects, chart instances, observers,
timers, and focus handlers clean up on unmount.
