# XAS Lite Metadata Tracker Spec

Purpose: define a lightweight, fast metadata database and scripts that work with the mounted Google Drive desktop path.

This spec is for the lite pipeline only.

## Goals

- Fast indexing and header parsing from local mounted drive paths.
- Keep essential metadata only.
- Track binary files and their converted ASCII equivalents.
- Preserve BCR tracking fields needed for future sidecar/config work.
- Keep legacy scripts separate so they can be archived later without breaking lite.

## Capabilities

What the lite pipeline can do end-to-end:

- **Session and scan indexing**: Walk a mounted Google Drive path, discover all top-level session folders and per-session data files, and write one row per session and one row per file into SQLite. Supports checkpoint-based resumability so partial ingests can continue without re-scanning completed folders.
- **Depositor enrichment**: Pull Google Drive folder owner metadata via the Drive API and store the depositor label per session. Does not rely on local mount credentials.
- **Header parsing**: Read the first N bytes of each ASCII scan file (default 4096) to extract absorbing element, k-range, detector channel count, detector mode (T/F), scan category (data/reference), and MED flag. No full-file read required.
- **Binary conversion tracking**: Index the XAS_binary_conversion tree, record binary-to-ASCII filename mappings, capture optional point/column counts from conversion_log.txt, and link converted ASCII paths back to their source scan rows.
- **BCR file linking**: Match BCR_-prefixed scan rows to their original source scan rows via session, sample name, scan number, and channel. Discover local .bcr.json sidecars and record their paths.
- **Dataset export**: Filter scans by beamline, detector mode (F/T), MED presence, element, and scan category to produce a deduplicated ASCII file path list. Handles the case where the original scan was binary by following the binary_conversions link to the converted ASCII path.
- **Null element reporting**: Identify F-mode MED scans from 7-3 and 9-3 that are missing element labels, re-read their .rgn header lines from disk, and summarize unique patterns to support manual curation.
- **Health and coverage stats**: Print per-stage coverage counts (ingest, header parse, binary conversion linking, BCR linking, depositor enrichment) to verify pipeline completeness without modifying the DB.

## Data Source and Read/Write Scope

Primary source path:
- /Users/stetef/Library/CloudStorage/GoogleDrive-stetef@stanford.edu/Shared drives/XAS_Beamline_Data

Lite scripts read from mounted files and write to SQLite only, except where explicitly noted.

Write behavior by script:
- scripts/01_ingest_lite.py: writes DB only.
- scripts/01b_enrich_lite_depositors.py: writes DB only (depositor metadata enrichment via Drive API).
- scripts/02_parse_headers_lite.py: writes DB only.
- scripts/08_ingest_binary_conversions.py: writes DB only.
- scripts/09_lite_stats.py: read-only.
- scripts/10_export_dataset_report.py: read-only (queries DB; writes a .txt report to disk).
- scripts/11_export_null_data_rgn_report.py: read-only (queries DB and reads source files; writes a .txt report to disk).
- scripts/12_link_bcr.py: writes DB only (updates scans.bcr_source_id and scans.bcr_config_path).

## Lite Schema

Database file default: xas-catalog-lite.db

For current table definitions, column names, and value ranges, see [README.md](README.md).

## Script Behavior Notes

### scripts/01_ingest_lite.py

Walks the mounted data root, creates one `sessions` row per top-level folder and one `scans` row per file. Parses session metadata (year, beamline) and file metadata (path, size, sample name, scan number, channel, format) from directory and filename structure only — no file content is read. Records progress in `ingest_checkpoints` to support resumable runs. Does not parse headers.

### scripts/01b_enrich_lite_depositors.py

Calls the Google Drive API to fetch folder owner metadata for each session folder in the DB and writes the depositor label back to `sessions`. Does not use local mount credentials and does not touch any source files.

### scripts/02_parse_headers_lite.py

Reads the first N bytes of each ASCII scan file (default 4096) to extract header fields: element, k-range, detector mode, scan category, channel count, and MED flag. Updates the corresponding `scans` rows. No full-file read required.

### scripts/08_ingest_binary_conversions.py

Walks the XAS_binary_conversion tree, records binary-to-ASCII filename mappings in `binary_conversions`, and links each conversion row back to its source `scans` row where the session and filename match. Optionally parses `conversion_log.txt` for point and column counts.

### scripts/09_lite_stats.py

Read-only. Prints per-stage coverage counts to verify pipeline completeness: session and scan totals, format breakdown, header coverage (element, k_max, n_channels), BCR linking status, binary conversion linking, beamline distribution, and depositor enrichment coverage.

### scripts/10_export_dataset_report.py

Read-only (DB + disk write of report). Queries the DB to produce a deduplicated list of ASCII file paths for F-mode MED scans on beamlines 7-3 and 9-3. Handles scans that were originally ingested as binary by following the `binary_conversions` link to the converted ASCII path. Writes output to a .txt file.

### scripts/11_export_null_data_rgn_report.py

Read-only (DB + source file reads + disk write of report). Identifies F-mode MED data scans from 7-3 and 9-3 that have a NULL element, re-reads the `.rgn` header line from each file on disk, and summarizes unique patterns to support manual element curation. Writes output to a .txt file.

### scripts/12_link_bcr.py

Matches BCR_-prefixed scan rows to their original source `scans` rows via session, sample name, scan number, and channel. Discovers `.bcr.json` sidecars on disk and records their paths. Updates `scans` in the DB only.

## Recommended Lite Run Order

1) Build/refresh lite DB from mounted drive:
- python scripts/01_ingest_lite.py --db xas_catalog_lite.db --data-root "<mounted XAS_Beamline_Data>"

2) Enrich depositor names from Drive owners metadata:
- python scripts/01b_enrich_lite_depositors.py --db xas_catalog_lite.db --raw-root-id "<XAS_Beamline_Data_folder_id>" --gdrive-credentials "<service_account.json>"

3) Parse minimal headers:
- python scripts/02_parse_headers_lite.py --db xas_catalog_lite.db --local-root "<mounted XAS_Beamline_Data>" --max-bytes 4096

4) Ingest binary conversion mappings:
- python scripts/08_ingest_binary_conversions.py --db xas_catalog_lite.db --conversion-root "<mounted XAS_binary_conversion>"

5) Link BCR files and discover sidecars:
- python scripts/12_link_bcr.py --db xas_catalog_lite.db --local-root "<mounted XAS_Beamline_Data>"

6) Quick health/stats check:
- python scripts/09_lite_stats.py --db xas_catalog_lite.db

7) Export F-mode MED ASCII file list (7-3 and 9-3):
- python scripts/10_export_dataset_report.py --db xas_catalog_lite.db --out ascii_fmode_med_7-3_9-3_report.txt

8) Report null-element data scans for curation:
- python scripts/11_export_null_data_rgn_report.py --db xas_catalog_lite.db --local-root "<mounted XAS_Beamline_Data>"

## Migration and Compatibility Notes

- xas_lite_db.create_schema includes a migration path that adds sessions.depositor_name if it is missing in an existing lite DB.
