# XAS Lite Metadata Tracker Spec

Purpose: define a lightweight, fast metadata database and scripts that work with the mounted Google Drive desktop path.

This spec is for the lite pipeline only.

## Goals

- Fast indexing and header parsing from local mounted drive paths.
- Keep essential metadata only.
- Track binary files and their converted ASCII equivalents.
- Preserve BCR tracking fields needed for future sidecar/config work.
- Keep legacy scripts separate so they can be archived later without breaking lite.

## Data Source and Read/Write Scope

Primary source path:
- /Users/stetef/Library/CloudStorage/GoogleDrive-stetef@stanford.edu/Shared drives/XAS_Beamline_Data

Lite scripts read from mounted files and write to SQLite only, except where explicitly noted.

Write behavior by script:
- scripts/01_ingest_lite.py: writes DB only.
- scripts/02_parse_headers_lite.py: writes DB only.
- scripts/08_ingest_binary_conversions.py: writes DB only.
- scripts/09_lite_stats.py: read-only.
- scripts/01b_enrich_lite_depositors.py: writes DB only (depositor metadata enrichment via Drive API).
- scripts/06_import_stubs.py: may write .bcr.json sidecars only when --write-sidecars is passed.

## Lite Schema

Database file default: xas_catalog_lite.db

### Table: sessions

One row per top-level session folder.

Columns:
- id: INTEGER PRIMARY KEY.
- dir_name: TEXT NOT NULL UNIQUE.
  Meaning: top-level folder name under XAS_Beamline_Data, for example 2022_7-3_December_SteveRagsdale_NiMCRCrystal.
  Source: folder name from mounted drive walk.
- year: INTEGER.
  Meaning: parsed year.
  Parse rule: regex ^(\\d{4})_(\\d-\\d)_ from dir_name, group 1.
- beamline: TEXT.
  Meaning: parsed beamline string such as 7-3 or 9-3.
  Parse rule: same regex, group 2.
- owner: TEXT.
  Meaning: curator owner bucket for downstream grouping (RS, AA, KZ, MA).
  Source: mapping logic from xas_db_builder.assign_owner_and_abbrev(session_dir).
- depositor_name: TEXT.
  Meaning: depositor label from Google Drive folder owner metadata.
  Source: Drive API `owners` field for the top-level session folder, populated by scripts/01b_enrich_lite_depositors.py.
  Storage rule: first owner email when available, otherwise display name.
  Desktop ingest note: not inferred from local mount username or local credentials.

### Table: scans

One row per data file discovered during ingest.

Columns:
- id: INTEGER PRIMARY KEY.
- session_id: INTEGER NOT NULL REFERENCES sessions(id).
  Meaning: foreign key to session folder.
- file_path: TEXT NOT NULL UNIQUE.
  Meaning: relative path from data root, for example 2009_7-3_Jun/FEFOIL_001_A.001.
- file_size_bytes: INTEGER.
  Meaning: on-disk size in bytes.
  Source: Path.stat().st_size during ingest.
- sample_name: TEXT.
  Meaning: parsed sample token from filename.
  Parse rule: from xas_db_builder.parse_data_filename.
- scan_number: INTEGER.
  Meaning: parsed run number (typically 3-digit token).
- repeat: INTEGER.
  Meaning: repeat index parsed from extension or second token depending on format.
- channel: TEXT.
  Meaning: channel letter for _A style scans; NULL for .dat style.
- spot_label: TEXT.
  Meaning: optional spot token such as spot1a or s2.
- format: TEXT CHECK IN ('binary', 'ascii', 'unknown').
  Meaning: file type classification.
  Initial source: filename heuristic via parser (_A. or .dat => ascii, else unknown).
  Refined source: header parse bytes can set binary/ascii.
- is_processed_output: INTEGER NOT NULL DEFAULT 0.
  Meaning: 1 if filename matches known processed/non-raw output patterns.
  Source: xas_db_builder.is_processed_output_name.
- has_med: INTEGER.
  Meaning: multi-element detector marker flag.
  Values: 1 yes, 0 no, NULL unknown/not parsed.
  Parse rule: regex markers in header text (ICR, FF, SCA tokens).
- element: TEXT.
  Meaning: absorbing element from header.
  Parse rule: from .rgn token first, fallback Element: line.
- k_max: REAL.
  Meaning: max k value from .rgn token.
  Parse rule: regex on filename-like token in .rgn header line.
- n_channels: INTEGER.
  Meaning: detector channel count inferred from header Data section.
  Parse rule: count lines between Data: marker and first numeric row.
- is_bcr: INTEGER NOT NULL DEFAULT 0.
  Meaning: 1 if filename starts with BCR_.
- bcr_source_id: INTEGER REFERENCES scans(id).
  Meaning: link from BCR row to original source scan row.
  Population: future/optional linking step.
- bcr_config_path: TEXT.
  Meaning: relative path to .bcr.json sidecar if available.

### Table: binary_conversions

Tracks binary files and converted ASCII counterparts from XAS_binary_conversion tree.

Columns:
- id: INTEGER PRIMARY KEY.
- scan_id: INTEGER REFERENCES scans(id).
  Meaning: optional link to original row in scans.
  Linking rule: match by session_dir + binary_name against scans basename and session.
- session_dir: TEXT NOT NULL.
  Meaning: session folder under conversion root.
- binary_name: TEXT NOT NULL.
  Meaning: original binary filename, for example C265A_007.001.
- ascii_name: TEXT NOT NULL.
  Meaning: converted ASCII filename, for example C265A_007_A.001.
- ascii_path: TEXT NOT NULL UNIQUE.
  Meaning: relative path to converted ASCII file under conversion root.
- n_points: INTEGER.
  Meaning: optional point count parsed from conversion_log.txt Layout line.
- n_cols: INTEGER.
  Meaning: optional column count parsed from conversion_log.txt Layout line.

Constraints:
- UNIQUE(session_dir, binary_name)
- UNIQUE(ascii_path)

### Table: ingest_checkpoints

Resumability table for ingest.

Columns:
- folder_name: TEXT PRIMARY KEY.
  Meaning: top-level folder name already indexed.
- completed_at: TEXT.
  Meaning: SQLite datetime('now') timestamp of completion.
- files_inserted: INTEGER.
  Meaning: number of rows inserted/updated in that folder pass.

## Parsing and Column Population Rules

### Ingest phase (scripts/01_ingest_lite.py)

Inputs:
- mounted data root path

Populates:
- sessions: dir_name, year, beamline, owner
- scans: path/filename-derived fields and file_size_bytes
- ingest_checkpoints

Does not parse headers in this phase.

### Depositor enrichment phase (scripts/01b_enrich_lite_depositors.py)

Inputs:
- lite DB sessions table
- Google Drive root folder ID for XAS_Beamline_Data

Populates/updates:
- sessions.depositor_name (from Drive `owners` metadata)

Safety:
- DB write only; no source file writes.

### Header phase (scripts/02_parse_headers_lite.py)

Inputs:
- scans rows + local files
- reads first max-bytes per file (default 4096)

Populates/updates:
- scans.format
- scans.element
- scans.k_max
- scans.n_channels
- scans.has_med

No full-file read required.

### Binary conversion phase (scripts/08_ingest_binary_conversions.py)

Inputs:
- conversion root folder
- optional conversion_log.txt for layout metrics

Populates/updates:
- binary_conversions rows
- binary_conversions.scan_id where a match is found

## Lite File Inventory (Keep vs Archive)

Keep these for lite workflow:
- xas_lite_db.py
- scripts/01_ingest_lite.py
- scripts/01b_enrich_lite_depositors.py
- scripts/02_parse_headers_lite.py
- scripts/08_ingest_binary_conversions.py
- scripts/09_lite_stats.py
- spec_lite.md

Keep conditionally (shared utility or optional write path):
- xas_db_builder.py
  Reason: lite parser reuses stable filename/session parsing functions.
- scripts/06_import_stubs.py
  Reason: only needed when importing manual BCR config stubs; write behavior is gated by --write-sidecars.

Legacy scripts safe to archive once lite is validated end-to-end:
- scripts/01_list_gdrive.py
- scripts/02_parse_headers.py
- scripts/03_link_bcr.py
- scripts/04_link_curated.py
- scripts/05_emit_stubs.py
- scripts/07_validate_curated.py

Note: archive after confirming no active dependencies in your current runbook.

## Recommended Lite Run Order

1) Build/refresh lite DB from mounted drive:
- python scripts/01_ingest_lite.py --db xas_catalog_lite.db --data-root "<mounted XAS_Beamline_Data>"

2) Enrich depositor names from Drive owners metadata:
- python scripts/01b_enrich_lite_depositors.py --db xas_catalog_lite.db --raw-root-id "<XAS_Beamline_Data_folder_id>" --gdrive-credentials "<service_account.json>"

3) Parse minimal headers:
- python scripts/02_parse_headers_lite.py --db xas_catalog_lite.db --local-root "<mounted XAS_Beamline_Data>" --max-bytes 4096

4) Ingest binary conversion mappings:
- python scripts/08_ingest_binary_conversions.py --db xas_catalog_lite.db --conversion-root "<mounted XAS_binary_conversion>"

5) Quick health/stats check:
- python scripts/09_lite_stats.py --db xas_catalog_lite.db

## Migration and Compatibility Notes

- xas_lite_db.create_schema includes a migration path that adds sessions.depositor_name if it is missing in an existing lite DB.
- Existing lite DB files created before depositor_name will continue working.

## Future-safe Notes

- Keep old and lite pipelines separated by DB filename.
- Use xas_catalog_lite.db for lite scripts.
- Avoid mixing legacy scripts against lite DB unless explicitly designed for that schema.
