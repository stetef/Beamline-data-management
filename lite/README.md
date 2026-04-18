# XAS Lite DB — Schema Reference

Database file: `xas-catalog-lite.db`

This is the quick-reference for the lite SQLite schema. For design intent, parsing rules, and pipeline run order, see [spec_lite.md](spec_lite.md).

---

## Table: `sessions`

One row per top-level session folder under `XAS_Beamline_Data`.

| Column | Type | Values / Range |
|---|---|---|
| `id` | INTEGER | Auto-incremented primary key |
| `dir_name` | TEXT | Top-level folder name, e.g. `2022_7-3_December_SteveRagsdale_NiMCRCrystal`; unique |
| `year` | INTEGER | Parsed from folder name prefix, e.g. `2009`–`2025`; NULL if unparseable |
| `beamline` | TEXT | Parsed from folder name, e.g. `7-3`, `9-3`; NULL if unparseable |
| `depositor_name` | TEXT | Drive folder owner email or display name; NULL until script 01b runs |

---

## Table: `scans`

One row per data file discovered during ingest.

| Column | Type | Values / Range |
|---|---|---|
| `id` | INTEGER | Auto-incremented primary key |
| `session_id` | INTEGER | FK → `sessions.id` |
| `file_path` | TEXT | Relative path from data root, e.g. `2009_7-3_Jun/FEFOIL_001_A.001`; unique |
| `file_size_bytes` | INTEGER | On-disk size in bytes; NULL if stat failed |
| `sample_name` | TEXT | Parsed sample token from filename, e.g. `FEFOIL`; NULL if unparseable |
| `scan_number` | INTEGER | Typically `001`–`999`; NULL if unparseable |
| `repeat` | INTEGER | Extension repeat index, e.g. `001`–`999`; NULL if unparseable |
| `channel` | TEXT | Letter token parsed from `_LETTER.EXT` suffix in 7-3-style filenames, e.g. `A` for `_A.001` (ASCII-format files); NULL for 9-3 `.dat` style. Used as a matching key in BCR linking. |
| `spot_label` | TEXT | Optional spot token, e.g. `spot1a`, `s2`; NULL if absent |
| `format` | TEXT | `ascii`, `binary`, `other` |
| `is_processed_output` | INTEGER | `0` = raw data file (`.NNN` extension); `1` = non-raw or processed |
| `has_med` | INTEGER | `1` = multi-element detector present; `0` = absent; NULL = not yet parsed |
| `detector_mode` | TEXT | `T` (transmission), `F` (fluorescence), `?` (ambiguous); NULL if not parsed |
| `scan_category` | TEXT | `data`, `reference`; NULL if not parsed |
| `element` | TEXT | Absorbing element symbol, e.g. `Fe`, `Cu`, `Ni`, `Mn`; NULL if unparsed |
| `k_max` | REAL | Max k value in Å⁻¹, typical range ~5–20; NULL if unparsed |
| `k_max_source` | TEXT | `parsed` (from `.rgn` token in header), `calculated`; default `parsed` |
| `n_channels` | INTEGER | Detector channel count inferred from header `Data:` section; NULL or `1` for single-channel |
| `is_bcr` | INTEGER | `0` = normal scan; `1` = BCR-prefixed file |
| `bcr_source_id` | INTEGER | FK → `scans.id` of the original source scan; NULL until script 12 runs or if unmatched |
| `bcr_config_path` | TEXT | Relative path to `.bcr.json` sidecar; NULL if not found |

---

## Table: `binary_conversions`

Tracks binary files and their converted ASCII counterparts from the `XAS_binary_conversion` tree.

| Column | Type | Values / Range |
|---|---|---|
| `id` | INTEGER | Auto-incremented primary key |
| `scan_id` | INTEGER | FK → `scans.id` for the original binary scan row; NULL if no match found |
| `session_dir` | TEXT | Session folder name under conversion root |
| `binary_name` | TEXT | Original binary filename, e.g. `C265A_007.001` |
| `ascii_name` | TEXT | Converted ASCII filename, e.g. `C265A_007_A.001` |
| `ascii_path` | TEXT | Relative path to converted ASCII file under conversion root; unique |
| `n_points` | INTEGER | Data point count from `conversion_log.txt` Layout line; NULL if not parsed |
| `n_cols` | INTEGER | Column count from `conversion_log.txt` Layout line; NULL if not parsed |

Unique constraints: `(session_dir, binary_name)`, `ascii_path`.

---

## Table: `ingest_checkpoints`

Resumability table for incremental ingest.

| Column | Type | Values / Range |
|---|---|---|
| `folder_name` | TEXT | Top-level session folder name; primary key |
| `completed_at` | TEXT | SQLite datetime string, e.g. `2024-01-15 10:23:45` |
| `files_inserted` | INTEGER | Count of rows inserted or updated in that folder pass |
