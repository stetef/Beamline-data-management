#!/usr/bin/env python3
"""Export NULL-element data scan paths and summarize unique .rgn header lines."""

from __future__ import annotations

import argparse
import sqlite3
from collections import Counter
from pathlib import Path

DEFAULT_DATA_ROOT = "/Users/stetef/Library/CloudStorage/GoogleDrive-stetef@stanford.edu/Shared drives/XAS_Beamline_Data"


def select_null_data_paths(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        WITH selected AS (
          WITH direct_ascii AS (
            SELECT
              s.file_path AS out_path,
              s.element AS element,
              s.scan_category AS scan_category
            FROM scans s
            JOIN sessions ses ON ses.id = s.session_id
            WHERE ses.beamline IN ('7-3', '9-3')
              AND s.is_processed_output = 0
              AND s.format = 'ascii'
              AND s.detector_mode = 'F'
              AND COALESCE(s.has_med, 0) = 1
          ),
          binary_linked_ascii AS (
            SELECT
              bc.ascii_path AS out_path,
              s.element AS element,
              COALESCE(ascii_sc.scan_category, s.scan_category) AS scan_category
            FROM scans s
            JOIN sessions ses ON ses.id = s.session_id
            JOIN binary_conversions bc ON bc.scan_id = s.id
            LEFT JOIN scans ascii_sc ON ascii_sc.file_path = bc.ascii_path
            WHERE ses.beamline IN ('7-3', '9-3')
              AND s.is_processed_output = 0
              AND s.format = 'binary'
              AND s.detector_mode = 'F'
              AND COALESCE(s.has_med, 0) = 1
              AND bc.ascii_path IS NOT NULL
          )
          SELECT DISTINCT out_path, element, scan_category
          FROM (
            SELECT * FROM direct_ascii
            UNION ALL
            SELECT * FROM binary_linked_ascii
          )
        )
        SELECT out_path
        FROM selected
        WHERE element IS NULL AND scan_category = 'data'
        ORDER BY out_path
        """
    ).fetchall()
    return [row[0] for row in rows]


def first_rgn_line(file_path: Path, max_bytes: int) -> str:
    try:
        with file_path.open("rb") as f:
            chunk = f.read(max_bytes)
    except OSError:
        return "[MISSING_OR_UNREADABLE]"

    text = chunk.decode("utf-8", errors="replace")
    for line in text.splitlines():
        clean = line.replace("\x00", "").strip()
        if ".rgn" in clean.lower():
            return clean
    return "[NO_RGN_LINE_FOUND]"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export NULL-element data file paths and summarize unique .rgn lines"
    )
    parser.add_argument("--db", default="xas-catalog-lite.db", help="Path to SQLite DB")
    parser.add_argument(
        "--local-root",
        default=DEFAULT_DATA_ROOT,
        help="Filesystem root used to resolve scans.file_path/ascii_path",
    )
    parser.add_argument(
        "--out",
        default="null_element_data_rgn_report.txt",
        help="Output report file",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=65536,
        help="Max bytes to read from each file when searching for .rgn line",
    )
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    local_root = Path(args.local_root).resolve()
    out_path = Path(args.out).resolve()

    conn = sqlite3.connect(str(db_path))
    try:
        paths = select_null_data_paths(conn)
    finally:
        conn.close()

    rgn_counts: Counter[str] = Counter()
    for rel_path in paths:
        rgn_line = first_rgn_line(local_root / rel_path, args.max_bytes)
        rgn_counts[rgn_line] += 1

    with out_path.open("w", encoding="utf-8") as f:
        f.write("CONFIGURATION\n")
        f.write("subset: beamline IN (7-3, 9-3), format/ascii_path in F mode, has_med=1\n")
        f.write("filter: element IS NULL AND scan_category = data\n")
        f.write(f"local_root: {local_root}\n")
        f.write("\n")

        f.write("TOTAL_PATHS\n")
        f.write(f"{len(paths)}\n")
        f.write("\n")

        f.write("RGN_LINE_COUNTS (count|line)\n")
        for line, count in sorted(rgn_counts.items(), key=lambda kv: (-kv[1], kv[0])):
            f.write(f"{count}|{line}\n")
        f.write("\n")

        f.write("PATHS\n")
        for rel_path in paths:
            f.write(f"{rel_path}\n")

    print(f"Wrote report: {out_path}")
    print(f"Total paths: {len(paths)}")
    print(f"Unique .rgn lines: {len(rgn_counts)}")


if __name__ == "__main__":
    main()
