#!/usr/bin/env python3
"""Export F-mode MED ascii paths for beamlines 7-3 and 9-3 with summary stats."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def build_selected_view(conn: sqlite3.Connection) -> None:
    conn.execute("DROP VIEW IF EXISTS selected_ascii_fmode_med")
    conn.execute(
        """
        CREATE TEMP VIEW selected_ascii_fmode_med AS
        WITH direct_ascii AS (
          SELECT
            s.file_path AS out_path,
            s.element AS element,
                        COALESCE(s.n_channels, 0) AS n_channels,
                        s.scan_category AS scan_category
          FROM scans s
          JOIN sessions ses ON ses.id = s.session_id
          WHERE ses.beamline IN ('7-3', '9-3')
            AND s.is_processed_output = 0
            AND s.format = 'ascii'
            AND s.detector_mode = 'F'
            AND COALESCE(s.has_med, 0) = 1
        ),
                qualified_binary AS (
                    SELECT
                        s.id,
                        s.file_path,
                        s.element,
                        COALESCE(s.n_channels, 0) AS n_channels,
                        s.scan_category,
                        s.detector_mode,
                        s.has_med AS has_med
                    FROM scans s
                    JOIN sessions ses ON ses.id = s.session_id
                    WHERE ses.beamline IN ('7-3', '9-3')
                        AND s.is_processed_output = 0
                        AND s.format = 'binary'
                ),
        binary_linked_ascii AS (
          SELECT
            bc.ascii_path AS out_path,
                        COALESCE(ascii_sc.element, qb.element) AS element,
                        COALESCE(ascii_sc.n_channels, qb.n_channels, 0) AS n_channels,
                        COALESCE(ascii_sc.scan_category, qb.scan_category) AS scan_category
                    FROM qualified_binary qb
                    JOIN binary_conversions bc ON bc.scan_id = qb.id
                    LEFT JOIN scans ascii_sc ON ascii_sc.file_path = bc.ascii_path
            AND bc.ascii_path IS NOT NULL
                    WHERE COALESCE(ascii_sc.detector_mode, qb.detector_mode, 'F') = 'F'
                        AND COALESCE(ascii_sc.has_med, qb.has_med, 1) = 1
        )
                SELECT DISTINCT out_path, element, n_channels, scan_category
        FROM (
          SELECT * FROM direct_ascii
          UNION ALL
          SELECT * FROM binary_linked_ascii
        )
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export ASCII paths for 7-3/9-3 F-mode MED scans, including binary-linked ASCII."
    )
    parser.add_argument("--db", default="xas-catalog-lite.db", help="Path to SQLite DB")
    parser.add_argument(
        "--out",
        default="ascii_fmode_med_7-3_9-3_report.txt",
        help="Output report text file",
    )
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    out_path = Path(args.out).resolve()

    conn = sqlite3.connect(str(db_path))
    try:
        build_selected_view(conn)

        total_matching_paths = conn.execute(
            "SELECT COUNT(*) FROM selected_ascii_fmode_med"
        ).fetchone()[0]

        total_n_channels = conn.execute(
            "SELECT COALESCE(SUM(n_channels), 0) FROM selected_ascii_fmode_med"
        ).fetchone()[0]

        element_rows = conn.execute(
            """
            SELECT
              COALESCE(element, 'NULL') AS element,
                            SUM(CASE WHEN scan_category = 'reference' THEN 1 ELSE 0 END) AS reference_count,
                            SUM(CASE WHEN scan_category = 'data' THEN 1 ELSE 0 END) AS data_count,
                            COALESCE(SUM(CASE WHEN scan_category = 'data' THEN n_channels ELSE 0 END), 0) AS data_channels_sum
            FROM selected_ascii_fmode_med
                        WHERE element IS NOT NULL
            GROUP BY COALESCE(element, 'NULL')
                        ORDER BY data_count DESC, reference_count DESC, element
            """
        ).fetchall()

        path_rows = conn.execute(
            "SELECT out_path FROM selected_ascii_fmode_med ORDER BY out_path"
        ).fetchall()

    finally:
        conn.close()

    with out_path.open("w", encoding="utf-8") as f:
        f.write("CONFIGURATION\n")
        f.write("beamline IN (7-3, 9-3)\n")
        f.write("mode = F\n")
        f.write("multi-element detector required (has_med = 1)\n")
        f.write("includes direct ascii + binary-linked ascii_path\n")
        f.write("\n")

        f.write("TOTAL_MATCHING_PATHS\n")
        f.write(f"{total_matching_paths}\n")
        f.write("\n")

        f.write("ELEMENT_BREAKDOWN (element|reference_count|data_count|data_channels_sum)\n")
        for element, reference_count, data_count, data_channels_sum in element_rows:
            f.write(f"{element}|{reference_count}|{data_count}|{data_channels_sum}\n")
        f.write("\n")

        f.write("TOTAL_N_CHANNELS\n")
        f.write(f"{total_n_channels}\n")
        f.write("\n")

        f.write("PATHS\n")
        for (out_path_row,) in path_rows:
            f.write(f"{out_path_row}\n")

    print(f"Wrote report: {out_path}")
    print(f"Total matching paths: {total_matching_paths}")
    print(f"Total n_channels: {total_n_channels}")


if __name__ == "__main__":
    main()
