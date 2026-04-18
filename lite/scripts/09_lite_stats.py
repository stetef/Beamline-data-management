#!/usr/bin/env python3
"""Print lightweight DB stats for quick tracking and sanity checks."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def q1(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> int:
    row = conn.execute(sql, args).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def print_section(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Quick stats for xas_catalog_lite.db")
    parser.add_argument("--db", default="xas_catalog_lite.db")
    parser.add_argument("--top", type=int, default=10, help="Top N values to print in grouped stats")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        print(f"Lite DB: {db_path}")

        print_section("Core Counts (Script 01)")
        total_sessions = q1(conn, "SELECT COUNT(*) FROM sessions")
        total_scans = q1(conn, "SELECT COUNT(*) FROM scans")
        other_files = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_processed_output = 1")
        raw_data_files = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_processed_output = 0")
        total_binary_map = q1(conn, "SELECT COUNT(*) FROM binary_conversions")
        print(f"sessions: {total_sessions}")
        print(f"scans: {total_scans}")
        print(f"raw data files: {raw_data_files}")
        print(f"other files: {other_files}")
        print(f"binary_conversions: {total_binary_map}")

        print_section("Format Breakdown (Script 01)")
        binary_total = q1(conn, "SELECT COUNT(*) FROM scans WHERE format = 'binary'")
        other_total = q1(conn, "SELECT COUNT(*) FROM scans WHERE format = 'other'")
        ascii_data = q1(conn, "SELECT COUNT(*) FROM scans WHERE format = 'ascii' AND scan_category = 'data'")
        ascii_reference = q1(conn, "SELECT COUNT(*) FROM scans WHERE format = 'ascii' AND scan_category = 'reference'")
        print(f"{'binary':>8}: total={binary_total}")
        print(f"{'other':>8}: total={other_total}")
        print(f"{'ascii':>8}: data={ascii_data}, reference={ascii_reference}")

        print_section("Header Coverage (Raw ASCII Only) (Script 02)")
        has_element = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_processed_output = 0 AND format = 'ascii' AND element IS NOT NULL")
        has_k = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_processed_output = 0 AND format = 'ascii' AND k_max IS NOT NULL")
        has_nch = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_processed_output = 0 AND format = 'ascii' AND n_channels > 1")
        total_nch = q1(conn, "SELECT COALESCE(SUM(n_channels), 0) FROM scans WHERE is_processed_output = 0 AND format = 'ascii' AND n_channels > 1")
        has_med = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_processed_output = 0 AND format = 'ascii' AND n_channels > 1")
        if has_column(conn, "scans", "detector_mode"):
            mode_unknown = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_processed_output = 0 AND format = 'ascii' AND detector_mode IS NULL")
        else:
            mode_unknown = 0
        print(f"with element: {has_element}")
        print(f"with k_max: {has_k}")
        print(f"with n_channels (>1): {has_nch}")
        print(f"total channels (sum n_channels): {total_nch}")
        print(f"has multi-element detector: {has_med}")
        print(f"detector mode missing: {mode_unknown}")

        print_section("BCR Tracking (Script Pending)")
        bcr_rows = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_bcr = 1")
        bcr_linked = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_bcr = 1 AND bcr_source_id IS NOT NULL")
        bcr_cfg = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_bcr = 1 AND bcr_config_path IS NOT NULL")
        print(f"BCR rows: {bcr_rows}")
        print(f"BCR linked to source: {bcr_linked}")
        print(f"BCR with config path: {bcr_cfg}")

        print_section("Binary Conversion Mapping (Script 08)")
        total_binary = q1(conn, "SELECT COUNT(*) FROM scans WHERE is_processed_output = 0 AND format = 'binary'")
        linked_binary = q1(conn, "SELECT COUNT(DISTINCT scan_id) FROM binary_conversions WHERE scan_id IS NOT NULL")
        unlinked_binary = total_binary - linked_binary
        print(f"total binary scans: {total_binary}")
        print(f"linked to scans: {linked_binary}")
        print(f"unlinked: {unlinked_binary}")

        print_section("Top Elements (Script 02)")
        for elem, cnt in conn.execute(
            """
            SELECT element, COUNT(*) AS c
            FROM scans
            WHERE element IS NOT NULL
            GROUP BY element
            ORDER BY c DESC, element
            LIMIT ?
            """,
            (args.top,),
        ).fetchall():
            print(f"{elem:>4}: {cnt}")

        print_section("Beamline Summary (Script 01)")
        for beamline, cnt in conn.execute(
            """
            SELECT COALESCE(ses.beamline, 'NULL') AS beamline, COUNT(*)
            FROM scans sc
            JOIN sessions ses ON ses.id = sc.session_id
            GROUP BY COALESCE(ses.beamline, 'NULL')
            ORDER BY COUNT(*) DESC, beamline
            """
        ).fetchall():
            print(f"{beamline:>6}: {cnt}")

        print_section("Missing File Size (Script 01)")
        missing_size = q1(conn, "SELECT COUNT(*) FROM scans WHERE file_size_bytes IS NULL")
        print(f"rows missing file_size_bytes: {missing_size}")

        print_section("Session Depositors (Script 01b)")
        for depositor, cnt in conn.execute(
            """
            SELECT COALESCE(depositor_name, 'NULL') AS depositor, COUNT(*)
            FROM sessions
            GROUP BY COALESCE(depositor_name, 'NULL')
            ORDER BY COUNT(*) DESC, depositor
            """
        ).fetchall():
            print(f"{depositor:>24}: {cnt}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
