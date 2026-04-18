#!/usr/bin/env python3
"""Track binary files and their converted ASCII equivalents in lite DB."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import xas_lite_db as ldb


def is_ascii_variant_name(name: str) -> bool:
    if "_A." in name:
        return True
    stem = Path(name).stem.lower()
    return stem.endswith("_abad") or stem.endswith("_a_edited")


def parse_layout_from_log(log_text: str, binary_name: str) -> tuple[int | None, int | None]:
    pattern = re.compile(
        rf"Processing:\s*{re.escape(binary_name)}\s*\n(?:.*\n)*?\s*Layout:\s*\d+\s*block\(s\),\s*(\d+)\s*cols,\s*(\d+)\s*pts",
        flags=re.MULTILINE,
    )
    m = pattern.search(log_text)
    if not m:
        return None, None
    return int(m.group(2)), int(m.group(1))


def resolve_scan_id(conn, session_dir: str, binary_name: str) -> int | None:
    rows = conn.execute(
        """
        SELECT s.id
        FROM scans s
        JOIN sessions ses ON ses.id = s.session_id
        WHERE ses.dir_name = ?
          AND (s.file_path = ? OR s.file_path LIKE ?)
        ORDER BY s.id
        """,
        (session_dir, binary_name, f"%/{binary_name}"),
    ).fetchall()
    if not rows:
        return None
    return int(rows[0][0])


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest binary->ASCII conversion mappings into lite DB")
    parser.add_argument("--db", default="xas_catalog_lite.db")
    parser.add_argument(
        "--conversion-root",
        default="/Users/stetef/Library/CloudStorage/GoogleDrive-stetef@stanford.edu/Shared drives/XAS_Beamline_Data/XAS_binary_conversion",
    )
    parser.add_argument("--log-file", default="conversion_log.txt")
    args = parser.parse_args()

    conversion_root = Path(args.conversion_root).resolve()
    if not conversion_root.exists() or not conversion_root.is_dir():
        raise SystemExit(f"Conversion root not found: {conversion_root}")

    log_path = conversion_root / args.log_file
    log_text = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""

    conn = ldb.connect_db(args.db)
    try:
        ldb.create_schema(conn)

        sessions = sorted([d for d in conversion_root.iterdir() if d.is_dir() and not d.name.startswith(".")], key=lambda d: d.name)

        inserted = 0
        linked = 0
        for ses_dir in tqdm(sessions, desc="Sessions", unit="session"):
            files = {f.name: f for f in ses_dir.iterdir() if f.is_file()}

            for binary_name in tqdm(sorted(files), desc=f"{ses_dir.name}", unit="file", leave=False):
                if is_ascii_variant_name(binary_name) or binary_name.endswith(".bcr.json"):
                    continue

                parts = binary_name.rsplit(".", 1)
                if len(parts) != 2:
                    continue
                ascii_name = f"{parts[0]}_A.{parts[1]}"
                if ascii_name not in files:
                    continue

                rel_ascii = f"XAS_binary_conversion/{ses_dir.name}/{ascii_name}"
                n_points, n_cols = parse_layout_from_log(log_text, binary_name)

                conn.execute(
                    """
                    INSERT INTO binary_conversions (
                        scan_id, session_dir, binary_name, ascii_name, ascii_path, n_points, n_cols
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_dir, binary_name) DO UPDATE SET
                        ascii_name = excluded.ascii_name,
                        ascii_path = excluded.ascii_path,
                        n_points = COALESCE(excluded.n_points, binary_conversions.n_points),
                        n_cols = COALESCE(excluded.n_cols, binary_conversions.n_cols)
                    """,
                    (None, ses_dir.name, binary_name, ascii_name, rel_ascii, n_points, n_cols),
                )
                inserted += 1

                scan_id = resolve_scan_id(conn, ses_dir.name, binary_name)
                if scan_id is not None:
                    conn.execute(
                        """
                        UPDATE binary_conversions
                        SET scan_id = ?
                        WHERE session_dir = ? AND binary_name = ?
                        """,
                        (scan_id, ses_dir.name, binary_name),
                    )
                    linked += 1

        conn.commit()
        print(f"Mappings inserted/updated: {inserted}")
        print(f"Mappings linked to scans: {linked}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
