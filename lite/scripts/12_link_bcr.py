#!/usr/bin/env python3
"""
03_link_bcr.py

- Link BCR rows to source rows (bcr_source_id)
- Discover local .bcr.json sidecars and store bcr_config_path
- Report source files missing BCR counterparts
- Report BCR files missing config sidecars
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import xas_lite_db as xdb


def find_sidecar_path(local_root: Path, rel_file_path: str) -> str | None:
    src = local_root / rel_file_path
    sidecar = src.parent / f"{src.name}.bcr.json"
    if sidecar.exists() and sidecar.is_file():
        return str(sidecar.relative_to(local_root)).replace("\\", "/")
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Link BCR files and sidecars")
    parser.add_argument("--db", default="xas_catalog.db")
    parser.add_argument("--local-root", default=".")
    args = parser.parse_args()

    local_root = Path(args.local_root).resolve()

    conn = xdb.connect_db(args.db)
    try:
        linked, unmatched = xdb.link_bcr_rows(conn)

        bcr_rows = conn.execute(
            "SELECT id, file_path FROM scans WHERE is_bcr = 1"
        ).fetchall()

        sidecars_found = 0
        for rid, rel_path in bcr_rows:
            sidecar_rel = find_sidecar_path(local_root, rel_path)
            if sidecar_rel is None:
                continue
            conn.execute(
                "UPDATE scans SET bcr_config_path = ? WHERE id = ?",
                (sidecar_rel, int(rid)),
            )
            sidecars_found += 1

        # Report raw files missing any BCR counterpart.
        missing_bcr = conn.execute(
            """
            SELECT r.file_path
                        FROM scans r
                        WHERE r.is_bcr = 0
              AND r.format = 'ascii'
                            AND r.is_processed_output = 0
              AND NOT EXISTS (
                                    SELECT 1 FROM scans b
                  WHERE b.bcr_source_id = r.id
              )
            ORDER BY r.file_path
            """
        ).fetchall()

        # Report BCR files missing sidecar.
        missing_sidecar = conn.execute(
            """
            SELECT file_path
                        FROM scans
                        WHERE is_bcr = 1
              AND bcr_config_path IS NULL
            ORDER BY file_path
            """
        ).fetchall()

        conn.commit()

        print(f"Linked BCR rows: {linked}")
        print(f"Unmatched BCR rows: {unmatched}")
        print(f"BCR sidecars found: {sidecars_found}")
        print(f"ASCII source files missing BCR counterpart: {len(missing_bcr)}")
        print(f"BCR files missing sidecar: {len(missing_sidecar)}")

        if missing_bcr:
            print("\nFirst 25 missing BCR source files:")
            for (p,) in missing_bcr[:25]:
                print(f"  {p}")

        if missing_sidecar:
            print("\nFirst 25 BCR files missing sidecar:")
            for (p,) in missing_sidecar[:25]:
                print(f"  {p}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
