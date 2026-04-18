#!/usr/bin/env python3
"""Ingest mounted Google Drive folders into the lightweight SQLite schema."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import xas_lite_db as ldb


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest mounted Google Drive tree into lite DB")
    parser.add_argument("--db", default="xas_catalog_lite.db")
    parser.add_argument(
        "--data-root",
        default="/Users/stetef/Library/CloudStorage/GoogleDrive-stetef@stanford.edu/Shared drives/XAS_Beamline_Data",
        help="Mounted Google Drive path to XAS_Beamline_Data",
    )
    parser.add_argument("--limit-folders", type=int, default=0, help="Optional max number of top-level folders")
    parser.add_argument("--reindex", action="store_true", help="Ignore checkpoints and rescan all folders")
    args = parser.parse_args()

    data_root = Path(args.data_root).resolve()
    if not data_root.exists() or not data_root.is_dir():
        raise SystemExit(f"Data root not found: {data_root}")

    exclude_top = {"EXAFS_Diffusion_Model", "XAS_binary_conversion"}
    top_dirs = sorted(
        [
            d for d in data_root.iterdir()
            if d.is_dir() and d.name not in exclude_top and not d.name.startswith(".")
        ],
        key=lambda d: d.name,
    )
    if args.limit_folders > 0:
        top_dirs = top_dirs[: args.limit_folders]

    conn = ldb.connect_db(args.db)
    try:
        ldb.create_schema(conn)

        done = set() if args.reindex else ldb.get_completed_folders(conn)
        pending = [d for d in top_dirs if d.name not in done]

        print(f"Top-level folders total: {len(top_dirs)}")
        print(f"Already indexed: {len(done)}")
        print(f"Pending now: {len(pending)}")

        total_inserted = 0
        completed = len(done)
        for top_dir in pending:
            rels: list[str] = []
            for dirpath, _dirs, files in os.walk(top_dir, followlinks=False):
                for fname in files:
                    rel = os.path.relpath(os.path.join(dirpath, fname), data_root).replace("\\", "/")
                    rels.append(rel)

            inserted_this_folder = 0
            for rel in rels:
                if ldb.upsert_scan_from_rel_path(
                    conn,
                    rel,
                    local_root=data_root,
                    depositor_name=None,
                ):
                    inserted_this_folder += 1

            ldb.mark_folder_complete(conn, top_dir.name, inserted_this_folder)
            total_inserted += inserted_this_folder
            completed += 1
            print(f"[{completed}/{len(top_dirs)}] {top_dir.name}: +{inserted_this_folder} rows")

        print(f"Rows inserted/updated this run: {total_inserted}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
