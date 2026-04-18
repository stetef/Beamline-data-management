#!/usr/bin/env python3
"""Lightweight SQLite schema and helpers for XAS metadata tracking."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


RAW_DATA_SUFFIX_RE = re.compile(r"\.[0-9]{3}$")


@dataclass
class ParsedFile:
    file_path: str
    sample_name: Optional[str]
    scan_number: Optional[int]
    repeat: Optional[int]
    channel: Optional[str]
    spot_label: Optional[str]
    format: str
    is_processed_output: int
    has_bcr: int
    session_dir: str
    file_size_bytes: Optional[int]


def parse_session_dir(name: str) -> tuple[Optional[int], Optional[str]]:
    m = re.match(r"^(\d{4})_(\d-\d)_", name)
    if not m:
        return None, None
    return int(m.group(1)), m.group(2)


def is_ascii_name(name: str) -> tuple[bool, Optional[str]]:
    lower = name.lower()
    if not RAW_DATA_SUFFIX_RE.search(lower):
        return False, None

    if "_a." in lower:
        return True, "_A"
    stem = Path(lower).stem
    if stem.endswith("_abad"):
        return True, "_Abad"
    if stem.endswith("_a_edited"):
        return True, "_A_edited"
    return False, None


def is_processed_output_name(name: str) -> int:
    # Keep column name for compatibility; 0 means raw data file, 1 means other file.
    return 0 if RAW_DATA_SUFFIX_RE.search(name.lower()) else 1


def scans_supports_other_format(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'scans'"
    ).fetchone()
    ddl = (row[0] or "") if row else ""
    return "'other'" in ddl


def parse_data_filename(filename: str) -> tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[str]]:
    clean = filename
    if clean.startswith("BCR_"):
        clean = clean[4:]

    clean = re.sub(r"_+", "_", clean)

    # 7-3 style with explicit channel letter.
    m = re.match(r"^(.+?)_(spot\w+|s\d+)?_?(\d{3})_([A-Z])\.(\d{3})$", clean, re.IGNORECASE)
    if m:
        sample = m.group(1)
        spot = m.group(2)
        run = int(m.group(3))
        channel = m.group(4).upper()
        rep = int(m.group(5))

        if spot is None:
            spot_m = re.match(r"^(.+?)_(spot\w+|s\d+)$", sample, re.IGNORECASE)
            if spot_m:
                sample, spot = spot_m.group(1), spot_m.group(2)

        return sample, spot, run, rep, channel

    # 9-3 .dat style.
    m = re.match(r"^(.+?)[_-](spot\w+|s\d+)_(\d{3})_(\d{3})\.dat$", clean, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2), int(m.group(3)), int(m.group(4)), None

    m = re.match(r"^(.+?)_(\d{3})_(\d{3})\.dat$", clean, re.IGNORECASE)
    if m:
        sample = m.group(1)
        run = int(m.group(2))
        rep = int(m.group(3))
        spot = None
        spot_m = re.match(r"^(.+?)[_-](spot\w+|s\d+)$", sample, re.IGNORECASE)
        if spot_m:
            sample, spot = spot_m.group(1), spot_m.group(2)
        return sample, spot, run, rep, None

    return None, None, None, None, None


def parse_rel_path(rel_path: str, local_root: Optional[Path]) -> Optional[ParsedFile]:
    parts = rel_path.split("/")

    # Accept both "data/<session>/..." and "<session>/..." formats.
    if parts[0] == "data":
        if len(parts) < 3:
            return None
        offset = 1
    else:
        if len(parts) < 2:
            return None
        offset = 0

    basename = parts[-1]
    session_dir = parts[offset]

    ascii_flag, _ascii_style = is_ascii_name(basename)
    is_raw_data = bool(RAW_DATA_SUFFIX_RE.search(basename.lower()))
    if is_raw_data:
        fmt = "ascii" if ascii_flag else "binary"
    else:
        fmt = "other"
    sample_name, spot, run, rep, channel = parse_data_filename(basename)
    processed = is_processed_output_name(basename)

    file_size = None
    if local_root is not None:
        abs_path = local_root / rel_path
        if abs_path.exists() and abs_path.is_file():
            file_size = abs_path.stat().st_size

    return ParsedFile(
        file_path=rel_path,
        sample_name=sample_name,
        scan_number=run,
        repeat=rep,
        channel=channel,
        spot_label=spot,
        format=fmt,
        is_processed_output=processed,
        has_bcr=1 if basename.startswith("BCR_") else 0,
        session_dir=session_dir,
        file_size_bytes=file_size,
    )


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def link_bcr_rows(conn: sqlite3.Connection) -> tuple[int, int]:
    bcr_rows = conn.execute(
        """
        SELECT r.id, r.session_id, r.sample_name, r.scan_number, r.repeat, r.channel, COALESCE(r.spot_label, '')
        FROM scans r
        WHERE r.is_bcr = 1
        """
    ).fetchall()

    linked = 0
    unmatched = 0
    for row in bcr_rows:
        rid, session_id, sample_name, scan_number, repeat, channel, spot_label = row
        source = conn.execute(
            """
            SELECT id
            FROM scans
            WHERE session_id = ?
              AND sample_name IS ?
              AND is_bcr = 0
              AND scan_number IS ?
              AND repeat IS ?
              AND channel IS ?
              AND COALESCE(spot_label, '') = ?
            ORDER BY id
            LIMIT 1
            """,
            (session_id, sample_name, scan_number, repeat, channel, spot_label),
        ).fetchone()

        if source is None:
            unmatched += 1
            continue

        conn.execute(
            "UPDATE scans SET bcr_source_id = ? WHERE id = ?",
            (int(source[0]), int(rid)),
        )
        linked += 1

    return linked, unmatched


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY,
            dir_name TEXT NOT NULL UNIQUE,
            year INTEGER,
            beamline TEXT,
            depositor_name TEXT
        );

        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY,
            session_id INTEGER NOT NULL REFERENCES sessions(id),
            file_path TEXT NOT NULL UNIQUE,
            file_size_bytes INTEGER,
            sample_name TEXT,
            scan_number INTEGER,
            repeat INTEGER,
            channel TEXT,
            spot_label TEXT,
            format TEXT CHECK(format IN ('binary', 'ascii', 'other')),
            is_processed_output INTEGER NOT NULL DEFAULT 0,
            has_med INTEGER,
            detector_mode TEXT CHECK(detector_mode IN ('T', 'F', '?')),
            scan_category TEXT CHECK(scan_category IN ('data', 'reference')),
            element TEXT,
            k_max REAL,
            k_max_source TEXT CHECK(k_max_source IN ('parsed', 'calculated')) NOT NULL DEFAULT 'parsed',
            n_channels INTEGER,
            is_bcr INTEGER NOT NULL DEFAULT 0,
            bcr_source_id INTEGER REFERENCES scans(id),
            bcr_config_path TEXT
        );

        CREATE TABLE IF NOT EXISTS binary_conversions (
            id INTEGER PRIMARY KEY,
            scan_id INTEGER REFERENCES scans(id),
            session_dir TEXT NOT NULL,
            binary_name TEXT NOT NULL,
            ascii_name TEXT NOT NULL,
            ascii_path TEXT NOT NULL UNIQUE,
            n_points INTEGER,
            n_cols INTEGER,
            UNIQUE(session_dir, binary_name)
        );

        CREATE TABLE IF NOT EXISTS ingest_checkpoints (
            folder_name TEXT NOT NULL PRIMARY KEY,
            completed_at TEXT NOT NULL,
            files_inserted INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_scans_session ON scans(session_id);
        CREATE INDEX IF NOT EXISTS idx_scans_scan_repeat ON scans(scan_number, repeat);
        CREATE INDEX IF NOT EXISTS idx_scans_bcr ON scans(is_bcr, bcr_source_id);
        CREATE INDEX IF NOT EXISTS idx_scans_element ON scans(element, k_max);
        CREATE INDEX IF NOT EXISTS idx_binary_conv_session ON binary_conversions(session_dir, binary_name);
        """
    )

    # Backward-compatible migration for DBs created before depositor_name existed.
    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
    }
    if "depositor_name" not in cols:
        conn.execute("ALTER TABLE sessions ADD COLUMN depositor_name TEXT")

    # Backward-compatible migration for DBs created before k_max_source existed.
    scan_cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(scans)").fetchall()
    }
    if "k_max_source" not in scan_cols:
        conn.execute(
            "ALTER TABLE scans ADD COLUMN k_max_source TEXT CHECK(k_max_source IN ('parsed', 'calculated')) NOT NULL DEFAULT 'parsed'"
        )

    if "detector_mode" not in scan_cols:
        conn.execute("ALTER TABLE scans ADD COLUMN detector_mode TEXT CHECK(detector_mode IN ('T', 'F', '?'))")

    if "scan_category" not in scan_cols:
        conn.execute("ALTER TABLE scans ADD COLUMN scan_category TEXT CHECK(scan_category IN ('data', 'reference'))")

    # Normalize legacy labels where possible.
    if scans_supports_other_format(conn):
        conn.execute("UPDATE scans SET format = 'other' WHERE format = 'unknown'")


def get_completed_folders(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute("SELECT folder_name FROM ingest_checkpoints").fetchall()
    }


def mark_folder_complete(conn: sqlite3.Connection, folder_name: str, files_inserted: int) -> None:
    conn.execute(
        """
        INSERT INTO ingest_checkpoints (folder_name, completed_at, files_inserted)
        VALUES (?, datetime('now'), ?)
        ON CONFLICT(folder_name) DO UPDATE SET
            completed_at = excluded.completed_at,
            files_inserted = excluded.files_inserted
        """,
        (folder_name, files_inserted),
    )
    conn.commit()


def ensure_session(conn: sqlite3.Connection, session_dir: str, depositor_name: str | None = None) -> int:
    year, beamline = parse_session_dir(session_dir)

    conn.execute(
        """
        INSERT INTO sessions (dir_name, year, beamline, depositor_name)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(dir_name) DO UPDATE SET
            year = COALESCE(sessions.year, excluded.year),
            beamline = COALESCE(sessions.beamline, excluded.beamline),
            depositor_name = COALESCE(sessions.depositor_name, excluded.depositor_name)
        """,
        (session_dir, year, beamline, depositor_name),
    )
    row = conn.execute("SELECT id FROM sessions WHERE dir_name = ?", (session_dir,)).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to create/find session: {session_dir}")
    return int(row[0])


def upsert_scan_from_rel_path(
    conn: sqlite3.Connection,
    rel_path: str,
    local_root: Path,
    depositor_name: str | None = None,
) -> bool:
    rec = parse_rel_path(rel_path, local_root=local_root)
    if rec is None:
        return False

    session_id = ensure_session(conn, rec.session_dir, depositor_name=depositor_name)

    db_format = rec.format
    # Existing DBs may still enforce legacy CHECK(format IN ('binary','ascii','unknown')).
    if db_format == "other" and not scans_supports_other_format(conn):
        db_format = "unknown"

    conn.execute(
        """
        INSERT INTO scans (
            session_id, file_path, file_size_bytes, sample_name, scan_number,
            repeat, channel, spot_label, format, is_processed_output, is_bcr
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_path) DO UPDATE SET
            session_id = excluded.session_id,
            file_size_bytes = excluded.file_size_bytes,
            sample_name = excluded.sample_name,
            scan_number = excluded.scan_number,
            repeat = excluded.repeat,
            channel = excluded.channel,
            spot_label = excluded.spot_label,
            format = excluded.format,
            is_processed_output = excluded.is_processed_output,
            is_bcr = excluded.is_bcr
        """,
        (
            session_id,
            rec.file_path,
            rec.file_size_bytes,
            rec.sample_name,
            rec.scan_number,
            rec.repeat,
            rec.channel,
            rec.spot_label,
            db_format,
            rec.is_processed_output,
            rec.has_bcr,
        ),
    )
    return True
