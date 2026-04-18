#!/usr/bin/env python3
"""Parse only the header bytes needed for lite schema fields."""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path

from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import xas_lite_db as ldb

COMMIT_EVERY = 1000
MAX_ERROR_EXAMPLES = 5
DEFAULT_DATA_ROOT = "/Users/stetef/Library/CloudStorage/GoogleDrive-stetef@stanford.edu/Shared drives/XAS_Beamline_Data"

ELEMENT_SYMBOLS = {
    "H", "He", "Li", "Be", "B", "C", "N", "O", "F", "Ne",
    "Na", "Mg", "Al", "Si", "P", "S", "Cl", "Ar", "K", "Ca",
    "Sc", "Ti", "V", "Cr", "Mn", "Fe", "Co", "Ni", "Cu", "Zn",
    "Ga", "Ge", "As", "Se", "Br", "Kr", "Rb", "Sr", "Y", "Zr",
    "Nb", "Mo", "Tc", "Ru", "Rh", "Pd", "Ag", "Cd", "In", "Sn",
    "Sb", "Te", "I", "Xe", "Cs", "Ba", "La", "Ce", "Pr", "Nd",
    "Pm", "Sm", "Eu", "Gd", "Tb", "Dy", "Ho", "Er", "Tm", "Yb",
    "Lu", "Hf", "Ta", "W", "Re", "Os", "Ir", "Pt", "Au", "Hg",
    "Tl", "Pb", "Bi", "Po", "At", "Rn", "Fr", "Ra", "Ac", "Th",
    "Pa", "U", "Np", "Pu", "Am", "Cm", "Bk", "Cf", "Es", "Fm",
    "Md", "No", "Lr", "Rf", "Db", "Sg", "Bh", "Hs", "Mt", "Ds",
    "Rg", "Cn", "Nh", "Fl", "Mc", "Lv", "Ts", "Og",
}


def parse_detector_mode(header_text: str) -> str:
    """Infer detector mode from .det header lines.

    Rules:
    - token starts with trans and ends with .det -> T
    - any other .det token -> F
    - no .det token -> ?
    """
    for line in header_text.splitlines():
        line_clean = line.replace("\x00", "").strip()
        if not line_clean or ".det" not in line_clean.lower():
            continue

        m = re.search(r"([^\s]+\.det)\b", line_clean, flags=re.IGNORECASE)
        token = m.group(1) if m else line_clean.split()[-1]
        token = token.split("/")[-1].lower()

        if token.startswith("trans") and token.endswith(".det"):
            return "T"
        return "F"

    return "?"


def parse_element_kmax(header_text: str) -> tuple[str | None, float | None]:
    for line in header_text.splitlines():
        line_clean = line.replace("\x00", "").strip()
        if ".rgn" not in line_clean.lower():
            continue
        if not line_clean:
            continue
        token = line_clean.split()[-1].split("/")[-1]
        stem = re.sub(r"\.rgn$", "", token, flags=re.IGNORECASE)
        if not stem:
            continue
        if "time_scan" in stem.lower():
            return None, None

        elem: str | None = None
        elem_len = 0

        if len(stem) >= 2:
            elem2 = stem[:2][0].upper() + stem[:2][1].lower()
            if elem2 in ELEMENT_SYMBOLS:
                elem = elem2
                elem_len = 2

        if elem is None:
            elem1 = stem[:1].upper()
            if elem1 in ELEMENT_SYMBOLS:
                elem = elem1
                elem_len = 1

        if elem is None:
            continue

        suffix = stem[elem_len:]
        raw: float | None = None

        # Primary rule: first k after element, optional underscore, then numeric k value.
        mk = re.search(r"[kK]_?(\d+(?:\.\d+)?)", suffix)
        if mk:
            raw = float(mk.group(1))
        else:
            # Fallback for legacy names where k is omitted but value follows an underscore.
            mu = re.search(r"_(\d+(?:\.\d+)?)", suffix)
            if mu:
                raw = float(mu.group(1))

        if raw is None:
            return elem, None

        if 5.0 <= raw <= 25.0:
            return elem, raw
        if 5.0 <= (raw / 10.0) <= 25.0:
            return elem, (raw / 10.0)
        return elem, None

    m3 = re.search(r"^\s*Element\s*:\s*([A-Za-z]{1,3})\s*$", header_text, flags=re.IGNORECASE | re.MULTILINE)
    if m3:
        return m3.group(1), None
    return None, None


def parse_scan_category_from_rgn(header_text: str) -> str | None:
    """Classify from first .rgn token: contains foil/calib => reference, else data."""
    for line in header_text.splitlines():
        line_clean = line.replace("\x00", "").strip()
        if ".rgn" not in line_clean.lower():
            continue
        if not line_clean:
            continue

        token = line_clean.split()[-1]
        token = token.split("/")[-1]
        token_lower = token.lower()

        idx = token_lower.rfind(".rgn")
        stem = token_lower[:idx] if idx != -1 else token_lower
        return "reference" if ("foil" in stem or "calib" in stem) else "data"

    return None


def parse_n_channels(header_text: str) -> int | None:
    if "Data:" not in header_text:
        return parse_n_channels_from_labels(header_text)
    section = header_text.split("Data:", 1)[1]
    lines = [ln.strip() for ln in section.splitlines() if ln.strip()]
    if not lines:
        return parse_n_channels_from_labels(header_text)

    for idx, line in enumerate(lines):
        if re.match(r"^[\d\-.]", line):
            return idx
    return parse_n_channels_from_labels(header_text)


def parse_n_channels_from_labels(header_text: str) -> int | None:
    # Support both legacy SCA*_n style and compact SCA1CHn style labels.
    channel_nums: list[int] = []

    for match in re.findall(r"\bSCA\d*_(\d+)\b", header_text, flags=re.IGNORECASE):
        channel_nums.append(int(match))

    for match in re.findall(r"\bSCA\d*CH(\d+)\b", header_text, flags=re.IGNORECASE):
        channel_nums.append(int(match))

    if not channel_nums:
        return None
    return max(channel_nums)


def has_med_in_text(header_text: str) -> int:
    if re.search(r"ICR[_]?\d", header_text, re.IGNORECASE):
        return 1
    if re.search(r"\bFF\d+\b", header_text):
        return 1
    if re.search(r"SCA\d*_\d+", header_text, re.IGNORECASE):
        return 1
    if re.search(r"\bSCA\d*CH\d+\b", header_text, re.IGNORECASE):
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast header parser for lite DB")
    parser.add_argument("--db", default="xas_catalog_lite.db")
    parser.add_argument(
        "--local-root",
        default=DEFAULT_DATA_ROOT,
        help="Root folder where scans.file_path entries are rooted",
    )
    parser.add_argument("--max-bytes", type=int, default=4096)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--only-missing", action="store_true", default=True)
    parser.add_argument(
        "--all",
        action="store_true",
        help="Parse all eligible ASCII raw files, even if header fields are already populated",
    )
    args = parser.parse_args()

    local_root = Path(args.local_root).resolve()
    print(f"Using local root: {local_root}")
    conn = ldb.connect_db(args.db)
    try:
        ldb.create_schema(conn)

        where = (
            "WHERE s.is_processed_output = 0 "
            "AND s.format = 'ascii' "
            "AND s.file_path GLOB '*.[0-9][0-9][0-9]'"
        )
        if args.only_missing and not args.all:
            where += " AND (element IS NULL OR k_max IS NULL OR n_channels IS NULL OR has_med IS NULL OR detector_mode IS NULL OR scan_category IS NULL)"

        sql = f"""
            SELECT
                s.id,
                s.file_path
            FROM scans s
            {where}
            ORDER BY s.id
        """
        if args.limit > 0:
            sql += f" LIMIT {args.limit}"

        rows = conn.execute(sql).fetchall()
        print(f"Rows to parse: {len(rows)}")

        updated = 0
        errors = 0
        missing_files = 0
        read_or_decode_errors = 0
        db_errors = 0
        error_examples: list[str] = []

        for rid, rel_path in tqdm(rows, desc="Parsing headers", unit="file"):
            abs_path = local_root / rel_path
            if not abs_path.exists() or not abs_path.is_file():
                errors += 1
                missing_files += 1
                if len(error_examples) < MAX_ERROR_EXAMPLES:
                    error_examples.append(
                        f"id={rid} missing file: {abs_path}"
                    )
                continue

            try:
                with abs_path.open("rb") as f:
                    chunk = f.read(args.max_bytes)
                text = chunk.decode("utf-8", errors="replace")
                element, kmax = parse_element_kmax(text)
                scan_category = parse_scan_category_from_rgn(text)
                n_channels = parse_n_channels(text)
                detector_mode = parse_detector_mode(text)
                # Prefer channel-count based MED detection for consistency.
                if n_channels is not None:
                    has_med = 1 if n_channels > 1 else 0
                else:
                    has_med = has_med_in_text(text)
                conn.execute(
                    """
                    UPDATE scans
                    SET element = ?, k_max = ?, n_channels = ?, has_med = ?, detector_mode = ?, scan_category = ?, k_max_source = 'parsed'
                    WHERE id = ?
                    """,
                    (element, kmax, n_channels, has_med, detector_mode, scan_category, int(rid)),
                )

                updated += 1
                if updated % COMMIT_EVERY == 0:
                    conn.commit()
            except (OSError, UnicodeError) as exc:
                errors += 1
                read_or_decode_errors += 1
                if len(error_examples) < MAX_ERROR_EXAMPLES:
                    error_examples.append(f"id={rid} read/decode error: {exc}")
            except sqlite3.Error as exc:
                errors += 1
                db_errors += 1
                if len(error_examples) < MAX_ERROR_EXAMPLES:
                    error_examples.append(f"id={rid} sqlite error: {exc}")

        conn.commit()
        print(f"Updated rows: {updated}")
        print(f"Rows with read/update errors: {errors}")
        if errors:
            print(f"  missing files: {missing_files}")
            print(f"  read/decode errors: {read_or_decode_errors}")
            print(f"  sqlite errors: {db_errors}")
            if error_examples:
                print("Error examples:")
                for msg in error_examples:
                    print(f"  - {msg}")
            if missing_files:
                print(
                    "Hint: if file_path values are relative to another root, rerun with --local-root pointing to that data root."
                )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
