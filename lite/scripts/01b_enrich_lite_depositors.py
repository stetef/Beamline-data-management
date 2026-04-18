#!/usr/bin/env python3
"""Backfill lite sessions.depositor_name from Google Drive folder owners."""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import xas_lite_db as ldb

DRIVE_SCOPE = ["https://www.googleapis.com/auth/drive.readonly"]
DEFAULT_RAW_ROOT_ID = "0ADIrFq6kbSieUk9PVA"
DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder"
DEFAULT_MODIFIER_SAMPLE_SIZE = 3


def build_drive_service(credentials_path: str | None):
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        import google.auth
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing Google API packages. Install: google-api-python-client google-auth"
        ) from exc

    if credentials_path:
        creds = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=DRIVE_SCOPE,
        )
    else:
        creds, _project_id = google.auth.default(scopes=DRIVE_SCOPE)

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_children(service, folder_id: str):
    page_token = None
    while True:
        resp = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields=(
                    "nextPageToken, "
                    "files(id, name, mimeType, driveId, "
                    "owners(emailAddress,displayName), "
                    "permissions(emailAddress,displayName,role,type,deleted), "
                    "sharingUser(emailAddress,displayName), "
                    "lastModifyingUser(emailAddress,displayName))"
                ),
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page_token,
            )
            .execute()
        )
        for item in resp.get("files", []):
            yield item
        page_token = resp.get("nextPageToken")
        if not page_token:
            break


def majority_last_modifier_from_children(service, folder_id: str, sample_size: int = DEFAULT_MODIFIER_SAMPLE_SIZE) -> str | None:
    """Return majority lastModifyingUser label from up to sample_size direct children (files or folders)."""
    page_token = None
    labels: list[str] = []

    while len(labels) < sample_size:
        resp = (
            service.files()
            .list(
                q=(
                    f"'{folder_id}' in parents and trashed = false"
                ),
                fields="nextPageToken, files(lastModifyingUser(emailAddress,displayName))",
                pageSize=min(200, sample_size * 20),
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page_token,
            )
            .execute()
        )

        for item in resp.get("files", []):
            user = item.get("lastModifyingUser") or {}
            label = user.get("emailAddress") or user.get("displayName")
            if label:
                labels.append(label)
            if len(labels) >= sample_size:
                break

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    if not labels:
        return None

    counts = Counter(labels)
    # Stable tie-break: highest count, then lexicographic label.
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def owner_label(item: dict) -> tuple[str | None, str | None]:
    owners = item.get("owners") or []
    if owners:
        first = owners[0]
        label = first.get("emailAddress") or first.get("displayName")
        if label:
            return label, "owners"

    # Shared Drive items often have empty owners by design.
    permissions = item.get("permissions") or []
    for role in ("organizer", "fileOrganizer", "writer"):
        for perm in permissions:
            if perm.get("deleted"):
                continue
            if perm.get("role") != role:
                continue
            # Prefer human users, but accept group labels if identity policy
            # hides direct user profile info.
            if perm.get("type") not in {"user", "group"}:
                continue
            label = perm.get("emailAddress") or perm.get("displayName")
            if label:
                return label, "permissions"

    sharing_user = item.get("sharingUser") or {}
    label = sharing_user.get("emailAddress") or sharing_user.get("displayName")
    if label:
        return label, "sharingUser"

    return None, None


def folder_last_modifier_label(item: dict) -> str | None:
    modifying_user = item.get("lastModifyingUser") or {}
    return modifying_user.get("emailAddress") or modifying_user.get("displayName")


def normalize_session_name(name: str) -> str:
    # Normalize separator and punctuation differences across data sources.
    lowered = name.lower().replace("/", " ").replace("\\", " ")
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return " ".join(lowered.split())


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate lite session depositor names from Drive owners metadata")
    parser.add_argument("--db", default="xas_catalog_lite.db")
    parser.add_argument(
        "--raw-root-id",
        default=DEFAULT_RAW_ROOT_ID,
        help="Google Drive folder ID of XAS_Beamline_Data root",
    )
    parser.add_argument("--gdrive-credentials", help="Service account JSON path (optional if ADC configured)")
    args = parser.parse_args()

    service = build_drive_service(args.gdrive_credentials)
    conn = ldb.connect_db(args.db)
    try:
        ldb.create_schema(conn)

        # Build name lookup indexes once so folder->session mapping is stable and cheap.
        session_rows = conn.execute("SELECT id, dir_name FROM sessions").fetchall()
        exact_name_to_ids: dict[str, list[int]] = {}
        normalized_name_to_ids: dict[str, list[int]] = {}
        for sid, dir_name in session_rows:
            exact_name_to_ids.setdefault(dir_name, []).append(int(sid))
            normalized_name_to_ids.setdefault(normalize_session_name(dir_name), []).append(int(sid))

        updated = 0
        skipped = 0
        normalized_matches = 0
        ambiguous_name_matches = 0
        source_counts = {
            "owners": 0,
            "permissions": 0,
            "sharingUser": 0,
            "lastModifyingUser": 0,
        }
        no_identity_visible = 0
        for item in tqdm(list_children(service, args.raw_root_id), desc="Resolving depositors", unit="folder"):
            if item.get("mimeType") != DRIVE_FOLDER_MIME:
                continue

            session_name = item["name"]
            depositor, source = owner_label(item)
            if not depositor:
                depositor = majority_last_modifier_from_children(service, item["id"], sample_size=DEFAULT_MODIFIER_SAMPLE_SIZE)
                if depositor:
                    source = "childLastModifyingUserMajority"
                else:
                    depositor = folder_last_modifier_label(item)
                    if depositor:
                        source = "lastModifyingUser"
                    else:
                        skipped += 1
                        if not (item.get("owners") or item.get("permissions") or item.get("sharingUser") or item.get("lastModifyingUser")):
                            no_identity_visible += 1
                        continue
            if source:
                source_counts[source] = source_counts.get(source, 0) + 1

            target_ids = exact_name_to_ids.get(session_name)
            used_normalized = False
            if not target_ids:
                target_ids = normalized_name_to_ids.get(normalize_session_name(session_name))
                used_normalized = bool(target_ids)

            if not target_ids:
                skipped += 1
                continue
            if len(target_ids) > 1:
                ambiguous_name_matches += 1
                skipped += 1
                continue

            if used_normalized:
                normalized_matches += 1

            res = conn.execute(
                """
                UPDATE sessions
                SET depositor_name = ?
                WHERE id = ?
                """,
                (depositor, int(target_ids[0])),
            )
            if res.rowcount > 0:
                updated += res.rowcount

        conn.commit()
        print(f"Session depositor rows updated: {updated}")
        print(f"Folders skipped (no owner metadata): {skipped}")
        print(f"Resolved via owners: {source_counts['owners']}")
        print(f"Resolved via permissions fallback: {source_counts['permissions']}")
        print(f"Resolved via sharingUser fallback: {source_counts['sharingUser']}")
        print(f"Resolved via lastModifyingUser fallback: {source_counts['lastModifyingUser']}")
        print(f"Resolved via majority of {DEFAULT_MODIFIER_SAMPLE_SIZE} child last-modifiers: {source_counts.get('childLastModifyingUserMajority', 0)}")
        print(f"Session matches via normalized folder name: {normalized_matches}")
        print(f"Folders skipped (ambiguous normalized name match): {ambiguous_name_matches}")
        print(f"Folders with no identity fields visible at all: {no_identity_visible}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
