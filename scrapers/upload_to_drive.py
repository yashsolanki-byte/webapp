"""
Upload scraped data (DATA_Scraped/dd-mm-yy/UniversityName/*.csv) to Google Drive.
Uses same logic and credentials as NPF paid application/upload_to_drive.py.
Drive date folders use YYYY-MM-DD format (e.g. 2026-03-19); local folders stay dd-mm-yy.
"""

import os
import re
import sys

_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths


def _date_str_to_drive_format(date_str):
    """Convert dd-mm-yy to YYYY-MM-DD for Drive folder names (e.g. 19-03-26 -> 2026-03-19)."""
    if not date_str or not re.match(r"^\d{1,2}-\d{1,2}-\d{2,4}$", date_str.strip()):
        return date_str
    parts = date_str.strip().split("-")
    if len(parts) != 3:
        return date_str
    try:
        d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
        if y < 100:
            y = 2000 + y
        return f"{y:04d}-{m:02d}-{d:02d}"
    except (ValueError, TypeError):
        return date_str

DATA_SCRAPED_BASE = paths.DATA_SCRAPED_DIR
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# Default Drive folder (Shared Drive folder ID). Override with DRIVE_FOLDER_ID env.
DEFAULT_DRIVE_FOLDER_ID = "1eRiXMFHZQK0iVOZt9pB_vZELkV8bVf9j"

# Credentials: prefer NPF paid application/credentials.json, then data/runtime/credentials.json
def _default_credentials_path():
    parent_creds = os.path.join(paths.ROOT, "..", "NPF paid application", "credentials.json")
    parent_creds = os.path.abspath(parent_creds)
    if os.path.isfile(parent_creds):
        return parent_creds
    return paths.CREDENTIALS_JSON


def get_drive_service(credentials_path=None):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    path = credentials_path or _default_credentials_path()
    if not os.path.exists(path):
        raise FileNotFoundError(f"Credentials not found: {path}")
    creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def get_or_create_folder(service, parent_folder_id, folder_name):
    """Get or create a subfolder by name inside parent. Returns folder id."""
    # Escape single quotes in name for Drive query
    safe_name = (folder_name or "").replace("\\", "\\\\").replace("'", "\\'")
    q = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{safe_name}'"
    r = service.files().list(
        q=q,
        spaces="drive",
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = r.get("files", [])
    if files:
        return files[0]["id"]
    body = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    folder = service.files().create(
        body=body,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return folder["id"]


def upload_file(service, local_path, drive_folder_id, filename=None):
    """Upload a single file to a Drive folder. Returns True on success."""
    from googleapiclient.http import MediaFileUpload

    name = filename or os.path.basename(local_path)
    file_metadata = {"name": name, "parents": [drive_folder_id]}
    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return True


def upload_date_to_drive(
    date_str,
    drive_folder_id=None,
    credentials_path=None,
    data_scraped_base=None,
    existing_folder_ids=None,
    cancel_check=None,
):
    """
    Upload DATA_Scraped/<date_str>/ to Drive. Skips institutes already in existing_folder_ids.
    date_str: dd-mm-yy (e.g. 19-03-26).
    existing_folder_ids: { institute_name: drive_folder_id } — skip these, only process the rest.
    cancel_check: optional callable() -> bool; if True, stop between institutes/files.
    Returns dict: { "ok": bool, "uploaded": int, "failed": int, "error": str|None, "details": list, "folderIds": {} }.
    """
    drive_folder_id = drive_folder_id or os.getenv("DRIVE_FOLDER_ID") or DEFAULT_DRIVE_FOLDER_ID
    data_base = data_scraped_base or DATA_SCRAPED_BASE
    local_date_path = os.path.join(data_base, date_str)
    existing = dict(existing_folder_ids or {})

    result = {"ok": False, "uploaded": 0, "failed": 0, "error": None, "details": [], "folderIds": dict(existing)}

    if not os.path.isdir(local_date_path):
        result["error"] = f"No folder for date: {local_date_path}"
        return result

    try:
        service = get_drive_service(credentials_path)
    except Exception as e:
        result["error"] = str(e)
        return result

    drive_date_name = _date_str_to_drive_format(date_str)
    try:
        date_folder_id = get_or_create_folder(service, drive_folder_id, drive_date_name)
    except Exception as e:
        result["error"] = f"Drive date folder: {e}"
        return result

    for uni_name in sorted(os.listdir(local_date_path)):
        if cancel_check and cancel_check():
            result["error"] = result.get("error") or "Stopped by user"
            break
        uni_path = os.path.join(local_date_path, uni_name)
        if not os.path.isdir(uni_path):
            continue
        # Skip institutes already uploaded for this date (already have Drive folder ID)
        if uni_name in result["folderIds"]:
            paths.append_logs_runs_line(
                date_str,
                paths.safe_run_log_filename(uni_name, "upload"),
                "Skipped (already uploaded for this date on Drive).",
            )
            continue
        try:
            uni_folder_id = get_or_create_folder(service, date_folder_id, uni_name)
            result["folderIds"][uni_name] = uni_folder_id
        except Exception as e:
            paths.append_logs_runs_line(
                date_str,
                paths.safe_run_log_filename(uni_name, "upload"),
                f"Drive folder error: {e}",
            )
            result["details"].append({"university": uni_name, "success": False, "error": str(e)})
            result["failed"] += 1
            continue
        for fname in os.listdir(uni_path):
            if cancel_check and cancel_check():
                result["error"] = result.get("error") or "Stopped by user"
                break
            fpath = os.path.join(uni_path, fname)
            if not os.path.isfile(fpath):
                continue
            try:
                upload_file(service, fpath, uni_folder_id, fname)
                result["uploaded"] += 1
                result["details"].append({"university": uni_name, "file": fname, "success": True})
                paths.append_logs_runs_line(
                    date_str,
                    paths.safe_run_log_filename(uni_name, "upload"),
                    f"Uploaded: {fname}",
                )
            except Exception as e:
                result["failed"] += 1
                result["details"].append({"university": uni_name, "file": fname, "success": False, "error": str(e)})
                paths.append_logs_runs_line(
                    date_str,
                    paths.safe_run_log_filename(uni_name, "upload"),
                    f"Failed: {fname} — {e}",
                )
        if cancel_check and cancel_check():
            result["error"] = result.get("error") or "Stopped by user"
            break

    result["ok"] = result["failed"] == 0
    return result
