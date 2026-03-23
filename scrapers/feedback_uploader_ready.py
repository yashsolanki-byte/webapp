"""
Build "Feedback Uploader Ready" CSVs from already-uploaded Drive scrape CSVs.

Modes:
- Bulk by date: reads all institute folders under source/date and uploads transformed CSVs.
- Single file: transforms one Drive CSV by file id.

Output columns only:
panel_name, super_camp_id, lead_id, feedback_id, form_initiated, verified, is_primary

Mapping:
- panel_name = "NPF"
- super_camp_id = pcid (exact cell text)
- lead_id = name (exact cell text; legacy column "campaign" also supported if "name" is empty)
- feedback_id = FI (exact cell text)
- form_initiated, verified, is_primary = blank
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
from typing import Callable, Dict, List, Optional, Set

_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths

# Need read + write across existing files in Shared Drive folders.
# `drive.file` is often insufficient for reading files not created by this app.
SCOPES = [
    "https://www.googleapis.com/auth/drive",
]

# Drive folders:
# - SOURCE root is where scraped-data date folders exist
# - TARGET root is where transformed date folders are created
DEFAULT_SOURCE_ROOT_ID = os.getenv("DRIVE_FOLDER_ID", "1eRiXMFHZQK0iVOZt9pB_vZELkV8bVf9j")
DEFAULT_TARGET_ROOT_ID = os.getenv("FEEDBACK_READY_ROOT_ID", "")
DEFAULT_TARGET_FOLDER_NAME = os.getenv("FEEDBACK_READY_FOLDER_NAME", "Feedback Uploader Ready")
DEFAULT_SOURCE_DATA_FOLDER_NAME = os.getenv("DRIVE_SOURCE_DATA_FOLDER_NAME", "")
DEFAULT_LOCAL_OUTPUT_DIR = os.path.join(paths.ROOT, "Feedback_Uploader_Ready_Output")

FEEDBACK_READY_HISTORY_JSON = paths.FEEDBACK_READY_HISTORY_JSON


def _history_date_key(date_str: str) -> str:
    """Use same key style as web (typically dd-mm-yy)."""
    return (date_str or "").strip()


def _load_feedback_ready_history() -> Dict[str, List[str]]:
    try:
        if os.path.isfile(FEEDBACK_READY_HISTORY_JSON):
            with open(FEEDBACK_READY_HISTORY_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def _save_feedback_ready_history(history: Dict[str, List[str]]) -> None:
    try:
        os.makedirs(os.path.dirname(FEEDBACK_READY_HISTORY_JSON), exist_ok=True)
        with open(FEEDBACK_READY_HISTORY_JSON, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except OSError:
        pass


def _feedback_ready_done_for_date(date_str: str) -> Set[str]:
    key = _history_date_key(date_str)
    raw = _load_feedback_ready_history().get(key) or []
    if not isinstance(raw, list):
        return set()
    return {str(x).strip() for x in raw if str(x).strip()}


def _feedback_ready_mark_done(date_str: str, institute_name: str) -> None:
    key = _history_date_key(date_str)
    uni = (institute_name or "").strip()
    if not key or not uni:
        return
    h = _load_feedback_ready_history()
    lst = [x for x in (h.get(key) or []) if isinstance(x, str) and x.strip()]
    if uni not in lst:
        lst.append(uni)
    h[key] = lst
    _save_feedback_ready_history(h)


def _feedback_ready_history_enabled() -> bool:
    """Set FEEDBACK_READY_SKIP_HISTORY=1 to allow re-processing institutes for the same date."""
    v = os.getenv("FEEDBACK_READY_SKIP_HISTORY", "").strip().lower()
    return v not in ("1", "true", "yes")


def _date_to_drive_name(date_str: str) -> str:
    """Accept dd-mm-yy, dd-mm-yyyy, yyyy-mm-dd; return yyyy-mm-dd."""
    s = (date_str or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    if re.match(r"^\d{1,2}-\d{1,2}-\d{2,4}$", s):
        d, m, y = [int(x) for x in s.split("-")]
        if y < 100:
            y = 2000 + y
        return f"{y:04d}-{m:02d}-{d:02d}"
    return s


def _default_credentials_path() -> str:
    parent_creds = os.path.abspath(os.path.join(paths.ROOT, "..", "NPF paid application", "credentials.json"))
    if os.path.isfile(parent_creds):
        return parent_creds
    return paths.CREDENTIALS_JSON


def get_drive_service(credentials_path: Optional[str] = None):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    path = credentials_path or _default_credentials_path()
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Credentials not found: {path}")
    creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def _safe_name_q(name: str) -> str:
    return (name or "").replace("\\", "\\\\").replace("'", "\\'")


def find_folder_by_name(service, parent_id: str, folder_name: str) -> Optional[str]:
    q = (
        f"'{parent_id}' in parents and "
        "mimeType='application/vnd.google-apps.folder' and "
        f"name='{_safe_name_q(folder_name)}' and trashed=false"
    )
    r = service.files().list(
        q=q,
        spaces="drive",
        fields="files(id,name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    files = r.get("files", [])
    return files[0]["id"] if files else None


def get_or_create_folder(service, parent_id: str, folder_name: str) -> str:
    found = find_folder_by_name(service, parent_id, folder_name)
    if found:
        return found
    body = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    created = service.files().create(body=body, fields="id", supportsAllDrives=True).execute()
    return created["id"]


def list_child_folders(service, parent_id: str) -> List[Dict[str, str]]:
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    r = service.files().list(
        q=q,
        spaces="drive",
        fields="files(id,name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        pageSize=1000,
    ).execute()
    return r.get("files", [])


def list_csv_files(service, parent_id: str) -> List[Dict[str, str]]:
    q = (
        f"'{parent_id}' in parents and trashed=false and "
        "(mimeType='text/csv' or name contains '.csv')"
    )
    r = service.files().list(
        q=q,
        spaces="drive",
        fields="files(id,name,mimeType)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        pageSize=1000,
    ).execute()
    return r.get("files", [])


def download_drive_text(service, file_id: str) -> str:
    from googleapiclient.http import MediaIoBaseDownload

    req = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _status, done = dl.next_chunk()
    return buf.getvalue().decode("utf-8-sig", errors="replace")


def _normalize(name: str) -> str:
    return (name or "").strip().lower()


def transform_csv_text(csv_text: str) -> str:
    inp = io.StringIO(csv_text)
    out = io.StringIO()
    reader = csv.DictReader(inp)
    writer = csv.DictWriter(
        out,
        fieldnames=[
            "panel_name",
            "super_camp_id",
            "lead_id",
            "feedback_id",
            "form_initiated",
            "verified",
            "is_primary",
        ],
    )
    writer.writeheader()

    for row in reader:
        # Preserve source strings exactly (no strip, no substring) for output fields.
        nmap = {_normalize(k): ("" if v is None else v) for k, v in (row or {}).items()}
        # Scrape CSVs use header "name" for campaign/lead id; older files may use "campaign".
        _lead_id = nmap.get("name", "")
        if _lead_id == "":
            _lead_id = nmap.get("campaign", "")
        writer.writerow(
            {
                "panel_name": "NPF",
                "super_camp_id": nmap.get("pcid", ""),
                "lead_id": _lead_id,
                "feedback_id": nmap.get("fi", ""),
                "form_initiated": "",
                "verified": "",
                "is_primary": "",
            }
        )
    return out.getvalue()


def upload_csv_text(service, parent_folder_id: str, filename: str, csv_text: str) -> str:
    from googleapiclient.http import MediaIoBaseUpload

    meta = {"name": filename, "parents": [parent_folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(csv_text.encode("utf-8")), mimetype="text/csv", resumable=True)
    created = service.files().create(
        body=meta,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return created["id"]


def _resolve_source_root(service, source_root_id: str) -> str:
    if DEFAULT_SOURCE_DATA_FOLDER_NAME:
        child = find_folder_by_name(service, source_root_id, DEFAULT_SOURCE_DATA_FOLDER_NAME)
        if child:
            return child
    return source_root_id


def _resolve_target_root(service, source_root_id: str, target_root_id: Optional[str]) -> str:
    if target_root_id:
        return target_root_id
    return get_or_create_folder(service, source_root_id, DEFAULT_TARGET_FOLDER_NAME)


def run_bulk_for_date(
    service,
    source_root_id: str,
    target_root_id: str,
    date_str: str,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> Dict[str, object]:
    date_name = _date_to_drive_name(date_str)
    source_date_id = find_folder_by_name(service, source_root_id, date_name)
    if not source_date_id:
        return {"ok": False, "error": f"Source date folder not found: {date_name}", "processed": 0, "failed": 0, "skipped": 0}

    target_date_id = get_or_create_folder(service, target_root_id, date_name)
    processed = 0
    failed = 0
    skipped = 0
    details: List[Dict[str, str]] = []
    use_history = _feedback_ready_history_enabled()
    already_done = _feedback_ready_done_for_date(date_str) if use_history else set()

    for uni in list_child_folders(service, source_date_id):
        if cancel_check and cancel_check():
            return {
                "ok": False,
                "error": "Stopped by user",
                "processed": processed,
                "failed": failed,
                "skipped": skipped,
                "details": details,
            }
        uni_name = uni["name"]
        csv_list = list(list_csv_files(service, uni["id"]))
        if not csv_list:
            continue
        if uni_name in already_done:
            skipped += len(csv_list)
            details.append(
                {
                    "university": uni_name,
                    "file": f"({len(csv_list)} CSV file(s))",
                    "success": "skipped",
                    "note": "Already processed for this date",
                }
            )
            continue
        target_uni_id = get_or_create_folder(service, target_date_id, uni_name)
        inst_failed = 0
        for f in csv_list:
            if cancel_check and cancel_check():
                return {
                    "ok": False,
                    "error": "Stopped by user",
                    "processed": processed,
                    "failed": failed,
                    "skipped": skipped,
                    "details": details,
                }
            try:
                src_text = download_drive_text(service, f["id"])
                out_text = transform_csv_text(src_text)
                upload_csv_text(service, target_uni_id, f["name"], out_text)
                processed += 1
                details.append({"university": uni_name, "file": f["name"], "success": "true"})
            except Exception as e:
                inst_failed += 1
                failed += 1
                details.append({"university": uni_name, "file": f.get("name", ""), "success": "false", "error": str(e)})
        if use_history and inst_failed == 0:
            _feedback_ready_mark_done(date_str, uni_name)
            already_done.add(uni_name)

    return {"ok": failed == 0, "processed": processed, "failed": failed, "skipped": skipped, "details": details}


def list_files_for_date(service, source_root_id: str, date_str: str) -> Dict[str, object]:
    """Return nested file list for UI selection: institutes + CSV files for a date folder."""
    date_name = _date_to_drive_name(date_str)
    source_date_id = find_folder_by_name(service, source_root_id, date_name)
    if not source_date_id:
        return {"ok": False, "error": f"Source date folder not found: {date_name}", "institutes": []}

    institutes: List[Dict[str, object]] = []
    for uni in list_child_folders(service, source_date_id):
        uni_name = uni["name"]
        files = []
        for f in list_csv_files(service, uni["id"]):
            files.append({"id": f["id"], "name": f["name"]})
        institutes.append({"name": uni_name, "files": files})
    return {"ok": True, "institutes": institutes}


def run_selected_for_date(
    service,
    source_root_id: str,
    target_root_id: str,
    date_str: str,
    selected_file_ids: Set[str],
    cancel_check: Optional[Callable[[], bool]] = None,
) -> Dict[str, object]:
    """Process only selected Drive file ids under a date folder."""
    date_name = _date_to_drive_name(date_str)
    source_date_id = find_folder_by_name(service, source_root_id, date_name)
    if not source_date_id:
        return {"ok": False, "error": f"Source date folder not found: {date_name}", "processed": 0, "failed": 0, "skipped": 0}
    if not selected_file_ids:
        return {"ok": True, "processed": 0, "failed": 0, "skipped": 0, "details": []}

    target_date_id = get_or_create_folder(service, target_root_id, date_name)
    processed = 0
    failed = 0
    skipped = 0
    details: List[Dict[str, str]] = []
    use_history = _feedback_ready_history_enabled()
    already_done = _feedback_ready_done_for_date(date_str) if use_history else set()

    for uni in list_child_folders(service, source_date_id):
        if cancel_check and cancel_check():
            return {
                "ok": False,
                "error": "Stopped by user",
                "processed": processed,
                "failed": failed,
                "skipped": skipped,
                "details": details,
            }
        uni_name = uni["name"]
        to_process = [f for f in list_csv_files(service, uni["id"]) if f["id"] in selected_file_ids]
        if not to_process:
            continue
        if uni_name in already_done:
            skipped += len(to_process)
            details.append(
                {
                    "university": uni_name,
                    "file": f"({len(to_process)} selected file(s))",
                    "success": "skipped",
                    "note": "Already processed for this date",
                }
            )
            continue
        target_uni_id: Optional[str] = None
        inst_failed = 0
        for f in to_process:
            if cancel_check and cancel_check():
                return {
                    "ok": False,
                    "error": "Stopped by user",
                    "processed": processed,
                    "failed": failed,
                    "skipped": skipped,
                    "details": details,
                }
            try:
                if target_uni_id is None:
                    target_uni_id = get_or_create_folder(service, target_date_id, uni_name)
                src_text = download_drive_text(service, f["id"])
                out_text = transform_csv_text(src_text)
                upload_csv_text(service, target_uni_id, f["name"], out_text)
                processed += 1
                details.append({"university": uni_name, "file": f["name"], "success": "true"})
            except Exception as e:
                inst_failed += 1
                failed += 1
                details.append({"university": uni_name, "file": f.get("name", ""), "success": "false", "error": str(e)})
        if use_history and inst_failed == 0:
            _feedback_ready_mark_done(date_str, uni_name)
            already_done.add(uni_name)
    return {"ok": failed == 0, "processed": processed, "failed": failed, "skipped": skipped, "details": details}


def run_single_file(service, target_root_id: str, file_id: str, out_name: Optional[str] = None) -> Dict[str, object]:
    meta = service.files().get(fileId=file_id, fields="id,name", supportsAllDrives=True).execute()
    src_name = meta.get("name", "output.csv")
    filename = out_name or src_name
    src_text = download_drive_text(service, file_id)
    out_text = transform_csv_text(src_text)
    uploaded_id = upload_csv_text(service, target_root_id, filename, out_text)
    return {"ok": True, "file": filename, "uploadedId": uploaded_id}


def _safe_part(name: str) -> str:
    s = (name or "").strip()
    for c in ['\\', '/', ':', '*', '?', '"', '<', '>', '|']:
        s = s.replace(c, "_")
    return s or "unknown"


def _write_local_csv(local_dir: str, filename: str, csv_text: str) -> str:
    os.makedirs(local_dir, exist_ok=True)
    out_path = os.path.join(local_dir, filename)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        f.write(csv_text)
    return out_path


def run_bulk_for_date_local(
    service,
    source_root_id: str,
    date_str: str,
    local_output_base: str = DEFAULT_LOCAL_OUTPUT_DIR,
) -> Dict[str, object]:
    date_name = _date_to_drive_name(date_str)
    source_date_id = find_folder_by_name(service, source_root_id, date_name)
    if not source_date_id:
        return {"ok": False, "error": f"Source date folder not found: {date_name}", "processed": 0, "failed": 0}

    processed = 0
    failed = 0
    details: List[Dict[str, str]] = []
    out_root = os.path.join(local_output_base, date_name)

    for uni in list_child_folders(service, source_date_id):
        uni_name = uni["name"]
        uni_out = os.path.join(out_root, _safe_part(uni_name))
        for f in list_csv_files(service, uni["id"]):
            try:
                src_text = download_drive_text(service, f["id"])
                out_text = transform_csv_text(src_text)
                local_path = _write_local_csv(uni_out, f["name"], out_text)
                processed += 1
                details.append({"university": uni_name, "file": f["name"], "success": "true", "output": local_path})
            except Exception as e:
                failed += 1
                details.append({"university": uni_name, "file": f.get("name", ""), "success": "false", "error": str(e)})
    return {"ok": failed == 0, "processed": processed, "failed": failed, "details": details, "outputDir": out_root}


def run_selected_for_date_local(
    service,
    source_root_id: str,
    date_str: str,
    selected_file_ids: Set[str],
    local_output_base: str = DEFAULT_LOCAL_OUTPUT_DIR,
) -> Dict[str, object]:
    date_name = _date_to_drive_name(date_str)
    source_date_id = find_folder_by_name(service, source_root_id, date_name)
    if not source_date_id:
        return {"ok": False, "error": f"Source date folder not found: {date_name}", "processed": 0, "failed": 0}
    if not selected_file_ids:
        return {"ok": True, "processed": 0, "failed": 0, "details": [], "outputDir": os.path.join(local_output_base, date_name)}

    processed = 0
    failed = 0
    details: List[Dict[str, str]] = []
    out_root = os.path.join(local_output_base, date_name)

    for uni in list_child_folders(service, source_date_id):
        uni_name = uni["name"]
        uni_out = os.path.join(out_root, _safe_part(uni_name))
        for f in list_csv_files(service, uni["id"]):
            if f["id"] not in selected_file_ids:
                continue
            try:
                src_text = download_drive_text(service, f["id"])
                out_text = transform_csv_text(src_text)
                local_path = _write_local_csv(uni_out, f["name"], out_text)
                processed += 1
                details.append({"university": uni_name, "file": f["name"], "success": "true", "output": local_path})
            except Exception as e:
                failed += 1
                details.append({"university": uni_name, "file": f.get("name", ""), "success": "false", "error": str(e)})
    return {"ok": failed == 0, "processed": processed, "failed": failed, "details": details, "outputDir": out_root}


def parse_args():
    p = argparse.ArgumentParser(description="Create Feedback Uploader Ready CSVs from Drive CSVs")
    p.add_argument("--date", help="Date folder to process (dd-mm-yy or yyyy-mm-dd)")
    p.add_argument("--file-id", help="Single Drive CSV file id to transform")
    p.add_argument("--out-name", help="Output filename for --file-id mode")
    p.add_argument("--source-root-id", default=DEFAULT_SOURCE_ROOT_ID, help="Source Drive root folder id")
    p.add_argument("--target-root-id", default=DEFAULT_TARGET_ROOT_ID, help="Target Drive root folder id")
    p.add_argument("--credentials", default="", help="Path to service-account json")
    return p.parse_args()


def main():
    args = parse_args()
    if not args.date and not args.file_id:
        raise SystemExit("Pass either --date for bulk or --file-id for single-file mode.")

    service = get_drive_service(args.credentials or None)
    source_root_id = _resolve_source_root(service, args.source_root_id)
    target_root_id = _resolve_target_root(service, source_root_id, args.target_root_id or None)

    if args.file_id:
        res = run_single_file(service, target_root_id, args.file_id, args.out_name)
    else:
        res = run_bulk_for_date(service, source_root_id, target_root_id, args.date)
    print(res)


if __name__ == "__main__":
    main()

