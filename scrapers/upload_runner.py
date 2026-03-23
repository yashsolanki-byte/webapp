"""
Run upload-to-Drive job from web app. Updates in-memory status for API polling.
Per-institute: skips institutes already uploaded for this date, only processes the rest.
Writes to logs/app/upload.log (separate from scrape logs).
"""

import json
import os
import sys
import threading
from datetime import datetime

_upload_cancel = threading.Event()

_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths

UPLOAD_LOG_FILE = paths.UPLOAD_LOG_FILE
UPLOAD_HISTORY_JSON = paths.UPLOAD_HISTORY_JSON

_upload_status = {
    "running": False,
    "date": None,
    "error": None,
    "uploaded": 0,
    "failed": 0,
    "details": [],
    "skipped": False,
}


def get_upload_status():
    return dict(_upload_status)


def request_upload_stop():
    """Stop upload job between institute/file uploads (cooperative)."""
    _upload_cancel.set()


def _load_upload_history():
    """Load per-date upload history (folderIds = institutes already uploaded for that date)."""
    try:
        if os.path.isfile(UPLOAD_HISTORY_JSON):
            with open(UPLOAD_HISTORY_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def _save_upload_history(history):
    """Persist upload history (per date: uploaded count, failed count, uploadedAt)."""
    try:
        os.makedirs(os.path.dirname(UPLOAD_HISTORY_JSON), exist_ok=True)
        with open(UPLOAD_HISTORY_JSON, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except OSError:
        pass


def _log_upload(msg, date_str=None):
    """Append to logs/app/upload.log and logs/runs/<date>/upload_job.log when date_str is set."""
    try:
        os.makedirs(paths.LOGS_APP_DIR, exist_ok=True)
        line = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + msg + "\n"
        with open(UPLOAD_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        if date_str:
            paths.append_logs_runs_line(date_str, "upload_job.log", msg)
    except OSError:
        pass


def run_upload_job(date_str):
    """Run upload for DATA_Scraped/<date_str>/. Skips institutes already in history for this date; uploads the rest."""
    paths.ensure_layout_migrated()
    global _upload_status
    _upload_cancel.clear()
    _upload_status["running"] = True
    _upload_status["date"] = date_str
    _upload_status["error"] = None
    _upload_status["uploaded"] = 0
    _upload_status["failed"] = 0
    _upload_status["details"] = []
    _upload_status["skipped"] = False

    def _run():
        global _upload_status
        try:
            history = _load_upload_history()
            existing_folder_ids = (history.get(date_str) or {}).get("folderIds") or {}
            _log_upload(
                f"Upload started for {date_str} ({len(existing_folder_ids)} already on Drive, processing rest).",
                date_str=date_str,
            )
            from scrapers.upload_to_drive import upload_date_to_drive
            out = upload_date_to_drive(
                date_str=date_str,
                existing_folder_ids=existing_folder_ids,
                cancel_check=_upload_cancel.is_set,
            )
            _upload_status["uploaded"] = out.get("uploaded", 0)
            _upload_status["failed"] = out.get("failed", 0)
            _upload_status["details"] = out.get("details", [])
            _upload_status["error"] = out.get("error")

            # Merge with existing: folderIds = already-done + newly uploaded; save for next run and dashboard
            history[date_str] = {
                "uploaded": out.get("uploaded", 0),
                "failed": out.get("failed", 0),
                "uploadedAt": datetime.now().isoformat(),
                "folderIds": out.get("folderIds") or {},
            }
            _save_upload_history(history)

            _log_upload(
                f"Upload finished for {date_str}: {_upload_status['uploaded']} files uploaded, {_upload_status['failed']} failed ({len(history[date_str]['folderIds'])} institutes on Drive).",
                date_str=date_str,
            )
        except Exception as e:
            _upload_status["error"] = str(e)
            _log_upload(f"Upload error for {date_str}: {e}", date_str=date_str)
        finally:
            _upload_status["running"] = False

    t = threading.Thread(target=_run, daemon=False)
    t.start()
