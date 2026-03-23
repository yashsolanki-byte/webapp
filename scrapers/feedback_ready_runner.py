"""
Run Feedback Uploader Ready job in background for web app polling.
"""

import os
import sys
import threading
from datetime import datetime

_feedback_ready_cancel = threading.Event()

_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths

FEEDBACK_READY_LOG_FILE = paths.FEEDBACK_READY_LOG_FILE

_feedback_ready_status = {
    "running": False,
    "date": None,
    "error": None,
    "processed": 0,
    "failed": 0,
    "skipped": 0,
    "details": [],
}


def get_feedback_ready_status():
    return dict(_feedback_ready_status)


def request_feedback_ready_stop():
    """Stop feedback-ready job between institutes/files (cooperative)."""
    _feedback_ready_cancel.set()


def _log(msg, date_str=None):
    """Append to logs/app/feedback_ready.log and logs/runs/<date>/feedback_job.log."""
    try:
        os.makedirs(paths.LOGS_APP_DIR, exist_ok=True)
        line = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + msg + "\n"
        with open(FEEDBACK_READY_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        ds = (date_str or "").strip()
        if ds:
            paths.append_logs_runs_line(ds, "feedback_job.log", msg)
    except OSError:
        pass


def _log_feedback_institute(date_str: str, university: str, msg: str):
    uni = (university or "-").strip() or "-"
    paths.append_logs_runs_line(
        date_str,
        paths.safe_run_log_filename(uni, "feedback"),
        msg,
    )


def run_feedback_ready_job(date_str, selected_file_ids=None):
    paths.ensure_layout_migrated()
    global _feedback_ready_status
    n_sel = len(selected_file_ids or [])
    mode = f"selected files ({n_sel})" if n_sel else "bulk (all files)"
    _feedback_ready_cancel.clear()
    _log(f"Feedback-ready started for {date_str} — {mode}", date_str=date_str)
    _feedback_ready_status.update(
        {
            "running": True,
            "date": date_str,
            "error": None,
            "processed": 0,
            "failed": 0,
            "skipped": 0,
            "details": [],
            "selected": n_sel,
        }
    )

    def _run():
        global _feedback_ready_status
        try:
            from scrapers.feedback_uploader_ready import (
                DEFAULT_SOURCE_ROOT_ID,
                DEFAULT_TARGET_ROOT_ID,
                run_bulk_for_date,
                run_selected_for_date,
                get_drive_service,
                _resolve_source_root,
                _resolve_target_root,
            )

            service = get_drive_service()
            source_root_id = _resolve_source_root(service, DEFAULT_SOURCE_ROOT_ID)
            target_root_id = _resolve_target_root(service, source_root_id, DEFAULT_TARGET_ROOT_ID or None)
            sel = set(selected_file_ids or [])
            if sel:
                out = run_selected_for_date(
                    service,
                    source_root_id,
                    target_root_id,
                    date_str,
                    sel,
                    cancel_check=_feedback_ready_cancel.is_set,
                )
            else:
                out = run_bulk_for_date(
                    service,
                    source_root_id,
                    target_root_id,
                    date_str,
                    cancel_check=_feedback_ready_cancel.is_set,
                )
            _feedback_ready_status["processed"] = out.get("processed", 0)
            _feedback_ready_status["failed"] = out.get("failed", 0)
            _feedback_ready_status["skipped"] = out.get("skipped", 0)
            _feedback_ready_status["details"] = out.get("details", [])
            _feedback_ready_status["error"] = out.get("error")
            _feedback_ready_status["outputDir"] = ""
            _log(
                f"Feedback-ready finished for {date_str}: "
                f"{_feedback_ready_status['processed']} processed, "
                f"{_feedback_ready_status['failed']} failed, "
                f"{_feedback_ready_status['skipped']} skipped",
                date_str=date_str,
            )
            for d in (_feedback_ready_status.get("details") or []):
                uni = str(d.get("university", "-"))
                if d.get("success") == "false":
                    line = (
                        "FAIL: "
                        + uni
                        + " / "
                        + str(d.get("file", "-"))
                        + " -> "
                        + str(d.get("error", "unknown error"))
                    )
                    _log(line, date_str=date_str)
                    _log_feedback_institute(date_str, uni, line)
                elif d.get("success") == "skipped":
                    line = (
                        "SKIP: "
                        + uni
                        + " / "
                        + str(d.get("file", "-"))
                        + " — "
                        + str(d.get("note", "skipped"))
                    )
                    _log(line, date_str=date_str)
                    _log_feedback_institute(date_str, uni, line)
                else:
                    line = (
                        "OK: "
                        + uni
                        + " / "
                        + str(d.get("file", "-"))
                        + ((" -> " + str(d.get("output", ""))) if d.get("output") else "")
                    )
                    _log_feedback_institute(date_str, uni, line)
        except Exception as e:
            _feedback_ready_status["error"] = str(e)
            _log(f"Feedback-ready error for {date_str}: {e}", date_str=date_str)
        finally:
            _feedback_ready_status["running"] = False

    threading.Thread(target=_run, daemon=False).start()

