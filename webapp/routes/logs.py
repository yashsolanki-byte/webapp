"""Logs section: scraper log, institute logs, upload log, manual scrape, feedback-ready."""
import os
from typing import Optional, Tuple

from flask import Blueprint, jsonify, request

from webapp.config import (
    FEEDBACK_READY_LOG_FILE,
    LOGS_RUNS_DIR,
    MANUAL_SCRAPE_LOG_FILE,
    SCRAPER_LOG_FILE,
    UPLOAD_LOG_FILE,
)
from webapp.services.path_utils import safe_log_subpath

logs_bp = Blueprint("logs", __name__, url_prefix="")


def _read_log_file(path: str, tail: Optional[int]) -> Tuple[str, str, Optional[str]]:
    """Return (content, path, error_str_or_None)."""
    if not os.path.isfile(path):
        return "", path, None
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        if tail is not None and tail > 0:
            lines = lines[-tail:]
        return "".join(lines), path, None
    except OSError as e:
        return "", path, str(e)


@logs_bp.route("/api/scraper-logs/dates")
def api_scraper_log_dates():
    if not os.path.isdir(LOGS_RUNS_DIR):
        return jsonify({"dates": []})
    dates = []
    for name in sorted(os.listdir(LOGS_RUNS_DIR), reverse=True):
        path = os.path.join(LOGS_RUNS_DIR, name)
        if os.path.isdir(path) and ".." not in name and os.path.sep not in name:
            dates.append(name)
    return jsonify({"dates": dates})


def _log_file_display_label(kind: str, filename: str) -> str:
    """Short label for UI (strip known prefix)."""
    name = (filename or "").strip()
    if not name.endswith(".log"):
        return name
    stem = name[:-4]
    if stem in ("upload_job", "manual_job", "feedback_job"):
        return "Job summary"
    prefixes = ("upload_", "manual_", "feedback_")
    if kind in ("upload", "manual", "feedback"):
        for p in prefixes:
            if stem.startswith(p):
                return stem[len(p) :] or name
    return stem or name


@logs_bp.route("/api/scraper-logs/files")
def api_scraper_log_files():
    date_str = (request.args.get("date") or "").strip()
    kind = (request.args.get("kind") or "scrape").strip().lower()
    if kind not in ("scrape", "upload", "manual", "feedback"):
        kind = "scrape"
    if not date_str or ".." in date_str or os.path.sep in date_str:
        return jsonify({"files": [], "kind": kind})
    date_dir = os.path.join(LOGS_RUNS_DIR, date_str)
    if not os.path.isdir(date_dir):
        return jsonify({"files": [], "kind": kind})
    files = []
    for name in sorted(os.listdir(date_dir)):
        if not name.endswith(".log") or ".." in name:
            continue
        if kind == "scrape":
            if name.startswith(("upload_", "manual_", "feedback_")):
                continue
        elif kind == "upload":
            if not name.startswith("upload_"):
                continue
        elif kind == "manual":
            if not name.startswith("manual_"):
                continue
        elif kind == "feedback":
            if not name.startswith("feedback_"):
                continue
        files.append(
            {
                "name": name,
                "path": os.path.join(date_str, name),
                "label": _log_file_display_label(kind, name),
            }
        )
    return jsonify({"files": files, "kind": kind})


@logs_bp.route("/api/upload-logs")
def api_upload_logs():
    tail = request.args.get("tail", type=int)
    content, path, err = _read_log_file(UPLOAD_LOG_FILE, tail)
    body = {"log": content, "path": path}
    if err:
        body["error"] = err
    return jsonify(body)


@logs_bp.route("/api/manual-scrape-logs")
def api_manual_scrape_logs():
    tail = request.args.get("tail", type=int)
    content, path, err = _read_log_file(MANUAL_SCRAPE_LOG_FILE, tail)
    body = {"log": content, "path": path}
    if err:
        body["error"] = err
    return jsonify(body)


@logs_bp.route("/api/feedback-ready-logs")
def api_feedback_ready_logs():
    tail = request.args.get("tail", type=int)
    content, path, err = _read_log_file(FEEDBACK_READY_LOG_FILE, tail)
    body = {"log": content, "path": path}
    if err:
        body["error"] = err
    return jsonify(body)


@logs_bp.route("/api/scraper-logs")
def api_scraper_logs():
    date_str = (request.args.get("date") or "").strip()
    file_name = (request.args.get("file") or "").strip()
    if date_str and file_name:
        subpath = safe_log_subpath(date_str, file_name)
        if subpath:
            log_path = os.path.join(LOGS_RUNS_DIR, subpath)
        else:
            log_path = None
    else:
        log_path = SCRAPER_LOG_FILE
    if not log_path or not os.path.isfile(log_path):
        return jsonify({"log": "", "path": log_path or ""})
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        tail = request.args.get("tail", type=int)
        if tail is not None and tail > 0:
            lines = lines[-tail:]
        content = "".join(lines)
        return jsonify({"log": content, "path": log_path})
    except OSError as e:
        return jsonify({"log": "", "path": log_path, "error": str(e)})
