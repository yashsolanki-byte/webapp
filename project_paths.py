"""
Single source of truth for project directories (npf-scraper-webapp root).
Used by webapp/, scrapers/, and scripts run from the project root.

Layout (aligned with web app sections):
  data/history/   — scrape / upload / feedback-ready history (JSON)
  data/reference/ — factual / seed data (institutes, URLs, universities TSV)
  data/runtime/   — working state (scrape queue, filter cache, credentials on disk)
  logs/app/       — tool logs (scraper, upload, batch, manual, feedback-ready)
  logs/runs/      — per-date logs: scrape (<Institute>.log), upload_/manual_/feedback_ prefixes + *_job.log
  logs/script/    — script_scraper session logs

Legacy flat paths under data/ and logs/ are moved once by ensure_layout_migrated().
"""
import os
import re
import shutil
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))

# --- data/ sections ---
DATA_DIR = os.path.join(ROOT, "data")
DATA_HISTORY_DIR = os.path.join(DATA_DIR, "history")
DATA_REFERENCE_DIR = os.path.join(DATA_DIR, "reference")
DATA_RUNTIME_DIR = os.path.join(DATA_DIR, "runtime")

# --- logs/ sections ---
LOGS_DIR = os.path.join(ROOT, "logs")
LOGS_APP_DIR = os.path.join(LOGS_DIR, "app")
LOGS_RUNS_DIR = os.path.join(LOGS_DIR, "runs")
LOGS_SCRIPT_DIR = os.path.join(LOGS_DIR, "script")

# Scrape output (unchanged)
DATA_SCRAPED_DIR = os.path.join(ROOT, "DATA_Scraped")
TEMPLATES_DIR = os.path.join(ROOT, "templates")

# History JSON (data/history/)
SCRAPE_HISTORY_JSON = os.path.join(DATA_HISTORY_DIR, "scrape_history.json")
UPLOAD_HISTORY_JSON = os.path.join(DATA_HISTORY_DIR, "upload_history.json")
FEEDBACK_READY_HISTORY_JSON = os.path.join(DATA_HISTORY_DIR, "feedback_ready_history.json")

# Reference / factual (data/reference/)
INSTITUTES_JSON = os.path.join(DATA_REFERENCE_DIR, "Institutes.json")
MANUAL_INSTITUTES_JSON = os.path.join(DATA_REFERENCE_DIR, "manual_institutes.json")
URLS_JSON = os.path.join(DATA_REFERENCE_DIR, "urls.json")
UNIVERSITIES_TSV = os.path.join(DATA_REFERENCE_DIR, "universities")

# Runtime / app state (data/runtime/)
SCRAPE_LIST_JSON = os.path.join(DATA_RUNTIME_DIR, "scrape_list.json")
FILTER_CACHE_JSON = os.path.join(DATA_RUNTIME_DIR, "filter_cache.json")
MANUAL_CREDENTIALS_JSON = os.path.join(DATA_RUNTIME_DIR, "manual_credentials.json")
CREDENTIALS_JSON = os.path.join(DATA_RUNTIME_DIR, "credentials.json")
DATA_EXPORTS_FALLBACK = os.path.join(DATA_RUNTIME_DIR, "exports")
# Per-job manual scrape (RQ workers): browser profile + output CSV under data/runtime/manual_jobs/<job_id>/
MANUAL_JOBS_RUNTIME_DIR = os.path.join(DATA_RUNTIME_DIR, "manual_jobs")
# Per-job log files (lightweight; job dir also has logs.txt)
LOGS_MANUAL_JOBS_DIR = os.path.join(LOGS_DIR, "manual_jobs")

# App / tool logs (logs/app/)
UPLOAD_LOG_FILE = os.path.join(LOGS_APP_DIR, "upload.log")
FEEDBACK_READY_LOG_FILE = os.path.join(LOGS_APP_DIR, "feedback_ready.log")
MANUAL_SCRAPE_LOG_FILE = os.path.join(LOGS_APP_DIR, "manual_scrape.log")
SCRAPER_LOG_FILE = os.path.join(LOGS_APP_DIR, "scraper.log")
BATCH_SCRAPER_LOG_FILE = os.path.join(LOGS_APP_DIR, "batch_scraper.log")

_DATE_FOLDER_RE = re.compile(r"^\d{2}-\d{2}-\d{2}$")

_migrated_flag = False


def ensure_layout_migrated() -> None:
    """
    One-time idempotent move from legacy flat data/* and logs/* into sectioned folders.
    Safe to call on every app start.
    """
    global _migrated_flag
    if _migrated_flag:
        return
    _migrated_flag = True

    for d in (
        DATA_HISTORY_DIR,
        DATA_REFERENCE_DIR,
        DATA_RUNTIME_DIR,
        MANUAL_JOBS_RUNTIME_DIR,
        LOGS_APP_DIR,
        LOGS_RUNS_DIR,
        LOGS_SCRIPT_DIR,
        LOGS_MANUAL_JOBS_DIR,
    ):
        os.makedirs(d, exist_ok=True)

    def _move_if(src: str, dest: str) -> None:
        if not os.path.exists(src) or os.path.exists(dest):
            return
        try:
            shutil.move(src, dest)
        except OSError:
            pass

    # Legacy data/ root → sections
    legacy_data = DATA_DIR
    _move_if(os.path.join(legacy_data, "scrape_history.json"), SCRAPE_HISTORY_JSON)
    _move_if(os.path.join(legacy_data, "upload_history.json"), UPLOAD_HISTORY_JSON)
    _move_if(os.path.join(legacy_data, "feedback_ready_history.json"), FEEDBACK_READY_HISTORY_JSON)

    _move_if(os.path.join(legacy_data, "Institutes.json"), INSTITUTES_JSON)
    _move_if(os.path.join(legacy_data, "manual_institutes.json"), MANUAL_INSTITUTES_JSON)
    _move_if(os.path.join(legacy_data, "urls.json"), URLS_JSON)
    _move_if(os.path.join(legacy_data, "universities"), UNIVERSITIES_TSV)

    _move_if(os.path.join(legacy_data, "scrape_list.json"), SCRAPE_LIST_JSON)
    _move_if(os.path.join(legacy_data, "filter_cache.json"), FILTER_CACHE_JSON)
    _move_if(os.path.join(legacy_data, "manual_credentials.json"), MANUAL_CREDENTIALS_JSON)
    _move_if(os.path.join(legacy_data, "credentials.json"), CREDENTIALS_JSON)

    legacy_exports = os.path.join(legacy_data, "exports")
    if os.path.isdir(legacy_exports) and not os.path.exists(DATA_EXPORTS_FALLBACK):
        try:
            shutil.move(legacy_exports, DATA_EXPORTS_FALLBACK)
        except OSError:
            pass

    # Legacy logs/ root → app / runs / script
    app_log_names = (
        "scraper.log",
        "batch_scraper.log",
        "upload.log",
        "feedback_ready.log",
        "manual_scrape.log",
    )
    for name in app_log_names:
        _move_if(os.path.join(LOGS_DIR, name), os.path.join(LOGS_APP_DIR, name))

    if os.path.isdir(LOGS_DIR):
        for name in os.listdir(LOGS_DIR):
            if name in ("app", "runs", "script"):
                continue
            src = os.path.join(LOGS_DIR, name)
            if os.path.isfile(src) and name.startswith("script_scraper_") and name.endswith(".log"):
                _move_if(src, os.path.join(LOGS_SCRIPT_DIR, name))
            elif os.path.isdir(src) and _DATE_FOLDER_RE.match(name):
                dest = os.path.join(LOGS_RUNS_DIR, name)
                if not os.path.exists(dest):
                    try:
                        shutil.move(src, dest)
                    except OSError:
                        pass


def safe_run_log_filename(institute_or_label: str, prefix: str = "") -> str:
    """
    Basename for logs/runs/<date>/<file>.log.
    prefix '' -> scrape-style names (e.g. ABBS Institute.log).
    prefix 'upload'|'manual'|'feedback' -> upload_ABBS Institute.log, etc.
    """
    s = (institute_or_label or "").strip()
    for c in ("\\", "/", ":", "*", "?", '"', "<", ">", "|"):
        s = s.replace(c, "_")
    if not s:
        s = "unknown"
    s = s[:80] if len(s) > 80 else s
    p = (prefix or "").strip()
    if p:
        name = f"{p}_{s}"
    else:
        name = s
    return name + ".log" if not name.endswith(".log") else name


def append_logs_runs_line(date_str: str, log_filename: str, message: str) -> None:
    """Append one timestamped line to logs/runs/<date>/<log_filename> (basename only)."""
    ds = (date_str or "").strip()
    if not _DATE_FOLDER_RE.match(ds):
        return
    fn = (log_filename or "").strip()
    if not fn.endswith(".log") or ".." in fn or os.path.sep in fn or "/" in fn or "\\" in fn:
        return
    dir_path = os.path.join(LOGS_RUNS_DIR, ds)
    try:
        os.makedirs(dir_path, exist_ok=True)
        path = os.path.join(dir_path, fn)
        with open(path, "a", encoding="utf-8") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + message + "\n")
    except OSError:
        pass
