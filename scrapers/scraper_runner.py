"""
Run batch scrape from web app: uses local batch_scraper (copy of NPF paid application/Batch_Scraper logic).
Saves to DATA_Scraped/dd-mm-yy/university_name/ with headless=False.
Logs: logs/app/scraper.log (overall) and logs/runs/<dd-mm-yy>/<institute_name>.log per institute (see Settings).
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, date

# scrapers/ -> add project root for `import project_paths`
_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths
from project_paths import safe_run_log_filename

DATA_SCRAPED_BASE = paths.DATA_SCRAPED_DIR
SCRAPE_LIST_JSON = paths.SCRAPE_LIST_JSON
SCRAPE_HISTORY_JSON = paths.SCRAPE_HISTORY_JSON
LOGS_APP_DIR = paths.LOGS_APP_DIR
LOGS_RUNS_DIR = paths.LOGS_RUNS_DIR
SCRAPER_LOG_FILE = paths.SCRAPER_LOG_FILE

# In-memory status for the web app (single job at a time)
_scrape_status = {
    "running": False,
    "current": None,
    "total": 0,
    "done": 0,
    "results": [],
    "error": None,
    # dd-mm-yy written to scrape_history / DATA_Scraped for the last finished job (dashboard refresh)
    "lastRunHistoryDate": None,
    "stoppedByUser": False,
}

# Set True when user calls request_scrape_stop(); cleared when a new job starts.
_scrape_user_stop = False


def get_scrape_status():
    """Return current scrape job status for API."""
    return dict(_scrape_status)


def request_scrape_stop():
    """
    Ask the batch / dashboard retry scrape to stop after the current university finishes
    (Playwright cannot safely abort mid-college without deeper changes).
    """
    global _scrape_user_stop, _scrape_status
    _scrape_user_stop = True
    _scrape_status["running"] = False


def _scraper_log_path():
    """Path to scraper log file for Settings."""
    return SCRAPER_LOG_FILE


def _load_scrape_list():
    try:
        if os.path.exists(SCRAPE_LIST_JSON):
            with open(SCRAPE_LIST_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        pass
    return []


def _load_scrape_history():
    """Load per-university last scrape date and result. Used to skip same-date re-scrape when data was fetched."""
    try:
        if os.path.exists(SCRAPE_HISTORY_JSON):
            with open(SCRAPE_HISTORY_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def _save_scrape_history(history):
    """Persist scrape history (lastScrapeDate, success, recordCount, lastError per university)."""
    try:
        os.makedirs(os.path.dirname(SCRAPE_HISTORY_JSON), exist_ok=True)
        with open(SCRAPE_HISTORY_JSON, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except OSError:
        pass


def run_scrape_job(headless=False):
    """
    Run scrape for all universities in scrape_list.json.
    Saves to DATA_Scraped/dd-mm-yy/<university_name>/.
    headless: False = browser visible (default for web app).
    Updates _scrape_status; runs in background (call from thread).
    """
    paths.ensure_layout_migrated()
    global _scrape_status, _scrape_user_stop
    _scrape_user_stop = False
    rows = _load_scrape_list()
    if not rows:
        _scrape_status = {
            "running": False,
            "error": "Scrape list is empty",
            "results": [],
            "lastRunHistoryDate": _scrape_status.get("lastRunHistoryDate"),
        }
        return

    _scrape_status["running"] = True
    _scrape_status["total"] = len(rows)
    _scrape_status["done"] = 0
    _scrape_status["current"] = None
    _scrape_status["results"] = []
    _scrape_status["error"] = None
    _scrape_status["stoppedByUser"] = False
    history_folder_date = None

    os.makedirs(LOGS_APP_DIR, exist_ok=True)
    file_handler = None
    try:
        fh = logging.FileHandler(SCRAPER_LOG_FILE, mode="a", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        batch_logger = logging.getLogger("batch_scraper")
        batch_logger.addHandler(fh)
        file_handler = fh
        with open(SCRAPER_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "=" * 60 + "\n")
            f.write(f"Scrape job started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {len(rows)} universities\n")
            f.write("=" * 60 + "\n")
    except Exception:
        pass

    try:
        from scrapers.batch_scraper import BatchScraper, PLAYWRIGHT_AVAILABLE

        if not PLAYWRIGHT_AVAILABLE:
            msg = "Playwright not available. Run: pip install playwright && playwright install chromium"
            _scrape_status["error"] = msg
            _scrape_status["running"] = False
            try:
                with open(SCRAPER_LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(f"\n{msg}\n")
            except Exception:
                pass
            return

        scraper = BatchScraper()
        output_base = DATA_SCRAPED_BASE
        os.makedirs(output_base, exist_ok=True)

        batch_logger = logging.getLogger("batch_scraper")
        today_str = date.today().strftime("%d-%m-%y")
        history_folder_date = today_str

        try:
            from institute_helpers import enrich_row_from_institutes, load_institutes_lookup

            _inst_lookup = load_institutes_lookup()
        except Exception:
            _inst_lookup = None

        async def run_all():
            history = _load_scrape_history()
            for i, row in enumerate(rows):
                if not _scrape_status["running"]:
                    break
                if _inst_lookup is not None:
                    row = enrich_row_from_institutes(row, _inst_lookup)
                uni = (row.get("university") or row.get("college") or "").strip()
                _scrape_status["current"] = uni

                # Skip if already scraped today and data was fetched (success + recordCount > 0)
                entry = history.get(uni) or {}
                if (
                    entry.get("lastScrapeDate") == today_str
                    and entry.get("success") is True
                    and (entry.get("recordCount") or 0) > 0
                ):
                    batch_logger.info("Skipped %s (already scraped today with data)", uni)
                    _scrape_status["done"] = i + 1
                    _scrape_status["results"].append({
                        "university": uni,
                        "success": True,
                        "count": entry.get("recordCount", 0),
                        "error": None,
                        "filename": entry.get("filename"),
                        "skipped": True,
                        "skipReason": "Already scraped today with data",
                    })
                    continue

                # Institute-wise log under logs/<dd-mm-yy>/<institute>.log
                institute_logs_dir = os.path.join(LOGS_RUNS_DIR, today_str)
                os.makedirs(institute_logs_dir, exist_ok=True)
                institute_log_file = os.path.join(institute_logs_dir, safe_run_log_filename(uni, ""))
                inst_fh = None
                try:
                    inst_fh = logging.FileHandler(institute_log_file, mode="a", encoding="utf-8")
                    inst_fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
                    batch_logger.addHandler(inst_fh)
                except Exception:
                    pass
                try:
                    success, count, err, fname = await scraper.scrape_college(
                        row, headless=headless, output_base=output_base
                    )
                finally:
                    if inst_fh:
                        try:
                            batch_logger.removeHandler(inst_fh)
                            inst_fh.close()
                        except Exception:
                            pass

                # Update last scrape date and result for this university
                err_clean = (err or "").strip() if not success else ""
                history[uni] = {
                    "lastScrapeDate": today_str,
                    "success": success,
                    "recordCount": count if success else 0,
                    "filename": fname,
                    "lastError": None if success else (err_clean or "Error occurred"),
                }
                _save_scrape_history(history)

                _scrape_status["done"] = i + 1
                _scrape_status["results"].append({
                    "university": uni,
                    "success": success,
                    "count": count,
                    "error": err,
                    "filename": fname,
                    "skipped": False,
                })

        asyncio.run(run_all())
    except Exception as e:
        import traceback
        _scrape_status["error"] = str(e)
        try:
            with open(SCRAPER_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"\nScrape job error: {e}\n")
                f.write(traceback.format_exc())
        except Exception:
            pass
    finally:
        if file_handler:
            try:
                batch_logger = logging.getLogger("batch_scraper")
                batch_logger.removeHandler(file_handler)
                file_handler.close()
            except Exception:
                pass
        try:
            with open(SCRAPER_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"Scrape job finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {_scrape_status['done']}/{_scrape_status['total']} done\n")
        except Exception:
            pass
        if history_folder_date:
            _scrape_status["lastRunHistoryDate"] = history_folder_date
        user_stopped = _scrape_user_stop
        if user_stopped and _scrape_status.get("done", 0) < _scrape_status.get("total", 0):
            _scrape_status["stoppedByUser"] = True
            if not _scrape_status.get("error"):
                _scrape_status["error"] = "Stopped by user"
        _scrape_user_stop = False
        _scrape_status["running"] = False
        _scrape_status["current"] = None


def prepare_scrape_retry_row(university_name: str):
    """
    Build one scrape row for dashboard "retry paid application" (same batch scraper as Jobs).
    Uses scrape_list.json match, then fills from Institutes.json + .env passwords.
    Returns (row_dict, None) or (None, error_message).
    """
    try:
        from institute_helpers import enrich_row_from_institutes, load_institutes_lookup
        from credential_env import ensure_row_password
    except ImportError as e:
        return None, f"Helpers not available: {e}"

    uni_key = (university_name or "").strip()
    if not uni_key:
        return None, "Missing university name"

    rows = _load_scrape_list()
    row = None
    tl = uni_key.lower()
    for r in rows:
        if not isinstance(r, dict):
            continue
        n = (r.get("university") or r.get("college") or "").strip()
        if n and (n == uni_key or n.lower() == tl):
            row = dict(r)
            break
    if row is None:
        row = {"university": uni_key}

    lookup = load_institutes_lookup()
    row = enrich_row_from_institutes(row, lookup)
    ensure_row_password(row)

    if not (row.get("source") or "").strip():
        row["source"] = "Collegedunia"

    missing = []
    if not (row.get("url") or "").strip():
        missing.append("url")
    if not (row.get("email") or "").strip():
        missing.append("email")
    if not (row.get("pass") or "").strip():
        missing.append("password (check .env / Institutes)")
    if missing:
        return None, "Cannot start scrape: missing " + ", ".join(missing) + ". Add the institute to the Jobs scrape list or Institutes.json."

    return row, None


def run_single_scrape_worker(row: dict, headless: bool = False):
    """
    Run paid-application batch scrape for one institute. Same output as Jobs:
    DATA_Scraped/dd-mm-yy/<university>/, scrape_history.json updated for today.
    Uses _scrape_status (total=1) so Jobs poll UI works.
    """
    paths.ensure_layout_migrated()
    global _scrape_status, _scrape_user_stop
    if _scrape_status.get("running"):
        return

    _scrape_user_stop = False
    uni = (row.get("university") or row.get("college") or "").strip() or "?"
    _scrape_status["running"] = True
    _scrape_status["total"] = 1
    _scrape_status["done"] = 0
    _scrape_status["current"] = uni
    _scrape_status["results"] = []
    _scrape_status["error"] = None
    _scrape_status["stoppedByUser"] = False
    history_folder_date = None

    os.makedirs(LOGS_APP_DIR, exist_ok=True)
    file_handler = None
    try:
        fh = logging.FileHandler(SCRAPER_LOG_FILE, mode="a", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        batch_logger = logging.getLogger("batch_scraper")
        batch_logger.addHandler(fh)
        file_handler = fh
        with open(SCRAPER_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "=" * 60 + "\n")
            f.write(
                f"Single paid-application scrape retry at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {uni}\n"
            )
            f.write("=" * 60 + "\n")
    except Exception:
        pass

    try:
        from scrapers.batch_scraper import BatchScraper, PLAYWRIGHT_AVAILABLE

        if not PLAYWRIGHT_AVAILABLE:
            msg = "Playwright not available. Run: pip install playwright && playwright install chromium"
            _scrape_status["error"] = msg
            try:
                with open(SCRAPER_LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(f"\n{msg}\n")
            except Exception:
                pass
            return

        scraper = BatchScraper()
        output_base = DATA_SCRAPED_BASE
        os.makedirs(output_base, exist_ok=True)
        batch_logger = logging.getLogger("batch_scraper")
        today_str = date.today().strftime("%d-%m-%y")
        history_folder_date = today_str

        institute_logs_dir = os.path.join(LOGS_RUNS_DIR, today_str)
        os.makedirs(institute_logs_dir, exist_ok=True)
        institute_log_file = os.path.join(institute_logs_dir, safe_run_log_filename(uni, ""))
        inst_fh = None
        try:
            inst_fh = logging.FileHandler(institute_log_file, mode="a", encoding="utf-8")
            inst_fh.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
            )
            batch_logger.addHandler(inst_fh)

            async def _run_one():
                return await scraper.scrape_college(row, headless=headless, output_base=output_base)

            success, count, err, fname = asyncio.run(_run_one())
        finally:
            if inst_fh:
                try:
                    batch_logger.removeHandler(inst_fh)
                    inst_fh.close()
                except Exception:
                    pass

        history = _load_scrape_history()
        err_clean = (err or "").strip() if not success else ""
        history[uni] = {
            "lastScrapeDate": today_str,
            "success": success,
            "recordCount": count if success else 0,
            "filename": fname,
            "lastError": None if success else (err_clean or "Error occurred"),
        }
        _save_scrape_history(history)

        _scrape_status["done"] = 1
        _scrape_status["results"] = [
            {
                "university": uni,
                "success": success,
                "count": count,
                "error": err,
                "filename": fname,
                "skipped": False,
                "fromDashboardRetry": True,
            }
        ]
    except Exception as e:
        import traceback

        _scrape_status["error"] = str(e)
        try:
            with open(SCRAPER_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"\nSingle scrape retry error: {e}\n")
                f.write(traceback.format_exc())
        except Exception:
            pass
    finally:
        if file_handler:
            try:
                batch_logger = logging.getLogger("batch_scraper")
                batch_logger.removeHandler(file_handler)
                file_handler.close()
            except Exception:
                pass
        try:
            with open(SCRAPER_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(
                    f"Single scrape retry finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — "
                    f"{_scrape_status.get('done', 0)}/{_scrape_status.get('total', 1)}\n"
                )
        except Exception:
            pass
        if history_folder_date:
            _scrape_status["lastRunHistoryDate"] = history_folder_date
        _scrape_status["running"] = False
        _scrape_status["current"] = None
