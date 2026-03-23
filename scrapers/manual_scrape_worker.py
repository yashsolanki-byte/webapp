"""
RQ worker task: run manual scrape in isolation (own browser profile + output dir).

From project root (npf-scraper-webapp), after Redis is up:

  rq worker -u redis://127.0.0.1:6379/0 manual_scrape

Run 4–6 terminals with the same command for parallel jobs (one worker process per job at a time per process).
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Any, Callable, Dict, Optional

_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths
from scrapers.manual_scrape_errors import ManualScrapeLogicalError, ManualScrapeTransientError
from scrapers.script_scraper import ManualScrapeLeadsLimitExceeded, _is_retryable_error, run_headless

logger = logging.getLogger(__name__)

MAX_TRANSIENT_RETRIES = int(os.getenv("MANUAL_SCRAPE_TRANSIENT_RETRIES", "3"))


def _manual_scrape_headless_browser() -> bool:
    """
    If MANUAL_SCRAPE_HEADLESS is 0 / false / no / off → visible Chromium (local debugging).
    Default (unset or 1 / true) → headless (servers / background workers).
    """
    raw = (os.getenv("MANUAL_SCRAPE_HEADLESS") or "1").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def _job_paths(job_id: str) -> Dict[str, str]:
    root = os.path.join(paths.MANUAL_JOBS_RUNTIME_DIR, job_id)
    return {
        "root": root,
        "profile": os.path.join(root, "browser_profile"),
        "csv": os.path.join(root, "output.csv"),
        "logs_txt": os.path.join(root, "logs.txt"),
        "app_log": os.path.join(paths.LOGS_MANUAL_JOBS_DIR, f"{job_id}.txt"),
    }


def _append_log(path: str, msg: str) -> None:
    line = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + msg + "\n"
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass


def _save_meta(job, updates: Dict[str, Any]) -> None:
    if job is None:
        return
    meta = dict(job.meta or {})
    meta.update(updates)
    job.meta = meta
    job.save_meta()


def _cancel_checker(job) -> Callable[[], bool]:
    def _check() -> bool:
        if job is None:
            return False
        try:
            job.refresh()
        except Exception:
            pass
        try:
            return bool((job.meta or {}).get("cancel"))
        except Exception:
            return False

    return _check


def run_manual_scrape_worker(job_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    RQ entrypoint. Enqueue with the same RQ ``job_id`` as this string so cancel/fetch work.
    """
    try:
        from rq import get_current_job
        from redis import Redis

        rq_job = get_current_job()
    except Exception:
        rq_job = None

    paths.ensure_layout_migrated()
    jp = _job_paths(job_id)
    os.makedirs(jp["root"], exist_ok=True)
    os.makedirs(jp["profile"], exist_ok=True)
    os.makedirs(paths.LOGS_MANUAL_JOBS_DIR, exist_ok=True)

    redis_conn = None
    try:
        from webapp.services.manual_scrape_queue import REDIS_URL, register_active_job, unregister_active_job

        redis_conn = Redis.from_url(REDIS_URL, decode_responses=False)
        register_active_job(redis_conn, job_id)
    except Exception as ex:
        logger.warning("Could not register active manual job: %s", ex)

    wp = dict(params)
    screenshot_mode = bool(wp.get("screenshot_mode"))
    if screenshot_mode:
        wp["screenshot_path"] = os.path.join(jp["root"], "screenshot.png")
    else:
        wp["output_dir"] = jp["root"]
        wp["filename"] = "output.csv"
    wp["browser_user_data_dir"] = jp["profile"]
    wp["headless_browser"] = _manual_scrape_headless_browser()

    timeout_sec = os.getenv("MANUAL_SCRAPE_JOB_TIMEOUT_SEC", "1800")
    try:
        tsec = float(timeout_sec) if timeout_sec else None
    except ValueError:
        tsec = 1800.0
    if tsec is not None and tsec <= 0:
        tsec = None

    cancel_check = _cancel_checker(rq_job)

    def status_hook(msg: str):
        _save_meta(rq_job, {"progress": str(msg)[:2000]})
        _append_log(jp["logs_txt"], msg)
        _append_log(jp["app_log"], msg)

    _save_meta(
        rq_job,
        {
            "status": "running",
            "progress": "Starting browser…"
            + (" (visible window)" if not wp["headless_browser"] else " (headless)"),
            "error": None,
            "error_code": None,
            "output": jp["root"],
            "logs_file": jp["logs_txt"],
            "app_log": jp["app_log"],
        },
    )
    _append_log(jp["logs_txt"], "Job started")
    _append_log(jp["app_log"], "Job started")

    out_path: Optional[str] = None
    out_flags: Dict[str, Any] = {}

    def _finish_failure(code: str, message: str) -> Dict[str, Any]:
        _save_meta(
            rq_job,
            {"status": "failed", "error": message[:2000], "error_code": code},
        )
        _append_log(jp["logs_txt"], f"FAILED {code}: {message}")
        return {"ok": False, "error": message, "error_code": code}

    def _finish_cancelled() -> Dict[str, Any]:
        _save_meta(
            rq_job,
            {
                "status": "cancelled",
                "progress": "Cancelled",
                "error": "Cancelled by user",
                "error_code": "CANCELLED",
            },
        )
        return {"ok": False, "error": "Cancelled", "error_code": "CANCELLED"}

    try:
        for attempt in range(1, MAX_TRANSIENT_RETRIES + 1):
            out_flags.clear()
            try:
                status_hook(f"Attempt {attempt}/{MAX_TRANSIENT_RETRIES}")
                out_path = run_headless(
                    wp,
                    status_callback=status_hook,
                    cancel_check=cancel_check,
                    browser_user_data_dir=wp["browser_user_data_dir"],
                    headless_browser=wp["headless_browser"],
                    job_timeout_sec=tsec,
                    out_flags=out_flags,
                )
                if out_flags.get("stopped_by_user") or cancel_check():
                    return _finish_cancelled()
                break
            except ManualScrapeLeadsLimitExceeded as e:
                return _finish_failure("LEADS_LIMIT_EXCEEDED", str(e))
            except ManualScrapeLogicalError as e:
                return _finish_failure(getattr(e, "code", "LOGICAL_ERROR"), str(e))
            except (ManualScrapeTransientError, asyncio.TimeoutError) as e:
                code = getattr(e, "code", "TIMEOUT") if hasattr(e, "code") else "TIMEOUT"
                msg = str(e)
                status_hook(f"Transient {code}: {msg} (attempt {attempt}/{MAX_TRANSIENT_RETRIES})")
                if attempt >= MAX_TRANSIENT_RETRIES:
                    return _finish_failure(code, msg)
            except Exception as e:
                if _is_retryable_error(e) and attempt < MAX_TRANSIENT_RETRIES:
                    status_hook(f"Retry after error: {e}")
                    continue
                logger.exception("Manual scrape job failed")
                return _finish_failure("UNKNOWN", str(e))

        rel_csv = "screenshot.png" if screenshot_mode else "output.csv"
        result_output = out_path or (wp.get("screenshot_path") if screenshot_mode else jp["csv"])
        if screenshot_mode:
            sp = wp.get("screenshot_path") or ""
            if not (sp and os.path.isfile(sp)):
                return _finish_failure("NO_OUTPUT", "Screenshot file was not written.")
        else:
            if not os.path.isfile(jp["csv"]):
                return _finish_failure("NO_OUTPUT", "output.csv was not written.")

        _save_meta(
            rq_job,
            {
                "status": "success",
                "progress": "Done",
                "error": None,
                "error_code": None,
                "output": jp["root"],
                "output_csv": None if screenshot_mode else rel_csv,
                "output_screenshot": rel_csv if screenshot_mode else None,
                "result_path": result_output,
            },
        )
        _append_log(jp["logs_txt"], f"SUCCESS: {result_output}")
        return {
            "ok": True,
            "output_path": result_output,
            "job_dir": jp["root"],
        }
    finally:
        try:
            if redis_conn:
                from webapp.services.manual_scrape_queue import unregister_active_job

                unregister_active_job(redis_conn, job_id)
        except Exception:
            pass


__all__ = ["run_manual_scrape_worker"]
