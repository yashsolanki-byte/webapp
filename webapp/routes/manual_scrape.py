"""Manual scrape: filters/subfilters in-process; run via Redis RQ + workers."""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Optional

from flask import Blueprint, jsonify, request, send_file

import project_paths as _project_paths

from webapp.config import DATA_SCRAPED_DIR, LOGS_APP_DIR, MANUAL_SCRAPE_LOG_FILE, URLS_JSON
from webapp.services.json_store import (
    load_filter_cache,
    load_manual_credentials,
    save_filter_cache,
)
from webapp.services.manual_scrape_queue import (
    cancel_all_active_manual_jobs,
    enqueue_manual_scrape,
    fetch_manual_job,
    manual_scrape_queue_available,
    new_job_id,
)
from webapp.state import manual_scrape_status

manual_scrape_bp = Blueprint("manual_scrape", __name__, url_prefix="")


def _rq_job_status_and_running(job) -> tuple[str, bool]:
    """
    RQ returns JobStatus enum — comparing to plain strings is always False, which made the UI
    think every job had finished immediately.
    """
    try:
        from rq.job import JobStatus

        st = job.get_status(refresh=True)
        if isinstance(st, JobStatus):
            key = st.value
            running = st in (
                JobStatus.QUEUED,
                JobStatus.STARTED,
                JobStatus.DEFERRED,
                JobStatus.SCHEDULED,
            )
            return key, running
    except Exception:
        pass
    raw = job.get_status(refresh=True)
    key = getattr(raw, "value", None) or str(raw)
    key = str(key).lower()
    return key, key in ("queued", "started", "deferred", "scheduled")


def _log_manual(msg: str, institute: Optional[str] = None) -> None:
    """Append to logs/app/manual_scrape.log and logs/runs/<today>/manual_<institute>.log."""
    try:
        os.makedirs(LOGS_APP_DIR, exist_ok=True)
        line = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + msg + "\n"
        with open(MANUAL_SCRAPE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        today_str = date.today().strftime("%d-%m-%y")
        inst = (institute or "").strip()
        fname = (
            _project_paths.safe_run_log_filename(inst, "manual")
            if inst
            else "manual_job.log"
        )
        _project_paths.append_logs_runs_line(today_str, fname, msg)
    except OSError:
        pass


def _cache_key(institute, source):
    return f"{str(institute or '').strip().lower()}|{str(source or '').strip().lower()}"


_DEFAULT_PUBLISHER_URLS = ["https://publisher.nopaperforms.com/lead/details"]

_ALLOWED_JOB_ARTIFACTS = frozenset({"output.csv", "screenshot.png", "logs.txt"})


@manual_scrape_bp.route("/api/manual-scrape/urls")
def api_manual_scrape_urls():
    """Publisher base URLs from data/reference/urls.json for manual scrape dropdown."""
    try:
        with open(URLS_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            urls = [str(u).strip() for u in data if str(u).strip()]
        else:
            urls = []
        if not urls:
            urls = list(_DEFAULT_PUBLISHER_URLS)
        return jsonify({"urls": sorted(set(urls), key=lambda x: x.lower())})
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return jsonify({"urls": list(_DEFAULT_PUBLISHER_URLS)})


@manual_scrape_bp.route("/api/manual-scrape/filters", methods=["POST"])
def api_manual_scrape_filters():
    data = request.get_json(silent=True) or {}
    institute = (data.get("institute") or "").strip()
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    creds = load_manual_credentials()
    cred_key = (data.get("credentials") or "central").strip().lower()
    if cred_key not in creds:
        return jsonify({"error": f"Unknown credentials: {cred_key}"}), 400
    params = {
        "login_url": (data.get("url") or "https://publisher.nopaperforms.com/lead/details").strip(),
        "email": creds[cred_key]["email"],
        "password": creds[cred_key]["password"],
        "institute": institute,
        "source": (data.get("source") or "Collegedunia").strip(),
    }
    try:
        from scrapers.script_scraper import SUBFILTER_CONFIG, fetch_advanced_filters

        key = _cache_key(params["institute"], params["source"])
        cache = load_filter_cache()
        cached = cache.get(key) if isinstance(cache, dict) else None
        if isinstance(cached, dict) and isinstance(cached.get("filters"), list) and cached.get("filters"):
            filters = cached.get("filters")
        else:
            filters = fetch_advanced_filters(params)
            cache = load_filter_cache()
            cache[key] = {
                "filters": filters or [],
                "subfilter_options": (cache.get(key) or {}).get("subfilter_options", {}),
                "institute": params["institute"],
                "source": params["source"],
                "updatedAt": datetime.now().isoformat(timespec="seconds"),
            }
            save_filter_cache(cache)
        subfilter_filter_ids = list(SUBFILTER_CONFIG.keys())
        return jsonify({"filters": filters, "subfilterFilterIds": subfilter_filter_ids})
    except Exception as e:
        _log_manual(f"Load filters failed ({institute}): {e}", institute=institute)
        return jsonify({"error": str(e)[:500]}), 500


@manual_scrape_bp.route("/api/manual-scrape/subfilter-options", methods=["POST"])
def api_manual_scrape_subfilter_options():
    data = request.get_json(silent=True) or {}
    institute = (data.get("institute") or "").strip()
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    creds = load_manual_credentials()
    cred_key = (data.get("credentials") or "central").strip().lower()
    if cred_key not in creds:
        return jsonify({"error": f"Unknown credentials: {cred_key}"}), 400
    filter_ids = data.get("filterIds") or data.get("filter_ids") or []
    if not filter_ids:
        return jsonify({"options": {}})
    params = {
        "login_url": (data.get("url") or "https://publisher.nopaperforms.com/lead/details").strip(),
        "email": creds[cred_key]["email"],
        "password": creds[cred_key]["password"],
        "institute": institute,
        "source": (data.get("source") or "Collegedunia").strip(),
    }
    try:
        from scrapers.script_scraper import fetch_subfilter_options

        key = _cache_key(params["institute"], params["source"])
        cache = load_filter_cache()
        cached_sf = (cache.get(key) or {}).get("subfilter_options", {})
        missing = [fid for fid in filter_ids if not cached_sf.get(fid)]
        if not missing:
            options = {fid: cached_sf.get(fid, []) for fid in filter_ids}
            return jsonify({"options": options})

        fetched = fetch_subfilter_options(params, missing)
        merged = {fid: cached_sf.get(fid, []) for fid in filter_ids}
        for fid in missing:
            merged[fid] = fetched.get(fid, [])

        if fetched:
            cache = load_filter_cache()
            entry = cache.get(key) if isinstance(cache.get(key), dict) else {}
            sf = entry.get("subfilter_options", {}) if isinstance(entry, dict) else {}
            if not isinstance(sf, dict):
                sf = {}
            sf.update(fetched)
            cache[key] = {
                "filters": entry.get("filters", []),
                "subfilter_options": sf,
                "institute": params["institute"],
                "source": params["source"],
                "updatedAt": datetime.now().isoformat(timespec="seconds"),
            }
            save_filter_cache(cache)
        options = merged
        return jsonify({"options": options})
    except Exception as e:
        _log_manual(f"Load subfilter options failed ({institute}): {e}", institute=institute)
        return jsonify({"error": str(e)[:500], "options": {}}), 500


def _build_enqueue_params(data: dict, cred_key: str, creds: dict) -> dict:
    institute = (data.get("institute") or "").strip()
    from_date = (data.get("from_date") or "").strip()
    to_date = (data.get("to_date") or "").strip()
    do_screenshot = data.get("screenshot") is True
    return {
        "login_url": (data.get("url") or "https://publisher.nopaperforms.com/lead/details").strip(),
        "email": creds[cred_key]["email"],
        "password": creds[cred_key]["password"],
        "institute": institute,
        "source": (data.get("source") or "Collegedunia").strip(),
        "from_date": from_date,
        "to_date": to_date,
        "instance": (data.get("instance") or "All").strip(),
        "rows_per_page": str(data.get("rows") or "5000"),
        "order": (data.get("order") or "Ascending").strip(),
        "advanced_filter_ids": data.get("advanced_filter_ids") or [],
        "subfilter_options": data.get("subfilter_options") or {},
        "screenshot_mode": do_screenshot,
    }


@manual_scrape_bp.route("/api/manual-scrape/run", methods=["POST"])
def api_manual_scrape_run():
    if not manual_scrape_queue_available():
        return (
            jsonify(
                {
                    "error": "Redis is not available. Set REDIS_URL, start Redis, and run RQ workers (see README).",
                }
            ),
            503,
        )
    data = request.get_json(silent=True) or {}
    institute = (data.get("institute") or "").strip()
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    creds = load_manual_credentials()
    cred_key = (data.get("credentials") or "central").strip().lower()
    if cred_key not in creds:
        return jsonify({"error": f"Unknown credentials: {cred_key}"}), 400
    from_date = (data.get("from_date") or "").strip()
    to_date = (data.get("to_date") or "").strip()
    do_screenshot = data.get("screenshot") is True
    if not from_date or not to_date:
        return jsonify({"error": "From date and To date are required (DD-MM-YYYY)"}), 400

    job_id = new_job_id()
    job_root = os.path.join(_project_paths.MANUAL_JOBS_RUNTIME_DIR, job_id)
    try:
        os.makedirs(job_root, exist_ok=True)
    except OSError as e:
        return jsonify({"error": f"Could not create job directory: {e}"}), 500

    params = _build_enqueue_params(data, cred_key, creds)
    _log_manual(
        f"Enqueued manual scrape job_id={job_id} institute={institute!r} {from_date}–{to_date} screenshot={do_screenshot}",
        institute=institute,
    )
    enqueue_manual_scrape(job_id, params)

    # Legacy UI fields (optional)
    manual_scrape_status["running"] = True
    manual_scrape_status["job_id"] = job_id
    manual_scrape_status["status"] = "Queued…"
    manual_scrape_status["error"] = None
    manual_scrape_status["output_path"] = ""
    manual_scrape_status["output_path_relative"] = ""
    manual_scrape_status["output_download_file"] = ""

    return jsonify({"ok": True, "job_id": job_id, "message": "Manual scrape queued"})


@manual_scrape_bp.route("/api/manual-scrape/status")
def api_manual_scrape_status():
    job_id = (request.args.get("job_id") or "").strip()
    if not job_id:
        return (
            jsonify(
                {
                    "error": "job_id is required",
                    "running": False,
                }
            ),
            400,
        )
    job = fetch_manual_job(job_id)
    if job is None:
        return jsonify({"error": "Job not found", "running": False, "job_id": job_id}), 404

    rq_status_str, running = _rq_job_status_and_running(job)
    meta = dict(job.meta or {})
    err = meta.get("error")
    code = meta.get("error_code")

    # Mirror legacy keys for the dashboard
    manual_scrape_status["running"] = running
    manual_scrape_status["job_id"] = job_id
    manual_scrape_status["status"] = meta.get("progress") or rq_status_str or ""
    manual_scrape_status["error"] = err
    if meta.get("status") == "success" and meta.get("result_path"):
        manual_scrape_status["output_path"] = meta.get("result_path") or ""
        manual_scrape_status["output_download_file"] = ""
    elif not running and err:
        manual_scrape_status["output_path"] = ""

    return jsonify(
        {
            "job_id": job_id,
            "rq_status": rq_status_str,
            "running": running,
            "status": meta.get("progress") or rq_status_str,
            "meta_status": meta.get("status"),
            "error": err,
            "error_code": code,
            "output_dir": meta.get("output"),
            "result_path": meta.get("result_path"),
            "output_csv": meta.get("output_csv"),
            "output_screenshot": meta.get("output_screenshot"),
            "logs_file": meta.get("logs_file"),
            "cancel": bool(meta.get("cancel")),
        }
    )


@manual_scrape_bp.route("/api/manual-scrape/cancel", methods=["POST"])
def api_manual_scrape_cancel():
    data = request.get_json(silent=True) or {}
    job_id = (data.get("job_id") or "").strip()
    if not job_id:
        return jsonify({"error": "job_id is required"}), 400
    job = fetch_manual_job(job_id)
    if job is None:
        return jsonify({"error": "Job not found"}), 404
    meta = dict(job.meta or {})
    meta["cancel"] = True
    meta["progress"] = (meta.get("progress") or "") + " | Cancel requested"
    job.meta = meta
    job.save_meta()
    return jsonify({"ok": True, "message": "Cancel requested. Worker will stop after the current step."})


@manual_scrape_bp.route("/api/manual-scrape/download")
def api_manual_scrape_download():
    """
    Download artifacts from a finished job: ?job_id=&file=output.csv|screenshot.png|logs.txt
    Legacy: ?file= basename under Downloads or ?path= under DATA_Scraped.
    """
    job_id = (request.args.get("job_id") or "").strip()
    if job_id:
        if ".." in job_id or "/" in job_id or "\\" in job_id:
            return jsonify({"error": "Invalid job_id"}), 400
        fname = (request.args.get("file") or "output.csv").strip()
        if fname not in _ALLOWED_JOB_ARTIFACTS:
            return jsonify({"error": "Invalid file"}), 400
        base = os.path.abspath(os.path.join(_project_paths.MANUAL_JOBS_RUNTIME_DIR, job_id))
        full = os.path.abspath(os.path.join(base, fname))
        if os.path.normcase(os.path.dirname(full)) != os.path.normcase(base) or not os.path.isfile(full):
            return jsonify({"error": "File not found"}), 404
        mt = None
        if fname.endswith(".png"):
            mt = "image/png"
        elif fname.endswith(".csv"):
            mt = "text/csv; charset=utf-8"
        elif fname.endswith(".txt"):
            mt = "text/plain; charset=utf-8"
        return send_file(
            full,
            as_attachment=True,
            download_name=fname,
            mimetype=mt,
            max_age=0,
        )

    file_only = (request.args.get("file") or "").strip()
    if file_only:
        from webapp.services.path_utils import get_user_downloads_dir

        if ".." in file_only or "/" in file_only or "\\" in file_only:
            return jsonify({"error": "Invalid file name"}), 400
        d = os.path.abspath(get_user_downloads_dir())
        full = os.path.abspath(os.path.join(d, file_only))
        if os.path.normcase(os.path.dirname(full)) != os.path.normcase(d):
            return jsonify({"error": "Invalid path"}), 400
        if not os.path.isfile(full):
            return jsonify({"error": "File not found"}), 404
        return send_file(full, as_attachment=True, download_name=os.path.basename(full))

    rel = (request.args.get("path") or "").strip().replace("\\", "/")
    if not rel or ".." in rel or rel.startswith("/"):
        return jsonify({"error": "Invalid path"}), 400
    full = os.path.normpath(os.path.join(DATA_SCRAPED_DIR, rel))
    if not full.startswith(os.path.normpath(DATA_SCRAPED_DIR)) or not os.path.isfile(full):
        return jsonify({"error": "File not found"}), 404
    return send_file(full, as_attachment=True, download_name=os.path.basename(full))
