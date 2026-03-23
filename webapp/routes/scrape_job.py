"""Scrape list + batch scrape job (scraper_runner)."""
import threading

from flask import Blueprint, jsonify, request

from institute_helpers import (
    enrich_row_from_institutes,
    load_institutes_lookup,
    sanitize_list_for_api,
    sanitize_record_for_api,
)
from webapp.config import SCRAPE_LIST_MAX_LENGTH
from webapp.services.json_store import load_scrape_list, save_scrape_list

scrape_job_bp = Blueprint("scrape_job", __name__, url_prefix="")


@scrape_job_bp.route("/api/scrape-list", methods=["GET"])
def api_scrape_list_get():
    return jsonify(
        {"list": sanitize_list_for_api(load_scrape_list()), "maxLength": SCRAPE_LIST_MAX_LENGTH}
    )


@scrape_job_bp.route("/api/scrape-list", methods=["POST"])
def api_scrape_list_add():
    data = request.get_json()
    to_add = []
    if isinstance(data, dict):
        to_add = [data]
    elif isinstance(data, list):
        to_add = [x for x in data if isinstance(x, dict)]
    else:
        return jsonify({"error": "Invalid body: expected an institute object or array"}), 400
    current = load_scrape_list()
    lookup = load_institutes_lookup()
    existing_names = {(item.get("university") or "").strip() for item in current}
    added = 0
    for item in to_add:
        if len(current) >= SCRAPE_LIST_MAX_LENGTH:
            break
        university = (item.get("university") or "").strip()
        if not university or university in existing_names:
            continue
        full_row = enrich_row_from_institutes(item, lookup)
        # Do not persist passwords to scrape_list.json — resolved from .env at scrape time
        current.append(sanitize_record_for_api(full_row))
        existing_names.add(university)
        added += 1
    save_scrape_list(current)
    return jsonify(
        {
            "list": sanitize_list_for_api(current),
            "added": added,
            "maxLength": SCRAPE_LIST_MAX_LENGTH,
        }
    )


@scrape_job_bp.route("/api/scrape-list", methods=["DELETE"])
def api_scrape_list_remove():
    data = request.get_json(silent=True) or {}
    university = (data.get("university") or request.args.get("university") or "").strip()
    if not university:
        return jsonify({"error": "Missing university name"}), 400
    current = load_scrape_list()
    new_list = [item for item in current if (item.get("university") or "").strip() != university]
    if len(new_list) == len(current):
        return (
            jsonify(
                {
                    "error": "Not found in list",
                    "list": sanitize_list_for_api(current),
                }
            ),
            404,
        )
    save_scrape_list(new_list)
    return jsonify({"list": sanitize_list_for_api(new_list)})


@scrape_job_bp.route("/api/run-scrape", methods=["POST"])
def api_run_scrape():
    try:
        from scrapers.scraper_runner import get_scrape_status, run_scrape_job
    except ImportError as e:
        return jsonify({"error": f"Scraper not available: {e}"}), 500
    status = get_scrape_status()
    if status.get("running"):
        return jsonify({"error": "Scrape already running", "status": status}), 409
    headless = request.get_json(silent=True) or {}
    if isinstance(headless, dict):
        headless = headless.get("headless", False)
    else:
        headless = False
    thread = threading.Thread(target=run_scrape_job, kwargs={"headless": headless}, daemon=False)
    thread.start()
    return jsonify({"ok": True, "message": "Scrape started (browser visible)"})


@scrape_job_bp.route("/api/scrape-status")
def api_scrape_status():
    try:
        from scrapers.scraper_runner import get_scrape_status

        return jsonify(get_scrape_status())
    except ImportError:
        return jsonify({"running": False, "error": "Scraper not available"})


@scrape_job_bp.route("/api/scrape-retry", methods=["POST"])
def api_scrape_retry():
    """
    Dashboard: retry paid-application batch scrape for one institute (failed run).
    Same engine and DATA_Scraped/dd-mm-yy/ output as Jobs → Run scrape.
    """
    try:
        from scrapers.scraper_runner import (
            get_scrape_status,
            prepare_scrape_retry_row,
            run_single_scrape_worker,
        )
    except ImportError as e:
        return jsonify({"error": f"Scraper not available: {e}"}), 500

    if get_scrape_status().get("running"):
        return jsonify({"error": "A scrape is already running (Jobs or another retry)"}), 409

    data = request.get_json(silent=True) or {}
    university = (data.get("university") or data.get("name") or "").strip()
    if not university:
        return jsonify({"error": "Missing university (name)"}), 400

    row, prep_err = prepare_scrape_retry_row(university)
    if prep_err:
        return jsonify({"error": prep_err}), 400

    headless = False
    if isinstance(data, dict) and data.get("headless") is True:
        headless = True

    thread = threading.Thread(
        target=run_single_scrape_worker,
        kwargs={"row": row, "headless": headless},
        daemon=False,
    )
    thread.start()
    return jsonify(
        {
            "ok": True,
            "message": "Paid application scrape started for this institute (same output folder as Jobs).",
            "university": (row.get("university") or university or "").strip(),
        }
    )
