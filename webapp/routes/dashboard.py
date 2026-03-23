"""Dashboard section: stats, date filter, scrape history summary."""
import json
import os

from flask import Blueprint, jsonify, request

from webapp.config import DATA_SCRAPED_DIR, INSTITUTES_JSON, SCRAPE_HISTORY_JSON
from webapp.services.json_store import load_scrape_history, load_upload_history

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="")

# Top-level DATA_Scraped folders that are not batch date folders (dd-mm-yy).
_DASHBOARD_IGNORE_SCRAPE_DIRS = frozenset({"manual"})


def _short_scrape_message(msg: str, max_len: int = 90) -> str:
    """Trim long Playwright errors for table cells; full text still in scrapeErrorFull."""
    m = (msg or "").strip() or "Error occurred"
    if len(m) <= max_len:
        return m
    return m[: max_len - 1] + "…"


def _norm_uni_key(name: str) -> str:
    return (name or "").strip().lower()


def _dashboard_row_for_scrape(
    uni: str,
    ent: object,
    date_str: str,
    folder_ids: dict,
) -> dict:
    """One institute row: scrape/upload status for a given date from history + Drive folders."""
    ent = ent if isinstance(ent, dict) else {}
    last_date = ent.get("lastScrapeDate")
    success = ent.get("success")
    scraped_this_date = last_date == date_str and success is True
    failed_this_date = last_date == date_str and success is False
    last_err = (ent.get("lastError") or "").strip()

    if scraped_this_date:
        scrape_status = "Scraped"
        scrape_error_full = None
    elif failed_this_date:
        detail = last_err or "Error occurred"
        scrape_status = f"Failed: {_short_scrape_message(detail)}"
        scrape_error_full = detail
    else:
        scrape_status = "—"
        scrape_error_full = None

    upload_status = "Uploaded" if (uni in folder_ids) else "—"
    if scraped_this_date or failed_this_date:
        record_count = ent.get("recordCount")
    else:
        record_count = None
    drive_id = folder_ids.get(uni)
    drive_link = ("https://drive.google.com/drive/folders/" + drive_id) if drive_id else None
    return {
        "name": uni or "—",
        "scrapeStatus": scrape_status,
        "scrapeErrorFull": scrape_error_full,
        "uploadStatus": upload_status,
        "recordCount": record_count,
        "driveLink": drive_link,
        "retryPaidScrape": failed_this_date,
    }


@dashboard_bp.route("/api/dashboard-dates")
def api_dashboard_dates():
    dates = set()
    if os.path.isdir(DATA_SCRAPED_DIR):
        for name in os.listdir(DATA_SCRAPED_DIR):
            path = os.path.join(DATA_SCRAPED_DIR, name)
            if (
                os.path.isdir(path)
                and ".." not in name
                and name.lower() not in _DASHBOARD_IGNORE_SCRAPE_DIRS
            ):
                dates.add(name)
    for date_key in load_upload_history():
        if date_key and ".." not in str(date_key):
            dates.add(date_key)
    for _uni, ent in load_scrape_history().items():
        d = (ent or {}).get("lastScrapeDate")
        if d and ".." not in str(d):
            dates.add(d)
    dates.discard("manual")
    dates.discard("Manual")
    return jsonify({"dates": sorted(dates, reverse=True)})


@dashboard_bp.route("/api/dashboard-stats")
def api_dashboard_stats():
    date_str = (request.args.get("date") or "").strip()
    if not date_str or ".." in date_str or os.path.sep in date_str:
        return jsonify({"error": "Invalid or missing date"}), 400

    try:
        with open(INSTITUTES_JSON, "r", encoding="utf-8") as f:
            institutes = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        institutes = []

    if not isinstance(institutes, list):
        institutes = []

    scrape_history = load_scrape_history()
    upload_history = load_upload_history()

    scraped_count = sum(
        1
        for _uni, ent in scrape_history.items()
        if (ent or {}).get("lastScrapeDate") == date_str and (ent or {}).get("success") is True
    )
    failed_scrape_count = sum(
        1
        for _uni, ent in scrape_history.items()
        if (ent or {}).get("lastScrapeDate") == date_str and (ent or {}).get("success") is False
    )
    upload_ent = upload_history.get(date_str) or {}
    folder_ids = upload_ent.get("folderIds") or {}
    uploaded_count = len(folder_ids)

    rows = []
    seen = set()
    # Only institutes that actually ran batch scrape on this date (any outcome).
    for inst in institutes:
        uni = (inst.get("university") or inst.get("college") or "").strip()
        if not uni:
            continue
        ent = scrape_history.get(uni) or {}
        if not isinstance(ent, dict) or ent.get("lastScrapeDate") != date_str:
            continue
        seen.add(_norm_uni_key(uni))
        rows.append(_dashboard_row_for_scrape(uni, ent, date_str, folder_ids))

    # Institutes that ran batch scrape today but are not on the master Institutes.json list
    # (e.g. only on scrape_list) — still show Scrape status / failure for that date.
    extras = []  # (display_name, ent) for scrape runs today not on master institute list
    for hist_uni, ent in scrape_history.items():
        if not isinstance(ent, dict):
            continue
        if ent.get("lastScrapeDate") != date_str:
            continue
        key = _norm_uni_key(hist_uni)
        if not key or key in seen:
            continue
        seen.add(key)
        display = (hist_uni or "").strip() or str(hist_uni)
        extras.append((display, ent))

    for uni, ent in sorted(extras, key=lambda x: x[0].lower()):
        rows.append(_dashboard_row_for_scrape(uni, ent, date_str, folder_ids))

    return jsonify(
        {
            "date": date_str,
            "totalInstitutes": len(rows),
            "scrapedCount": scraped_count,
            "failedScrapeCount": failed_scrape_count,
            "uploadedCount": uploaded_count,
            "institutes": rows,
        }
    )


@dashboard_bp.route("/api/scrape-history")
def api_scrape_history():
    try:
        if os.path.isfile(SCRAPE_HISTORY_JSON):
            with open(SCRAPE_HISTORY_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return jsonify(data if isinstance(data, dict) else {})
        return jsonify({})
    except (json.JSONDecodeError, OSError) as e:
        return jsonify({"error": str(e)}), 500
