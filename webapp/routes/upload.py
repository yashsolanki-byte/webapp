"""Upload to Drive section."""
import os
from datetime import date

from flask import Blueprint, jsonify, request

from webapp.config import DATA_SCRAPED_DIR

upload_bp = Blueprint("upload", __name__, url_prefix="")


@upload_bp.route("/api/upload-dates")
def api_upload_dates():
    if not os.path.isdir(DATA_SCRAPED_DIR):
        return jsonify({"dates": []})
    dates = []
    for name in sorted(os.listdir(DATA_SCRAPED_DIR), reverse=True):
        path = os.path.join(DATA_SCRAPED_DIR, name)
        if os.path.isdir(path) and ".." not in name and os.path.sep not in name:
            dates.append(name)
    return jsonify({"dates": dates})


@upload_bp.route("/api/upload-to-drive", methods=["POST"])
def api_upload_to_drive():
    try:
        from scrapers.upload_runner import get_upload_status, run_upload_job
    except ImportError as e:
        return jsonify({"error": f"Upload not available: {e}"}), 500
    if get_upload_status().get("running"):
        return jsonify({"error": "Upload already running"}), 409
    data = request.get_json(silent=True) or {}
    date_str = (data.get("date") or "").strip()
    if not date_str:
        date_str = date.today().strftime("%d-%m-%y")
    if ".." in date_str or os.path.sep in date_str or len(date_str) > 20:
        return jsonify({"error": "Invalid date"}), 400
    run_upload_job(date_str)
    return jsonify({"ok": True, "message": f"Upload started for {date_str}"})


@upload_bp.route("/api/upload-status")
def api_upload_status():
    try:
        from scrapers.upload_runner import get_upload_status

        return jsonify(get_upload_status())
    except ImportError:
        return jsonify({"running": False, "error": "Upload not available"})
