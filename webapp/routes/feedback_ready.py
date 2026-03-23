"""Feedback Uploader Ready section."""
import os

from flask import Blueprint, jsonify, request

from webapp.services.json_store import load_upload_history

feedback_ready_bp = Blueprint("feedback_ready", __name__, url_prefix="")


@feedback_ready_bp.route("/api/feedback-ready-dates")
def api_feedback_ready_dates():
    # Source is Drive uploaded-date history (keys are dd-mm-yy in this app)
    dates = sorted(load_upload_history().keys(), reverse=True)
    return jsonify({"dates": dates})


@feedback_ready_bp.route("/api/feedback-ready/files")
def api_feedback_ready_files():
    date_str = (request.args.get("date") or "").strip()
    if not date_str:
        return jsonify({"error": "Date is required", "institutes": []}), 400
    try:
        from scrapers.feedback_uploader_ready import (
            DEFAULT_SOURCE_ROOT_ID,
            get_drive_service,
            list_files_for_date,
            _resolve_source_root,
        )

        service = get_drive_service()
        source_root_id = _resolve_source_root(service, DEFAULT_SOURCE_ROOT_ID)
        return jsonify(list_files_for_date(service, source_root_id, date_str))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "institutes": []}), 500


@feedback_ready_bp.route("/api/feedback-ready/run", methods=["POST"])
def api_feedback_ready_run():
    try:
        from scrapers.feedback_ready_runner import (
            get_feedback_ready_status,
            run_feedback_ready_job,
        )
    except ImportError as e:
        return jsonify({"error": f"Feedback-ready not available: {e}"}), 500
    if get_feedback_ready_status().get("running"):
        return jsonify({"error": "Feedback-ready already running"}), 409
    data = request.get_json(silent=True) or {}
    date_str = (data.get("date") or "").strip()
    if not date_str:
        return jsonify({"error": "Date is required"}), 400
    if ".." in date_str or os.path.sep in date_str or len(date_str) > 20:
        return jsonify({"error": "Invalid date"}), 400
    file_ids = data.get("fileIds") or []
    if file_ids and not isinstance(file_ids, list):
        return jsonify({"error": "fileIds must be an array"}), 400
    file_ids = [str(x).strip() for x in file_ids if str(x).strip()]
    run_feedback_ready_job(date_str, selected_file_ids=file_ids)
    return jsonify({"ok": True, "message": f"Feedback-ready started for {date_str}"})


@feedback_ready_bp.route("/api/feedback-ready/status")
def api_feedback_ready_status():
    try:
        from scrapers.feedback_ready_runner import get_feedback_ready_status

        return jsonify(get_feedback_ready_status())
    except ImportError:
        return jsonify({"running": False, "error": "Feedback-ready not available"})

