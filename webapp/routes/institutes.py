"""Institutes section: list from Institutes.json."""
import json
import os

from flask import Blueprint, jsonify

from institute_helpers import sanitize_list_for_api
from webapp.config import INSTITUTES_JSON

institutes_bp = Blueprint("institutes", __name__, url_prefix="")


@institutes_bp.route("/api/institutes")
def api_institutes():
    try:
        with open(INSTITUTES_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return jsonify([])
        return jsonify(sanitize_list_for_api(data))
    except FileNotFoundError:
        return jsonify([]), 404
    except (json.JSONDecodeError, OSError) as e:
        return jsonify({"error": str(e)}), 500
