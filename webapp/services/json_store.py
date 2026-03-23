"""Load/save JSON files (paths from project_paths: data/history, data/runtime, etc.)."""
import json
import os
import sys

# webapp/services -> project root
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths

from credential_env import merge_manual_credentials_from_env

MANUAL_CREDENTIALS_JSON = project_paths.MANUAL_CREDENTIALS_JSON
FILTER_CACHE_JSON = project_paths.FILTER_CACHE_JSON
SCRAPE_HISTORY_JSON = project_paths.SCRAPE_HISTORY_JSON
SCRAPE_LIST_JSON = project_paths.SCRAPE_LIST_JSON
UPLOAD_HISTORY_JSON = project_paths.UPLOAD_HISTORY_JSON


def load_scrape_history():
    try:
        if os.path.isfile(SCRAPE_HISTORY_JSON):
            with open(SCRAPE_HISTORY_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def load_upload_history():
    try:
        if os.path.isfile(UPLOAD_HISTORY_JSON):
            with open(UPLOAD_HISTORY_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def load_manual_credentials():
    raw = {}
    try:
        if os.path.isfile(MANUAL_CREDENTIALS_JSON):
            with open(MANUAL_CREDENTIALS_JSON, "r", encoding="utf-8") as f:
                raw = json.load(f)
                if not isinstance(raw, dict):
                    raw = {}
    except (json.JSONDecodeError, OSError):
        raw = {}
    return merge_manual_credentials_from_env(raw)


def load_scrape_list():
    try:
        if os.path.exists(SCRAPE_LIST_JSON):
            with open(SCRAPE_LIST_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        return []
    except (json.JSONDecodeError, OSError):
        return []


def save_scrape_list(data):
    os.makedirs(os.path.dirname(SCRAPE_LIST_JSON), exist_ok=True)
    with open(SCRAPE_LIST_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_filter_cache():
    try:
        if os.path.isfile(FILTER_CACHE_JSON):
            with open(FILTER_CACHE_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def save_filter_cache(data):
    payload = data if isinstance(data, dict) else {}
    os.makedirs(os.path.dirname(FILTER_CACHE_JSON), exist_ok=True)
    with open(FILTER_CACHE_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
