"""
Shared helpers: strip secrets from dicts sent to browsers, merge credentials from Institutes.json.

Keep passwords only server-side (disk + in-memory for scrapers). API responses use sanitize_*.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths

from credential_env import ensure_row_password

# Keys never sent to clients (case-insensitive match on key)
_SENSITIVE_KEY_LOWER = frozenset(
    {
        "pass",
        "password",
        "pwd",
        "secret",
        "client_secret",
        "api_key",
        "apikey",
        "token",
        "authorization",
        "auth",
    }
)


def _is_sensitive_key(key: str) -> bool:
    return str(key).lower() in _SENSITIVE_KEY_LOWER


def sanitize_record_for_api(record: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow copy omitting password-like fields."""
    if not isinstance(record, dict):
        return {}
    return {k: v for k, v in record.items() if not _is_sensitive_key(k)}


def sanitize_list_for_api(records: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in records or []:
        if isinstance(r, dict):
            out.append(sanitize_record_for_api(r))
    return out


def load_institutes_lookup() -> Dict[str, Dict[str, Any]]:
    """
    Map normalized university name -> full institute row (includes pass on disk).
    """
    path = paths.INSTITUTES_JSON
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, list):
        return {}
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in data:
        if not isinstance(row, dict):
            continue
        name = (row.get("university") or "").strip().lower()
        if name:
            lookup[name] = row
    return lookup


def enrich_row_from_institutes(
    row: Dict[str, Any],
    lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Copy row and fill missing auth/url fields from Institutes.json by university name.
    Used when the client only had a sanitized institute object (no password).
    """
    if not isinstance(row, dict):
        return {}
    out = dict(row)
    if lookup is None:
        lookup = load_institutes_lookup()
    key = (out.get("university") or "").strip().lower()
    master = lookup.get(key) if key else None
    if not master:
        return out
    # Prefer explicit row values; fill gaps from master
    merge_keys = (
        "email",
        "pass",
        "password",
        "url",
        "pcid",
        "college_id",
        "FI",
        "source",
        "File_name",
        "Current_status",
    )
    for k in merge_keys:
        if out.get(k) in (None, "") and master.get(k) not in (None, ""):
            out[k] = master[k]
    # Normalize password field name for batch scraper
    if "pass" not in out or not out.get("pass"):
        pw = master.get("pass") or master.get("password")
        if pw:
            out["pass"] = pw
    ensure_row_password(out)
    return out
