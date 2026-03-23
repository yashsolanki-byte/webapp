"""
Load NPF login passwords from environment (.env via python-dotenv).

Never commit real secrets: use .env locally (see .env.example).
Profiles match manual scrape keys: sanjay, central, amit.
"""
from __future__ import annotations

import os
from typing import Any, Dict

# Project root = directory containing this file
_ROOT = os.path.dirname(os.path.abspath(__file__))

_dotenv_loaded = False


def load_npf_dotenv() -> None:
    """Load ``<project>/.env`` once (does not override existing OS env vars)."""
    global _dotenv_loaded
    if _dotenv_loaded:
        return
    try:
        from dotenv import load_dotenv

        env_path = os.path.join(_ROOT, ".env")
        if os.path.isfile(env_path):
            load_dotenv(env_path, override=False)
    except ImportError:
        pass
    _dotenv_loaded = True


# Normalized email -> env suffix (NPF_PASSWORD_<SUFFIX>)
_EMAIL_TO_SUFFIX = {
    "sanjay.meena@collegedunia.com": "SANJAY",
    "central.crm@collegedunia.com": "CENTRAL",
    "amit.swami@collegedunia.com": "AMIT",
}

_PROFILE_TO_SUFFIX = {
    "sanjay": "SANJAY",
    "central": "CENTRAL",
    "amit": "AMIT",
}

# Display emails for GUI / scraper profile picker (not secrets)
PROFILE_EMAILS: Dict[str, str] = {
    "sanjay": "sanjay.meena@collegedunia.com",
    "central": "central.crm@collegedunia.com",
    "amit": "Amit.swami@collegedunia.com",
}


def password_for_profile(profile_key: str) -> str:
    load_npf_dotenv()
    pk = (profile_key or "").strip().lower()
    suffix = _PROFILE_TO_SUFFIX.get(pk)
    if not suffix:
        return ""
    return (os.environ.get(f"NPF_PASSWORD_{suffix}") or "").strip()


def password_for_email(email: str) -> str:
    """Resolve password for a known institute email (batch/manual rows)."""
    load_npf_dotenv()
    key = (email or "").strip().lower()
    suffix = _EMAIL_TO_SUFFIX.get(key)
    if not suffix:
        return ""
    return (os.environ.get(f"NPF_PASSWORD_{suffix}") or "").strip()


def build_gui_credentials_dict() -> Dict[str, Dict[str, str]]:
    """For ScraperApp: profile -> {email, password} with password from env."""
    out: Dict[str, Dict[str, str]] = {}
    for profile, email in PROFILE_EMAILS.items():
        out[profile] = {
            "email": email,
            "password": password_for_profile(profile),
        }
    return out


def ensure_row_password(row: Dict[str, Any]) -> None:
    """
    If row has no usable pass/password, fill from env using ``email`` (mutates dict).
    Used for batch scrape rows and Institutes.json rows with empty pass on disk.
    """
    if not isinstance(row, dict):
        return
    existing = (row.get("pass") or row.get("password") or "").strip()
    if existing:
        return
    email = row.get("email") or ""
    pw = password_for_email(str(email))
    if pw:
        row["pass"] = pw


def merge_manual_credentials_from_env(raw: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Merge JSON manual_credentials (email only on disk) with passwords from env.
    """
    load_npf_dotenv()
    out: Dict[str, Dict[str, str]] = {}
    if not isinstance(raw, dict):
        return out
    for key, val in raw.items():
        if not isinstance(val, dict):
            continue
        pk = str(key).strip().lower()
        email = str(val.get("email") or PROFILE_EMAILS.get(pk, "")).strip()
        pwd = (str(val.get("password") or "")).strip() or password_for_profile(pk)
        out[pk] = {"email": email or PROFILE_EMAILS.get(pk, ""), "password": pwd}
    # Ensure all known profiles exist if missing from file
    for pk, email in PROFILE_EMAILS.items():
        if pk not in out:
            out[pk] = {"email": email, "password": password_for_profile(pk)}
    return out
