"""
CSV export: remove phone / mobile–style columns only; keep all other fields (incl. name, email).
Used by batch_scraper and script_scraper before to_csv.
"""
from __future__ import annotations

from typing import Any, Iterable, List


# Normalized (lower, stripped) exact matches
_PHONE_MOBILE_EXACT = frozenset(
    {
        "mobile",
        "phone",
        "mobileno",
        "phoneno",
        "mobile number",
        "phone number",
        "phone no",
        "phone no.",
        "mobile no",
        "mobile no.",
        "contact number",
        "whatsapp",
        "whatsapp no",
        "whatsapp number",
        "student mobile",
        "student phone",
        "alternate mobile",
        "alternate phone",
        "primary mobile",
        "secondary mobile",
        "cell",
        "cell phone",
        "telephone",
        "tel",
        "father mobile",
        "mother mobile",
        "guardian mobile",
        "guardian phone",
    }
)


def _normalize_header(name: str) -> str:
    return str(name).strip().lower()


def columns_matching_phone_mobile(columns: Iterable[Any]) -> List[Any]:
    """Original column keys to drop (phone / mobile / whatsapp style only)."""
    out: List[Any] = []
    seen = set()
    exact_compact = frozenset(
        e.replace(" ", "").replace("_", "").replace(".", "") for e in _PHONE_MOBILE_EXACT
    )
    for col in columns:
        if col in seen:
            continue
        n = _normalize_header(col)
        nc = n.replace(" ", "").replace("_", "").replace(".", "")
        if n in _PHONE_MOBILE_EXACT or nc in exact_compact:
            out.append(col)
            seen.add(col)
            continue
        if "whatsapp" in n:
            out.append(col)
            seen.add(col)
            continue
        if "mobile" in n:
            out.append(col)
            seen.add(col)
            continue
        if "phone" in n:
            out.append(col)
            seen.add(col)
            continue
        if n in ("cell", "tel"):
            out.append(col)
            seen.add(col)
    return out


def drop_phone_mobile_columns(df):
    """
    Return a copy of the DataFrame without phone/mobile-like columns.
    Does not drop name, email, or other PII unless the header matches phone/mobile rules.
    """
    to_drop = columns_matching_phone_mobile(df.columns)
    if not to_drop:
        return df
    return df.drop(columns=to_drop, errors="ignore")
