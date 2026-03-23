"""
One-off / repeatable: clear pass/password fields in data JSON and universities TSV.
Passwords are supplied via .env (see credential_env.py).
Run from repo root: python tools/strip_passwords_from_data.py
"""
from __future__ import annotations

import csv
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths


def _scrub_json_obj(o) -> None:
    if isinstance(o, dict):
        for k in list(o.keys()):
            if str(k).lower() in ("pass", "password") and o[k]:
                o[k] = ""
            else:
                _scrub_json_obj(o[k])
    elif isinstance(o, list):
        for x in o:
            _scrub_json_obj(x)


def scrub_json(path: str) -> None:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    _scrub_json_obj(data)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("Scrubbed JSON:", path)


def scrub_universities_tsv(path: str) -> None:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f, delimiter="\t"):
            rows.append(row)
    if not rows:
        print("Empty:", path)
        return
    header = rows[0]
    if "pass" not in header:
        print("No pass column:", path)
        return
    pi = header.index("pass")
    for r in rows[1:]:
        while len(r) <= pi:
            r.append("")
        r[pi] = ""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        for r in rows:
            w.writerow(r)
    print("Scrubbed TSV:", path)


def main() -> None:
    paths.ensure_layout_migrated()
    for p in (paths.INSTITUTES_JSON, paths.SCRAPE_LIST_JSON):
        if os.path.isfile(p):
            scrub_json(p)
    uni = paths.UNIVERSITIES_TSV
    if os.path.isfile(uni):
        scrub_universities_tsv(uni)


if __name__ == "__main__":
    main()
