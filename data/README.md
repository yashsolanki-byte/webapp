# Data layout (matches web app sections)

Paths are defined in **`project_paths.py`** at the project root. On first start, **`ensure_layout_migrated()`** moves files from the old flat `data/*.json` layout into these folders if needed.

| Folder | Role | Files |
|--------|------|--------|
| **`history/`** | Scrape / upload / feedback-ready **history** (append-only style state) | `scrape_history.json`, `upload_history.json`, `feedback_ready_history.json` |
| **`reference/`** | **Factual / seed** data (institutes, URLs, TSV) | `Institutes.json`, `manual_institutes.json`, `urls.json`, `universities` |
| **`runtime/`** | **Working state** (queues, cache, optional local credentials) | `scrape_list.json`, `filter_cache.json`, `manual_credentials.json`, optional `credentials.json`, `exports/` |

This file stays at `data/README.md` as an overview.
