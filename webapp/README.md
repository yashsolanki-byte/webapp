# Web app layout (easy to edit by section)

All JSON (under `data/history`, `data/reference`, `data/runtime`), logs (`logs/app`, `logs/runs`, `logs/script`), and `DATA_Scraped/` stay in the **project root** (`npf-scraper-webapp/`). Route and helper code is split here; paths are centralized in **`project_paths.py`**.

| Section | File | Routes |
|--------|------|--------|
| Pages | `routes/pages.py` | `/` |
| Dashboard | `routes/dashboard.py` | `/api/dashboard-dates`, `/api/dashboard-stats`, `/api/scrape-history` |
| Institutes | `routes/institutes.py` | `/api/institutes` |
| Manual scrape | `routes/manual_scrape.py` | `/api/manual-scrape/urls`, `/filters`, `/subfilter-options`, `/run` (RQ enqueue), `/status?job_id=`, `/cancel`, `/download?job_id=` |
| Scrape list & job | `routes/scrape_job.py` | `/api/scrape-list`, `/api/run-scrape`, `/api/scrape-status`, `/api/scrape-retry` |
| Logs | `routes/logs.py` | `/api/scraper-logs/*` (dates, files by `kind`, content), legacy `/api/*-logs` |
| Settings | `routes/settings_info.py` | `/api/settings/auth-summary` |
| Upload | `routes/upload.py` | `/api/upload-dates`, `/api/upload-to-drive`, `/api/upload-status` |

**Shared**

- `config.py` — paths (`APP_DIR`, `DATA_SCRAPED_DIR`, JSON file paths, limits)
- `state.py` — in-memory mirror of last manual job status (UI polling uses `job_id` + Redis/RQ)
- `services/manual_scrape_queue.py` — Redis connection, enqueue, active-job cancel helpers
- `services/json_store.py` — load/save scrape list, history, credentials JSON
- `services/path_utils.py` — safe paths for logs / manual output names

Scraper engines live in **`scrapers/`** (`script_scraper.py`, `batch_scraper.py`, `scraper_runner.py`, `upload_runner.py`, `upload_to_drive.py`). Shared JSON is under **`data/`** (see `data/README.md`); paths are centralized in **`project_paths.py`**.

## API & secrets

- **`/api/institutes`** and **`/api/scrape-list`** return rows **without** password-like fields (`pass`, `password`, tokens, etc.). Full credentials stay in `data/reference/Institutes.json` and `data/runtime/scrape_list.json` on the server only.
- When adding to the scrape list, the server **merges** credentials from `Institutes.json` by university name so scraping still works without the browser ever receiving passwords.
- For production, also use **HTTPS**, **login / session** on the app, bind to **localhost** or a **VPN**, and avoid exposing the Flask port publicly—hiding fields in JSON does not stop someone who can call your APIs directly.
