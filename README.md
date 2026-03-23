# NPF Scraper Web App

**Full documentation (structure, flows, APIs, credentials, data layout):** [`docs/PROJECT_GUIDE.md`](docs/PROJECT_GUIDE.md)

## Folder layout

| Location | Purpose |
|----------|---------|
| **`app.py`** | Run the Flask server only |
| **`run_stack.py`** | **One command:** Flask + RQ manual-scrape worker(s) (Redis must already be running) |
| **`docker-compose.yml`** | Optional: `docker compose up -d` → Redis on port 6379, then `python run_stack.py` |
| **`project_paths.py`** | All directory paths (edit once if you move folders) |
| **`data/`** | Sectioned JSON (see `data/README.md`): **`history/`** (scrape/upload/feedback history), **`reference/`** (Institutes, URLs, universities TSV), **`runtime/`** (scrape list, filter cache, credentials, exports) |
| **`scrapers/`** | Scraper engines: `script_scraper.py`, `batch_scraper.py`, `scraper_runner.py`, `upload_runner.py`, `upload_to_drive.py` |
| **`webapp/`** | HTTP routes by UI section (`webapp/README.md`) |
| **`templates/`** | HTML |
| **`logs/`** | **`app/`** (scraper, upload, batch, manual, feedback-ready logs), **`runs/<dd-mm-yy>/`** (per-institute scrape logs), **`script/`** (script_scraper session logs) |
| **`DATA_Scraped/`** | CSV output |

Imports use the `scrapers` package (e.g. `from scrapers.script_scraper import …`). Run commands from **this directory** so paths resolve.

### Quick start (manual scrape with Redis + workers)

1. **Redis** listening on `127.0.0.1:6379` (Windows service, [Docker](docker-compose.yml), WSL, etc.).
2. From this folder:
   ```bash
   python run_stack.py
   ```
   This starts **one RQ worker** and **Flask** on `http://127.0.0.1:5000/`.  
   More workers: `python run_stack.py --workers 4`  
   Flask only: `python run_stack.py --no-worker` (run workers in other terminals).

If you use Docker only for Redis: `docker compose up -d` then `python run_stack.py`.

`python app.py` still works but does **not** start workers — manual scrape jobs would stay queued unless you run `rq worker …` separately.

**Windows:** use **RQ 1.x** (`pip install "rq>=1.15,<2"`). RQ 2.x fails on import (`cannot find context for 'fork'`). Start workers with **`SimpleWorker`**:

```bat
python -m rq.cli worker --worker-class rq.worker.SimpleWorker -u redis://127.0.0.1:6379/0 manual_scrape
```

(`run_stack.py` uses **`scrapers.manual_rq_worker`** on Windows so job timeouts use **`TimerDeathPenalty`**, not **`SIGALRM`** (which Windows does not support).)

**Visible browser for queued manual scrape:** in `.env` set **`MANUAL_SCRAPE_HEADLESS=0`** (requires a display on the machine running the worker).

## Credentials (`.env`)

Publisher passwords are **not** stored in Python source or committed JSON. Copy `.env.example` to **`.env`** in this folder and set:

- `NPF_PASSWORD_CENTRAL`
- `NPF_PASSWORD_SANJAY`
- `NPF_PASSWORD_AMIT`

Install deps with `pip install -r requirements.txt` (includes `python-dotenv`). **`.env` is gitignored** — do not commit it. If these passwords were ever in git history, rotate them in NPF and update `.env`.

`data/reference/Institutes.json`, `data/runtime/scrape_list.json`, and `data/runtime/manual_credentials.json` keep **emails only** (empty `pass` on disk); the app fills passwords from `.env` at runtime.

## Feedback Uploader Ready tool

Standalone script: `scrapers/feedback_uploader_ready.py`

- **Bulk mode (by date):** reads uploaded Drive CSVs date-wise and writes transformed CSVs into `Feedback Uploader Ready/<YYYY-MM-DD>/<Institute>/`.
- **No double-processing:** for each date key (same string you pass to `--date` / the web UI), each institute is recorded in `data/history/feedback_ready_history.json` after a **fully successful** run (all CSVs for that institute uploaded). Later runs skip that institute for that date. Set **`FEEDBACK_READY_SKIP_HISTORY=1`** in `.env` to disable this check and re-process everyone.
- **Single mode:** transform one Drive file id and upload output CSV.
- **Output columns only:** `panel_name`, `super_camp_id`, `lead_id`, `feedback_id`, `form_initiated`, `verified`, `is_primary`
- Mapping: `panel_name="NPF"`, `super_camp_id=pcid`, `lead_id=name` (fallback: `campaign` if `name` is empty), `feedback_id=FI` — values are copied **exactly** from the source cells (no trim, no substring), last 3 columns blank.

Examples:

- Bulk: `python scrapers/feedback_uploader_ready.py --date 20-03-26`
- Single: `python scrapers/feedback_uploader_ready.py --file-id <DRIVE_FILE_ID> --out-name output.csv`

Optional env/config:

- `DRIVE_FOLDER_ID` (source root; defaults to current upload root)
- `DRIVE_SOURCE_DATA_FOLDER_NAME` (optional source child folder name, e.g. `scraped data`)
- `FEEDBACK_READY_ROOT_ID` (target root folder id; if empty, script auto-creates/finds `Feedback Uploader Ready` under source root)
- `FEEDBACK_READY_FOLDER_NAME` (target folder name override)
