# Localhost Testing Guide (Windows + Anaconda + Native Redis)

This document lists the exact steps used to run and test this project locally on Windows.

## 1) Open project in Anaconda prompt

```bat
cd /d "C:\Users\CollegeDunia\NPF WEB APP\npf-scraper-webapp"
```

## 2) Create/activate conda env

If you already have an env (example: `webapp`), just activate it:

```bat
conda activate webapp
```

Otherwise create one first:

```bat
conda create -n webapp python=3.10 -y
conda activate webapp
```

## 3) Install Python dependencies

```bat
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 4) Configure environment variables

Copy template once:

```bat
copy .env.example .env
```

Required values in `.env`:

- `NPF_PASSWORD_CENTRAL`
- `NPF_PASSWORD_SANJAY`
- `NPF_PASSWORD_AMIT`
- `REDIS_URL=redis://127.0.0.1:6379/0`
- `MANUAL_SCRAPE_RQ_QUEUE=manual_scrape`
- `MANUAL_SCRAPE_HEADLESS=1`

## 5) Start Redis (native Windows)

Redis binaries are under `C:\Program Files\Redis`.

Terminal A:

```bat
cd /d "C:\Program Files\Redis"
redis-server.exe redis.windows.conf
```

If Redis is already running, this command may show bind/port-in-use. That is fine.

Terminal B (health check):

```bat
cd /d "C:\Program Files\Redis"
redis-cli.exe ping
```

Expected result: `PONG`

## 6) Start app + workers

### Option A: One command (recommended)

From project root:

```bat
cd /d "C:\Users\CollegeDunia\NPF WEB APP\npf-scraper-webapp"
conda activate webapp
python run_stack.py --workers 4
```

This starts:
- Flask web app on `http://127.0.0.1:5000`
- 4 RQ workers on queue `manual_scrape`

### Option B: Separate terminals

Terminal 1 (worker):

```bat
cd /d "C:\Users\CollegeDunia\NPF WEB APP\npf-scraper-webapp"
conda activate webapp
python -m scrapers.manual_rq_worker
```

Terminal 2 (web app):

```bat
cd /d "C:\Users\CollegeDunia\NPF WEB APP\npf-scraper-webapp"
conda activate webapp
python app.py
```

Repeat worker terminal for multiple workers.

## 7) Verify workers and queue

```bat
cd /d "C:\Users\CollegeDunia\NPF WEB APP\npf-scraper-webapp"
conda activate webapp
rq info -u redis://127.0.0.1:6379/0
```

Look for:
- `N workers`
- queue `manual_scrape`
- queued/executing counts

## 8) Open app and run test

Open:

- `http://127.0.0.1:5000`

Run a small manual scrape job and confirm:
- UI status moves from queued -> running -> success/fail
- worker terminal logs show job execution

## 9) Common issues and quick fixes

- **`redis-cli is not recognized`**
  - Use full path: `C:\Program Files\Redis\redis-cli.exe ping`

- **`Could not create server TCP listening socket ... 6379`**
  - Redis already running; verify with `redis-cli.exe ping`

- **Job stays queued**
  - Ensure at least one worker is running and listening on `manual_scrape`
  - Confirm same `REDIS_URL` in app + worker

- **`ModuleNotFoundError: tkinter` on server-like env**
  - Fixed in code path for headless worker import; use worker command from this guide

## 10) Stop local test stack

- If using `run_stack.py`: `Ctrl + C` in that terminal.
- If using separate terminals: stop worker/app terminals with `Ctrl + C`.
- Stop Redis if needed by closing Redis terminal or stopping the Redis service.

