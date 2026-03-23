# Deploy on a Linux server (Flask + Redis + RQ workers)

Assumes **Ubuntu 22.04+** (or similar). Paths: app at `/opt/npf-scraper-webapp`, user `npf`.

## 1. Install system packages

```bash
sudo apt update
sudo apt install -y python3-venv python3-pip nginx redis-server git
```

Redis listens on `127.0.0.1:6379` by default (good). Enable it:

```bash
sudo systemctl enable --now redis-server
redis-cli ping   # expect PONG
```

## 2. App user and code

```bash
sudo useradd -r -m -d /opt/npf -s /bin/bash npf || true
sudo mkdir -p /opt/npf-scraper-webapp
# Copy or git clone your repo here, then:
sudo chown -R npf:npf /opt/npf-scraper-webapp
sudo -u npf bash
cd /opt/npf-scraper-webapp
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip wheel
pip install -r requirements.txt gunicorn
playwright install chromium
playwright install-deps chromium
```

## 3. Environment file

```bash
sudo -u npf nano /opt/npf-scraper-webapp/.env
```

Minimum:

```env
REDIS_URL=redis://127.0.0.1:6379/0
NPF_PASSWORD_CENTRAL=...
NPF_PASSWORD_SANJAY=...
NPF_PASSWORD_AMIT=...
# Production: keep browsers headless
MANUAL_SCRAPE_HEADLESS=1
```

## 4. Gunicorn (Flask) — **does not** run workers

Bind to localhost; Nginx will proxy HTTPS → here.

```bash
# Test manually:
cd /opt/npf-scraper-webapp && source .venv/bin/activate
gunicorn -w 2 -b 127.0.0.1:8000 --timeout 120 app:app
```

**systemd** `/etc/systemd/system/npf-web.service`:

```ini
[Unit]
Description=NPF Scraper Web (Gunicorn)
After=network.target redis-server.service

[Service]
User=npf
Group=npf
WorkingDirectory=/opt/npf-scraper-webapp
Environment=PATH=/opt/npf-scraper-webapp/.venv/bin
ExecStart=/opt/npf-scraper-webapp/.venv/bin/gunicorn -w 2 -b 127.0.0.1:8000 --timeout 120 app:app
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now npf-web
```

## 5. RQ workers (manual scrape)

Run **one service per worker** (e.g. 4 workers = 4 services), or one service with a small wrapper script. Example **single worker** unit `/etc/systemd/system/npf-rq-worker@.service`:

```ini
[Unit]
Description=NPF manual_scrape RQ worker (%i)
After=network.target redis-server.service

[Service]
User=npf
Group=npf
WorkingDirectory=/opt/npf-scraper-webapp
Environment=PATH=/opt/npf-scraper-webapp/.venv/bin
Environment=PYTHONPATH=/opt/npf-scraper-webapp
# Load REDIS_URL from .env — dotenv is loaded inside the worker module
ExecStart=/opt/npf-scraper-webapp/.venv/bin/python -m scrapers.manual_rq_worker
Restart=always

[Install]
WantedBy=multi-user.target
```

**Note:** `manual_rq_worker` uses **TimerDeathPenalty** (works on Linux too). Alternatively use:

`ExecStart=.../rq worker -u redis://127.0.0.1:6379/0 manual_scrape`

Enable 4 workers:

```bash
sudo systemctl enable --now npf-rq-worker@1 npf-rq-worker@2 npf-rq-worker@3 npf-rq-worker@4
```

(Ensure `EnvironmentFile=/opt/npf-scraper-webapp/.env` is **not** used unless you convert `.env` to `KEY=value` lines systemd accepts; the app loads `.env` via `credential_env` when the Python process starts — **manual_rq_worker** calls `load_npf_dotenv()`, so **REDIS_URL** in `.env` is enough.)

## 6. Nginx (HTTPS + reverse proxy)

Example server block (replace `your-domain.com` and SSL paths):

```nginx
server {
    listen 443 ssl http2;
    server_name your-domain.com;

    ssl_certificate     /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
    }
}
```

Get certs: `sudo apt install certbot python3-certbot-nginx && sudo certbot --nginx -d your-domain.com`

```bash
sudo nginx -t && sudo systemctl reload nginx
```

## 7. Firewall

```bash
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw enable
```

**Do not** open port **6379** to the internet.

## 8. Checklist

| Check | Command / note |
|--------|----------------|
| Redis | `redis-cli ping` |
| Web | `curl -sI http://127.0.0.1:8000/` |
| Workers | `journalctl -u npf-rq-worker@1 -f` |
| Manual job | Enqueue from UI; worker logs should show Playwright |

## 9. Scaling

- **More concurrency** → more `npf-rq-worker@N` instances (same `REDIS_URL`).
- **Redis elsewhere** → set `REDIS_URL` to managed Redis URL on **web + all workers**.
- **Heavy RAM**: each Chromium ~300MB–1GB+; size the VM for `workers × browsers`.

## 10. Docker (optional)

Use one Compose stack: `redis`, `web` (gunicorn), `worker` scaled with `docker compose up -d --scale worker=4`. Mount a volume for `data/` and `DATA_Scraped/`. Use Playwright’s official image or install Chromium in your Dockerfile.

---

**Summary:** Redis is a **separate service**. **Gunicorn** serves the site and **only enqueues** jobs. **RQ worker processes** (4× if you want) connect to the **same Redis** and run Playwright jobs. Nginx terminates TLS and proxies to Gunicorn.
