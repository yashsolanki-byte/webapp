# Docker deployment

Run **web (Gunicorn)**, **Redis**, and **RQ workers** with one stack. Playwright/Chromium use the official **Playwright Python** base image.

## Prerequisites

- [Docker](https://docs.docker.com/engine/install/) and [Docker Compose v2](https://docs.docker.com/compose/install/) on the server.
- `.env` in the project root (copy from `.env.example`) with **`NPF_PASSWORD_*`** and any Drive-related vars you use.
- For Google Drive upload: place **`data/runtime/credentials.json`** on the host (it is mounted into the container via `./data`).

## Important: `REDIS_URL` inside Docker

Compose sets **`REDIS_URL=redis://redis:6379/0`** for `web` and `rq-worker`. That **overrides** a host-style `REDIS_URL` from `.env`.

The **Redis** service in this file has **no password** (only reachable on the internal Docker network). Do not publish Redis to the internet.

If you need Redis **password** in Docker, add a custom `command` or `redis.conf` and set e.g. `REDIS_URL=redis://:yourpass@redis:6379/0` in `docker-compose.yml` under `environment`.

## Commands

From the project root (`webapp2` on your server — same folder as `docker-compose.yml`):

```bash
# Build images
docker compose build

# Start (1 web + 1 worker + redis)
docker compose up -d

# More parallel manual scrapes
docker compose up -d --scale rq-worker=3

# Logs
docker compose logs -f web
docker compose logs -f rq-worker

# Stop
docker compose down
```

## URLs and firewall

- Default: **`http://YOUR_SERVER_IP:8000`**
- Open port **8000** in the cloud firewall **or** run **Nginx** on the host proxying to `127.0.0.1:8000` and only open **80/443**.

## Data directories

Host folders are mounted read-write:

| Host | Container |
|------|-----------|
| `./data` | `/app/data` |
| `./DATA_Scraped` | `/app/DATA_Scraped` |
| `./logs` | `/app/logs` |
| `./Feedback_Uploader_Ready_Output` | `/app/Feedback_Uploader_Ready_Output` |

Create them on the host if missing:

```bash
mkdir -p data DATA_Scraped logs Feedback_Uploader_Ready_Output
```

## Updates after `git pull`

```bash
docker compose build --no-cache
docker compose up -d
```

## Host Redis vs Docker Redis

- **This compose file** runs its **own** Redis container. Stop/disable **host** `redis-server` if it still binds **port 6379**, or change the compose **web** `ports` / Redis mapping to avoid conflicts.
- To use **host Redis** instead: remove the `redis` service, set `REDIS_URL` to reach the host (`host.docker.internal` on Docker Desktop; on Linux use `network_mode: host` or the host gateway IP — more advanced). The simple path is **use compose Redis only**.
