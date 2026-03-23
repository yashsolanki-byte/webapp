#!/usr/bin/env bash
# From npf-scraper-webapp root
cd "$(dirname "$0")/.." || exit 1
export PYTHONPATH="$PWD"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
Q="${MANUAL_SCRAPE_RQ_QUEUE:-manual_scrape}"
exec python -m rq.cli worker -u "$REDIS_URL" "$Q"
