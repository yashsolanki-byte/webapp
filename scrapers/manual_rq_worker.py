"""
RQ worker for manual_scrape on Windows.

Default RQ uses UnixSignalDeathPenalty (SIGALRM), which does not exist on Windows.
This module uses SimpleWorker + TimerDeathPenalty (threading), which RQ ships for this purpose.

Run from project root (npf-scraper-webapp):
  python -m scrapers.manual_rq_worker

Env: REDIS_URL, MANUAL_SCRAPE_RQ_QUEUE (same as rq CLI).
"""
from __future__ import annotations

import os
import sys

_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.chdir(_ROOT)


def main() -> None:
    from credential_env import load_npf_dotenv
    from redis import Redis
    from rq import Connection
    from rq.timeouts import TimerDeathPenalty
    from rq.worker import SimpleWorker

    load_npf_dotenv()

    class TimerSimpleWorker(SimpleWorker):
        death_penalty_class = TimerDeathPenalty

    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    queue_name = os.getenv("MANUAL_SCRAPE_RQ_QUEUE", "manual_scrape")

    conn = Redis.from_url(redis_url, decode_responses=False)
    with Connection(conn):
        w = TimerSimpleWorker([queue_name], connection=conn)
        w.work()


if __name__ == "__main__":
    main()
