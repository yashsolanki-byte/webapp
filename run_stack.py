#!/usr/bin/env python3
"""
Start Flask + one RQ manual-scrape worker in a single process tree.

Redis must already be listening (default redis://127.0.0.1:6379/0).
Does NOT start the Redis server — use a native service (Windows/Linux), WSL, etc.

Usage (from npf-scraper-webapp):
  python run_stack.py

Optional:
  python run_stack.py --workers 2    # two worker subprocesses
  python run_stack.py --no-worker    # only Flask (you run workers elsewhere)
"""
from __future__ import annotations

import argparse
import atexit
import os
import signal
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.chdir(ROOT)

from credential_env import load_npf_dotenv

load_npf_dotenv()
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")
os.environ.setdefault("PYTHONPATH", ROOT)


def _ping_redis(url: str) -> None:
    try:
        import redis

        r = redis.from_url(url, decode_responses=False)
        r.ping()
    except Exception as e:
        print(
            "Cannot connect to Redis at",
            url,
            "\nStart Redis first (service, WSL, etc.), or set REDIS_URL in .env\n",
            f"Error: {e}",
            file=sys.stderr,
        )
        sys.exit(1)


def _queue_name() -> str:
    return os.getenv("MANUAL_SCRAPE_RQ_QUEUE", "manual_scrape")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Flask + RQ manual_scrape worker(s)")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of RQ worker subprocesses (default 1)",
    )
    parser.add_argument(
        "--no-worker",
        action="store_true",
        help="Only run Flask; start workers in other terminals",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    redis_url = os.environ["REDIS_URL"]
    _ping_redis(redis_url)

    workers: list = []
    if not args.no_worker:
        n = max(1, args.workers)
        qn = _queue_name()
        env = os.environ.copy()
        env["PYTHONPATH"] = ROOT
        for _ in range(n):
            p = subprocess_popen_worker(redis_url, qn, env)
            workers.append(p)
        print(f"Started {n} RQ worker(s) on queue {qn!r} ({redis_url})")

        def _cleanup(*_a):
            for p in workers:
                try:
                    if p.poll() is None:
                        p.terminate()
                except Exception:
                    pass
            deadline = time.time() + 12
            for p in workers:
                while time.time() < deadline and p.poll() is None:
                    time.sleep(0.1)
            for p in workers:
                try:
                    if p.poll() is None:
                        p.kill()
                except Exception:
                    pass

        atexit.register(_cleanup)
        if hasattr(signal, "SIGINT"):
            signal.signal(signal.SIGINT, lambda s, f: (_cleanup(), sys.exit(0)))
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, lambda s, f: (_cleanup(), sys.exit(0)))

    from webapp import create_app

    app = create_app()
    print(f"Flask http://{args.host}:{args.port}/  (Ctrl+C stops app" + (" + workers)" if workers else ")"))
    app.run(host=args.host, port=args.port, debug=True, use_reloader=False)


def subprocess_popen_worker(redis_url: str, queue_name: str, env: dict):
    import subprocess

    # Windows: RQ's default UnixSignalDeathPenalty uses SIGALRM (not available). Use our
    # SimpleWorker + TimerDeathPenalty entrypoint instead of `rq worker`.
    if sys.platform == "win32":
        env = dict(env)
        env["REDIS_URL"] = redis_url
        env["MANUAL_SCRAPE_RQ_QUEUE"] = queue_name
        cmd = [sys.executable, "-m", "scrapers.manual_rq_worker"]
    else:
        # RQ 2.x does not import on Windows; use rq<2 (requirements.txt).
        cmd = [
            sys.executable,
            "-m",
            "rq.cli",
            "worker",
            "-u",
            redis_url,
            queue_name,
        ]

    return subprocess.Popen(
        cmd,
        cwd=ROOT,
        env=env,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )


if __name__ == "__main__":
    main()
