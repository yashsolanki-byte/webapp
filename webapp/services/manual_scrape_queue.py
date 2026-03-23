"""Redis + RQ queue for manual scrape jobs (web app enqueues; workers execute)."""
from __future__ import annotations

import os
import uuid
from typing import Any, Dict, Optional, Tuple

MANUAL_SCRAPE_QUEUE_NAME = os.getenv("MANUAL_SCRAPE_RQ_QUEUE", "manual_scrape")
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
ACTIVE_JOBS_SET_KEY = "manual_scrape:active_jobs"


def redis_connection_optional():
    """Return Redis client or None if redis/rq unavailable."""
    try:
        from redis import Redis

        return Redis.from_url(REDIS_URL, decode_responses=False)
    except Exception:
        return None


def manual_scrape_queue_available() -> bool:
    conn = redis_connection_optional()
    if conn is None:
        return False
    try:
        conn.ping()
        return True
    except Exception:
        return False


def get_manual_queue():
    from redis import Redis
    from rq import Queue

    conn = Redis.from_url(REDIS_URL, decode_responses=False)
    timeout = int(os.getenv("MANUAL_SCRAPE_RQ_TIMEOUT_SEC", "1860"))
    return Queue(MANUAL_SCRAPE_QUEUE_NAME, connection=conn, default_timeout=timeout)


def register_active_job(redis_conn, job_id: str) -> None:
    try:
        redis_conn.sadd(ACTIVE_JOBS_SET_KEY, job_id)
    except Exception:
        pass


def unregister_active_job(redis_conn, job_id: str) -> None:
    try:
        redis_conn.srem(ACTIVE_JOBS_SET_KEY, job_id)
    except Exception:
        pass


def cancel_all_active_manual_jobs() -> int:
    """Set cancel flag on all jobs in active set. Returns count attempted."""
    from rq.job import Job

    conn = redis_connection_optional()
    if not conn:
        return 0
    n = 0
    try:
        members = conn.smembers(ACTIVE_JOBS_SET_KEY)
        for raw in members or []:
            jid = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
            try:
                job = Job.fetch(jid, connection=conn)
                meta = dict(job.meta or {})
                meta["cancel"] = True
                meta["progress"] = (meta.get("progress") or "") + " | Stop-all requested"
                job.meta = meta
                job.save_meta()
                n += 1
            except Exception:
                continue
    except Exception:
        pass
    return n


def enqueue_manual_scrape(job_id: str, params: Dict[str, Any]) -> Tuple[str, Any]:
    """
    Enqueue RQ job. Returns (job_id, rq_job).
    Caller must create job workspace dirs before enqueue.
    """
    q = get_manual_queue()
    rq_job = q.enqueue(
        "scrapers.manual_scrape_worker.run_manual_scrape_worker",
        job_id,
        params,
        job_id=job_id,
        job_timeout=int(os.getenv("MANUAL_SCRAPE_JOB_TIMEOUT_SEC", "1860")),
    )
    meta = {
        "status": "queued",
        "progress": "Queued…",
        "error": None,
        "error_code": None,
        "output": None,
        "output_csv": None,
        "output_screenshot": None,
        "logs_file": None,
        "cancel": False,
        "institute": (params.get("institute") or "")[:200],
    }
    rq_job.meta = meta
    rq_job.save_meta()
    return job_id, rq_job


def fetch_manual_job(job_id: str):
    """Return RQ Job or None."""
    from rq.job import Job
    from redis import Redis

    try:
        conn = Redis.from_url(REDIS_URL, decode_responses=False)
        return Job.fetch(job_id, connection=conn)
    except Exception:
        return None


def new_job_id() -> str:
    return str(uuid.uuid4())
