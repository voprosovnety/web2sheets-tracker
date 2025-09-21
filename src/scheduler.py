from __future__ import annotations
import signal
import sys
from datetime import datetime
from typing import Callable, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from tzlocal import get_localzone
from .log import get_logger

log = get_logger("scheduler")


def run_forever(add_jobs: Callable[[BackgroundScheduler], None]) -> None:
    """
    Start a background scheduler, register jobs via `add_jobs`, and block
    the main thread until interrupted (SIGINT/SIGTERM).
    """
    tz = get_localzone()
    sched = BackgroundScheduler(timezone=tz)
    add_jobs(sched)
    sched.start()
    log.info("Scheduler started (timezone: %s). Press Ctrl+C to stop.", tz)

    def _shutdown(signum, frame):
        log.info("Signal %s received, shutting down scheduler...", signum)
        sched.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Simple wait loop
    try:
        while True:
            signal.pause()
    except AttributeError:
        # Windows fallback
        import time
        while True:
            time.sleep(1)



def add_interval_job(
    sched: BackgroundScheduler,
    func: Callable,
    every_minutes: int,
    jitter_seconds: int = 15,
    job_id: Optional[str] = None,
) -> None:
    """
    Add an interval job with small jitter to avoid thundering herd.
    """
    trigger = IntervalTrigger(minutes=every_minutes, jitter=jitter_seconds)
    sched.add_job(func, trigger=trigger, id=job_id, max_instances=1, coalesce=True)
    log.info("Added interval job: every %d min (jitter ~%ds)", every_minutes, jitter_seconds)


# --- New helper: add_daily_at ---

def add_daily_at(
    sched: BackgroundScheduler,
    func: Callable,
    time_str: str = "09:00",
    job_id: Optional[str] = None,
) -> None:
    """Add a daily job at local time HH:MM (24h format).

    Example: add_daily_at(sched, func, "09:00")  # every day at 09:00 local time
    """
    try:
        hh, mm = time_str.split(":", 1)
        hh_i = int(hh)
        mm_i = int(mm)
        if not (0 <= hh_i <= 23 and 0 <= mm_i <= 59):
            raise ValueError
    except Exception:
        raise ValueError("time_str must be 'HH:MM' in 24h format, e.g. '09:00'")

    trigger = CronTrigger(minute=str(mm_i), hour=str(hh_i))
    sched.add_job(func, trigger=trigger, id=job_id, max_instances=1, coalesce=True)
    log.info("Added daily job: %02d:%02d local time", hh_i, mm_i)


def add_cron_job(
    sched: BackgroundScheduler,
    func: Callable,
    cron_expr: str,
    job_id: Optional[str] = None,
) -> None:
    """
    Add a daily/cron job. Examples:
      - '0 * * * *'  → every hour at :00
      - '0 9 * * *'  → every day 09:00
    """
    fields = cron_expr.split()
    if len(fields) != 5:
        raise ValueError("Cron expression must have 5 fields (min hour dom mon dow)")
    minute, hour, day, month, dow = fields
    trigger = CronTrigger(minute=minute, hour=hour, day=day, month=month, day_of_week=dow)
    sched.add_job(func, trigger=trigger, id=job_id, max_instances=1, coalesce=True)
    log.info("Added cron job: %s", cron_expr)