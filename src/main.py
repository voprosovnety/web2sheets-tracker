from __future__ import annotations
import argparse
import time
from functools import partial
from urllib.parse import urlparse
import os

from .log import setup_logging, get_logger
from .fetch import http_get
from .parse.generic import extract_title
from .parse.amazon import parse_product as parse_amazon
from .parse.books_toscrape import parse_product as parse_books
from .parse.ebay import parse_product as parse_ebay
from . import sheets
from .sheets import append_log
from .diff import diff_product
from .alerts import send_telegram_message, send_email_alert
from . import scheduler as schedmod

log = get_logger("main")


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y")


def _time_to_cron(time_str: str) -> str:
    """Convert 'HH:MM' (24h) into a cron expression 'MM HH * * *'. Fallback to 09:00 if parse fails."""
    try:
        hh, mm = time_str.strip().split(":", 1)
        h = max(0, min(23, int(hh)))
        m = max(0, min(59, int(mm)))
    except Exception:
        h, m = 9, 0
    return f"{m} {h} * * *"


def cmd_run_once(url: str, write_to_sheet: bool, notify_telegram: bool, notify_always: bool = False,
                 price_delta_pct: float | None = None, alert_on_availability: bool | None = None,
                 notify_email: bool = False, user_agent_override: str | None = None,
                 write_on_change_only: bool = False) -> int:
    """Fetch the URL once, parse key fields, optionally write to Google Sheets and notify."""
    resp = http_get(url, user_agent_override=user_agent_override)
    html = resp.text
    status = resp.status_code
    log.info(f"Status: {status}")

    host = (urlparse(url).hostname or "").lower()

    if "books.toscrape.com" in host:
        data = parse_books(html)
        log.info("Parsed (BooksToScrape):")
    elif "amazon." in host:
        data = parse_amazon(html)
        # Fallback title via generic parser
        data["title"] = data.get("title") or (extract_title(html) or "<no title>")
        log.info("Parsed (Amazon):")
    elif "ebay." in host:
        data = parse_ebay(html)
        data["title"] = data.get("title") or (extract_title(html) or "<no title>")
        log.info("Parsed (eBay):")
    else:
        # Fallback: only title
        data = {
            "title": extract_title(html) or "<no title>",
            "price": None,
            "availability": None,
            "asin": None,
            "sku": None,
        }
        log.info("Parsed (Generic):")

    for k, v in data.items():
        log.info(f"  {k}: {v}")

    # Add source_url for traceability before writing
    data["source_url"] = url

    # Compare with previous snapshot from the sheet (if any)
    prev = sheets.get_last_row_by_url(url)
    changed, summary = diff_product(prev, data, price_delta_override=price_delta_pct,
                                    alert_avail_override=alert_on_availability)
    log.info(f"Diff: {summary}")

    if write_to_sheet:
        write_on_change_only_env = os.getenv("WRITE_ON_CHANGE_ONLY", "").strip().lower() in ("1", "true", "yes", "y")
        write_on_change_only_final = write_on_change_only or write_on_change_only_env
        if not (write_on_change_only_final and not changed):
            sheets.write_product_row(data)
        else:
            log.info("Skipped writing row (WRITE_ON_CHANGE_ONLY enabled and no change detected)")

    if notify_telegram and (changed or notify_always):
        title = (data.get("title") or "<no title>")
        prefix = "Price/stock change for:" if changed else "Status snapshot for:"
        msg = f"{prefix} {title}\n{summary}\n{url}"
        send_telegram_message(msg)

    if notify_email or os.getenv("NOTIFY_EMAIL", "").strip().lower() in ("1", "true", "yes", "y"):
        subject = f"[Tracker] {'Change' if changed else 'Snapshot'}: {data.get('title') or '<no title>'}"
        body = f"{summary}\n{url}"
        send_email_alert(subject, body)

    try:
        append_log(
            url=url,
            status=str(status),
            title=data.get("title") or "<no title>",
            summary=summary,
            wrote=bool(write_to_sheet),
            alerted=bool(notify_telegram and (changed or notify_always)),
        )
    except Exception as e:
        log.warning("append_log failed: %r", e)

    return 0


# Helper for scheduler jobs

def _job_run_once(url: str, write_to_sheet: bool, notify_telegram: bool) -> None:
    """Wrapper to run a single cycle from the scheduler context."""
    try:
        cmd_run_once(url, write_to_sheet, notify_telegram)
    except Exception as e:
        log.warning("Scheduled job failed: %r", e)
        try:
            append_log(
                url=url,
                status="error",
                title="(scheduler job)",
                summary="Scheduled job failed",
                wrote=False,
                alerted=False,
                error=str(e),
            )
        except Exception:
            pass


# Run over a list of URLs from the Inputs sheet

def cmd_run_list(write_to_sheet: bool, notify_telegram: bool, sleep_seconds: float) -> int:
    from .sheets import get_input_urls

    urls = get_input_urls()
    if not urls:
        log.info("No URLs found in Inputs sheet. Add rows with a 'url' column.")
        return 0

    log.info("Processing %d URL(s) from Inputs sheet...", len(urls))
    for i, cfg in enumerate(urls, 1):
        url = cfg["url"]
        if not cfg.get("enabled", True):
            log.info("[%d/%d] %s (disabled)", i, len(urls), url)
            continue
        log.info("[%d/%d] %s", i, len(urls), url)
        try:
            cmd_run_once(
                url,
                write_to_sheet,
                notify_telegram,
                price_delta_pct=cfg.get("price_delta_pct"),
                alert_on_availability=cfg.get("alert_on_availability"),
                user_agent_override=cfg.get("user_agent"),
            )
        except Exception as e:
            log.warning("Run failed for %s: %r", url, e)
            try:
                append_log(
                    url=url,
                    status="error",
                    title="(run_list)",
                    summary="Run failed",
                    wrote=bool(write_to_sheet),
                    alerted=bool(notify_telegram),
                    error=str(e),
                )
            except Exception:
                pass
        effective_sleep = cfg.get("delay_seconds") or sleep_seconds
        time.sleep(max(0.0, effective_sleep))
    return 0


def cmd_digest(notify_telegram: bool, notify_email: bool, hours: int = 24) -> int:
    """Generate a clean digest of meaningful changes from Logs for the past N hours."""
    from datetime import datetime, timedelta
    from urllib.parse import urlsplit, urlunsplit
    from .sheets import _get_client

    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    log_sheet = os.getenv("LOG_SHEET_NAME", "Logs")
    if not sheet_id:
        log.warning("GOOGLE_SHEET_ID not set; cannot run digest")
        return 1

    client = _get_client()
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(log_sheet)
    except Exception as e:
        log.warning("Logs worksheet '%s' not found: %r", log_sheet, e)
        return 1

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log.info("No logs found to build digest.")
        return 0

    header = [h.strip().lower() for h in values[0]]
    try:
        ts_idx = header.index("timestamp")
        sum_idx = header.index("summary")
        url_idx = header.index("url")
        title_idx = header.index("title")
    except ValueError:
        log.warning("Logs sheet header is missing required columns")
        return 1

    cutoff = datetime.utcnow() - timedelta(hours=hours)

    def _normalize_url(u: str) -> str:
        if not u:
            return u
        p = urlsplit(u)
        # strip query & fragment to merge tracking variants
        return urlunsplit((p.scheme, p.netloc, p.path, "", ""))

    # Keep the most recent meaningful entry per normalized URL
    by_url = {}  # norm_url -> (ts, title, summary, shown_url)
    for row in values[1:]:
        if ts_idx >= len(row) or sum_idx >= len(row) or url_idx >= len(row):
            continue
        ts_str = row[ts_idx]
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", ""))
        except Exception:
            continue
        if ts < cutoff:
            continue

        summary = (row[sum_idx] or "").strip()
        if not summary:
            continue
        low = summary.lower()
        # Filter out noise
        if low.startswith("no changes") or low.startswith("initial snapshot"):
            continue

        url = (row[url_idx] or "").strip()
        title = ((row[title_idx] if title_idx < len(row) else "") or "").strip() or "<no title>"

        norm = _normalize_url(url)
        prev = by_url.get(norm)
        if (not prev) or (ts > prev[0]):
            by_url[norm] = (ts, title, summary, norm)

    if not by_url:
        log.info("No meaningful changes in last %d hours", hours)
        return 0

    # Sort by time (desc) and render
    items = sorted(by_url.values(), key=lambda x: x[0], reverse=True)
    lines = [f"• {title} — {summary}\n  {url}" for (_, title, summary, url) in items]
    body = "Digest of changes in last %d hours:\n%s" % (hours, "\n".join(lines))

    if notify_telegram:
        send_telegram_message(body)
    if notify_email or os.getenv("NOTIFY_EMAIL", "").strip().lower() in ("1", "true", "yes", "y"):
        send_email_alert("[Tracker] Daily Digest", body)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="web2sheets-tracker", description="Web → Sheets tracker")
    sub = p.add_subparsers(dest="command", required=True)

    # run_once
    p_run = sub.add_parser("run_once", help="Run a single fetch-parse cycle")
    p_run.add_argument(
        "--url",
        required=False,
        default="https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html",
        help="Target URL (default: training product page)",
    )
    p_run.add_argument(
        "--write-to-sheet",
        action="store_true",
        help="If set, append the parsed row to the configured Google Sheet.",
    )
    p_run.add_argument(
        "--notify-telegram",
        action="store_true",
        help="If set, send a Telegram alert when key fields change.",
    )
    p_run.add_argument(
        "--notify-always",
        action="store_true",
        help="If set, send a Telegram message even when nothing changed.",
    )
    p_run.add_argument(
        "--notify-email",
        action="store_true",
        help="If set, send an Email alert when key fields change.",
    )
    p_run.add_argument(
        "--write-on-change-only",
        action="store_true",
        help="If set, only write to sheet when a change is detected.",
    )

    # run_list
    p_list = sub.add_parser("run_list", help="Run over URLs listed in the Inputs sheet")
    p_list.add_argument(
        "--write-to-sheet",
        action="store_true",
        help="Append parsed rows to the configured Google Sheet.",
    )
    p_list.add_argument(
        "--notify-telegram",
        action="store_true",
        help="Send Telegram alert on change for each URL.",
    )
    p_list.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Delay between URLs (default: 2.0)",
    )

    # schedule
    p_sched = sub.add_parser("schedule", help="Run scheduler")
    p_sched.add_argument(
        "--url",
        required=False,
        default="https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html",
        help="Target URL for scheduled runs",
    )
    p_sched.add_argument(
        "--every-minutes",
        type=int,
        default=60,
        help="Interval in minutes (mutually exclusive with --cron)",
    )
    p_sched.add_argument(
        "--cron",
        type=str,
        default=None,
        help="Cron expression (5 fields), e.g. '0 9 * * *' for 09:00 daily",
    )
    p_sched.add_argument(
        "--write-to-sheet",
        action="store_true",
        help="Append parsed row on each run.",
    )
    p_sched.add_argument(
        "--notify-telegram",
        action="store_true",
        help="Send Telegram alert on change.",
    )

    # digest
    p_digest = sub.add_parser("digest", help="Send a digest of recent changes")
    p_digest.add_argument("--notify-telegram", action="store_true", help="Send digest via Telegram")
    p_digest.add_argument("--notify-email", action="store_true", help="Send digest via Email")
    p_digest.add_argument(
        "--hours",
        type=int,
        default=int(os.getenv("DIGEST_HOURS_DEFAULT", os.getenv("DIGEST_HOURS", "24"))),
        help="Look back this many hours (env DIGEST_HOURS_DEFAULT or DIGEST_HOURS; default: 24)",
    )

    # schedule_daily_digest
    p_sdd = sub.add_parser(
        "schedule_daily_digest",
        help="Schedule a daily digest at a fixed local time (uses cron under the hood)",
    )
    p_sdd.add_argument(
        "--time",
        default=os.getenv("DAILY_DIGEST_TIME", "09:00"),
        help="Local time HH:MM (default from DAILY_DIGEST_TIME or 09:00)",
    )
    p_sdd.add_argument(
        "--hours",
        type=int,
        default=int(os.getenv("DIGEST_HOURS_DEFAULT", os.getenv("DIGEST_HOURS", "24"))),
        help="Lookback window in hours for the digest (env DIGEST_HOURS_DEFAULT or DIGEST_HOURS)",
    )
    p_sdd.add_argument(
        "--notify-telegram",
        action="store_true",
        default=_env_bool("DIGEST_NOTIFY_TELEGRAM", True),
        help="Send digest via Telegram (default from DIGEST_NOTIFY_TELEGRAM, default true)",
    )
    p_sdd.add_argument(
        "--notify-email",
        action="store_true",
        default=_env_bool("DIGEST_NOTIFY_EMAIL", _env_bool("NOTIFY_EMAIL", False)),
        help="Send digest via Email (default from DIGEST_NOTIFY_EMAIL or NOTIFY_EMAIL)",
    )

    # export_csv
    p_export = sub.add_parser(
        "export_csv",
        help="Export rows from a given Google Sheet tab to a local CSV file",
    )
    p_export.add_argument(
        "--sheet",
        required=False,
        default=os.getenv("EXPORT_DEFAULT_SHEET", "Snapshots"),
        help="Sheet/tab name to export (default from EXPORT_DEFAULT_SHEET or 'Snapshots')",
    )
    p_export.add_argument(
        "--out",
        required=True,
        help="Output CSV filename",
    )
    p_export.add_argument(
        "--since-hours",
        type=int,
        default=None,
        help="Only include rows with timestamp newer than this many hours ago (optional)",
    )

    return p


def main() -> int:
    setup_logging()
    args = build_parser().parse_args()

    if args.command == "run_once":
        return cmd_run_once(
            args.url,
            args.write_to_sheet,
            args.notify_telegram,
            getattr(args, "notify_always", False),
            price_delta_pct=None,
            alert_on_availability=None,
            notify_email=getattr(args, "notify_email", False),
            write_on_change_only=getattr(args, "write_on_change_only", False),
        )

    elif args.command == "run_list":
        return cmd_run_list(args.write_to_sheet, args.notify_telegram, args.sleep_seconds)

    elif args.command == "schedule":
        job = partial(_job_run_once, args.url, args.write_to_sheet, args.notify_telegram)

        def add_jobs(s):
            if args.cron:
                schedmod.add_cron_job(s, job, args.cron, job_id="web2sheets_job")
            else:
                mins = args.every_minutes if args.every_minutes and args.every_minutes > 0 else 60
                schedmod.add_interval_job(s, job, every_minutes=mins, job_id="web2sheets_job")

        schedmod.run_forever(add_jobs)
        return 0

    elif args.command == "digest":
        return cmd_digest(args.notify_telegram, getattr(args, "notify_email", False), getattr(args, "hours", 24))

    elif args.command == "schedule_daily_digest":
        # Build a job that runs the digest with configured flags
        job = lambda: cmd_digest(
            notify_telegram=getattr(args, "notify_telegram", True),
            notify_email=getattr(args, "notify_email", False),
            hours=getattr(args, "hours", 24),
        )

        def add_jobs(s):
            cron = _time_to_cron(getattr(args, "time", os.getenv("DAILY_DIGEST_TIME", "09:00")))
            schedmod.add_cron_job(s, job, cron, job_id="daily_digest")

        schedmod.run_forever(add_jobs)
        return 0

    elif args.command == "export_csv":
        return sheets.export_sheet_to_csv(
            sheet_name=args.sheet,
            out_path=args.out,
            since_hours=getattr(args, "since_hours", None),
        )
    else:
        raise SystemExit(2)


if __name__ == "__main__":
    raise SystemExit(main())
