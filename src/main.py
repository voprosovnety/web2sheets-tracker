from __future__ import annotations
import argparse
import time
from functools import partial
from urllib.parse import urlparse

from .log import setup_logging, get_logger
from .fetch import http_get
from .parse.generic import extract_title
from .parse.amazon import parse_product as parse_amazon
from .parse.books_toscrape import parse_product as parse_books
from .parse.ebay import parse_product as parse_ebay
from . import sheets
from .sheets import append_log
from .diff import diff_product
from .alerts import send_telegram_message
from . import scheduler as schedmod

log = get_logger("main")


def cmd_run_once(url: str, write_to_sheet: bool, notify_telegram: bool, notify_always: bool = False) -> int:
    """Fetch the URL once, parse key fields, optionally write to Google Sheets and notify."""
    resp = http_get(url)
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
    changed, summary = diff_product(prev, data)
    log.info(f"Diff: {summary}")

    if write_to_sheet:
        sheets.write_product_row(data)

    if notify_telegram and (changed or notify_always):
        title = (data.get("title") or "<no title>")
        prefix = "Price/stock change for:" if changed else "Status snapshot for:"
        msg = f"{prefix} {title}\n{summary}\n{url}"
        send_telegram_message(msg)

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
    for i, url in enumerate(urls, 1):
        log.info("[%d/%d] %s", i, len(urls), url)
        try:
            cmd_run_once(url, write_to_sheet, notify_telegram)
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
        time.sleep(max(0.0, sleep_seconds))
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

    return p


def main() -> int:
    setup_logging()
    args = build_parser().parse_args()

    if args.command == "run_once":
        return cmd_run_once(args.url, args.write_to_sheet, args.notify_telegram, getattr(args, "notify_always", False))

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

    else:
        raise SystemExit(2)


if __name__ == "__main__":
    raise SystemExit(main())
