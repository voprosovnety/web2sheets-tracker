from __future__ import annotations
import os
import requests
from .log import get_logger
from . import config  # ensure .env is loaded via load_dotenv()

log = get_logger("alerts")

TELEGRAM_API_BASE = "https://api.telegram.org"


def send_telegram_message(text: str) -> None:
    """Send a plain text message via Telegram Bot API using env vars.

    Required env vars:
      - TELEGRAM_BOT_TOKEN
      - TELEGRAM_CHAT_ID
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log.warning("Telegram not configured: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID missing")
        return

    url = f"{TELEGRAM_API_BASE}/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log.warning("Telegram send failed: %s %s", r.status_code, r.text[:200])
        else:
            log.info("Telegram message sent")
    except Exception as e:
        log.warning("Telegram send exception: %r", e)
