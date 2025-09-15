from __future__ import annotations
import os
import requests
import smtplib
from email.message import EmailMessage
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


def send_email_alert(subject: str, body: str) -> None:
    """Send a plain text email using SMTP with env vars.

    Required env vars:
      - SMTP_HOST
      - SMTP_PORT
      - SMTP_USER
      - SMTP_PASSWORD
      - ALERT_EMAIL_TO (comma-separated)
    Optional:
      - SMTP_USE_TLS (default true)
    """
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    pwd = os.getenv("SMTP_PASSWORD")
    to_addrs = os.getenv("ALERT_EMAIL_TO")
    use_tls = os.getenv("SMTP_USE_TLS", "true").strip().lower() in ("1","true","yes","y")

    if not (host and port and user and pwd and to_addrs):
        log.warning("Email not configured: missing SMTP_* or ALERT_EMAIL_TO")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_addrs
    msg.set_content(body)

    try:
        if use_tls:
            server = smtplib.SMTP(host, port, timeout=15)
            server.starttls()
        else:
            server = smtplib.SMTP(host, port, timeout=15)
        server.login(user, pwd)
        server.send_message(msg)
        server.quit()
        log.info("Email alert sent to %s", to_addrs)
    except Exception as e:
        log.warning("Email send exception: %r", e)
