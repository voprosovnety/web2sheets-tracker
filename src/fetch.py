from __future__ import annotations
import typing as t
import time
import requests
import random
from urllib.parse import urlparse, urlunparse

from .config import USER_AGENT, REQUEST_TIMEOUT, RETRY_COUNT, RETRY_BACKOFF
from .log import get_logger

log = get_logger("fetch")

Headers = t.Dict[str, str]

# Minimal browser-like headers improve compatibility on sites that vary HTML by
# locale or block default clients. USER_AGENT stays configurable via .env.
_DEFAULT_HEADERS: Headers = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Lightweight UA rotation (desktop + mobile) to reduce trivial blocks
_UA_POOL = [
    # Desktop
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Mobile
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]


def http_get(url: str, headers: t.Optional[Headers] = None, user_agent_override: str | None = None, proxy: str | None = None) -> requests.Response:
    """
    Perform a GET with retries and exponential backoff.

    Returns a `requests.Response` on success. Treats 5xx as transient errors.
    Raises the last exception if all attempts fail.
    """
    h: Headers = dict(_DEFAULT_HEADERS)
    if user_agent_override:
        h["User-Agent"] = user_agent_override
    if headers:
        h.update(headers)

    last_err: Exception | None = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            log.info(f"GET {url} (attempt {attempt}/{RETRY_COUNT})")
            # Rotate UA per attempt and add a generic referer
            if not user_agent_override:
                h["User-Agent"] = random.choice(_UA_POOL)
            h.setdefault("Referer", "https://www.google.com/")
            proxies = None
            if proxy:
                proxies = {"http": proxy, "https": proxy}
            resp = requests.get(url, headers=h, timeout=REQUEST_TIMEOUT, proxies=proxies)
            # Encoding correction: some servers send wrong or missing charset
            # (e.g., defaulting to ISO-8859-1) which yields artifacts like "Â£".
            # Prefer apparent_encoding when the declared encoding is missing or
            # a common placeholder.
            enc = (resp.encoding or "").lower()
            if not enc or enc in ("iso-8859-1", "latin-1", "us-ascii"):
                try:
                    apparent = resp.apparent_encoding
                    if apparent:
                        log.debug(f"Adjusting encoding: {enc or 'None'} -> {apparent}")
                        resp.encoding = apparent
                except Exception:
                    pass
            # Treat server errors and common anti-bot statuses as transient
            if resp.status_code in (403, 429):
                raise requests.HTTPError(f"Transient block {resp.status_code}")
            if 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"Server error {resp.status_code}")
            # eBay anti-bot interstitial fallback: try mobile host if detected
            try:
                host = (urlparse(url).hostname or "").lower()
            except Exception:
                host = ""
            if "ebay." in host and resp.status_code == 200 and "Pardon Our Interruption" in resp.text:
                log.info("Detected eBay interstitial; retrying via m.* host")
                u = urlparse(url)
                mobile_host = u.hostname or ""
                if not mobile_host.startswith("m."):
                    mobile_host = "m." + mobile_host
                u2 = u._replace(netloc=mobile_host)
                m_url = urlunparse(u2)
                # Try mobile with a mobile UA
                h_mobile = dict(h)
                if not user_agent_override:
                    h_mobile["User-Agent"] = random.choice(_UA_POOL)
                resp = requests.get(m_url, headers=h_mobile, timeout=REQUEST_TIMEOUT, proxies=proxies)
                # Re-apply encoding fix
                enc2 = (resp.encoding or "").lower()
                if not enc2 or enc2 in ("iso-8859-1", "latin-1", "us-ascii"):
                    try:
                        apparent2 = resp.apparent_encoding
                        if apparent2:
                            log.debug(f"Adjusting encoding (mobile): {enc2 or 'None'} -> {apparent2}")
                            resp.encoding = apparent2
                    except Exception:
                        pass
            return resp
        except Exception as e:
            last_err = e
            if attempt == RETRY_COUNT:
                break
            sleep_s = RETRY_BACKOFF * (2 ** (attempt - 1))
            log.warning(f"Request failed: {e!r}. Retry in {sleep_s:.2f}s")
            time.sleep(sleep_s)

    assert last_err is not None
    raise last_err
