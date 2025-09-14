from __future__ import annotations
import typing as t
import time
import requests

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


def http_get(url: str, headers: t.Optional[Headers] = None) -> requests.Response:
    """
    Perform a GET with retries and exponential backoff.

    Returns a `requests.Response` on success. Treats 5xx as transient errors.
    Raises the last exception if all attempts fail.
    """
    h: Headers = dict(_DEFAULT_HEADERS)
    if headers:
        h.update(headers)

    last_err: Exception | None = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            log.info(f"GET {url} (attempt {attempt}/{RETRY_COUNT})")
            resp = requests.get(url, headers=h, timeout=REQUEST_TIMEOUT)
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
            if 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"Server error {resp.status_code}")
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
