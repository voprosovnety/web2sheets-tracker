from __future__ import annotations
import os
from typing import Dict, Optional, List
import gspread
from google.oauth2.service_account import Credentials
from .log import get_logger

log = get_logger("sheets")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _get_client() -> gspread.Client:
    """
    Build a gspread client from a service account JSON path in .env.
    """
    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    if not sa_file or not os.path.exists(sa_file):
        raise FileNotFoundError(
            "GOOGLE_SERVICE_ACCOUNT_FILE is missing or does not exist. "
            "Set it in .env and ensure the file path is correct."
        )
    creds = Credentials.from_service_account_file(sa_file, scopes=SCOPES)
    return gspread.authorize(creds)


def _open_worksheet():
    """
    Open the target worksheet based on GOOGLE_SHEET_ID and GOOGLE_SHEET_WORKSHEET.
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    ws_name = os.getenv("GOOGLE_SHEET_WORKSHEET", "Sheet1")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID is not set in .env")
    client = _get_client()
    sh = client.open_by_key(sheet_id)
    return sh.worksheet(ws_name)


def _ensure_header(ws, header: List[str]) -> None:
    """
    Ensure the first row equals the desired header (create or update if needed).
    """
    try:
        existing = ws.row_values(1)
    except Exception:
        existing = []
    if existing != header:
        # Either empty or mismatched header → overwrite row 1
        ws.update("A1", [header])  # gspread expects a list of rows


def write_product_row(data: Dict[str, Optional[str]]) -> None:
    """
    Append a product row with a stable header. Missing fields become empty strings.
    """
    header = ["title", "price", "availability", "asin", "sku", "source_url"]
    ws = _open_worksheet()
    _ensure_header(ws, header)

    row = [
        data.get("title") or "",
        data.get("price") or "",
        data.get("availability") or "",
        data.get("asin") or "",
        data.get("sku") or "",
        data.get("source_url") or "",
    ]

    ws.append_row(row, value_input_option="USER_ENTERED")
    log.info("Row appended to Google Sheet.")


# --- Diff lookup support ---
def _get_all_rows(ws) -> List[List[str]]:
    """Safely return all values from the worksheet (or an empty list on error)."""
    try:
        return ws.get_all_values()
    except Exception as e:
        log.warning("Failed to fetch sheet values: %r", e)
        return []


def get_last_row_by_url(source_url: str) -> Optional[Dict[str, str]]:
    """Return the last row (as a dict) that matches `source_url`, or None."""
    ws = _open_worksheet()
    rows = _get_all_rows(ws)
    if not rows:
        return None

    header = rows[0]
    idx_by_name = {name: i for i, name in enumerate(header)}
    try:
        url_idx = idx_by_name["source_url"]
    except KeyError:
        # No header yet
        return None

    for row in reversed(rows[1:]):
        if url_idx < len(row) and row[url_idx] == source_url:
            data: Dict[str, str] = {}
            for name, i in idx_by_name.items():
                data[name] = row[i] if i < len(row) else ""
            return data
    return None


# --- Inputs sheet support ---
def get_input_urls() -> List[Dict[str, object]]:
    """Read list of URL configs from Inputs worksheet.

    Expected header (case-insensitive):
      - url                  (required)
      - enabled              (optional, truthy values: 1, true, yes, y)
      - price_delta_pct      (optional, float)
      - alert_on_availability(optional, truthy values)
      - delay_seconds        (optional, float)
      - user_agent           (optional, string)

    Returns list of dicts: {url, enabled, price_delta_pct, alert_on_availability}
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID is not set in .env")

    ws_name = os.getenv("INPUT_SHEET_NAME", "Inputs")
    client = _get_client()
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(ws_name)
    except Exception as e:
        raise RuntimeError(
            f"Inputs worksheet '{ws_name}' not found. Create it with header: url, enabled, price_delta_pct, alert_on_availability"
        ) from e

    values = ws.get_all_values()
    if not values:
        return []

    header = [h.strip().lower() for h in values[0]]
    try:
        url_idx = header.index("url")
    except ValueError as e:
        raise RuntimeError("Inputs sheet must have a 'url' column in the header row") from e

    enabled_idx = header.index("enabled") if "enabled" in header else None
    price_delta_idx = header.index("price_delta_pct") if "price_delta_pct" in header else None
    avail_idx = header.index("alert_on_availability") if "alert_on_availability" in header else None
    delay_idx = header.index("delay_seconds") if "delay_seconds" in header else None
    ua_idx = header.index("user_agent") if "user_agent" in header else None

    configs: List[Dict[str, object]] = []
    truthy = {"1", "true", "yes", "y"}

    for row in values[1:]:
        if url_idx >= len(row):
            continue
        url = (row[url_idx] or "").strip()
        if not url:
            continue

        enabled = True
        if enabled_idx is not None:
            flag = (row[enabled_idx] or "").strip().lower() if enabled_idx < len(row) else ""
            if flag and flag not in truthy:
                enabled = False

        price_delta_pct: float | None = None
        if price_delta_idx is not None and price_delta_idx < len(row):
            try:
                price_delta_pct = float(row[price_delta_idx]) if row[price_delta_idx] else None
            except ValueError:
                price_delta_pct = None

        alert_on_avail: bool | None = None
        if avail_idx is not None and avail_idx < len(row):
            flag = (row[avail_idx] or "").strip().lower()
            if flag:
                alert_on_avail = flag in truthy

        delay_seconds: float | None = None
        if delay_idx is not None and delay_idx < len(row):
            try:
                delay_seconds = float(row[delay_idx]) if row[delay_idx] else None
            except ValueError:
                delay_seconds = None

        user_agent: str | None = None
        if ua_idx is not None and ua_idx < len(row):
            ua = (row[ua_idx] or "").strip()
            user_agent = ua or None

        configs.append(
            {
                "url": url,
                "enabled": enabled,
                "price_delta_pct": price_delta_pct,
                "alert_on_availability": alert_on_avail,
                "delay_seconds": delay_seconds,
                "user_agent": user_agent,
            }
        )

    return configs


# --- Logs sheet support ---
def append_log(
    url: str,
    status: str,
    title: str,
    summary: str,
    wrote: bool,
    alerted: bool,
    error: Optional[str] = None,
) -> None:
    """
    Append a log entry into a Logs worksheet (name from LOG_SHEET_NAME env, default "Logs").
    Header: timestamp | url | status | title | summary | wrote | alerted | error
    """
    import datetime

    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        log.warning("No GOOGLE_SHEET_ID set, skip append_log.")
        return

    ws_name = os.getenv("LOG_SHEET_NAME", "Logs")
    client = _get_client()
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(ws_name)
    except Exception:
        # Create if missing
        ws = sh.add_worksheet(title=ws_name, rows="100", cols="8")

    header = ["timestamp", "url", "status", "title", "summary", "wrote", "alerted", "error"]
    _ensure_header(ws, header)

    ts = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    row = [
        ts,
        url,
        status,
        title,
        summary,
        "yes" if wrote else "no",
        "yes" if alerted else "no",
        error or "",
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    log.info("Log row appended.")
