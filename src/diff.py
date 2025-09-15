from __future__ import annotations
import os
import re
from typing import Dict, Optional, Tuple

Product = Dict[str, Optional[str]]

TRUTHY = {"1", "true", "yes", "y", "on"}


def normalize_price(p: Optional[str]) -> Optional[str]:
    """Return a trimmed price string or None."""
    if p is None:
        return None
    return p.strip()


def _parse_price_to_float(p: Optional[str]) -> Optional[float]:
    """Best-effort parse of a price string into a float.

    Handles common currency formats, thousands separators, and decimal marks.
    Examples:
      "£1,234.56" -> 1234.56
      "1.234,56 €" -> 1234.56
      "$999"       -> 999.0
    Returns None if parsing fails or input is None/empty.
    """
    if not p:
        return None
    s = p.strip()
    # Keep digits, dots, commas; drop currency symbols and other text
    s = re.sub(r"[^0-9.,]", "", s)
    if not s:
        return None

    # If both comma and dot present, assume comma is thousands and dot is decimal
    if "," in s and "." in s:
        s = s.replace(",", "")
        try:
            return float(s)
        except ValueError:
            return None

    # If only comma present, treat comma as decimal separator
    if "," in s and "." not in s:
        s = s.replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    # Otherwise assume dot is decimal or integer number
    try:
        return float(s)
    except ValueError:
        return None


def diff_product(prev: Product | None, curr: Product) -> Tuple[bool, str]:
    """Compare key fields with thresholds and toggles.

    Checks price change with a configurable percentage threshold (PRICE_DELTA_PCT)
    and optionally availability change (ALERT_ON_AVAILABILITY=true|false).

    Returns (changed, summary). If `prev` is None (first snapshot), we report a
    short summary but do not treat it as a change.
    """
    # Config from environment
    try:
        price_delta_pct = float(os.getenv("PRICE_DELTA_PCT", "0").strip() or "0")
    except ValueError:
        price_delta_pct = 0.0
    alert_on_avail = (os.getenv("ALERT_ON_AVAILABILITY", "true").strip().lower() in TRUTHY)

    if not prev:
        fields = ("price", "availability")
        summary = "Initial snapshot. " + ", ".join(f"{k}={curr.get(k) or ''}" for k in fields)
        return False, summary

    changes = []

    # --- Price diff with threshold ---
    prev_price_raw = normalize_price(prev.get("price"))
    curr_price_raw = normalize_price(curr.get("price"))
    pa = _parse_price_to_float(prev_price_raw)
    pb = _parse_price_to_float(curr_price_raw)

    price_changed = False
    if pa is None and pb is None:
        price_changed = False
    elif pa is None or pb is None:
        # One side missing -> treat as change
        price_changed = True
        changes.append(f"price: {prev_price_raw or ''} → {curr_price_raw or ''}")
    else:
        if pa != pb:
            # Percentage relative to previous (avoid div by zero)
            if pa == 0:
                pct = 100.0
            else:
                pct = abs(pb - pa) / abs(pa) * 100.0
            if pct + 1e-9 >= price_delta_pct:
                price_changed = True
                changes.append(
                    f"price: {prev_price_raw or ''} → {curr_price_raw or ''} (Δ{pct:.2f}%)"
                )
            else:
                # below threshold: do not count as change, but mention in summary note
                pass

    # --- Availability diff (optional) ---
    if alert_on_avail:
        prev_av = (prev.get("availability") or "").strip()
        curr_av = (curr.get("availability") or "").strip()
        if prev_av != curr_av:
            changes.append(f"availability: {prev_av} → {curr_av}")

    if not changes:
        if price_changed is False and pa is not None and pb is not None and pa != pb and price_delta_pct > 0:
            return False, f"No changes (price delta below {price_delta_pct:.2f}%)"
        return False, "No changes"

    return True, "; ".join(changes)
