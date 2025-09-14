from __future__ import annotations
from typing import Dict, Optional, Tuple

Product = Dict[str, Optional[str]]


def normalize_price(p: Optional[str]) -> Optional[str]:
    """Return a trimmed price string or None."""
    if p is None:
        return None
    return p.strip()


def diff_product(prev: Product | None, curr: Product) -> Tuple[bool, str]:
    """Compare key fields (price, availability).

    Returns (changed, summary). If `prev` is None (first snapshot),
    we report a short summary but do not treat it as a change.
    """
    fields = ("price", "availability")

    if not prev:
        summary = "Initial snapshot. " + ", ".join(f"{k}={curr.get(k) or ''}" for k in fields)
        return False, summary

    changes = []
    for k in fields:
        a = normalize_price(prev.get(k))
        b = normalize_price(curr.get(k))
        if a != b:
            changes.append(f"{k}: {a or ''} → {b or ''}")

    if not changes:
        return False, "No changes"
    return True, "; ".join(changes)
