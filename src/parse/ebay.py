from __future__ import annotations
from bs4 import BeautifulSoup
import typing as t


def _text(el) -> str | None:
    return el.get_text(strip=True) if el else None


def parse_product(html: str) -> t.Dict[str, t.Optional[str]]:
    """
    Parse an eBay product page (new/old layouts).
    Returns: title, price, availability, asin(None), sku(None)
    """
    soup = BeautifulSoup(html, "lxml")

    # --- Title (several layouts) ---
    title = None
    # Old layout: #itemTitle with leading "Details about  " text
    t1 = soup.select_one("#itemTitle")
    if t1:
        txt = _text(t1)
        if txt:
            title = txt.replace("Details about  ", "").strip() or None
    # New layout:
    if not title:
        t2 = soup.select_one("h1.x-item-title__mainTitle span.ux-textspans")
        title = _text(t2) or title
    if not title:
        t3 = soup.select_one("h1[itemprop='name'], h1.ux-textspans")
        title = _text(t3) or title

    # --- Price (several layouts) ---
    price = None
    # Old layout:
    for sel in ("#mm-saleDscPrc", "#prcIsum", "span[itemprop='price']"):
        el = soup.select_one(sel)
        if el:
            # itemprop=price often has content attribute
            price = el.get("content") or _text(el)
            if price:
                break
    # New layout:
    if not price:
        el = soup.select_one(".x-price-primary .ux-textspans")
        price = _text(el) or price
    if not price:
        el = soup.select_one("span[itemprop='price']")  # fallback
        price = el.get("content") if el and el.get("content") else price

    # --- Availability (best-effort) ---
    availability = None
    for sel in (
            "#qtySubTxt",  # old
            ".d-quantity__availability",  # old
            ".x-quantity__availability .ux-textspans",  # new
            "[data-testid='x-buybox-availability'] .ux-textspans",
    ):
        availability = _text(soup.select_one(sel))
        if availability:
            break

    # eBay pages don't expose ASIN; SKU/item number sometimes appears but unstable.
    return {
        "title": title,
        "price": price,
        "availability": availability,
        "asin": None,
        "sku": None,
    }
