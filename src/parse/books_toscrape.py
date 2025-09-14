from __future__ import annotations
from bs4 import BeautifulSoup
import typing as t


def parse_product(html: str) -> t.Dict[str, t.Optional[str]]:
    """Parse a Books to Scrape product page (stable training site).

    Returns a dict with keys: title, price, availability, asin, sku.
    ASIN/SKU are not present on this site, so they remain None.
    """
    soup = BeautifulSoup(html, "lxml")

    title_el = soup.select_one(".product_main h1")
    title = title_el.get_text(strip=True) if title_el else None

    price_el = soup.select_one(".product_main .price_color")
    price = price_el.get_text(strip=True) if price_el else None

    avail_el = soup.select_one(".product_main .availability")
    availability = avail_el.get_text(strip=True) if avail_el else None

    return {
        "title": title,
        "price": price,
        "availability": availability,
        "asin": None,
        "sku": None,
    }