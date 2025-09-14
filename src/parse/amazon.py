from __future__ import annotations
from bs4 import BeautifulSoup
import typing as t


def _text(el) -> str | None:
    if not el:
        return None
    return el.get_text(strip=True)


def parse_product(html: str) -> t.Dict[str, t.Optional[str]]:
    soup = BeautifulSoup(html, "lxml")

    title = _text(soup.select_one("#productTitle")) or _text(soup.select_one("span#title"))

    price = None
    candidates = [
        "#corePrice_desktop .a-offscreen",
        "#corePrice_feature_div .a-offscreen",
        "#apex_desktop .a-offscreen",
        ".a-price .a-offscreen",
    ]
    for sel in candidates:
        el = soup.select_one(sel)
        if el:
            price = _text(el)
            if price:
                break

    availability = None
    avail_candidates = [
        "#availability .a-color-success",
        "#availability .a-color-state",
        "#availability span",
    ]
    for sel in avail_candidates:
        el = soup.select_one(sel)
        if el:
            availability = _text(el)
            if availability:
                break

    asin = None
    asin_input = soup.select_one('input#ASIN')
    if asin_input and asin_input.has_attr('value'):
        asin = asin_input['value']
    if not asin:
        body = soup.select_one("body")
        if body and body.has_attr("data-asin"):
            asin = body["data-asin"]

    sku = None
    for th_text in ["Item model number", "Model Number", "SKU"]:
        th = soup.find("th", string=lambda s: isinstance(s, str) and th_text in s)
        if th:
            td = th.find_next("td")
            sku = _text(td)
            if sku:
                break

    return {
        "title": title,
        "price": price,
        "availability": availability,
        "asin": asin,
        "sku": sku,
    }
