from __future__ import annotations
from bs4 import BeautifulSoup


def extract_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None


def extract_text_by_selector(html: str, selector: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    el = soup.select_one(selector)
    if el:
        return el.get_text(strip=True)
    return None
