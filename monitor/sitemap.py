from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import List, Tuple

import requests


def parse_news_sitemap(xml_text: str, allow_lastmod_fallback: bool = False) -> List[Tuple[str, str]]:
    """Returns list of (url, publication_datetime_raw)."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    def local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[1] if "}" in tag else tag

    def child_text(node: ET.Element, key: str) -> str:
        for child in list(node):
            if local_name(child.tag) == key and child.text:
                return child.text.strip()
        return ""

    def publication_date(node: ET.Element) -> str:
        for child in list(node):
            if local_name(child.tag) != "news":
                continue
            for grandchild in list(child):
                if local_name(grandchild.tag) == "publication_date" and grandchild.text:
                    return grandchild.text.strip()
        return ""

    entries: List[Tuple[str, str]] = []
    for url_node in list(root):
        if local_name(url_node.tag) != "url":
            continue
        loc = child_text(url_node, "loc")
        published = publication_date(url_node)
        if not published and allow_lastmod_fallback:
            published = child_text(url_node, "lastmod")
        if loc and published:
            entries.append((loc, published))
    return entries


def fetch_sitemap_urls(
    session: requests.Session,
    sitemap_url: str,
    allow_lastmod_fallback: bool = False,
    timeout_seconds: int = 30,
) -> List[Tuple[str, str]]:
    response = session.get(sitemap_url, timeout=timeout_seconds)
    response.raise_for_status()
    return parse_news_sitemap(response.text, allow_lastmod_fallback=allow_lastmod_fallback)
