from __future__ import annotations

import html
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from .config import BASE_URL

PAGE_PATH_RE = re.compile(r"page[-_/]?(\d+)", re.IGNORECASE)
META_REFRESH_RE = re.compile(
    r'http-equiv=["\']refresh["\'][^>]*content=["\'][^;]+;\s*([^"\']+)["\']',
    re.IGNORECASE,
)


def collapse_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def unique_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        cleaned = collapse_whitespace(item)
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(cleaned)
    return ordered


def split_multivalue(value: str | None) -> list[str]:
    if not value:
        return []
    normalized = collapse_whitespace(value).replace(";", ",")
    parts = [part.strip(" ,") for part in normalized.split(",")]
    return unique_preserve_order(parts)


def normalize_url(url: str, base_url: str = BASE_URL) -> str:
    absolute = urljoin(base_url, html.unescape(url))
    split_url = urlsplit(absolute)
    query_items = sorted(parse_qsl(split_url.query, keep_blank_values=True))
    normalized_query = urlencode(query_items, doseq=True)
    return urlunsplit(
        (
            split_url.scheme or "https",
            split_url.netloc,
            split_url.path,
            normalized_query,
            "",
        )
    )


def is_company_url(url: str) -> bool:
    split_url = urlsplit(url)
    if "/company/" not in split_url.path:
        return False
    query = dict(parse_qsl(split_url.query))
    return "Id" in query


def is_rubric_url(url: str) -> bool:
    split_url = urlsplit(url)
    if "/rubrics/" not in split_url.path:
        return False
    query = dict(parse_qsl(split_url.query))
    return "Id" in query


def extract_query_value(url: str, key: str) -> str:
    return dict(parse_qsl(urlsplit(url).query)).get(key, "")


def extract_company_id(url: str) -> str:
    return extract_query_value(url, "Id")


def extract_page_number(url: str) -> int:
    page_value = extract_query_value(url, "Page")
    if page_value.isdigit():
        return int(page_value)
    match = PAGE_PATH_RE.search(url)
    if match:
        return int(match.group(1))
    return 1


def coerce_website(value: str) -> str:
    cleaned = collapse_whitespace(value)
    if not cleaned:
        return ""
    if cleaned.startswith(("http://", "https://")):
        return cleaned
    if "." in cleaned and " " not in cleaned:
        return f"https://{cleaned.lstrip('/')}"
    return cleaned


def parse_meta_refresh_target(html_text: str) -> str:
    match = META_REFRESH_RE.search(html_text)
    if not match:
        return ""
    target = collapse_whitespace(match.group(1))
    return target.rstrip(";")


def timestamp_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def timestamp_iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)
