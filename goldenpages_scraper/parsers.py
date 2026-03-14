from __future__ import annotations

import json
from typing import Callable

from bs4 import BeautifulSoup

from .config import BASE_URL
from .models import CompanyRecord
from .utils import (
    coerce_website,
    collapse_whitespace,
    extract_company_id,
    extract_query_value,
    is_company_url,
    is_rubric_url,
    normalize_url,
    split_multivalue,
    timestamp_iso_now,
    unique_preserve_order,
)

WebsiteResolver = Callable[[str], str]


def extract_rubric_urls(html_text: str, base_url: str = BASE_URL) -> list[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    rubric_urls: list[str] = []
    for anchor in soup.select('a[href*="/rubrics/?Id="]'):
        href = anchor.get("href")
        if not href:
            continue
        url = normalize_url(href, base_url)
        if is_rubric_url(url):
            rubric_urls.append(url)
    return unique_preserve_order(rubric_urls)


def extract_company_urls(html_text: str, base_url: str = BASE_URL) -> list[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    company_urls: list[str] = []
    for anchor in soup.select('a[href*="/company/?Id="]'):
        href = anchor.get("href")
        if not href:
            continue
        url = normalize_url(href, base_url)
        if is_company_url(url):
            company_urls.append(url)
    return unique_preserve_order(company_urls)


def extract_pagination_urls(
    html_text: str,
    current_url: str,
    base_url: str = BASE_URL,
) -> list[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    pagination_urls: list[str] = []
    current_rubric_id = extract_query_value(current_url, "Id")

    for anchor in soup.select('a[href*="Page="], a[href*="page-"]'):
        href = anchor.get("href")
        if not href:
            continue
        url = normalize_url(href, base_url)
        if current_rubric_id and extract_query_value(url, "Id") != current_rubric_id:
            continue
        if not is_rubric_url(url):
            continue
        pagination_urls.append(url)

    pagination_urls.append(normalize_url(current_url, base_url))
    return sorted(unique_preserve_order(pagination_urls), key=_page_sort_key)


def extract_company_record(
    html_text: str,
    company_url: str,
    source_listing_url: str = "",
    website_resolver: WebsiteResolver | None = None,
) -> CompanyRecord:
    soup = BeautifulSoup(html_text, "html.parser")
    structured_data = _extract_structured_data(soup)
    local_business = next(
        (item for item in structured_data if item.get("@type") == "LocalBusiness"),
        {},
    )
    faq_page = next(
        (item for item in structured_data if item.get("@type") == "FAQPage"),
        {},
    )

    company_name = collapse_whitespace(str(local_business.get("name", "")))
    if not company_name:
        heading = soup.select_one("h1")
        company_name = collapse_whitespace(heading.get_text(" ", strip=True) if heading else "")

    return CompanyRecord(
        company_id=extract_company_id(company_url),
        company_name=company_name,
        phones=split_multivalue(str(local_business.get("telephone", ""))),
        address=_extract_address(local_business, faq_page),
        landmark=_extract_landmarks(soup, faq_page),
        website=_extract_website(soup, website_resolver),
        emails=split_multivalue(str(local_business.get("email", ""))),
        activity_types=_extract_activity_types(soup),
        rating_value=_extract_rating_value(local_business),
        rating_count=_extract_rating_count(local_business),
        source_url=company_url,
        source_listing_url=source_listing_url,
        scraped_at=timestamp_iso_now(),
    )


def _extract_structured_data(soup: BeautifulSoup) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.get_text(strip=True)
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            payloads.append(parsed)
        elif isinstance(parsed, list):
            payloads.extend(item for item in parsed if isinstance(item, dict))
    return payloads


def _extract_address(
    local_business: dict[str, object],
    faq_page: dict[str, object],
) -> str:
    address = local_business.get("address", {})
    if isinstance(address, dict):
        street_address = collapse_whitespace(str(address.get("streetAddress", "")))
        if street_address:
            return street_address

    for entity in faq_page.get("mainEntity", []):
        if not isinstance(entity, dict):
            continue
        question = collapse_whitespace(str(entity.get("name", ""))).casefold()
        if "manzil" not in question:
            continue
        answer_text = _faq_answer_text(entity)
        if ":" in answer_text:
            return collapse_whitespace(answer_text.split(":", 1)[1])
        return answer_text
    return ""


def _extract_landmarks(soup: BeautifulSoup, faq_page: dict[str, object]) -> str:
    visible_landmarks = [
        collapse_whitespace(item.get_text(" ", strip=True)).strip(" ,")
        for item in soup.select("ul.gp_landmark li")
    ]
    if visible_landmarks:
        return " | ".join(unique_preserve_order(visible_landmarks))

    for entity in faq_page.get("mainEntity", []):
        if not isinstance(entity, dict):
            continue
        question = collapse_whitespace(str(entity.get("name", ""))).casefold()
        if "mo'ljal" not in question:
            continue
        answer_text = _faq_answer_text(entity)
        if ":" in answer_text:
            answer_text = answer_text.split(":", 1)[1]
        return " | ".join(split_multivalue(answer_text))
    return ""


def _extract_website(
    soup: BeautifulSoup,
    website_resolver: WebsiteResolver | None = None,
) -> str:
    for anchor in soup.select('a[href*="/go/?u="]'):
        href = anchor.get("href", "")
        text = collapse_whitespace(anchor.get_text(" ", strip=True))
        title = collapse_whitespace(anchor.get("title", ""))

        if text and "." in text and " " not in text:
            return coerce_website(text)

        if "Sayt" in title and website_resolver is not None:
            resolved = coerce_website(website_resolver(href))
            if resolved:
                return resolved

    page_text = collapse_whitespace(soup.get_text(" ", strip=True))
    for token in page_text.split():
        if "." not in token:
            continue
        candidate = token.strip(" ,;()[]{}")
        if candidate.startswith(("www.", "http://", "https://")) and "goldenpages.uz" not in candidate:
            return coerce_website(candidate)
    return ""


def _extract_activity_types(soup: BeautifulSoup) -> list[str]:
    for container in soup.select("div.gp_tabContent"):
        heading = collapse_whitespace(container.get_text(" ", strip=True))
        if not heading.startswith("Faoliyat turlari") and "Faoliyat turlari -" not in heading[:80]:
            continue
        links = [
            collapse_whitespace(anchor.get_text(" ", strip=True))
            for anchor in container.select('a[href*="/rubrics/?Id="]')
        ]
        return unique_preserve_order(links)

    links = [
        collapse_whitespace(anchor.get_text(" ", strip=True))
        for anchor in soup.select('div.gp_tabContent a[href*="/rubrics/?Id="]')
    ]
    return unique_preserve_order(links)


def _extract_rating_value(local_business: dict[str, object]) -> float:
    aggregate_rating = local_business.get("aggregateRating", {})
    if not isinstance(aggregate_rating, dict):
        return 0.0
    try:
        return float(aggregate_rating.get("ratingValue", 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _extract_rating_count(local_business: dict[str, object]) -> int:
    aggregate_rating = local_business.get("aggregateRating", {})
    if not isinstance(aggregate_rating, dict):
        return 0
    try:
        return int(float(aggregate_rating.get("ratingCount", 0) or 0))
    except (TypeError, ValueError):
        return 0


def _faq_answer_text(entity: dict[str, object]) -> str:
    accepted_answer = entity.get("acceptedAnswer", {})
    if not isinstance(accepted_answer, dict):
        return ""
    return collapse_whitespace(str(accepted_answer.get("text", "")))


def _page_sort_key(url: str) -> tuple[int, str]:
    page_value = extract_query_value(url, "Page")
    page_number = int(page_value) if page_value.isdigit() else 1
    return page_number, url
