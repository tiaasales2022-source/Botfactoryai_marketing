from __future__ import annotations

import random
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import requests
from requests import Response
from rich.console import Console
from tqdm import tqdm

from .config import (
    DEFAULT_MAX_DELAY,
    DEFAULT_MIN_DELAY,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENTS,
    HOME_URL,
    RETRYABLE_STATUS_CODES,
)
from .exporters import append_backup_row, build_output_paths, export_final
from .parsers import (
    extract_company_record,
    extract_company_urls,
    extract_pagination_urls,
    extract_rubric_urls,
)
from .state import ScrapeState
from .utils import (
    coerce_website,
    collapse_whitespace,
    extract_company_id,
    extract_page_number,
    is_company_url,
    normalize_url,
    parse_meta_refresh_target,
    timestamp_now,
    unique_preserve_order,
)


@dataclass(slots=True)
class ScraperSettings:
    seed_urls: list[str]
    discover_rubrics_from_home: bool = False
    max_rubrics: int | None = None
    max_pages_per_seed: int | None = None
    max_companies: int | None = None
    min_delay: float = DEFAULT_MIN_DELAY
    max_delay: float = DEFAULT_MAX_DELAY
    retries: int = DEFAULT_RETRIES
    timeout: float = DEFAULT_TIMEOUT
    output_dir: Path = Path("output")
    resume_state: Path | None = None


class RunSummary(NamedTuple):
    discovered_companies: int
    exported_rows: int
    failed_count: int
    csv_path: Path
    xlsx_path: Path
    state_path: Path


class GoldenPagesScraper:
    def __init__(self, settings: ScraperSettings, console: Console | None = None) -> None:
        self.settings = settings
        self.console = console or Console()
        self.session = requests.Session()
        self.user_agents = list(DEFAULT_USER_AGENTS)

        if settings.resume_state:
            self.state_path = settings.resume_state
            self.state = ScrapeState.load(settings.resume_state)
            self.csv_path = Path(self.state.csv_path)
            self.xlsx_path = Path(self.state.xlsx_path)
            self.console.log(f"[bold yellow]Resuming state[/bold yellow] {self.state_path}")
        else:
            run_id = timestamp_now()
            self.csv_path, self.xlsx_path, self.state_path = build_output_paths(
                settings.output_dir,
                run_id,
            )
            self.state = ScrapeState(
                run_id=run_id,
                csv_path=str(self.csv_path),
                xlsx_path=str(self.xlsx_path),
            )
            self.save_state()

    def run(self) -> RunSummary:
        seed_urls = self._resolve_seed_urls()
        self.state.seed_urls = unique_preserve_order(self.state.seed_urls + seed_urls)
        self.save_state()

        company_to_source = self._discover_company_urls(seed_urls)
        if self.settings.max_companies is not None:
            limited_items = list(company_to_source.items())[: self.settings.max_companies]
            company_to_source = dict(limited_items)

        self._scrape_companies(company_to_source)
        dataframe = export_final(self.state.records, self.csv_path, self.xlsx_path)
        self.save_state()

        return RunSummary(
            discovered_companies=len(company_to_source),
            exported_rows=len(dataframe.index),
            failed_count=len(self.state.failed_urls),
            csv_path=self.csv_path,
            xlsx_path=self.xlsx_path,
            state_path=self.state_path,
        )

    def save_state(self) -> None:
        self.state.save(self.state_path)

    def _resolve_seed_urls(self) -> list[str]:
        normalized_seeds = [
            normalize_url(url)
            for url in self.state.seed_urls
            if collapse_whitespace(url)
        ]
        normalized_seeds.extend(
            [
            normalize_url(url)
            for url in self.settings.seed_urls
            if collapse_whitespace(url)
            ]
        )

        should_discover_from_home = (
            self.settings.discover_rubrics_from_home or not normalized_seeds
        )
        if should_discover_from_home:
            home_html = self.fetch_text(HOME_URL, purpose="home discovery")
            discovered_rubrics = extract_rubric_urls(home_html)
            if self.settings.max_rubrics is not None:
                discovered_rubrics = discovered_rubrics[: self.settings.max_rubrics]
            normalized_seeds.extend(discovered_rubrics)
            self.console.log(
                f"[green]Discovered[/green] {len(discovered_rubrics)} rubric URLs from homepage"
            )

        final_seeds = unique_preserve_order(normalized_seeds)
        if not final_seeds:
            raise RuntimeError("No seed URLs were found. Provide a rubric/company URL or enable homepage discovery.")
        return final_seeds

    def _discover_company_urls(self, seed_urls: list[str]) -> dict[str, str]:
        company_to_source: dict[str, str] = dict(self.state.company_sources)
        for url in self.state.discovered_company_urls:
            company_to_source.setdefault(url, "")
        listing_queue: deque[tuple[str, str]] = deque()
        queued_listing_urls: set[str] = set()

        for seed_url in seed_urls:
            if is_company_url(seed_url):
                if not company_to_source.get(seed_url):
                    company_to_source[seed_url] = seed_url
                self.state.discovered_company_urls.add(seed_url)
                if not self.state.company_sources.get(seed_url):
                    self.state.company_sources[seed_url] = seed_url
                continue

            normalized_seed = normalize_url(seed_url)
            if normalized_seed in self.state.visited_listing_urls:
                continue
            if normalized_seed not in queued_listing_urls:
                listing_queue.append((normalized_seed, normalized_seed))
                queued_listing_urls.add(normalized_seed)

        progress = tqdm(
            total=len(listing_queue),
            desc="Listing pages",
            unit="page",
            dynamic_ncols=True,
        )

        while listing_queue:
            listing_url, source_seed = listing_queue.popleft()
            progress.set_postfix_str(f"{len(company_to_source)} companies")

            if listing_url in self.state.visited_listing_urls:
                progress.update(1)
                continue

            try:
                html_text = self.fetch_text(
                    listing_url,
                    referer=source_seed,
                    purpose="listing page",
                )
            except Exception as exc:
                self.console.log(f"[red]Listing page failed[/red] {listing_url} -> {exc}")
                self.state.failed_urls[listing_url] = str(exc)
                self.state.visited_listing_urls.add(listing_url)
                self.save_state()
                progress.update(1)
                continue

            self.state.visited_listing_urls.add(listing_url)
            self.state.failed_urls.pop(listing_url, None)

            company_urls = extract_company_urls(html_text)
            for company_url in company_urls:
                if not company_to_source.get(company_url):
                    company_to_source[company_url] = source_seed
                self.state.discovered_company_urls.add(company_url)
                if not self.state.company_sources.get(company_url):
                    self.state.company_sources[company_url] = source_seed

            pagination_urls = extract_pagination_urls(html_text, listing_url)
            new_pages_added = 0
            for pagination_url in pagination_urls:
                if pagination_url in self.state.visited_listing_urls or pagination_url in queued_listing_urls:
                    continue
                if self.settings.max_pages_per_seed is not None:
                    if extract_page_number(pagination_url) > self.settings.max_pages_per_seed:
                        continue
                listing_queue.append((pagination_url, source_seed))
                queued_listing_urls.add(pagination_url)
                new_pages_added += 1

            if new_pages_added:
                progress.total += new_pages_added
                progress.refresh()

            self.save_state()
            progress.update(1)

            if self.settings.max_companies is not None and len(company_to_source) >= self.settings.max_companies:
                break

        progress.close()
        self.console.log(
            f"[bold green]Discovered {len(company_to_source)} unique company URLs[/bold green]"
        )
        return company_to_source

    def _scrape_companies(self, company_to_source: dict[str, str]) -> None:
        pending_company_urls = [
            company_url
            for company_url in company_to_source
            if company_url not in self.state.completed_company_urls
        ]

        progress = tqdm(
            total=len(pending_company_urls),
            desc="Companies",
            unit="company",
            dynamic_ncols=True,
        )

        for company_url in pending_company_urls:
            company_id = extract_company_id(company_url) or "unknown"
            self.console.log(f"[cyan]Scraping company[/cyan] #{company_id} {company_url}")

            try:
                html_text = self.fetch_text(
                    company_url,
                    referer=company_to_source.get(company_url) or HOME_URL,
                    purpose="company page",
                )
                record = extract_company_record(
                    html_text,
                    company_url=company_url,
                    source_listing_url=company_to_source.get(company_url, ""),
                    website_resolver=self.resolve_website_url,
                )
                if not record.company_name:
                    record.company_name = f"Company {company_id}"

                self.state.records.append(record)
                self.state.completed_company_urls.add(company_url)
                self.state.failed_urls.pop(company_url, None)
                append_backup_row(record, self.csv_path)
            except Exception as exc:
                self.state.failed_urls[company_url] = str(exc)
                self.console.log(f"[red]Company failed[/red] {company_url} -> {exc}")
            finally:
                self.save_state()
                progress.update(1)

        progress.close()

    def fetch_text(
        self,
        url: str,
        *,
        referer: str | None = None,
        purpose: str = "page",
    ) -> str:
        normalized_url = normalize_url(url)
        last_error: Exception | None = None

        for attempt in range(1, self.settings.retries + 1):
            self._sleep_between_requests()
            headers = self._build_headers(referer)

            try:
                response = self.session.get(
                    normalized_url,
                    headers=headers,
                    timeout=self.settings.timeout,
                )
                self._raise_for_retryable_status(response)
                response.raise_for_status()
                response.encoding = response.apparent_encoding or response.encoding
                return response.text
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.settings.retries:
                    break
                backoff_seconds = min(
                    30.0,
                    (2 ** (attempt - 1)) + random.uniform(0.5, 1.5),
                )
                self.console.log(
                    f"[yellow]Retry {attempt}/{self.settings.retries}[/yellow] "
                    f"{purpose} {normalized_url} after {exc!s}. Waiting {backoff_seconds:.1f}s"
                )
                time.sleep(backoff_seconds)

        raise RuntimeError(f"Could not fetch {purpose} after retries: {normalized_url}") from last_error

    def resolve_website_url(self, website_href: str) -> str:
        normalized_url = normalize_url(website_href)
        html_text = self.fetch_text(
            normalized_url,
            referer=HOME_URL,
            purpose="website redirect",
        )
        resolved = coerce_website(parse_meta_refresh_target(html_text))
        return resolved or normalized_url

    def _build_headers(self, referer: str | None = None) -> dict[str, str]:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "uz,en-US;q=0.9,en;q=0.8,ru;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "User-Agent": random.choice(self.user_agents),
        }
        if referer:
            headers["Referer"] = normalize_url(referer)
        return headers

    def _sleep_between_requests(self) -> None:
        time.sleep(random.uniform(self.settings.min_delay, self.settings.max_delay))

    @staticmethod
    def _raise_for_retryable_status(response: Response) -> None:
        if response.status_code not in RETRYABLE_STATUS_CODES:
            return
        http_error = requests.HTTPError(f"HTTP {response.status_code} for {response.url}")
        http_error.response = response
        raise http_error
