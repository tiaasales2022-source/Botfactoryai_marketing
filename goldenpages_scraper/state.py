from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .models import CompanyRecord
from .utils import atomic_write_json


@dataclass
class ScrapeState:
    run_id: str
    csv_path: str
    xlsx_path: str
    seed_urls: list[str] = field(default_factory=list)
    company_sources: dict[str, str] = field(default_factory=dict)
    visited_listing_urls: set[str] = field(default_factory=set)
    discovered_company_urls: set[str] = field(default_factory=set)
    completed_company_urls: set[str] = field(default_factory=set)
    failed_urls: dict[str, str] = field(default_factory=dict)
    records: list[CompanyRecord] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "csv_path": self.csv_path,
            "xlsx_path": self.xlsx_path,
            "seed_urls": sorted(self.seed_urls),
            "company_sources": dict(sorted(self.company_sources.items())),
            "visited_listing_urls": sorted(self.visited_listing_urls),
            "discovered_company_urls": sorted(self.discovered_company_urls),
            "completed_company_urls": sorted(self.completed_company_urls),
            "failed_urls": dict(sorted(self.failed_urls.items())),
            "records": [record.to_state() for record in self.records],
        }

    def save(self, path: Path) -> None:
        atomic_write_json(path, self.to_dict())

    @classmethod
    def load(cls, path: Path) -> "ScrapeState":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            run_id=str(payload["run_id"]),
            csv_path=str(payload["csv_path"]),
            xlsx_path=str(payload["xlsx_path"]),
            seed_urls=[str(item) for item in payload.get("seed_urls", [])],
            company_sources={
                str(url): str(source)
                for url, source in payload.get("company_sources", {}).items()
            },
            visited_listing_urls={str(item) for item in payload.get("visited_listing_urls", [])},
            discovered_company_urls={str(item) for item in payload.get("discovered_company_urls", [])},
            completed_company_urls={str(item) for item in payload.get("completed_company_urls", [])},
            failed_urls={
                str(url): str(message)
                for url, message in payload.get("failed_urls", {}).items()
            },
            records=[
                CompanyRecord.from_state(item)
                for item in payload.get("records", [])
                if isinstance(item, dict)
            ],
        )
