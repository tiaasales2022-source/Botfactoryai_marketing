from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(slots=True)
class CompanyRecord:
    company_id: str = ""
    company_name: str = ""
    phones: list[str] = field(default_factory=list)
    address: str = ""
    landmark: str = ""
    website: str = ""
    emails: list[str] = field(default_factory=list)
    activity_types: list[str] = field(default_factory=list)
    rating_value: float = 0.0
    rating_count: int = 0
    source_url: str = ""
    source_listing_url: str = ""
    scraped_at: str = ""

    def to_row(self) -> dict[str, str]:
        return {
            "company_id": self.company_id,
            "company_name": self.company_name,
            "phones": " | ".join(self.phones),
            "address": self.address,
            "landmark": self.landmark,
            "website": self.website,
            "emails": " | ".join(self.emails),
            "activity_types": " | ".join(self.activity_types),
            "rating_value": self.rating_value,
            "rating_count": self.rating_count,
            "source_url": self.source_url,
            "source_listing_url": self.source_listing_url,
            "scraped_at": self.scraped_at,
        }

    def to_state(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_state(cls, payload: dict[str, object]) -> "CompanyRecord":
        return cls(
            company_id=str(payload.get("company_id", "")),
            company_name=str(payload.get("company_name", "")),
            phones=[str(item) for item in payload.get("phones", [])],
            address=str(payload.get("address", "")),
            landmark=str(payload.get("landmark", "")),
            website=str(payload.get("website", "")),
            emails=[str(item) for item in payload.get("emails", [])],
            activity_types=[str(item) for item in payload.get("activity_types", [])],
            rating_value=float(payload.get("rating_value", 0.0) or 0.0),
            rating_count=int(payload.get("rating_count", 0) or 0),
            source_url=str(payload.get("source_url", "")),
            source_listing_url=str(payload.get("source_listing_url", "")),
            scraped_at=str(payload.get("scraped_at", "")),
        )
