from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from .utils import collapse_whitespace

DEFAULT_SMS_MOBILE_PREFIXES = (
    "33",
    "50",
    "90",
    "91",
    "93",
    "94",
    "95",
    "97",
    "98",
    "99",
)

SMS_EXPORT_COLUMNS = [
    "company_id",
    "company_name",
    "raw_phone",
    "normalized_phone",
    "mobile_prefix",
    "sms_capable",
    "address",
    "landmark",
    "website",
    "emails",
    "activity_types",
    "source_url",
    "source_listing_url",
    "scraped_at",
]

GOOGLE_CONTACTS_COLUMNS = [
    "First Name",
    "Phone 1 - Label",
    "Phone 1 - Value",
    "Email 1 - Label",
    "Email 1 - Value",
    "Website 1 - Label",
    "Website 1 - Value",
    "Organization Name",
    "Address 1 - Label",
    "Address 1 - Street",
    "Notes",
    "Labels",
]

DEFAULT_GOOGLE_CONTACTS_LABELS = "Botfactory SMS Leads:::GoldenPages"

PHONE_SPLIT_RE = re.compile(r"[|,;\n]+")
NON_DIGIT_RE = re.compile(r"\D+")


def parse_sms_mobile_prefixes(raw_value: str | None) -> tuple[str, ...]:
    if not raw_value:
        return DEFAULT_SMS_MOBILE_PREFIXES
    prefixes: list[str] = []
    for chunk in raw_value.split(","):
        cleaned = NON_DIGIT_RE.sub("", collapse_whitespace(chunk))
        if len(cleaned) == 2 and cleaned not in prefixes:
            prefixes.append(cleaned)
    return tuple(prefixes) or DEFAULT_SMS_MOBILE_PREFIXES


def split_phone_values(value: object) -> list[str]:
    if value is None:
        return []
    text = collapse_whitespace(str(value))
    if not text:
        return []
    return [
        collapse_whitespace(part)
        for part in PHONE_SPLIT_RE.split(text)
        if collapse_whitespace(part)
    ]


def normalize_uzbek_phone(value: object) -> str:
    digits = NON_DIGIT_RE.sub("", collapse_whitespace(str(value)))
    if not digits:
        return ""
    if len(digits) == 9:
        digits = "998" + digits
    elif len(digits) == 10 and digits.startswith("0"):
        digits = "998" + digits[1:]
    elif len(digits) == 12 and digits.startswith("998"):
        pass
    else:
        return ""
    return f"+{digits}"


def mobile_prefix_for_phone(normalized_phone: str) -> str:
    digits = NON_DIGIT_RE.sub("", normalized_phone)
    if len(digits) != 12 or not digits.startswith("998"):
        return ""
    return digits[3:5]


def is_sms_capable_phone(normalized_phone: str, mobile_prefixes: Iterable[str]) -> bool:
    prefix = mobile_prefix_for_phone(normalized_phone)
    return prefix in set(mobile_prefixes)


def build_sms_leads_dataframe(
    scraped_dataframe: pd.DataFrame,
    *,
    mobile_prefixes: Iterable[str] = DEFAULT_SMS_MOBILE_PREFIXES,
) -> pd.DataFrame:
    prefixes = tuple(dict.fromkeys(str(prefix) for prefix in mobile_prefixes if str(prefix)))
    rows: list[dict[str, str]] = []

    for record in scraped_dataframe.fillna("").to_dict(orient="records"):
        phones = split_phone_values(record.get("phones", ""))
        if not phones:
            continue

        for raw_phone in phones:
            normalized_phone = normalize_uzbek_phone(raw_phone)
            if not normalized_phone:
                continue
            if not is_sms_capable_phone(normalized_phone, prefixes):
                continue

            rows.append(
                {
                    "company_id": collapse_whitespace(str(record.get("company_id", ""))),
                    "company_name": collapse_whitespace(str(record.get("company_name", ""))),
                    "raw_phone": raw_phone,
                    "normalized_phone": normalized_phone,
                    "mobile_prefix": mobile_prefix_for_phone(normalized_phone),
                    "sms_capable": "yes",
                    "address": collapse_whitespace(str(record.get("address", ""))),
                    "landmark": collapse_whitespace(str(record.get("landmark", ""))),
                    "website": collapse_whitespace(str(record.get("website", ""))),
                    "emails": collapse_whitespace(str(record.get("emails", ""))),
                    "activity_types": collapse_whitespace(str(record.get("activity_types", ""))),
                    "source_url": collapse_whitespace(str(record.get("source_url", ""))),
                    "source_listing_url": collapse_whitespace(str(record.get("source_listing_url", ""))),
                    "scraped_at": collapse_whitespace(str(record.get("scraped_at", ""))),
                }
            )

    if not rows:
        return pd.DataFrame(columns=SMS_EXPORT_COLUMNS)

    dataframe = pd.DataFrame(rows, columns=SMS_EXPORT_COLUMNS)
    dataframe = dataframe.drop_duplicates(subset=["normalized_phone"], keep="first")
    return dataframe.sort_values(["company_name", "normalized_phone"], na_position="last").reset_index(drop=True)


def build_sms_export_paths(output_dir: Path, run_id: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"sms_phone_leads_{run_id}.csv"
    xlsx_path = output_dir / f"sms_phone_leads_{run_id}.xlsx"
    return csv_path, xlsx_path


def export_sms_leads(dataframe: pd.DataFrame, output_dir: Path, run_id: str) -> tuple[Path, Path]:
    csv_path, xlsx_path = build_sms_export_paths(output_dir, run_id)
    dataframe.to_csv(csv_path, index=False, encoding="utf-8-sig")
    dataframe.to_excel(xlsx_path, index=False)
    return csv_path, xlsx_path


def build_google_contacts_dataframe(
    sms_leads_dataframe: pd.DataFrame,
    *,
    labels: str = DEFAULT_GOOGLE_CONTACTS_LABELS,
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []

    for record in sms_leads_dataframe.fillna("").to_dict(orient="records"):
        company_name = collapse_whitespace(str(record.get("company_name", "")))
        phone = collapse_whitespace(str(record.get("normalized_phone", "")))
        if not company_name or not phone:
            continue

        email_values = [
            collapse_whitespace(chunk)
            for chunk in re.split(r"[|,;\n]+", str(record.get("emails", "")))
            if collapse_whitespace(chunk)
        ]
        website = collapse_whitespace(str(record.get("website", "")))
        address = collapse_whitespace(str(record.get("address", "")))
        landmark = collapse_whitespace(str(record.get("landmark", "")))
        activity_types = collapse_whitespace(str(record.get("activity_types", "")))
        notes = " | ".join(
            part
            for part in [
                f"Kategoriya: {activity_types}" if activity_types else "",
                f"Mo'ljal: {landmark}" if landmark else "",
                f"Manba: {collapse_whitespace(str(record.get('source_url', '')))}",
            ]
            if part
        )

        rows.append(
            {
                "First Name": company_name,
                "Phone 1 - Label": "Mobile",
                "Phone 1 - Value": phone,
                "Email 1 - Label": "Work" if email_values else "",
                "Email 1 - Value": email_values[0] if email_values else "",
                "Website 1 - Label": "Work" if website else "",
                "Website 1 - Value": website,
                "Organization Name": company_name,
                "Address 1 - Label": "Work" if address else "",
                "Address 1 - Street": address,
                "Notes": notes,
                "Labels": collapse_whitespace(labels),
            }
        )

    if not rows:
        return pd.DataFrame(columns=GOOGLE_CONTACTS_COLUMNS)

    dataframe = pd.DataFrame(rows, columns=GOOGLE_CONTACTS_COLUMNS)
    return dataframe.drop_duplicates(subset=["Phone 1 - Value"], keep="first").reset_index(drop=True)


def export_google_contacts_csv(
    dataframe: pd.DataFrame,
    output_dir: Path,
    run_id: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"google_contacts_sms_{run_id}.csv"
    dataframe.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return csv_path
