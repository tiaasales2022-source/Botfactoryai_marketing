from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from .models import CompanyRecord

EXPORT_COLUMNS = [
    "company_id",
    "company_name",
    "phones",
    "address",
    "landmark",
    "website",
    "emails",
    "activity_types",
    "rating_value",
    "rating_count",
    "source_url",
    "source_listing_url",
    "scraped_at",
]


def build_output_paths(output_dir: Path, run_id: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"backup_data_{run_id}.csv"
    xlsx_path = output_dir / f"goldenpages_data_{run_id}.xlsx"
    state_path = output_dir / f"scrape_state_{run_id}.json"
    return csv_path, xlsx_path, state_path


def append_backup_row(record: CompanyRecord, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    row = record.to_row()
    file_exists = csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPORT_COLUMNS)
        if not file_exists or csv_path.stat().st_size == 0:
            writer.writeheader()
        writer.writerow(row)


def export_final(records: list[CompanyRecord], csv_path: Path, xlsx_path: Path) -> pd.DataFrame:
    rows = [record.to_row() for record in records]
    if rows:
        dataframe = pd.DataFrame(rows, columns=EXPORT_COLUMNS)
        dataframe = dataframe.drop_duplicates(
            subset=["company_id", "source_url"],
            keep="last",
        ).sort_values(["company_name", "company_id"], na_position="last")
    else:
        dataframe = pd.DataFrame(columns=EXPORT_COLUMNS)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(csv_path, index=False, encoding="utf-8-sig")
    dataframe.to_excel(xlsx_path, index=False)
    return dataframe
