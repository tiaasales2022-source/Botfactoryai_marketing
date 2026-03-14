from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from rich.console import Console
from rich.panel import Panel

from .scraper import GoldenPagesScraper, ScraperSettings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="goldenpages-scraper",
        description="Scrape company data from GoldenPages.uz with retries, progress bars, and Excel/CSV export.",
    )
    parser.add_argument(
        "seed_urls",
        nargs="*",
        help="Rubric or company URLs. If omitted, rubric links are auto-discovered from the homepage.",
    )
    parser.add_argument(
        "--discover-rubrics-from-home",
        action="store_true",
        help="Always add rubric URLs discovered on the GoldenPages homepage.",
    )
    parser.add_argument(
        "--max-rubrics",
        type=int,
        default=None,
        help="Limit homepage-discovered rubrics for safer trial runs.",
    )
    parser.add_argument(
        "--max-pages-per-seed",
        type=int,
        default=None,
        help="Limit the number of paginated listing pages per rubric seed.",
    )
    parser.add_argument(
        "--max-companies",
        type=int,
        default=None,
        help="Stop after this many unique companies are discovered.",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=1.2,
        help="Minimum random delay between HTTP requests in seconds.",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=3.2,
        help="Maximum random delay between HTTP requests in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="How many times each request should be retried.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory for CSV/XLSX/state files.",
    )
    parser.add_argument(
        "--resume-state",
        type=Path,
        default=None,
        help="Resume from an earlier JSON state file.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.min_delay < 0 or args.max_delay < 0:
        parser.error("Delays must be zero or positive.")
    if args.min_delay > args.max_delay:
        parser.error("--min-delay cannot be greater than --max-delay.")
    if args.retries < 1:
        parser.error("--retries must be at least 1.")

    console = Console()
    settings = ScraperSettings(
        seed_urls=list(args.seed_urls),
        discover_rubrics_from_home=args.discover_rubrics_from_home,
        max_rubrics=args.max_rubrics,
        max_pages_per_seed=args.max_pages_per_seed,
        max_companies=args.max_companies,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        retries=args.retries,
        timeout=args.timeout,
        output_dir=args.output_dir,
        resume_state=args.resume_state,
    )

    console.print(
        Panel.fit(
            "GoldenPages Intelligence Scraper\n"
            "Retries, pagination, backup CSV, and XLSX export are enabled.",
            title="Run Config",
            border_style="cyan",
        )
    )

    scraper = GoldenPagesScraper(settings=settings, console=console)

    try:
        summary = scraper.run()
    except KeyboardInterrupt:
        scraper.save_state()
        console.print("[bold yellow]Interrupted.[/bold yellow] Progress was saved to the state file.")
        return 130
    except Exception:
        console.print_exception(show_locals=False)
        return 1

    console.print(
        Panel.fit(
            "\n".join(
                [
                    f"Discovered companies: {summary.discovered_companies}",
                    f"Exported rows: {summary.exported_rows}",
                    f"Failed URLs: {summary.failed_count}",
                    f"CSV: {summary.csv_path}",
                    f"XLSX: {summary.xlsx_path}",
                    f"State: {summary.state_path}",
                ]
            ),
            title="Completed",
            border_style="green",
        )
    )
    return 0
