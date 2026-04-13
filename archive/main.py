import argparse
import logging
from pathlib import Path

from scraper import GoogleMapsScraper, export_target_leads, init_db, upsert_lead


def setup_logging(log_level: str, log_file: str) -> None:
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
        force=True,
    )


def run_scrape(args) -> None:
    init_db(args.db)

    with GoogleMapsScraper(
        headless=args.headless,
        country_code=args.country_code,
        user_data_dir=args.user_data_dir,
    ) as scraper:
        leads = scraper.scrape(keyword=args.keyword, max_results=args.results)

    inserted = 0
    for lead in leads:
        if upsert_lead(lead, db_path=args.db):
            inserted += 1

    print(f"Scraped: {len(leads)}")
    print(f"Inserted (new): {inserted}")
    print(f"Skipped (duplicates): {len(leads) - inserted}")

    if args.export_targets:
        exported = export_target_leads(
            db_path=args.db,
            output_csv=args.output,
            min_rating=args.min_rating,
        )
        print(f"Exported target leads: {exported} -> {args.output}")


def run_export(args) -> None:
    init_db(args.db)
    exported = export_target_leads(
        db_path=args.db,
        output_csv=args.output,
        min_rating=args.min_rating,
    )
    print(f"Exported target leads: {exported} -> {args.output}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Google Maps (GBP) scraper with SQLite persistence and target filtering."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging verbosity.",
    )
    parser.add_argument(
        "--log-file",
        default="logs/scraper.log",
        help="Path to log file.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape_cmd = subparsers.add_parser("scrape", help="Scrape Google Maps results into SQLite.")
    scrape_cmd.add_argument("--keyword", required=True, help="Search phrase, e.g. 'Roofers in Miami'.")
    scrape_cmd.add_argument("--results", type=int, required=True, help="Number of results to collect.")
    scrape_cmd.add_argument("--db", default="leads.db", help="SQLite DB file path.")
    scrape_cmd.add_argument(
        "--country-code",
        default="us",
        help="Country code for Google domain/locale adaptation, e.g. us, si, de.",
    )
    scrape_cmd.add_argument(
        "--user-data-dir",
        default="profiles/maps_profile",
        help="Persistent browser profile directory to emulate a stable residential user profile.",
    )
    scrape_cmd.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode (less human-like, but useful on servers).",
    )
    scrape_cmd.add_argument(
        "--export-targets",
        action="store_true",
        help="Export filtered targets right after scraping.",
    )
    scrape_cmd.add_argument(
        "--output",
        default="target_leads.csv",
        help="CSV output used when --export-targets is enabled.",
    )
    scrape_cmd.add_argument(
        "--min-rating",
        type=float,
        default=3.5,
        help="Rating threshold for target export filter.",
    )
    scrape_cmd.set_defaults(func=run_scrape)

    export_cmd = subparsers.add_parser(
        "export-targets",
        help="Export companies with missing website OR rating below threshold.",
    )
    export_cmd.add_argument("--db", default="leads.db", help="SQLite DB file path.")
    export_cmd.add_argument("--output", default="target_leads.csv", help="CSV output file path.")
    export_cmd.add_argument(
        "--min-rating",
        type=float,
        default=3.5,
        help="Rating threshold for target export filter.",
    )
    export_cmd.set_defaults(func=run_export)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    setup_logging(log_level=args.log_level, log_file=args.log_file)

    args.func(args)


if __name__ == "__main__":
    main()
