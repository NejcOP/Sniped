#!/usr/bin/env python3
"""Reset project data while keeping source files.

Usage:
  python reset_for_tomorrow.py --execute

What it clears:
- All rows in application Postgres tables (keeps schema)
- CSV exports used by pipeline
- Runtime temporary files
- Log files (truncates .log files)

What it keeps:
- source code and project structure
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from sqlalchemy import text

from backend.scraper.db import get_engine


ROOT = Path(__file__).resolve().parent
LOGS_DIR = ROOT / "logs"
RUNTIME_DIR = ROOT / "runtime"
EXPORT_FILES = [
    ROOT / "target_leads.csv",
    ROOT / "ai_mailer_ready.csv",
]


@dataclass
class ResetReport:
    actions: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add(self, message: str) -> None:
        self.actions.append(message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)


def clear_database_data(report: ResetReport, execute: bool) -> None:
    protected_tables = {"users", "stripe_webhook_events", "alembic_version"}
    engine = get_engine()
    with engine.begin() as conn:
        tables = [
            row["table_name"]
            for row in conn.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name ASC
                    """
                )
            ).mappings()
        ]

        user_tables = [table for table in tables if table not in protected_tables]
        if not user_tables:
            report.add("No application tables found in Postgres.")
            return

        if execute:
            conn.execute(text("SET session_replication_role = replica"))
        try:
            for table in user_tables:
                if execute:
                    conn.execute(text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'))
                report.add(f"Cleared table: {table}")
        finally:
            if execute:
                conn.execute(text("SET session_replication_role = DEFAULT"))


def remove_exports(report: ResetReport, execute: bool) -> None:
    for path in EXPORT_FILES:
        if not path.exists():
            continue
        if execute:
            path.unlink(missing_ok=True)
        report.add(f"Removed export file: {path.name}")


def clear_logs(report: ResetReport, execute: bool) -> None:
    if not LOGS_DIR.exists():
        return
    for log_file in LOGS_DIR.glob("*.log"):
        if execute:
            log_file.write_text("", encoding="utf-8")
        report.add(f"Truncated log file: {log_file.name}")


def clear_runtime(report: ResetReport, execute: bool) -> None:
    if not RUNTIME_DIR.exists():
        return

    for item in RUNTIME_DIR.rglob("*"):
        if not item.is_file():
            continue
        if execute:
            item.unlink(missing_ok=True)
        report.add(f"Removed runtime file: {item.relative_to(ROOT)}")


def remove_backups(report: ResetReport, execute: bool) -> None:
    for backup in ROOT.glob("*.db.backup*"):
        if execute:
            backup.unlink(missing_ok=True)
        report.add(f"Removed legacy backup file: {backup.name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clear project data for a fresh start while keeping source files."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply reset actions. Without this flag, script runs in preview mode.",
    )
    parser.add_argument(
        "--delete-backups",
        action="store_true",
        help="Also delete old *.db.backup* files if any still exist.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    execute = bool(args.execute)

    report = ResetReport()

    clear_database_data(report, execute)
    remove_exports(report, execute)
    clear_logs(report, execute)
    clear_runtime(report, execute)

    if args.delete_backups:
        remove_backups(report, execute)

    mode = "EXECUTE" if execute else "PREVIEW"
    print(f"[reset_for_tomorrow] Mode: {mode}")
    for line in report.actions:
        print(f"- {line}")

    if report.warnings:
        print("Warnings:")
        for warning in report.warnings:
            print(f"- {warning}")

    if not execute:
        print("\nNo changes were made. Re-run with --execute to apply reset.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
