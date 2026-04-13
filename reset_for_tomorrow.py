#!/usr/bin/env python3
"""Reset project data while keeping configuration files.

Usage:
  python reset_for_tomorrow.py --execute

What it clears:
- All rows in local SQLite user tables (keeps schema)
- CSV exports used by pipeline
- Runtime temporary files
- Log files (truncates .log files)

What it keeps:
- config.json
- source code and project structure
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "leads.db"
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


def clear_sqlite_data(report: ResetReport, execute: bool) -> None:
    if not DB_PATH.exists():
        report.warn("SQLite file leads.db not found, skipping DB reset.")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        tables = [row[0] for row in cur.fetchall()]

        if not tables:
            report.add("No user tables found in leads.db.")
            return

        for table in tables:
            if execute:
                cur.execute(f"DELETE FROM {table}")
            report.add(f"Cleared table: {table}")

        if execute:
            cur.execute("DELETE FROM sqlite_sequence")
            conn.commit()
            cur.execute("VACUUM")
        report.add("Reset SQLite autoincrement and vacuumed database.")
    finally:
        conn.close()


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
    for backup in ROOT.glob("leads.db.backup*"):
        if execute:
            backup.unlink(missing_ok=True)
        report.add(f"Removed backup DB file: {backup.name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clear project data for a fresh start while keeping config files."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply reset actions. Without this flag, script runs in preview mode.",
    )
    parser.add_argument(
        "--delete-backups",
        action="store_true",
        help="Also delete leads.db.backup* files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    execute = bool(args.execute)

    report = ResetReport()

    clear_sqlite_data(report, execute)
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
