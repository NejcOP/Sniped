from __future__ import annotations

import re
from pathlib import Path

TARGET_FILE = Path("backend/app.py")

# Tenant-scoped tables that must include user_id constraints in app SQL statements.
TENANT_TABLES = {
    "leads",
    "saved_segments",
    "client_folders",
    "delivery_tasks",
    "workers",
    "jobs",
    "system_tasks",
    "lead_blacklist",
    "revenue_log",
    "revenue_logs",
}

# Patterns that are intentionally global/admin/migration logic and are excluded.
ALLOWLIST_SUBSTRINGS = (
    "SYSTEM-WIDE: intentionally unscoped",
    "PRAGMA table_info",
    "CREATE TABLE",
    "CREATE INDEX",
    "ALTER TABLE",
    "INSERT INTO",
    "UPDATE users",
    "FROM users",
    "SELECT 1 FROM leads WHERE id = ? LIMIT 1",
    "SELECT COUNT(*) FROM leads",
    "FROM lead_history",
    "FROM lead_reports",
    "{uid_clause}",
    "{where_fragment}",
    '"database": "supabase"',
)


def _line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def main() -> int:
    if not TARGET_FILE.exists():
        print(f"ERROR: file not found: {TARGET_FILE}")
        return 2

    text = TARGET_FILE.read_text(encoding="utf-8")

    # Scan SQL-ish string literals to find SELECT/UPDATE/DELETE on tenant tables without user_id.
    string_pattern = re.compile(r'(["\']{1,3})(?P<body>.*?)(?:\1)', re.DOTALL)
    sql_verb_pattern = re.compile(r"\b(?:SELECT|UPDATE|DELETE)\b", re.IGNORECASE)
    table_use_pattern = re.compile(
        r"\b(?:FROM|UPDATE|DELETE\s+FROM)\s+(?P<table>[A-Za-z_][A-Za-z0-9_]*)",
        re.IGNORECASE,
    )

    findings: list[tuple[int, str]] = []

    for match in string_pattern.finditer(text):
        body = match.group("body")
        if not body or not sql_verb_pattern.search(body):
            continue

        body_upper = body.upper()

        for stmt in table_use_pattern.finditer(body):
            table = str(stmt.group("table") or "").strip().lower()
            if table not in TENANT_TABLES:
                continue

            stmt_text = body
            upper_stmt = body_upper
            if any(token in stmt_text for token in ALLOWLIST_SUBSTRINGS):
                continue

            if "USER_ID" in upper_stmt:
                continue

            absolute_offset = match.start("body")
            line_no = _line_number(text, absolute_offset)
            compact = " ".join(body.strip().split())
            findings.append((line_no, compact[:220]))

    if findings:
        print("User-isolation audit failed. Potential unscoped tenant-table queries:")
        for line_no, snippet in findings:
            print(f"- backend/app.py:{line_no}: {snippet}")
        return 1

    print("User-isolation audit passed: no unscoped tenant-table SELECT/UPDATE/DELETE detected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
