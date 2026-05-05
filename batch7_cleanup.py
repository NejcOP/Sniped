"""
Batch 7: Remove db_path from report functions, dashboard stats, and _list_reporting_users.
"""

import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# ---- 1. Fix function signatures ----

SIGS = [
    ("def _load_user_email_for_reports(db_path: Path, user_id:",
     "def _load_user_email_for_reports(user_id:"),
    ("def build_weekly_report_summary(db_path: Path, user_id:",
     "def build_weekly_report_summary(user_id:"),
    ("def build_monthly_report_summary(db_path: Path, user_id:",
     "def build_monthly_report_summary(user_id:"),
    ("def build_client_dashboard_snapshot(db_path: Path, user_id:",
     "def build_client_dashboard_snapshot(user_id:"),
    ("def get_dashboard_stats(db_path: Path, user_id:",
     "def get_dashboard_stats(user_id:"),
    ("def _list_reporting_users(db_path: Path) ->",
     "def _list_reporting_users() ->"),
]
for old, new in SIGS:
    text = text.replace(old, new)

# ---- 2. Callsite fixes: remove db_path as first positional arg ----

FUNCS = [
    "_load_user_email_for_reports",
    "build_weekly_report_summary",
    "build_monthly_report_summary",
    "build_client_dashboard_snapshot",
    "get_dashboard_stats",
    "_list_reporting_users",
]
for func in FUNCS:
    # func(SOME_VAR, rest → func(rest
    text = re.sub(rf"\b{func}\([A-Za-z_][A-Za-z0-9_]*,\s*", f"{func}(", text)
    # func(SOME_VAR) → func()
    text = re.sub(rf"\b{func}\([A-Za-z_][A-Za-z0-9_]*\)", f"{func}()", text)

SRC.write_text(text, encoding="utf-8")
print("Batch 7 transformations applied.")
