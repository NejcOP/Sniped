"""
Batch 1: Remove db_path from all ensure_* table functions in backend/app.py.
Strategy:
  1. Update each function signature: remove `db_path: Path` (and leading comma if needed)
  2. Remove `init_db(db_path=str(db_path))` line (SQLite-specific, irrelevant for PG)
  3. Replace `pgdb.connect(db_path)` -> `pgdb.connect()` everywhere
  4. Replace all callsites of ensure_* functions that pass a single db_path argument
"""

import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# ---- 1. Fix function signatures -------------------------------------------------
# Pattern: def ensure_foo(db_path: Path) -> None:
#          def ensure_foo(db_path: Path, ...) -> None: (shouldn't exist for these, but safe)
# All Batch 1 ensure_* functions only have db_path as first/only positional param.
BATCH1_FUNCS = [
    "ensure_dashboard_columns",
    "ensure_blacklist_table",
    "ensure_revenue_log_table",
    "ensure_revenue_logs_table",
    "ensure_jobs_queue_table",
    "ensure_workers_table",
    "ensure_worker_audit_table",
    "ensure_delivery_tasks_table",
    "ensure_system_task_table",
    "ensure_runtime_table",
    "ensure_credit_logs_table",
    "ensure_users_table",
    "ensure_lead_history_table",
    "ensure_lead_report_table",
    "ensure_system_tables",
    "ensure_scrape_tables",
    "ensure_client_success_tables",
    "ensure_mailer_campaign_tables",
]

for func in BATCH1_FUNCS:
    # Signature: def func(db_path: Path) -> None:
    text = re.sub(
        rf"(def {func}\()db_path: Path\)",
        r"\1)",
        text,
    )
    # Signature: def func(db_path: Path, ...):
    text = re.sub(
        rf"(def {func}\()db_path: Path,\s*",
        r"\1",
        text,
    )

# ---- 2. Remove init_db(db_path=str(db_path)) line --------------------------------
text = re.sub(r"\n\s+init_db\(db_path=str\(db_path\)\)\n", "\n", text)

# ---- 3. Replace pgdb.connect(db_path) -> pgdb.connect() -------------------------
text = text.replace("pgdb.connect(db_path)", "pgdb.connect()")

# ---- 4. Replace callsites: ensure_foo(ANYTHING) -> ensure_foo() -----------------
# For each batch1 function, replace calls that pass a single arg (db_path variant)
# Pattern: ensure_foo(variable_name) - where variable_name is a single identifier
# Also handle: ensure_foo(DEFAULT_DB_PATH), ensure_foo(auth_db_path), ensure_foo(resolved_db_path) etc.
for func in BATCH1_FUNCS:
    # Replaces ensure_foo(SOME_VAR) with ensure_foo()
    # but NOT ensure_foo() (no args, already clean) or ensure_foo(x, y) (multiple args)
    text = re.sub(
        rf"\b{func}\([A-Za-z_][A-Za-z0-9_]*\)",
        f"{func}()",
        text,
    )

SRC.write_text(text, encoding="utf-8")
print("Batch 1 transformations applied.")
