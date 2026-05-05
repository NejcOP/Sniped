"""
Batch 4: Remove db_path from task helpers, runtime helpers, reconcile/enqueue functions.
"""

import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# ---- 1. Fix multi-line function signatures (db_path on its own line) -----------

# create_task_record, finish_task_record, enqueue_task: remove "    db_path: Path,\n"
for func_name in ["create_task_record", "finish_task_record", "enqueue_task"]:
    text = re.sub(
        rf"(def {func_name}\(\n)\s+db_path: Path,\n",
        r"\1",
        text,
    )

# reconcile_orphaned_active_tasks(app: FastAPI, db_path: Path) -> None:
# db_path is the SECOND param: remove ", db_path: Path"
text = text.replace(
    "def reconcile_orphaned_active_tasks(app: FastAPI, db_path: Path) -> None:",
    "def reconcile_orphaned_active_tasks(app: FastAPI) -> None:",
)

# ---- 2. Fix single-line function signatures ------------------------------------
single_sigs = [
    ("def mark_task_running(db_path: Path, task_id:", "def mark_task_running(task_id:"),
    ("def update_task_progress(db_path: Path, task_id:", "def update_task_progress(task_id:"),
    ("def fetch_latest_task(db_path: Path, task_type:", "def fetch_latest_task(task_type:"),
    ("def fetch_all_latest_tasks(db_path: Path, user_id:", "def fetch_all_latest_tasks(user_id:"),
    ("def fetch_task_history(db_path: Path, limit:", "def fetch_task_history(limit:"),
    ("def fetch_task_by_id(db_path: Path, task_id:", "def fetch_task_by_id(task_id:"),
    ("def task_is_active(db_path: Path, task_type:", "def task_is_active(task_type:"),
    ("def get_runtime_value(db_path: Path, key:", "def get_runtime_value(key:"),
    ("def set_runtime_value(db_path: Path, key:", "def set_runtime_value(key:"),
]
for old, new in single_sigs:
    text = text.replace(old, new)

# ---- 3. Fix single-line callsites (db_path as first positional arg) -----------

# mark_task_running, update_task_progress, fetch_task_by_id, task_is_active, fetch_latest_task:
# they always take db_path as first positional arg.
# Replace: func(db_path_var, ...) → func(...)
for func_name in [
    "mark_task_running",
    "update_task_progress",
    "fetch_task_by_id",
    "task_is_active",
    "fetch_latest_task",
    "fetch_all_latest_tasks",
    "fetch_task_history",
    "create_task_record",
]:
    # Single-line: func(SOME_VAR, next) → func(next)
    text = re.sub(
        rf"\b{func_name}\([A-Za-z_][A-Za-z0-9_]*,\s*",
        f"{func_name}(",
        text,
    )

# finish_task_record: db_path first arg (single-line)
text = re.sub(
    r"\bfinish_task_record\([A-Za-z_][A-Za-z0-9_]*,\s*",
    "finish_task_record(",
    text,
)

# get_runtime_value: db_path first arg
text = re.sub(
    r"\bget_runtime_value\([A-Za-z_][A-Za-z0-9_]*,\s*",
    "get_runtime_value(",
    text,
)

# set_runtime_value: db_path first arg (single-line)
text = re.sub(
    r"\bset_runtime_value\([A-Za-z_][A-Za-z0-9_]*,\s*",
    "set_runtime_value(",
    text,
)

# reconcile_orphaned_active_tasks(app, db_path) → reconcile_orphaned_active_tasks(app)
text = re.sub(
    r"\breconcile_orphaned_active_tasks\(([^,\n]+),\s*[A-Za-z_][A-Za-z0-9_]*\)",
    r"reconcile_orphaned_active_tasks(\1)",
    text,
)

# ---- 4. Fix multi-line callsites: remove "    db_path[_variant],\n" first arg -----
# These are calls where db_path is on its own indented line after the opening paren.
# Pattern: func(\n    db_path_var,\n  → func(\n
for func_name in [
    "finish_task_record",
    "create_task_record",
    "set_runtime_value",
    "enqueue_task",
]:
    text = re.sub(
        rf"({func_name}\(\n)\s+[A-Za-z_][A-Za-z0-9_]*,\n",
        r"\1",
        text,
    )

# ---- 5. Fix enqueue_task positional calls (app, background_tasks, db_path, user_id, ...) ----
# Single-line: enqueue_task(app, background_tasks, db_path, user_id, "task_type", ...)
# After removing db_path param, the call becomes (app, background_tasks, user_id, ...)
# Pattern: the 3rd positional arg (after 2 commas from opening paren) is db_path
# Approach: replace "enqueue_task(X, Y, db_path_var, " → "enqueue_task(X, Y, "
# We handle this with a specific pattern since the first two args have known forms:
text = re.sub(
    r"\benqueue_task\(([^,]+),\s*([^,]+),\s*[A-Za-z_][A-Za-z0-9_]*,\s*([A-Za-z_])",
    r"enqueue_task(\1, \2, \3",
    text,
)

# ---- 6. Fix enqueue_task keyword callsites: remove "db_path=VAR,\n" line ------
# Pattern: whitespace db_path=VARNAME, newline
text = re.sub(
    r"(\n[ \t]+)db_path\s*=\s*[A-Za-z_][A-Za-z0-9_]*,\n",
    r"\n",
    text,
)

SRC.write_text(text, encoding="utf-8")
print("Batch 4 transformations applied.")
