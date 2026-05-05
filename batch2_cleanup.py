"""
Batch 2: Remove db_path from blacklist helpers, worker helpers, and maybe_sync_supabase.
Functions: add_worker_audit, auto_assign_worker_to_paid_lead, ensure_delivery_task_for_paid_lead,
           get_workers_snapshot, fetch_blacklist_sets, sync_blacklisted_leads,
           blacklist_lead_and_matches, add_blacklist_entry, restore_released_blacklisted_leads,
           remove_blacklist_entry, remove_lead_blacklist_and_matches, maybe_sync_supabase
"""

import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# ---- 1. Function signatures (single-line, db_path as first positional param) -----
# Pattern: def func(db_path: Path, rest) → def func(rest)
single_line_sigs = [
    # (old_prefix, new_prefix) where prefix is "def func(db_path: Path, "
    ("def fetch_blacklist_sets(db_path: Path, user_id:", "def fetch_blacklist_sets(user_id:"),
    ("def sync_blacklisted_leads(db_path: Path, user_id:", "def sync_blacklisted_leads(user_id:"),
    ("def blacklist_lead_and_matches(db_path: Path, lead_id:", "def blacklist_lead_and_matches(lead_id:"),
    ("def add_blacklist_entry(db_path: Path, *, user_id:", "def add_blacklist_entry(*, user_id:"),
    ("def restore_released_blacklisted_leads(db_path: Path, removed_entries:", "def restore_released_blacklisted_leads(removed_entries:"),
    ("def remove_blacklist_entry(db_path: Path, *, user_id:", "def remove_blacklist_entry(*, user_id:"),
    ("def remove_lead_blacklist_and_matches(db_path: Path, lead_id:", "def remove_lead_blacklist_and_matches(lead_id:"),
    ("def auto_assign_worker_to_paid_lead(db_path: Path, lead_id:", "def auto_assign_worker_to_paid_lead(lead_id:"),
    ("def ensure_delivery_task_for_paid_lead(db_path: Path, lead_id:", "def ensure_delivery_task_for_paid_lead(lead_id:"),
    ("def get_workers_snapshot(db_path: Path, user_id:", "def get_workers_snapshot(user_id:"),
    ("def maybe_sync_supabase(db_path: Path, config_path:", "def maybe_sync_supabase(config_path:"),
]

for old, new in single_line_sigs:
    text = text.replace(old, new)

# ---- 2. Multi-line add_worker_audit signature ------------------------------------
# def add_worker_audit(
#     db_path: Path,
#     *,
text = re.sub(
    r"(def add_worker_audit\(\n)\s+db_path: Path,\n(\s+\*,)",
    r"\1\2",
    text,
)

# ---- 3. maybe_sync_supabase body: make it a stub (SQLite→PG sync irrelevant) -----
# Replace the full body of maybe_sync_supabase to just _invalidate_leads_cache()
text = re.sub(
    r"(def maybe_sync_supabase\(config_path: Path\) -> None:\n)"
    r"    _invalidate_leads_cache\(\)\n"
    r"    settings = load_supabase_settings\(config_path\)\n"
    r"    if not settings\[\"enabled\"\]:\n"
    r"        return\n"
    r"    result = sync_all_to_supabase\(db_path, config_path\)\n"
    r"    if not result\[\"ok\"\]:\n"
    r"        logging\.warning\(\"Supabase sync had errors: %s\", result\[\"errors\"\]\)\n",
    r"\1    _invalidate_leads_cache()\n",
    text,
)

# ---- 4. Update all maybe_sync_supabase callsites ---------------------------------
text = text.replace(
    "maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)",
    "maybe_sync_supabase(DEFAULT_CONFIG_PATH)",
)
text = text.replace(
    "maybe_sync_supabase(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)",
    "maybe_sync_supabase(DEFAULT_CONFIG_PATH)",
)

# ---- 5. Update callsites for functions where db_path is first positional arg ------

# fetch_blacklist_sets(db_path, user_id=...) → fetch_blacklist_sets(user_id=...)
text = re.sub(
    r"\bfetch_blacklist_sets\(db_path,\s*",
    "fetch_blacklist_sets(",
    text,
)

# sync_blacklisted_leads(db_path, user_id=...) → sync_blacklisted_leads(user_id=...)
# sync_blacklisted_leads(db_path) → sync_blacklisted_leads()
text = re.sub(
    r"\bsync_blacklisted_leads\(db_path,\s*",
    "sync_blacklisted_leads(",
    text,
)
text = re.sub(
    r"\bsync_blacklisted_leads\(db_path\)",
    "sync_blacklisted_leads()",
    text,
)

# restore_released_blacklisted_leads(db_path, ...) → restore_released_blacklisted_leads(...)
text = re.sub(
    r"\brestore_released_blacklisted_leads\(db_path,\s*",
    "restore_released_blacklisted_leads(",
    text,
)

# blacklist_lead_and_matches(db_path, lead_id, ...) → blacklist_lead_and_matches(lead_id, ...)
text = re.sub(
    r"\bblacklist_lead_and_matches\(db_path,\s*",
    "blacklist_lead_and_matches(",
    text,
)

# remove_lead_blacklist_and_matches(db_path, lead_id) → remove_lead_blacklist_and_matches(lead_id)
text = re.sub(
    r"\bremove_lead_blacklist_and_matches\(db_path,\s*",
    "remove_lead_blacklist_and_matches(",
    text,
)

# remove_blacklist_entry(db_path, user_id=...) → remove_blacklist_entry(user_id=...)
text = re.sub(
    r"\bremove_blacklist_entry\(db_path,\s*",
    "remove_blacklist_entry(",
    text,
)

# auto_assign_worker_to_paid_lead(db_path, lead_id) → auto_assign_worker_to_paid_lead(lead_id)
text = re.sub(
    r"\bauto_assign_worker_to_paid_lead\(db_path,\s*",
    "auto_assign_worker_to_paid_lead(",
    text,
)

# ensure_delivery_task_for_paid_lead(db_path, lead_id) → ensure_delivery_task_for_paid_lead(lead_id)
# Also handle multi-line: ensure_delivery_task_for_paid_lead(db_path, lead_id)
text = re.sub(
    r"\bensure_delivery_task_for_paid_lead\(db_path,\s*",
    "ensure_delivery_task_for_paid_lead(",
    text,
)

# get_workers_snapshot(db_path, user_id=...) → get_workers_snapshot(user_id=...)
# get_workers_snapshot(db_path) → get_workers_snapshot()
text = re.sub(
    r"\bget_workers_snapshot\(db_path,\s*",
    "get_workers_snapshot(",
    text,
)
text = re.sub(
    r"\bget_workers_snapshot\(db_path\)",
    "get_workers_snapshot()",
    text,
)

# ---- 6. add_worker_audit callsites: remove db_path positional arg ----------------
# Pattern (multi-line): add_worker_audit(\n    db_path,\n    action= ...
text = re.sub(
    r"(add_worker_audit\(\n)\s+db_path,\n",
    r"\1",
    text,
)

# ---- 7. add_blacklist_entry callsites: remove db_path positional arg (multi-line)--
# Pattern: add_blacklist_entry(\n    db_path,\n    user_id=
text = re.sub(
    r"(add_blacklist_entry\(\n)\s+db_path,\n",
    r"\1",
    text,
)
# Also single-line: add_blacklist_entry(db_path, user_id=
text = re.sub(
    r"\badd_blacklist_entry\(db_path,\s*",
    "add_blacklist_entry(",
    text,
)

SRC.write_text(text, encoding="utf-8")
print("Batch 2 transformations applied.")
