"""
Batch 6: Remove db_path from sync helpers, analytics functions, and client folder functions.
"""

import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# ---- 1. Fix function signatures (db_path as first positional arg) -----------

BATCH6_FUNCS = [
    # (old_prefix, new_prefix)
    ("def get_table_rows_snapshot(db_path: Path, table_name:", "def get_table_rows_snapshot(table_name:"),
    ("def get_table_columns_snapshot(db_path: Path, table_name:", "def get_table_columns_snapshot(table_name:"),
    ("def replace_table_rows_snapshot(db_path: Path, table_name:", "def replace_table_rows_snapshot(table_name:"),
    ("def sync_table_from_supabase(db_path: Path, table_name:", "def sync_table_from_supabase(table_name:"),
    ("def sync_table_to_supabase(db_path: Path, table_name:", "def sync_table_to_supabase(table_name:"),
    ("def sync_all_to_supabase(db_path: Path, config_path:", "def sync_all_to_supabase(config_path:"),
    ("def extract_keyword_performance(db_path: Path, limit:", "def extract_keyword_performance(limit:"),
    ("def get_niche_recommendation(db_path: Path, config_path:", "def get_niche_recommendation(config_path:"),
    ("def get_scraped_lead_count(db_path: Path) -> int:", "def get_scraped_lead_count() -> int:"),
    ("def get_queued_mail_count(db_path: Path, user_id:", "def get_queued_mail_count(user_id:"),
    ("def create_client_folder(db_path: Path, user_id:", "def create_client_folder(user_id:"),
    ("def list_client_folders(db_path: Path, user_id:", "def list_client_folders(user_id:"),
    ("def create_saved_segment(db_path: Path, user_id:", "def create_saved_segment(user_id:"),
    ("def list_saved_segments(db_path: Path, user_id:", "def list_saved_segments(user_id:"),
    ("def delete_saved_segment(db_path: Path, user_id:", "def delete_saved_segment(user_id:"),
    ("def assign_lead_to_client_folder(db_path: Path, user_id:", "def assign_lead_to_client_folder(user_id:"),
]

for old, new in BATCH6_FUNCS:
    text = text.replace(old, new)

# ---- 2. Update callsites: remove db_path as first positional arg -----------

# Use the batch-pattern: func(SOME_VAR, next_arg) → func(next_arg) for single-line
FUNC_NAMES = [
    "get_table_rows_snapshot",
    "get_table_columns_snapshot",
    "replace_table_rows_snapshot",
    "sync_table_from_supabase",
    "sync_table_to_supabase",
    "extract_keyword_performance",
    "get_scraped_lead_count",
    "get_queued_mail_count",
    "create_client_folder",
    "list_client_folders",
    "create_saved_segment",
    "list_saved_segments",
    "delete_saved_segment",
    "assign_lead_to_client_folder",
    "list_client_folders",
]

for func_name in FUNC_NAMES:
    # Single-line: func(SOME_VAR, ...) → func(...)
    text = re.sub(
        rf"\b{func_name}\([A-Za-z_][A-Za-z0-9_]*,\s*",
        f"{func_name}(",
        text,
    )
    # Single arg (no more args): func(SOME_VAR) → func()
    text = re.sub(
        rf"\b{func_name}\([A-Za-z_][A-Za-z0-9_]*\)",
        f"{func_name}()",
        text,
    )

# sync_all_to_supabase: db_path first, config_path second
# sync_all_to_supabase(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH) → sync_all_to_supabase(DEFAULT_CONFIG_PATH)
# sync_all_to_supabase(db_path, config_path) → sync_all_to_supabase(config_path)
text = re.sub(
    r"\bsync_all_to_supabase\([A-Za-z_][A-Za-z0-9_]*,\s*",
    "sync_all_to_supabase(",
    text,
)

# get_niche_recommendation: db_path first, config_path second, optional country_code
text = re.sub(
    r"\bget_niche_recommendation\([A-Za-z_][A-Za-z0-9_]*,\s*",
    "get_niche_recommendation(",
    text,
)

# ---- 3. Fix multi-line create_client_folder callsite -------------------------
# create_client_folder(\n    db_path,\n    user_id, → create_client_folder(\n    user_id,
text = re.sub(
    r"(create_client_folder\(\n)\s+[A-Za-z_][A-Za-z0-9_]*,\n",
    r"\1",
    text,
)

# ---- 4. Fix sync_table_to_supabase inside sync_all_to_supabase ---------------
# sync_table_to_supabase(db_path, table_name, config_path) → already covered by loop above

SRC.write_text(text, encoding="utf-8")
print("Batch 6 transformations applied.")
