"""
Batch 5: Remove db_path from SMTP helper functions.
Functions: load_user_smtp_accounts, count_system_smtp_emails_sent,
           resolve_mailer_smtp_accounts_for_send, save_user_smtp_accounts,
           get_primary_user_smtp_account, ensure_user_mailer_smtp_ready
"""

import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# ---- 1. Fix function signatures: remove ", db_path: Path = DEFAULT_DB_PATH" ----

# Single-line signatures (inline): remove ", db_path: Path = DEFAULT_DB_PATH"
text = text.replace(
    ", db_path: Path = DEFAULT_DB_PATH) -> list[dict[str, Any]]:",
    ") -> list[dict[str, Any]]:",
)
text = text.replace(
    ", db_path: Path = DEFAULT_DB_PATH) -> int:",
    ") -> int:",
)
text = text.replace(
    ", db_path: Path = DEFAULT_DB_PATH) -> None:",
    ") -> None:",
)
text = text.replace(
    ", db_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:",
    ") -> dict[str, Any]:",
)

# resolve_mailer_smtp_accounts_for_send has db_path on its own line:
# "    db_path: Path = DEFAULT_DB_PATH,\n"
text = re.sub(
    r"(def resolve_mailer_smtp_accounts_for_send\([^)]*)\n    db_path: Path = DEFAULT_DB_PATH,\n",
    lambda m: m.group(0).replace("\n    db_path: Path = DEFAULT_DB_PATH,\n", "\n"),
    text,
)
# Simpler: just remove the line
text = re.sub(
    r"(\n    db_path: Path = DEFAULT_DB_PATH,\n)(?=\) -> dict\[str, Any\]:)",
    "\n",
    text,
)

# ---- 2. Remove db_path keyword arg from SMTP function callsites ---------------
# Pattern: ", db_path=VARNAME" or ", db_path=DEFAULT_DB_PATH" after these function names
SMTP_FUNCS = [
    "load_user_smtp_accounts",
    "count_system_smtp_emails_sent",
    "resolve_mailer_smtp_accounts_for_send",
    "save_user_smtp_accounts",
    "get_primary_user_smtp_account",
    "ensure_user_mailer_smtp_ready",
]

# For each SMTP function, remove db_path=VAR keyword argument from its calls.
# The keyword arg can appear:
# a) inline: "func(..., db_path=VAR)" → "func(...)"
# b) on its own line inside multi-line call (already handled by Batch 4)
#
# Since we process the whole file, we use a general "remove db_path=VAR" only within
# these specific function call contexts. Simplest approach: targeted regex per function.
for func_name in SMTP_FUNCS:
    # Remove inline ", db_path=VARNAME" before closing paren
    text = re.sub(
        rf"(\b{func_name}\([^)]*),\s*db_path\s*=\s*[A-Za-z_][A-Za-z0-9_]*(\s*\))",
        r"\1\2",
        text,
    )

SRC.write_text(text, encoding="utf-8")
print("Batch 5 transformations applied.")
