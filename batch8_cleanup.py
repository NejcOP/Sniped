"""
Batch 8: Remove db_path Optional param from credit helper functions.
These all have db_path: Optional[Path] = None (last or near-last positional param).
"""

import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# ---- 1. Fix multi-line function signature: _append_credit_log ----
# The signature is multi-line ending with:  db_path: Optional[Path] = None,\n) -> None:
text = re.sub(
    r"(\ndef _append_credit_log\(.*?),\n    db_path: Optional\[Path\] = None,\n\) -> None:",
    r"\1,\n) -> None:",
    text,
    flags=re.DOTALL,
)

# ---- 2. Fix body of _append_credit_log: remove target_db_path line and use pgdb.connect() ----
text = text.replace(
    "    target_db_path = db_path or DEFAULT_DB_PATH\n    ensure_credit_logs_table()\n    with pgdb.connect(target_db_path) as conn:",
    "    ensure_credit_logs_table()\n    with pgdb.connect() as conn:",
)

# ---- 3. Fix multi-line signature: deduct_credits_on_success ----
text = re.sub(
    r"(def deduct_credits_on_success\(\n    user_id: str,\n    credits_to_deduct: int = 1,\n)    db_path: Optional\[Path\] = None,\n",
    r"\1",
    text,
)

# ---- 4. Fix single-line signatures with db_path: Optional[Path] = None ----
OPTIONAL_SIGS = [
    ("def _load_user_credit_snapshot(user_id: str, db_path: Optional[Path] = None)",
     "def _load_user_credit_snapshot(user_id: str)"),
    ("def has_enough_credits(user_id: str, required_credits: int = 1, db_path: Optional[Path] = None)",
     "def has_enough_credits(user_id: str, required_credits: int = 1)"),
    ("def reserve_ai_credits_or_raise(user_id: str, feature_key: str, db_path: Optional[Path] = None)",
     "def reserve_ai_credits_or_raise(user_id: str, feature_key: str)"),
    ("def charge_ai_credits_after_success(user_id: str, feature_key: str, db_path: Optional[Path] = None)",
     "def charge_ai_credits_after_success(user_id: str, feature_key: str)"),
]
for old, new in OPTIONAL_SIGS:
    text = text.replace(old, new)

# run_ai_with_credit_policy multi-line signature
text = re.sub(
    r"(def run_ai_with_credit_policy\(\n    user_id: str,\n    feature_key: str,\n    generate_fn: Callable\[\[\], Any\],\n)    db_path: Optional\[Path\] = None,\n",
    r"\1",
    text,
)

# ---- 5. Remove db_path= keyword arg callsites for these functions ----
# Internal calls: _load_user_credit_snapshot(target_user_id, db_path=db_path)
# reserve_ai_credits_or_raise(user_id, feature_key=feature_key, db_path=db_path)
# charge_ai_credits_after_success(user_id, feature_key=feature_key, db_path=db_path)
# has_enough_credits(..., db_path=...) etc.

# Remove trailing ", db_path=<expr>)" patterns
text = re.sub(r",\s*db_path\s*=\s*[A-Za-z_][A-Za-z0-9_]*\s*\)", ")", text)
# Remove trailing ", db_path=<expr>," patterns (mid-arg)
text = re.sub(r",\s*db_path\s*=\s*[A-Za-z_][A-Za-z0-9_]*,", ",", text)

import sys
print("Batch 8 transformations applied.")
