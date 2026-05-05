"""
Batch 3: Remove db_path from mailer campaign functions + reset_due_monthly_credits.
Functions: create_mailer_campaign_sequence, list_mailer_campaign_sequences,
           auth_email_exists (remove db_path keyword arg), create_saved_mail_template,
           list_saved_mail_templates, record_mailer_campaign_event, get_mailer_campaign_stats,
           reset_due_monthly_credits
"""

import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# ---- 1. Function signatures -------------------------------------------------------

# Simple: def func(db_path: Path, user_id: ...) → def func(user_id: ...)
simple_sigs = [
    ("def create_mailer_campaign_sequence(db_path: Path, user_id:", "def create_mailer_campaign_sequence(user_id:"),
    ("def list_mailer_campaign_sequences(db_path: Path, user_id:", "def list_mailer_campaign_sequences(user_id:"),
    ("def create_saved_mail_template(db_path: Path, user_id:", "def create_saved_mail_template(user_id:"),
    ("def list_saved_mail_templates(db_path: Path, user_id:", "def list_saved_mail_templates(user_id:"),
    ("def record_mailer_campaign_event(db_path: Path, user_id:", "def record_mailer_campaign_event(user_id:"),
    ("def get_mailer_campaign_stats(db_path: Path, user_id:", "def get_mailer_campaign_stats(user_id:"),
    ("def reset_due_monthly_credits(db_path: Path, config_path:", "def reset_due_monthly_credits(config_path:"),
]
for old, new in simple_sigs:
    text = text.replace(old, new)

# auth_email_exists: remove keyword-only db_path param with default
text = text.replace(
    "def auth_email_exists(email: str, *, config_path: Path = DEFAULT_CONFIG_PATH, db_path: Path = DEFAULT_DB_PATH) -> bool:",
    "def auth_email_exists(email: str, *, config_path: Path = DEFAULT_CONFIG_PATH) -> bool:",
)

# ---- 2. Internal calls within get_mailer_campaign_stats --------------------------
text = text.replace(
    '"sequences": list_mailer_campaign_sequences(db_path, user_id=user_id),',
    '"sequences": list_mailer_campaign_sequences(user_id=user_id),',
)
text = text.replace(
    '"saved_templates": list_saved_mail_templates(db_path, user_id=user_id),',
    '"saved_templates": list_saved_mail_templates(user_id=user_id),',
)

# ---- 3. Update all callsites -----------------------------------------------------

# reset_due_monthly_credits(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)
text = text.replace(
    "reset_due_monthly_credits(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)",
    "reset_due_monthly_credits(DEFAULT_CONFIG_PATH)",
)

# list_mailer_campaign_sequences(DEFAULT_DB_PATH, user_id=...) → list_mailer_campaign_sequences(user_id=...)
text = re.sub(
    r"\blist_mailer_campaign_sequences\(DEFAULT_DB_PATH,\s*",
    "list_mailer_campaign_sequences(",
    text,
)
text = re.sub(
    r"\blist_mailer_campaign_sequences\(db_path,\s*",
    "list_mailer_campaign_sequences(",
    text,
)

# create_mailer_campaign_sequence(DEFAULT_DB_PATH, user_id=..., ...) → create_mailer_campaign_sequence(user_id=..., ...)
text = re.sub(
    r"\bcreate_mailer_campaign_sequence\(DEFAULT_DB_PATH,\s*",
    "create_mailer_campaign_sequence(",
    text,
)
text = re.sub(
    r"\bcreate_mailer_campaign_sequence\(db_path,\s*",
    "create_mailer_campaign_sequence(",
    text,
)

# list_saved_mail_templates
text = re.sub(
    r"\blist_saved_mail_templates\(DEFAULT_DB_PATH,\s*",
    "list_saved_mail_templates(",
    text,
)
text = re.sub(
    r"\blist_saved_mail_templates\(db_path,\s*",
    "list_saved_mail_templates(",
    text,
)

# create_saved_mail_template
text = re.sub(
    r"\bcreate_saved_mail_template\(DEFAULT_DB_PATH,\s*",
    "create_saved_mail_template(",
    text,
)
text = re.sub(
    r"\bcreate_saved_mail_template\(db_path,\s*",
    "create_saved_mail_template(",
    text,
)

# record_mailer_campaign_event(DEFAULT_DB_PATH, user_id, ...) — positional
text = re.sub(
    r"\brecord_mailer_campaign_event\(DEFAULT_DB_PATH,\s*",
    "record_mailer_campaign_event(",
    text,
)
text = re.sub(
    r"\brecord_mailer_campaign_event\(db_path,\s*",
    "record_mailer_campaign_event(",
    text,
)

# get_mailer_campaign_stats(DEFAULT_DB_PATH, user_id=...)
text = re.sub(
    r"\bget_mailer_campaign_stats\(DEFAULT_DB_PATH,\s*",
    "get_mailer_campaign_stats(",
    text,
)
text = re.sub(
    r"\bget_mailer_campaign_stats\(db_path,\s*",
    "get_mailer_campaign_stats(",
    text,
)

# auth_email_exists: remove db_path=DEFAULT_DB_PATH keyword arg
# Pattern: auth_email_exists(email, config_path=..., db_path=DEFAULT_DB_PATH)
text = re.sub(
    r"\b(auth_email_exists\([^)]*),\s*db_path\s*=\s*DEFAULT_DB_PATH\s*(\))",
    r"\1\2",
    text,
)

SRC.write_text(text, encoding="utf-8")
print("Batch 3 transformations applied.")
