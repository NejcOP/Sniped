"""
Final pass: Remove all remaining db_path= keyword arguments from callsites.
"""
import re
from pathlib import Path

SRC = Path("backend/app.py")
text = SRC.read_text(encoding="utf-8")

# Remove trailing keyword arg: , db_path=EXPR) — EXPR can be any identifier
text = re.sub(r",\s*db_path\s*=\s*[A-Za-z_][A-Za-z0-9_]*\s*\)", ")", text)
# Remove mid-arg keyword: , db_path=EXPR, — followed by more args
text = re.sub(r",\s*db_path\s*=\s*[A-Za-z_][A-Za-z0-9_]*\s*,", ",", text)
# Also handle newline before closing paren: ,\n    db_path=EXPR\n)
text = re.sub(r",\s*\n\s*db_path\s*=\s*[A-Za-z_][A-Za-z0-9_]*\s*\n\)", "\n)", text)

SRC.write_text(text, encoding="utf-8")
print("Final db_path kwarg callsite cleanup done.")
