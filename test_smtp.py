import os
import smtplib
import ssl
import sys

sys.path.insert(0, "backend")

from app import load_user_smtp_accounts

def main() -> int:
    user_id = str(os.environ.get("SMTP_TEST_USER_ID") or "").strip()
    session_token = str(os.environ.get("SMTP_TEST_SESSION_TOKEN") or "").strip()
    accounts = load_user_smtp_accounts(user_id=user_id or None, session_token=session_token or None)

    if not accounts:
        print("No SMTP accounts found. Set SMTP_TEST_USER_ID or SMTP_TEST_SESSION_TOKEN.")
        return 1

    for acc in accounts:
        email = acc.get("email")
        password = acc.get("password", "")
        host = acc.get("host", "smtp.gmail.com")
        port = acc.get("port", 587)

        if not password:
            print(f"[SKIP] {email} - no password set")
            continue

        print(f"Testing {email} via {host}:{port}...")
        try:
            ctx = ssl.create_default_context()
            with smtplib.SMTP(host, port, timeout=15) as smtp:
                smtp.ehlo()
                smtp.starttls(context=ctx)
                smtp.ehlo()
                smtp.login(email, password)
                print(f"  [OK] Login successful for {email}")
        except Exception as exc:
            print(f"  [FAIL] {email}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
