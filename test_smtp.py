import smtplib, ssl, json

with open("config.json") as f:
    cfg = json.load(f)

accounts = cfg.get("smtp_accounts", [])

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
    except Exception as e:
        print(f"  [FAIL] {email}: {e}")
