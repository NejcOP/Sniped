import argparse
import json
import logging
import os
import random
import re
import smtplib
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from openai import OpenAI

from scraper.db import init_db

MODEL_NAME = "gpt-4o-mini"
BANNED_SPAM_PHRASES = [
    "free",
    "guaranteed",
    "no cost",
    "urgent",
    "limited time",
    "earn money",
    "risk-free",
    "i hope this finds you well",
]
US_TIMEZONE_BY_STATE = {
    "AL": "America/Chicago",
    "AK": "America/Anchorage",
    "AZ": "America/Phoenix",
    "AR": "America/Chicago",
    "CA": "America/Los_Angeles",
    "CO": "America/Denver",
    "CT": "America/New_York",
    "DE": "America/New_York",
    "FL": "America/New_York",
    "GA": "America/New_York",
    "HI": "Pacific/Honolulu",
    "ID": "America/Denver",
    "IL": "America/Chicago",
    "IN": "America/Indiana/Indianapolis",
    "IA": "America/Chicago",
    "KS": "America/Chicago",
    "KY": "America/New_York",
    "LA": "America/Chicago",
    "ME": "America/New_York",
    "MD": "America/New_York",
    "MA": "America/New_York",
    "MI": "America/Detroit",
    "MN": "America/Chicago",
    "MS": "America/Chicago",
    "MO": "America/Chicago",
    "MT": "America/Denver",
    "NE": "America/Chicago",
    "NV": "America/Los_Angeles",
    "NH": "America/New_York",
    "NJ": "America/New_York",
    "NM": "America/Denver",
    "NY": "America/New_York",
    "NC": "America/New_York",
    "ND": "America/Chicago",
    "OH": "America/New_York",
    "OK": "America/Chicago",
    "OR": "America/Los_Angeles",
    "PA": "America/New_York",
    "RI": "America/New_York",
    "SC": "America/New_York",
    "SD": "America/Chicago",
    "TN": "America/Chicago",
    "TX": "America/Chicago",
    "UT": "America/Denver",
    "VT": "America/New_York",
    "VA": "America/New_York",
    "WA": "America/Los_Angeles",
    "WV": "America/New_York",
    "WI": "America/Chicago",
    "WY": "America/Denver",
    "DC": "America/New_York",
}
STATE_NAME_TO_CODE = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}


@dataclass
class SMTPAccount:
    email: str
    password: str
    host: str
    port: int
    use_tls: bool = True
    use_ssl: bool = False
    from_name: Optional[str] = None
    signature: Optional[str] = None


class AIMailer:
    def __init__(self, db_path: str = "leads.db", config_path: str = "config.json") -> None:
        self.db_path = db_path
        self.config_path = Path(config_path)
        init_db(db_path=self.db_path)
        self._ensure_mailer_columns()

        self.config = self._load_config()
        self.model_name = self.config.get("openai", {}).get("model", MODEL_NAME)
        api_key = os.environ.get("OPENAI_API_KEY") or self.config.get("openai", {}).get("api_key", "")
        if not api_key or api_key == "YOUR_OPENAI_API_KEY":
            raise ValueError("Set a valid OpenAI API key in config.json under openai.api_key.")

        self.client = OpenAI(api_key=api_key)
        self.accounts = self._load_accounts()
        self._next_account_index = 0
        self.used_openers: set[str] = set()

        warmup_cfg = self.config.get("warmup", {})
        self.warmup_enabled = bool(warmup_cfg.get("enabled", True))
        self.warmup_start_cap = int(warmup_cfg.get("start_cap", 5))
        self.warmup_daily_step = int(warmup_cfg.get("daily_step", 3))
        self.warmup_max_cap = int(warmup_cfg.get("max_cap", 40))
        self.subject_spintax_template = self.config.get(
            "subject_spintax_template",
            "{Hi|Hello|Hey} {Business Name} - {Subject}",
        )

    def send(self, limit: int = 10, delay_min: int = 400, delay_max: int = 900) -> tuple[int, int, int]:
        if delay_min < 0 or delay_max < 0 or delay_min > delay_max:
            raise ValueError("Delay range must satisfy 0 <= delay_min <= delay_max.")

        requested_limit = max(0, int(limit))
        if requested_limit == 0:
            logging.info("Requested limit is 0, nothing to send.")
            return 0, 0, 0

        daily_cap = self.compute_daily_cap()
        sent_today = self.get_sent_today_count()
        remaining_today = max(0, daily_cap - sent_today)
        effective_limit = min(requested_limit, remaining_today)

        if remaining_today <= 0:
            logging.info("Warm-up cap reached for today. cap=%s, sent_today=%s", daily_cap, sent_today)
            return 0, 0, 0

        logging.info(
            "Warm-up control: cap=%s, sent_today=%s, remaining=%s, requested=%s, effective=%s",
            daily_cap,
            sent_today,
            remaining_today,
            requested_limit,
            effective_limit,
        )

        leads = self._fetch_sendable_leads(limit=effective_limit)
        if not leads:
            logging.info("No sendable leads found. Ensure leads have e-mail addresses and are not already emailed.")
            return 0, 0, 0

        sent = 0
        skipped = 0
        failed = 0

        for idx, lead in enumerate(leads):
            if not self.is_within_business_hours(lead["address"]):
                logging.info("Skipping %s because local time is outside 09:00-17:00.", lead["business_name"])
                skipped += 1
                continue

            city = self.extract_city(lead["address"])
            website = self.normalize_website(lead["website_url"])
            shortcoming = self.build_shortcoming(
                website_url=lead["website_url"],
                rating=lead["rating"],
                main_shortcoming=lead["main_shortcoming"],
            )

            try:
                preferred_account = self.peek_next_account()
                subject, body = self.generate_email_with_guardrails(
                    business_name=lead["business_name"],
                    city=city,
                    website=website,
                    rating=lead["rating"],
                    main_shortcoming=shortcoming,
                    signature=preferred_account.signature,
                )
                sender_email = self.send_message(
                    recipient_email=lead["email"],
                    subject=subject,
                    body=body,
                )
                self.mark_emailed(lead_id=lead["id"], status="emailed", sender_email=sender_email)
                sent += 1
                logging.info("Sent mail to %s via %s", lead["email"], sender_email)
            except Exception as exc:
                failed += 1
                logging.exception("Failed sending to %s: %s", lead["email"], exc)
                self.mark_emailed(lead_id=lead["id"], status="failed", sender_email=None)
                continue

            if idx < len(leads) - 1:
                wait_seconds = self.compute_sleep_with_jitter(delay_min=delay_min, delay_max=delay_max)
                logging.info("Waiting %s seconds before next message.", wait_seconds)
                time.sleep(wait_seconds)

        return sent, skipped, failed

    def generate_email_with_guardrails(
        self,
        business_name: str,
        city: str,
        website: Optional[str],
        rating: Optional[float],
        main_shortcoming: str,
        signature: Optional[str],
    ) -> tuple[str, str]:
        for attempt in range(4):
            subject, body = self.generate_email(
                business_name=business_name,
                city=city,
                website=website,
                rating=rating,
                main_shortcoming=main_shortcoming,
                signature=signature,
            )

            body = self.enforce_max_sentences(body, max_sentences=4)
            body = self.ensure_signature(body, signature)
            opener_key = self.extract_opener_key(body)

            banned = self.contains_banned_spam_phrase(f"{subject} {body}")
            duplicate_opener = opener_key in self.used_openers

            if not banned and not duplicate_opener:
                if opener_key:
                    self.used_openers.add(opener_key)
                return subject, body

            logging.info(
                "Regenerating email due to guardrail breach. attempt=%s banned=%s duplicate_opener=%s",
                attempt + 1,
                banned or "none",
                duplicate_opener,
            )

        subject, body = self.generate_email(
            business_name=business_name,
            city=city,
            website=website,
            rating=rating,
            main_shortcoming=main_shortcoming,
            signature=signature,
        )
        body = self.ensure_signature(self.enforce_max_sentences(body, max_sentences=4), signature)
        opener_key = self.extract_opener_key(body)
        if opener_key:
            self.used_openers.add(opener_key)
        return subject, body

    def generate_email(
        self,
        business_name: str,
        city: str,
        website: Optional[str],
        rating: Optional[float],
        main_shortcoming: str,
        signature: Optional[str],
    ) -> tuple[str, str]:
        website_value = website or "None"
        rating_value = f"{rating:.1f}" if isinstance(rating, (int, float)) else "None"
        forbidden_words = ", ".join(BANNED_SPAM_PHRASES)
        recently_used_openers = "; ".join(sorted(self.used_openers)) or "none"
        signature_value = (signature or "").strip()

        system_prompt = (
            "You are a local business consultant. "
            "Write a casual, helpful email to a business owner. "
            "Use a helpful tone, not a sales tone. "
            "Mention a specific detail about their business (City or Rating) to prove you are a real person who looked at their profile."
        )
        user_prompt = f"""
Create a JSON object with keys subject and body.

Lead details:
- Business Name: {business_name}
- City: {city or 'Unknown'}
- Website: {website_value}
- Rating: {rating_value}
- Main Shortcoming: {main_shortcoming}
- Signature to include verbatim at the end:
{signature_value or '[No signature provided]'}

Requirements:
- Use American English.
- Keep body to 3-4 sentences maximum.
- Use a different opening sentence structure than previous emails.
- Never start with: "I hope this finds you well".
- Avoid these phrases entirely: {forbidden_words}
- Start by acknowledging positive local reputation or review signal.
- Mention one concrete detail (city or rating) and one specific shortcoming.
- Offer a short 15-minute audit call naturally.
- Include the exact signature block at the end of the body.
- Previously used opener signatures to avoid repeating: {recently_used_openers}
- Do not use markdown.
- Subject should be short and natural and may include spintax groups like {{Hi|Hello|Hey}}.
""".strip()

        response = self.client.chat.completions.create(
            model=self.model_name,
            temperature=0.7,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )

        content = response.choices[0].message.content or "{}"
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError(f"OpenAI returned invalid JSON: {content}") from exc

        subject = str(payload.get("subject", "Quick idea for your website")).strip()
        body = str(payload.get("body", "")).strip()
        if not body:
            raise ValueError("OpenAI returned an empty body.")

        subject = self.render_subject(subject=subject, business_name=business_name)

        return subject, body

    def send_message(self, recipient_email: str, subject: str, body: str) -> str:
        if not self.accounts:
            raise ValueError("No SMTP accounts configured in config.json.")

        last_error = None
        for offset in range(len(self.accounts)):
            account_index = (self._next_account_index + offset) % len(self.accounts)
            account = self.accounts[account_index]
            prepared_subject = subject
            prepared_body = self.ensure_signature(body, account.signature)

            message = EmailMessage()
            message["To"] = recipient_email
            message["Subject"] = prepared_subject
            message["From"] = self.format_from_header(account)
            message.set_content(prepared_body)

            try:
                self._send_via_account(account=account, message=message)
                self._next_account_index = (account_index + 1) % len(self.accounts)
                return account.email
            except Exception as exc:
                last_error = exc
                logging.warning("SMTP account %s failed: %s", account.email, exc)

        raise RuntimeError(f"All SMTP accounts failed. Last error: {last_error}")

    def _send_via_account(self, account: SMTPAccount, message: EmailMessage) -> None:
        if account.use_ssl or account.port == 465:
            with smtplib.SMTP_SSL(account.host, account.port, timeout=30) as smtp:
                smtp.login(account.email, account.password)
                smtp.send_message(message)
            return

        with smtplib.SMTP(account.host, account.port, timeout=30) as smtp:
            smtp.ehlo()
            if account.use_tls or account.port == 587:
                smtp.starttls()
                smtp.ehlo()
            smtp.login(account.email, account.password)
            smtp.send_message(message)

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with self.config_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _load_accounts(self) -> list[SMTPAccount]:
        raw_accounts = self.config.get("smtp_accounts", [])
        accounts = []
        for item in raw_accounts:
            accounts.append(
                SMTPAccount(
                    email=item["email"],
                    password=item["password"],
                    host=item["host"],
                    port=int(item["port"]),
                    use_tls=bool(item.get("use_tls", True)),
                    use_ssl=bool(item.get("use_ssl", False)),
                    from_name=item.get("from_name"),
                    signature=item.get("signature"),
                )
            )
        return accounts

    def _ensure_mailer_columns(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}

            if "status" not in columns:
                conn.execute("ALTER TABLE leads ADD COLUMN status TEXT")

            if "sent_at" not in columns:
                conn.execute("ALTER TABLE leads ADD COLUMN sent_at TEXT")

            if "last_sender_email" not in columns:
                conn.execute("ALTER TABLE leads ADD COLUMN last_sender_email TEXT")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mailer_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

            conn.commit()

    def _fetch_sendable_leads(self, limit: int) -> list[sqlite3.Row]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(
                """
                SELECT
                    id,
                    business_name,
                    email,
                    website_url,
                    rating,
                    main_shortcoming,
                    address
                FROM leads
                WHERE
                    email IS NOT NULL
                    AND email != ''
                    AND (status IS NULL OR status NOT IN ('emailed'))
                ORDER BY id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def mark_emailed(self, lead_id: int, status: str, sender_email: Optional[str]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            if status == "emailed":
                conn.execute(
                    """
                    UPDATE leads
                    SET
                        status = ?,
                        sent_at = CURRENT_TIMESTAMP,
                        last_sender_email = ?
                    WHERE id = ?
                    """,
                    (status, sender_email, lead_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE leads
                    SET
                        status = ?,
                        last_sender_email = ?
                    WHERE id = ?
                    """,
                    (status, sender_email, lead_id),
                )
            conn.commit()

    def peek_next_account(self) -> SMTPAccount:
        if not self.accounts:
            raise ValueError("No SMTP accounts configured in config.json.")
        return self.accounts[self._next_account_index % len(self.accounts)]

    @staticmethod
    def format_from_header(account: SMTPAccount) -> str:
        if account.from_name:
            return f"{account.from_name} <{account.email}>"
        return account.email

    @staticmethod
    def compute_sleep_with_jitter(delay_min: int, delay_max: int) -> int:
        base_delay = random.randint(delay_min, delay_max)
        jitter = random.randint(-max(15, delay_min // 8), max(30, delay_max // 7))
        return max(1, base_delay + jitter)

    def compute_daily_cap(self) -> int:
        if not self.warmup_enabled:
            return self.warmup_max_cap

        start_date = self.get_or_create_warmup_start_date()
        today = datetime.now().date()
        days_since = max(0, (today - start_date).days)
        return min(self.warmup_max_cap, self.warmup_start_cap + (days_since * self.warmup_daily_step))

    def get_sent_today_count(self) -> int:
        today = datetime.now().date().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status, '')) = 'emailed' AND DATE(sent_at) = ?",
                (today,),
            ).fetchone()
        return int(row[0] if row else 0)

    def get_or_create_warmup_start_date(self):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT value FROM mailer_meta WHERE key = 'warmup_start_date'"
            ).fetchone()

            if row and row[0]:
                return datetime.strptime(row[0], "%Y-%m-%d").date()

            today = datetime.now().date().isoformat()
            conn.execute(
                "INSERT OR REPLACE INTO mailer_meta (key, value) VALUES ('warmup_start_date', ?)",
                (today,),
            )
            conn.commit()
            return datetime.strptime(today, "%Y-%m-%d").date()

    @staticmethod
    def strip_known_signature_lines(text: str) -> str:
        lines = text.splitlines()
        while lines and not lines[-1].strip():
            lines.pop()
        return "\n".join(lines).strip()

    def ensure_signature(self, body: str, signature: Optional[str]) -> str:
        clean_body = self.strip_known_signature_lines(body)
        clean_signature = (signature or "").strip()
        if not clean_signature:
            return clean_body

        if clean_body.endswith(clean_signature):
            return clean_body

        return f"{clean_body}\n\n{clean_signature}"

    @staticmethod
    def split_sentences(text: str) -> list[str]:
        chunks = re.split(r"(?<=[.!?])\s+", text.strip())
        return [chunk.strip() for chunk in chunks if chunk.strip()]

    def enforce_max_sentences(self, body: str, max_sentences: int = 4) -> str:
        sentences = self.split_sentences(body)
        if len(sentences) <= max_sentences:
            return body.strip()
        return " ".join(sentences[:max_sentences]).strip()

    @staticmethod
    def extract_opener_key(body: str) -> str:
        sentences = re.split(r"[.!?]", body.strip())
        first = sentences[0].strip().lower() if sentences else ""
        words = [word for word in re.split(r"\s+", first) if word]
        return " ".join(words[:6])

    @staticmethod
    def contains_banned_spam_phrase(text: str) -> Optional[str]:
        haystack = (text or "").lower()
        for phrase in BANNED_SPAM_PHRASES:
            pattern = r"\b" + r"\s+".join(re.escape(part) for part in phrase.split()) + r"\b"
            if re.search(pattern, haystack, re.IGNORECASE):
                return phrase
        return None

    @staticmethod
    def render_spintax(template: str) -> str:
        if not template:
            return ""

        rendered = template
        for _ in range(20):
            match = re.search(r"\{([^{}]+)\}", rendered)
            if not match:
                break
            options = [piece.strip() for piece in match.group(1).split("|") if piece.strip()]
            replacement = random.choice(options) if options else ""
            rendered = rendered[: match.start()] + replacement + rendered[match.end() :]
        return rendered

    def render_subject(self, subject: str, business_name: str) -> str:
        subject_base = self.render_spintax(subject or "Quick idea")
        subject_base = subject_base.replace("{Business Name}", business_name or "your business")

        template = (self.subject_spintax_template or "").strip()
        if template:
            composed = template.replace("{Business Name}", business_name or "your business")
            composed = composed.replace("{Subject}", subject_base)
            subject_base = self.render_spintax(composed)

        return re.sub(r"\s+", " ", subject_base).strip()[:150]

    @staticmethod
    def normalize_website(website_url: Optional[str]) -> Optional[str]:
        if not website_url:
            return None
        cleaned = str(website_url).strip()
        if not cleaned or cleaned.lower() == "none":
            return None
        return cleaned

    @staticmethod
    def extract_city(address: Optional[str]) -> str:
        if not address:
            return ""
        parts = [part.strip() for part in str(address).split(",") if part.strip()]
        if len(parts) >= 3:
            return parts[-3]
        if len(parts) >= 2:
            return parts[-2]
        return parts[0] if parts else ""

    @staticmethod
    def build_shortcoming(
        website_url: Optional[str],
        rating: Optional[float],
        main_shortcoming: Optional[str],
    ) -> str:
        if main_shortcoming:
            return str(main_shortcoming)
        if not website_url or str(website_url).strip().lower() == "none":
            return "Missing website"
        if rating is not None and rating < 3.5:
            return f"Low Google rating ({rating:.1f})"
        return "Limited website conversion opportunities"

    @classmethod
    def infer_timezone(cls, address: Optional[str]) -> str:
        if not address:
            return "America/New_York"

        parts = [part.strip() for part in str(address).split(",") if part.strip()]
        for part in reversed(parts):
            tokens = part.replace(".", " ").split()
            for token in tokens:
                code = token.upper()
                if code in US_TIMEZONE_BY_STATE:
                    return US_TIMEZONE_BY_STATE[code]

            lowered = part.lower()
            for state_name, state_code in STATE_NAME_TO_CODE.items():
                if state_name in lowered:
                    return US_TIMEZONE_BY_STATE[state_code]

        return "America/New_York"

    @classmethod
    def is_within_business_hours(cls, address: Optional[str]) -> bool:
        timezone_name = cls.infer_timezone(address)
        local_now = datetime.now(ZoneInfo(timezone_name))
        return 9 <= local_now.hour < 17


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AI-personalized cold email sender with OpenAI and rotating SMTP accounts."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging verbosity.",
    )
    parser.add_argument(
        "--log-file",
        default="logs/mailer.log",
        help="Path to log file.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    send_cmd = subparsers.add_parser("send", help="Generate and send AI-personalized emails.")
    send_cmd.add_argument("--db", default="leads.db", help="SQLite DB path.")
    send_cmd.add_argument("--config", default="config.json", help="JSON config path.")
    send_cmd.add_argument("--limit", type=int, default=10, help="Maximum number of leads to process.")
    send_cmd.add_argument("--delay-min", type=int, default=400, help="Minimum delay between emails in seconds.")
    send_cmd.add_argument("--delay-max", type=int, default=900, help="Maximum delay between emails in seconds.")

    return parser


def setup_logging(log_level: str, log_file: str) -> None:
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
        force=True,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    setup_logging(log_level=args.log_level, log_file=args.log_file)

    if args.command == "send":
        mailer = AIMailer(db_path=args.db, config_path=args.config)
        sent, skipped, failed = mailer.send(
            limit=args.limit,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
        )
        print(f"Sent: {sent}")
        print(f"Skipped (outside hours): {skipped}")
        print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
