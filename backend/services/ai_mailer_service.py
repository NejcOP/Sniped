import argparse
import html
import json
import logging
import os
import random
import re
import secrets
import smtplib
import time
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from threading import Event
from typing import Any, Optional
from urllib.parse import quote, urlparse
from zoneinfo import ZoneInfo

from openai import OpenAI
from sqlalchemy import bindparam, text

from backend.services.prompt_service import PromptFactory
from backend.scraper.db import get_engine, init_db

FORCED_AI_MODEL = "gpt-4o-mini"   # hardcoded — no other models allowed
_AI_CALL_TIMEOUT = 15.0            # 15s timeout on every call
FOLLOW_UP_DELAY_DAYS = 3
ALLOWED_SENDING_STRATEGIES = {"round_robin", "random"}
TEST_LEAD_BLOCKED_DOMAINS = {
    "test.com",
    "invalid.com",
    "example.com",
    "example.org",
    "example.net",
    "example",
    "invalid",
    "test",
    "localhost",
}

# In-memory niche cache  {business_name_lower: niche_str}
_NICHE_CACHE: dict[str, str] = {}


def _clean_for_ai(text: str) -> str:
    """Regex-only: strip <script> blocks, HTML tags, collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()
DEFAULT_GHOST_SUBJECT_TEMPLATE = "question about {BusinessName}"
DEFAULT_GHOST_BODY_TEMPLATE = (
    "Hi,\n\n"
    "I was looking for your services in {City} today but couldn't find a website for {BusinessName} anywhere.\n\n"
    "Since most people search on their phones now, you're likely losing dozens of high-ticket jobs every month to the few guys who actually show up on the map.\n\n"
    "I build high-converting landing pages that get businesses online and ranking in under 48 hours.\n\n"
    "If helpful, I can send over a 2-minute video showing exactly what I’d build first. Would you be against me sending it?\n\n"
    "Best, {YourName}"
)
DEFAULT_GOLDEN_SUBJECT_TEMPLATE = "question about {BusinessName}'s local traffic"
DEFAULT_GOLDEN_BODY_TEMPLATE = (
    "Hi,\n\n"
    "I was looking at your website for {BusinessName} and it actually looks great. However, I noticed you're not currently appearing in the \"Sponsored\" section for {Niche} in {City}.\n\n"
    "That means nearby buyers are seeing competitors first even when your offer looks stronger.\n\n"
    "I've already mapped out a quick strategy to reclaim that traffic for {BusinessName}. If useful, I can send over a 2-minute breakdown with the exact gaps and fixes.\n\n"
    "Best, {YourName}"
)
DEFAULT_COMPETITOR_SUBJECT_TEMPLATE = "{BusinessName} - quick question"
DEFAULT_COMPETITOR_BODY_TEMPLATE = (
    "Hi,\n\n"
    "I noticed that your main competitors are currently taking up most of the top spots on Google for {Niche} in {City}, even though you have better local signals.\n\n"
    "The main reason is that your site is missing a few key SEO tags and a tracking pixel, so Google is essentially \"hiding\" you from new customers.\n\n"
    "My team and I help businesses reclaim those top spots and turn that traffic into actual booked jobs.\n\n"
    "If it helps, I can send over a short 2-minute video with the exact fixes I’d start with. Would you be against me sending it?\n\n"
    "Best, {YourName}"
)
DEFAULT_SPEED_SUBJECT_TEMPLATE = "{BusinessName} // quick question"
DEFAULT_SPEED_BODY_TEMPLATE = (
    "Hi,\n\n"
    "I was checking out {BusinessName}'s site and ran a quick speed test - it's loading slow enough on mobile that Google is likely penalizing your ranking for it.\n\n"
    "For {Niche} businesses in {City}, a slow site typically means Google drops you below competitors with faster pages, even if your reviews are better.\n\n"
    "I fix this for local service businesses - usually takes less than a week and the ranking bump shows up within 30 days.\n\n"
    "If useful, I can send over a 2-minute video showing exactly what's slowing things down and how I’d fix it.\n\n"
    "Best, {YourName}"
)
STOPPED_AUTOMATION_STATUSES = {
    "blacklisted",
    "closed",
    "interested",
    "invalid_email",
    "low_priority",
    "meeting set",
    "paid",
    "qualified_not_interested",
    "replied",
    "skipped (test lead)",
    "skipped (unsubscribed)",
    "zoom scheduled",
}
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
    def __init__(self, db_path: str = "runtime-db", config_path: str = "env", model_name_override: Optional[str] = None, user_id: Optional[str] = None, smtp_accounts_override: Optional[list[dict]] = None) -> None:
        self.db_path = db_path
        self.config_path = Path(config_path)
        self.user_id = str(user_id).strip() if user_id is not None and str(user_id).strip() else None
        init_db(db_path=self.db_path)
        self._ensure_mailer_columns()

        self.config = self._load_config()
        self.smtp_accounts_override = list(smtp_accounts_override) if smtp_accounts_override is not None else None
        if smtp_accounts_override is not None:
            self.config["smtp_accounts"] = list(smtp_accounts_override)
        self.model_name = str(model_name_override or FORCED_AI_MODEL).strip() or FORCED_AI_MODEL
        api_key = os.environ.get("OPENAI_API_KEY") or self.config.get("openai", {}).get("api_key", "")
        if not api_key or api_key == "YOUR_OPENAI_API_KEY":
            raise ValueError("Set a valid OPENAI_API_KEY environment variable.")

        self.client = OpenAI(api_key=api_key)
        self.accounts = self._load_accounts()
        self.last_send_summary: dict[str, int] = {
            "requested_limit": 0,
            "effective_limit": 0,
            "daily_cap": 0,
            "sent_today": 0,
            "remaining_today": 0,
            "candidate_count": 0,
        }

    def _fetchall(self, statement, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        with get_engine().begin() as conn:
            return [dict(row) for row in conn.execute(statement, params or {}).mappings().all()]

    def _fetchone(self, statement, params: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
        with get_engine().begin() as conn:
            row = conn.execute(statement, params or {}).mappings().first()
        return dict(row) if row is not None else None

    def _execute(self, statement, params: Optional[dict[str, Any]] = None) -> None:
        with get_engine().begin() as conn:
            conn.execute(statement, params or {})

    @staticmethod
    def _deserialize_json_payload(raw_value: Any) -> Any:
        if isinstance(raw_value, (dict, list)):
            return raw_value
        text_value = str(raw_value or "").strip()
        if not text_value:
            return None
        try:
            return json.loads(text_value)
        except Exception:
            return None

    def _load_user_settings_smtp_accounts(self) -> list[dict[str, Any]]:
        if not self.user_id:
            return []

        user_settings_sql = text(
            """
            SELECT smtp_accounts_json
            FROM user_settings
            WHERE user_id = :user_id
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """
        )
        try:
            row = self._fetchone(user_settings_sql, {"user_id": self.user_id})
            payload = self._deserialize_json_payload((row or {}).get("smtp_accounts_json"))
            if isinstance(payload, list):
                return [dict(item) for item in payload if isinstance(item, dict)]
        except Exception as exc:
            logging.warning("user_settings SMTP lookup failed for %s: %s", self.user_id, exc)

        try:
            row = self._fetchone(
                text("SELECT smtp_accounts_json FROM users WHERE id::text = :user_id LIMIT 1"),
                {"user_id": self.user_id},
            )
            payload = self._deserialize_json_payload((row or {}).get("smtp_accounts_json"))
            if isinstance(payload, list):
                return [dict(item) for item in payload if isinstance(item, dict)]
        except Exception as exc:
            logging.warning("users SMTP lookup fallback failed for %s: %s", self.user_id, exc)
        return []
        self._next_account_index = 0
        self.used_openers: set[str] = set()
        strategy_raw = str((self.config.get("mailer", {}) or {}).get("sending_strategy", "round_robin") or "round_robin")
        self.sending_strategy = strategy_raw.strip().lower().replace("-", "_")
        if self.sending_strategy not in ALLOWED_SENDING_STRATEGIES:
            self.sending_strategy = "round_robin"

        warmup_cfg = self.config.get("warmup", {})
        self.warmup_enabled = bool(warmup_cfg.get("enabled", False))
        self.warmup_start_cap = int(warmup_cfg.get("start_cap", 5))
        self.warmup_daily_step = int(warmup_cfg.get("daily_step", 3))
        self.warmup_max_cap = int(warmup_cfg.get("max_cap", 40))
        self.enforce_business_hours = bool((self.config.get("mailer", {}) or {}).get("enforce_business_hours", False))
        self.mail_signature = str(self.config.get("mail_signature", "") or "").strip()
        self.ghost_subject_template = str(self.config.get("ghost_subject_template", DEFAULT_GHOST_SUBJECT_TEMPLATE) or DEFAULT_GHOST_SUBJECT_TEMPLATE).strip()
        self.ghost_body_template = str(self.config.get("ghost_body_template", DEFAULT_GHOST_BODY_TEMPLATE) or DEFAULT_GHOST_BODY_TEMPLATE).strip()
        self.golden_subject_template = str(self.config.get("golden_subject_template", DEFAULT_GOLDEN_SUBJECT_TEMPLATE) or DEFAULT_GOLDEN_SUBJECT_TEMPLATE).strip()
        self.golden_body_template = str(self.config.get("golden_body_template", DEFAULT_GOLDEN_BODY_TEMPLATE) or DEFAULT_GOLDEN_BODY_TEMPLATE).strip()
        self.competitor_subject_template = str(self.config.get("competitor_subject_template", DEFAULT_COMPETITOR_SUBJECT_TEMPLATE) or DEFAULT_COMPETITOR_SUBJECT_TEMPLATE).strip()
        self.competitor_body_template = str(self.config.get("competitor_body_template", DEFAULT_COMPETITOR_BODY_TEMPLATE) or DEFAULT_COMPETITOR_BODY_TEMPLATE).strip()
        self.speed_subject_template = str(self.config.get("speed_subject_template", DEFAULT_SPEED_SUBJECT_TEMPLATE) or DEFAULT_SPEED_SUBJECT_TEMPLATE).strip()
        self.speed_body_template = str(self.config.get("speed_body_template", DEFAULT_SPEED_BODY_TEMPLATE) or DEFAULT_SPEED_BODY_TEMPLATE).strip()
        self.open_tracking_base_url = str(
            os.environ.get("OPEN_TRACKING_BASE_URL")
            or self.config.get("open_tracking_base_url", "")
            or ""
        ).strip().rstrip("/")

    def send(
        self,
        limit: int = 10,
        delay_min: int = 400,
        delay_max: int = 900,
        status_allowlist: Optional[list[str]] = None,
        stop_event: Optional[Event] = None,
        progress_callback=None,
    ) -> tuple[int, int, int]:
        if delay_min < 0 or delay_max < 0 or delay_min > delay_max:
            raise ValueError("Delay range must satisfy 0 <= delay_min <= delay_max.")

        requested_limit = max(0, int(limit))
        if requested_limit == 0:
            logging.info("Requested limit is 0, nothing to send.")
            self.last_send_summary = {
                "requested_limit": 0,
                "effective_limit": 0,
                "daily_cap": 0,
                "sent_today": 0,
                "remaining_today": 0,
                "candidate_count": 0,
            }
            return 0, 0, 0

        daily_cap = self.compute_daily_cap()
        sent_today = self.get_sent_today_count()
        remaining_today = max(0, daily_cap - sent_today)
        effective_limit = min(requested_limit, remaining_today)
        candidate_limit = max(effective_limit * 5, effective_limit + 25)

        if remaining_today <= 0:
            logging.info("Warm-up cap reached for today. cap=%s, sent_today=%s", daily_cap, sent_today)
            self.last_send_summary = {
                "requested_limit": requested_limit,
                "effective_limit": 0,
                "daily_cap": daily_cap,
                "sent_today": sent_today,
                "remaining_today": remaining_today,
                "candidate_count": 0,
            }
            return 0, 0, 0

        logging.info(
            "Warm-up control: cap=%s, sent_today=%s, remaining=%s, requested=%s, effective=%s",
            daily_cap,
            sent_today,
            remaining_today,
            requested_limit,
            effective_limit,
        )

        leads = self._fetch_sendable_leads(limit=candidate_limit, status_allowlist=status_allowlist)
        self.last_send_summary = {
            "requested_limit": requested_limit,
            "effective_limit": effective_limit,
            "daily_cap": daily_cap,
            "sent_today": sent_today,
            "remaining_today": remaining_today,
            "candidate_count": len(leads),
        }
        if not leads:
            logging.info("No sendable leads found for current filter.")
            return 0, 0, 0

        sent = 0
        skipped = 0
        failed = 0
        campaign_sequence = self.get_active_campaign_sequence()

        for idx, lead in enumerate(leads):
            if sent >= effective_limit:
                break
            if stop_event is not None and stop_event.is_set():
                logging.info("Emergency stop requested — halting mailer after %d sent.", sent)
                break

            step_index = self.resolve_campaign_step(lead, campaign_sequence)
            if step_index is None:
                continue

            if self.is_blacklisted(lead["email"], lead["website_url"]):
                logging.info("Skipping unsubscribed lead %s (%s).", lead["business_name"], lead["email"])
                skipped += 1
                self.mark_emailed(
                    lead_id=lead["id"],
                    status="Skipped (Unsubscribed)",
                    sender_email=None,
                    generated_email_body=None,
                )
                continue

            if self.is_test_lead(lead):
                logging.info("Skipping test lead %s (%s).", lead["business_name"], lead["email"])
                skipped += 1
                self.mark_emailed(
                    lead_id=lead["id"],
                    status="Skipped (Test Lead)",
                    sender_email=None,
                    generated_email_body=None,
                )
                continue

            if self.enforce_business_hours and not self.is_within_business_hours(lead["address"]):
                logging.info("Skipping %s because local time is outside 09:00-17:00.", lead["business_name"])
                skipped += 1
                continue

            city = self.extract_city(lead["address"])
            location = city or str(lead["address"] or "").strip() or "your area"
            website = self.normalize_website(lead["website_url"])
            is_ghost_business = not website
            shortcoming = self.build_shortcoming(
                website_url=lead["website_url"],
                rating=lead["rating"],
                main_shortcoming=lead["main_shortcoming"],
            )
            ai_description = str((lead["ai_description"] if "ai_description" in lead.keys() else "") or "").strip()
            score_value = float(lead["ai_score"] if "ai_score" in lead.keys() else 0.0)

            # Prefer persisted competitive_hook, then fall back to enrichment_data JSON for compatibility.
            competitive_hook = str((lead["competitive_hook"] if "competitive_hook" in lead.keys() else "") or "").strip()
            if not competitive_hook:
                try:
                    enc = lead["enrichment_data"] if "enrichment_data" in lead.keys() else None
                    if enc:
                        competitive_hook = str(json.loads(enc).get("competitive_hook", "") or "").strip()
                except (json.JSONDecodeError, AttributeError, TypeError):
                    pass
            is_golden = score_value >= 9.0
            if is_golden:
                logging.info("Sending high-priority template mail to %s (score>=8.5).", lead["business_name"])

            try:
                preferred_account = self.peek_next_account()
                ab_variant = None
                subject_override = None
                if step_index == 1:
                    ab_variant, subject_override = self.select_ab_subject_variant(int(lead["id"]), campaign_sequence)

                subject, body = self.build_template_email(
                    lead=lead,
                    city=city,
                    score=score_value,
                    website=website,
                    account=preferred_account,
                    sequence=campaign_sequence,
                    step_index=step_index,
                    subject_override=subject_override,
                )

                if not str(body or "").strip():
                    raise ValueError(f"Template body is empty for lead id={lead['id']}")

                if not str(subject or "").strip():
                    raise ValueError(f"Template subject is empty for lead id={lead['id']}")

                sender_email = self.send_message(
                    recipient_email=lead["email"],
                    subject=subject,
                    body=body,
                    account=preferred_account,
                    lead_id=int(lead["id"]),
                )
                self.mark_emailed(
                    lead_id=lead["id"],
                    status="emailed",
                    sender_email=sender_email,
                    generated_email_body=body,
                    is_follow_up=step_index > 1,
                    subject_line=subject,
                    ab_variant=ab_variant,
                    campaign_sequence_id=int(campaign_sequence.get("id")) if campaign_sequence and campaign_sequence.get("id") else None,
                    campaign_step=step_index,
                )
                sent += 1
                logging.info(
                    "Sent template mail to %s via %s",
                    lead["email"],
                    sender_email,
                )
            except Exception as exc:
                failed += 1
                logging.exception("Failed sending to %s: %s", lead["email"], exc)
                self.mark_emailed(
                    lead_id=lead["id"],
                    status="failed",
                    sender_email=None,
                    generated_email_body=None,
                )

            if progress_callback is not None:
                try:
                    progress_callback(sent, skipped, failed)
                except Exception:
                    pass

            if idx < len(leads) - 1:
                wait_seconds = self.compute_sleep_with_jitter(delay_min=delay_min, delay_max=delay_max)
                logging.info("Waiting %s seconds before next message.", wait_seconds)
                if stop_event is not None:
                    if stop_event.wait(timeout=wait_seconds):
                        logging.info("Emergency stop requested during delay wait — halting mailer after %d sent.", sent)
                        break
                else:
                    time.sleep(wait_seconds)

        return sent, skipped, failed

    def generate_hyper_personalized_email(
        self,
        business_name: str,
        location: str,
        ai_description: str,
        competitive_hook: str,
        signature: Optional[str],
    ) -> tuple[str, str]:
        raise RuntimeError("Legacy generator disabled: use strict 3-template flow.")

    def generate_ghost_email(
        self,
        business_name: str,
        city: str,
        signature: Optional[str],
    ) -> tuple[str, str]:
        raise RuntimeError("Legacy generator disabled: use strict 3-template flow.")

    def build_safe_fallback_email(
        self,
        business_name: str,
        location: str,
        issue: str,
        signature: Optional[str],
        is_ghost_business: bool = False,
    ) -> tuple[str, str]:
        raise RuntimeError("Fallback templates disabled: use strict 3-template flow.")

    def generate_email_with_guardrails(
        self,
        business_name: str,
        city: str,
        website: Optional[str],
        rating: Optional[float],
        main_shortcoming: str,
        signature: Optional[str],
        competitive_hook: str = "",
        is_golden: bool = False,
    ) -> tuple[str, str]:
        raise RuntimeError("Legacy generator disabled: use strict 3-template flow.")

    def generate_email(
        self,
        business_name: str,
        city: str,
        website: Optional[str],
        rating: Optional[float],
        main_shortcoming: str,
        signature: Optional[str],
        competitive_hook: str = "",
        is_golden: bool = False,
    ) -> tuple[str, str]:
        raise RuntimeError("Legacy generator disabled: use strict 3-template flow.")

    def build_follow_up_email(
        self,
        business_name: str,
        city: str,
        competitive_hook: str,
        signature: Optional[str],
    ) -> tuple[str, str]:
        raise RuntimeError("Follow-up generator disabled: use strict 3-template flow.")

    def generate_cold_outreach_email(
        self,
        business_name: str,
        city: str,
        niche: str = "",
        pain_point: str = "",
        competitors: Optional[list] = None,
        monthly_loss: str = "",
        website_content: str = "",
        linkedin_data: str = "",
        user_defined_icp: str = "",
    ) -> tuple[str, str]:
        """
        World-Class Cold Outreach Specialist generator.
        Rules enforced via prompt:
          - Subject: max 4 words, sounds like an internal question
          - Hook: proves research (city, real competitors mentioned)
          - Value: specific money-loss statement
          - CTA: ask to send PDF plan, never request a 1-hour meeting
          - Tone: confident, under 100 words, no robotic AI phrases
        Returns (subject, body).
        """
        competitor_str = ", ".join((competitors or [])[:2]) or "your top local competitors"
        loss_str = monthly_loss.strip() if monthly_loss.strip() else "thousands of euros every month"
        niche_str = niche.strip() or "your services"
        system_prompt = PromptFactory.get_email_generation_system_prompt(
            niche_str,
            "the same language as the business name and city",
        )

        payload = {
            "business_name": business_name,
            "city": city,
            "niche": niche_str,
            "pain_point": pain_point,
            "competitors": competitor_str,
            "monthly_loss_estimate": loss_str,
            "website_content": str(website_content or "").strip(),
            "linkedin_data": str(linkedin_data or "").strip(),
            "user_defined_icp": str(user_defined_icp or "").strip(),
        }

        try:
            response = self.client.chat.completions.create(
                model=self.model_name or FORCED_AI_MODEL,
                temperature=PromptFactory.get_temperature("email"),
                response_format={"type": "json_object"},
                timeout=_AI_CALL_TIMEOUT,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
            )
            content = response.choices[0].message.content or "{}"
            parsed = json.loads(content)
            subject = str(parsed.get("subject", "")).strip()
            body = str(parsed.get("body", "")).strip()
            if not subject or not body:
                raise ValueError("Empty subject or body from AI")
            return subject, body
        except TimeoutError:
            logging.warning("Cold outreach AI timed out (15s) — using fallback.")
        except Exception as exc:
            logging.warning("Cold outreach generation failed: %s", exc)
            # Safe deterministic fallback
            fallback_subject = f"question about {business_name}"[:60]
            fallback_body = (
                f"Hi,\n\n"
                f"I noticed people in {city} are searching for {niche_str}, "
                f"but {competitor_str} are outranking you on Google.\n\n"
                f"That likely means you're losing an estimated {loss_str} in business every month.\n\n"
                f"I've prepared a short 2-minute video and a quick plan for how to fix it. Would you be open to me sending it?\n\n"
                f"Best"
            )
            return fallback_subject, fallback_body

    def generate_preview_email(self, regenerate: bool = False) -> tuple[str, str]:
        sample_account = self.peek_next_account()
        sample_lead = {
            "id": 1,
            "business_name": "Apex Roofing",
            "ai_description": "Residential roofing in Dallas with strong reviews.",
            "search_keyword": "roofing contractor dallas",
            "status": "pending",
            "follow_up_count": 0,
            "last_contacted_at": None,
            "sent_at": None,
        }
        subject, body = self.build_template_email(
            lead=sample_lead,  # type: ignore[arg-type]
            city="Dallas",
            score=7.2,
            website="https://example-roofing-site.com",
            account=sample_account,
            sequence=self.get_active_campaign_sequence(),
            step_index=1,
        )
        return subject, body

    def get_active_campaign_sequence(self) -> Optional[dict]:
        try:
            return self._fetchone(
                text(
                    """
                    SELECT *
                    FROM "CampaignSequences"
                    WHERE user_id = :user_id AND COALESCE(active, true) = true
                    ORDER BY updated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                ),
                {"user_id": self.user_id or "legacy"},
            )
        except Exception:
            return None

    def select_ab_subject_variant(self, lead_id: int, sequence: Optional[dict]) -> tuple[Optional[str], Optional[str]]:
        if not sequence:
            return None, None
        subject_a = str(sequence.get("ab_subject_a") or "").strip()
        subject_b = str(sequence.get("ab_subject_b") or "").strip()
        if not subject_a and not subject_b:
            return None, None
        if subject_a and not subject_b:
            return "A", subject_a
        if subject_b and not subject_a:
            return "B", subject_b
        variant = "A" if int(lead_id) % 2 else "B"
        return variant, subject_a if variant == "A" else subject_b

    @staticmethod
    def _resolve_sequence_delay_days(sequence: Optional[dict], follow_up_count: int) -> int:
        if follow_up_count <= 0:
            return max(1, int((sequence or {}).get("step2_delay_days") or FOLLOW_UP_DELAY_DAYS or 3))
        return max(1, int((sequence or {}).get("step3_delay_days") or 7))

    def resolve_campaign_step(self, lead: dict[str, Any], sequence: Optional[dict]) -> Optional[int]:
        status = str(lead["status"] or "pending").strip().lower()
        follow_up_count = int(lead["follow_up_count"] or 0)
        if status != "emailed":
            return 1
        if follow_up_count >= 2:
            return None

        last_contacted = lead["last_contacted_at"] or lead["sent_at"]
        if not last_contacted:
            return 2 if follow_up_count == 0 else 3
        try:
            contact_date = datetime.fromisoformat(str(last_contacted).replace("Z", "+00:00"))
        except ValueError:
            return 2 if follow_up_count == 0 else 3

        delay_days = self._resolve_sequence_delay_days(sequence, follow_up_count)
        elapsed_days = (datetime.now(contact_date.tzinfo or None) - contact_date).total_seconds() / 86400
        if elapsed_days < delay_days:
            return None
        return 2 if follow_up_count == 0 else 3

    @staticmethod
    def resolve_sender_name(account: SMTPAccount) -> str:
        if account.from_name and str(account.from_name).strip():
            return str(account.from_name).strip()
        local = str(account.email or "").split("@", 1)[0].strip()
        return local or "Your Team"

    def infer_niche(self, lead: dict[str, Any]) -> str:
        ai_description = str(lead.get("ai_description") or "")
        search_keyword = str(lead.get("search_keyword") or "")
        business_name = str(lead.get("business_name") or "")
        shortcoming = str(lead.get("main_shortcoming") or "")
        source = " ".join([ai_description, search_keyword, business_name, shortcoming]).lower()

        keyword_map = {
            "roofer": "Roofer",
            "roof": "Roofer",
            "dentist": "Dentist",
            "dental": "Dentist",
            "plumber": "Plumber",
            "plumbing": "Plumber",
            "hvac": "HVAC",
            "solar": "Solar",
            "electric": "Electrician",
            "landscap": "Landscaper",
            "cleaning": "Cleaning",
            "pest": "Pest Control",
            "garage": "Garage Door",
            "attorney": "Attorney",
            "law": "Attorney",
            "med spa": "Med Spa",
            "spa": "Med Spa",
            "chiropr": "Chiropractor",
            "auto": "Auto Repair",
        }
        for token, niche in keyword_map.items():
            if token in source:
                return niche

        description = _clean_for_ai(ai_description).strip()
        if not description:
            return "business"

        # In-memory niche cache
        cache_key = business_name.lower().strip()
        if cache_key and cache_key in _NICHE_CACHE:
            return _NICHE_CACHE[cache_key]

        # Supabase cache — check if another lead for same business already has a niche inferred
        supabase_niche = self._check_supabase_niche_cache(business_name)
        if supabase_niche:
            if cache_key:
                _NICHE_CACHE[cache_key] = supabase_niche
            return supabase_niche

        try:
            response = self.client.chat.completions.create(
                model=self.model_name or FORCED_AI_MODEL,
                temperature=0,
                response_format={"type": "json_object"},
                timeout=_AI_CALL_TIMEOUT,
                messages=[
                    {
                        "role": "system",
                        "content": PromptFactory.get_niche_inference_prompt(),
                    },
                    {
                        "role": "user",
                        "content": description,
                    },
                ],
            )
            content = response.choices[0].message.content or "{}"
            parsed = json.loads(content)
            niche = str(parsed.get("niche", "")).strip() or "business"
            if cache_key:
                _NICHE_CACHE[cache_key] = niche
            return niche
        except TimeoutError:
            logging.warning("infer_niche timed out for %s", business_name)
            return "business"
        except Exception:
            return "business"

    def _check_supabase_niche_cache(self, business_name: str) -> Optional[str]:
        """Return niche from Supabase if another lead with same name already has ai_description."""
        if not business_name:
            return None
        try:
            row = self._fetchone(
                text(
                    """
                    SELECT ai_description, search_keyword
                    FROM leads
                    WHERE business_name = :business_name
                      AND COALESCE(ai_description, '') != ''
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ),
                {"business_name": business_name},
            )
            if not row:
                return None
            desc = str(row.get("ai_description") or "").strip()
            kw = str(row.get("search_keyword") or "").strip()
            combined = (desc + " " + kw).lower()
            keyword_map = {
                "roofer": "Roofer", "roof": "Roofer", "dentist": "Dentist", "dental": "Dentist",
                "plumber": "Plumber", "plumbing": "Plumber", "hvac": "HVAC", "solar": "Solar",
                "electric": "Electrician", "landscap": "Landscaper", "cleaning": "Cleaning",
                "pest": "Pest Control", "garage": "Garage Door", "attorney": "Attorney",
                "law": "Attorney", "med spa": "Med Spa", "spa": "Med Spa",
                "chiropr": "Chiropractor", "auto": "Auto Repair",
            }
            for token, niche in keyword_map.items():
                if token in combined:
                    return niche
            return None
        except Exception as exc:
            logging.debug("Supabase niche cache check failed: %s", exc)
            return None

    def build_template_email(
        self,
        lead: dict[str, Any],
        city: str,
        score: float,
        website: Optional[str],
        account: SMTPAccount,
        sequence: Optional[dict] = None,
        step_index: int = 1,
        subject_override: Optional[str] = None,
    ) -> tuple[str, str]:
        business_name = str(lead["business_name"] or "your business").strip() or "your business"
        city_value = str(city or "your area").strip() or "your area"
        niche = self.infer_niche(lead)
        your_name = self.resolve_sender_name(account)
        replacements = {
            "BusinessName": business_name,
            "Business Name": business_name,
            "City": city_value,
            "Niche": niche,
            "YourName": your_name,
            "Your Name": your_name,
        }

        if not website:
            default_subject = self.render_mail_template(self.ghost_subject_template, replacements)
            default_body = self.render_mail_template(self.ghost_body_template, replacements)
        elif float(score or 0) >= 9.2:
            default_subject = self.render_mail_template(self.golden_subject_template, replacements)
            default_body = self.render_mail_template(self.golden_body_template, replacements)
        elif float(score or 0) >= 8.5:
            default_subject = self.render_mail_template(self.competitor_subject_template, replacements)
            default_body = self.render_mail_template(self.competitor_body_template, replacements)
        else:
            default_subject = self.render_mail_template(self.speed_subject_template, replacements)
            default_body = self.render_mail_template(self.speed_body_template, replacements)

        if sequence:
            step_subject = self.render_mail_template(str(sequence.get(f"step{step_index}_subject") or "").strip(), replacements)
            step_body = self.render_mail_template(str(sequence.get(f"step{step_index}_body") or "").strip(), replacements)
            resolved_subject = step_subject or default_subject
            resolved_body = step_body or default_body
        else:
            resolved_subject = default_subject
            resolved_body = default_body

        if subject_override:
            resolved_subject = self.render_mail_template(subject_override, replacements)

        return resolved_subject, resolved_body

    def ensure_american_english(self, text: str) -> str:
        clean_text = _clean_for_ai(str(text or "").strip())
        if not clean_text:
            return str(text or "").strip()
        try:
            response = self.client.chat.completions.create(
                model=self.model_name or FORCED_AI_MODEL,
                temperature=0.2,
                response_format={"type": "json_object"},
                timeout=_AI_CALL_TIMEOUT,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Rewrite the provided email copy into natural American English only. "
                            "Preserve intent, keep it concise, and do not use markdown."
                        ),
                    },
                    {"role": "user", "content": "Return JSON with key 'text' only.\n\n" + clean_text},
                ],
            )
            content = response.choices[0].message.content or "{}"
            payload = json.loads(content)
            rewritten = str(payload.get("text", "")).strip()
            return rewritten or clean_text
        except TimeoutError:
            logging.warning("ensure_american_english timed out (15s) — returning original.")
            return clean_text
        except Exception:
            return clean_text

    def send_message(
        self,
        recipient_email: str,
        subject: str,
        body: str,
        account: SMTPAccount,
        lead_id: Optional[int] = None,
    ) -> str:
        if not self.accounts:
            raise ValueError("Please add your SMTP account in Settings.")

        prepared_subject = subject
        prepared_body = self.ensure_signature(body, self.mail_signature)
        prepared_body = self.ensure_unsubscribe_footer(prepared_body, recipient_email)

        message = EmailMessage()
        message["To"] = recipient_email
        message["Subject"] = prepared_subject
        message["From"] = self.format_from_header(account)
        message["Reply-To"] = account.email
        message.set_content(prepared_body)

        tracking_url = self._build_open_tracking_url(lead_id)
        if tracking_url:
            escaped_body = html.escape(prepared_body).replace("\n", "<br>\n")
            pixel_tag = (
                f'<img src="{tracking_url}" width="1" height="1" alt="" '
                'style="display:block;border:0;outline:none;text-decoration:none;" />'
            )
            html_body = f"<html><body>{escaped_body}{pixel_tag}</body></html>"
            message.add_alternative(html_body, subtype="html")

        self._send_via_account(account=account, message=message)

        if self.sending_strategy == "round_robin":
            try:
                account_index = self.accounts.index(account)
                self._next_account_index = (account_index + 1) % len(self.accounts)
            except ValueError:
                self._next_account_index = (self._next_account_index + 1) % len(self.accounts)

        return account.email

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

    def _account_candidate_indexes(self) -> list[int]:
        total = len(self.accounts)
        if total == 0:
            return []

        if self.sending_strategy == "random":
            indexes = list(range(total))
            random.shuffle(indexes)
            return indexes

        return [
            (self._next_account_index + offset) % total
            for offset in range(total)
        ]

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            return {}

        try:
            with self.config_path.open("r", encoding="utf-8") as handle:
                loaded = json.load(handle)
                return loaded if isinstance(loaded, dict) else {}
        except Exception as exc:
            logging.warning("Could not read AIMailer config %s: %s", self.config_path, exc)
            return {}

    def _load_accounts(self) -> list[SMTPAccount]:
        raw_accounts = self.smtp_accounts_override if self.smtp_accounts_override is not None else self._load_user_settings_smtp_accounts()
        accounts = []
        for item in raw_accounts:
            if not str(item.get("password") or "").strip():
                logging.warning("Skipping SMTP account %s: no password configured.", item.get("email"))
                continue
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
        statements = [
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS status text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS sent_at text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_sender_email text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_contacted_at text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS follow_up_count bigint DEFAULT 0',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS generated_email_body text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS open_tracking_token text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS open_count bigint DEFAULT 0',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS first_opened_at text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_opened_at text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS competitive_hook text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS campaign_sequence_id bigint',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS campaign_step bigint DEFAULT 1',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS ab_variant text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_subject_line text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS reply_detected_at text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS bounced_at text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS bounce_reason text',
            """
            CREATE TABLE IF NOT EXISTS lead_blacklist (
                id bigint generated by default as identity primary key,
                kind text NOT NULL,
                value text NOT NULL,
                reason text,
                created_at text NOT NULL
            )
            """,
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_blacklist_kind_value ON lead_blacklist(kind, value)',
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_open_tracking_token ON leads(open_tracking_token)',
            """
            CREATE TABLE IF NOT EXISTS mailer_meta (
                key text PRIMARY KEY,
                value text NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS \"CampaignSequences\" (
                id bigint generated by default as identity primary key,
                user_id text NOT NULL DEFAULT 'legacy',
                name text NOT NULL,
                step1_subject text,
                step1_body text,
                step2_delay_days bigint DEFAULT 3,
                step2_subject text,
                step2_body text,
                step3_delay_days bigint DEFAULT 7,
                step3_subject text,
                step3_body text,
                ab_subject_a text,
                ab_subject_b text,
                active boolean DEFAULT true,
                created_at text NOT NULL,
                updated_at text NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS \"SavedTemplates\" (
                id bigint generated by default as identity primary key,
                user_id text NOT NULL DEFAULT 'legacy',
                name text NOT NULL,
                category text DEFAULT 'general',
                prompt_text text,
                subject_template text,
                body_template text,
                created_at text NOT NULL,
                updated_at text NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS \"CampaignEvents\" (
                id bigint generated by default as identity primary key,
                lead_id bigint,
                user_id text NOT NULL DEFAULT 'legacy',
                email text,
                event_type text NOT NULL,
                subject_variant text,
                subject_line text,
                metadata_json text,
                occurred_at text NOT NULL
            )
            """,
            'CREATE INDEX IF NOT EXISTS idx_campaign_sequences_user_active ON "CampaignSequences"(user_id, active)',
            'CREATE INDEX IF NOT EXISTS idx_saved_templates_user_category ON "SavedTemplates"(user_id, category)',
            'CREATE INDEX IF NOT EXISTS idx_campaign_events_user_type ON "CampaignEvents"(user_id, event_type)',
        ]
        for statement in statements:
            self._execute(text(statement))

    def _get_or_create_tracking_token(self, lead_id: int) -> str:
        row = self._fetchone(
            text("SELECT open_tracking_token FROM leads WHERE id = :lead_id LIMIT 1"),
            {"lead_id": int(lead_id)},
        )
        existing = str((row or {}).get("open_tracking_token") or "").strip()
        if existing:
            return existing

        token = secrets.token_urlsafe(24)
        self._execute(
            text("UPDATE leads SET open_tracking_token = :token WHERE id = :lead_id"),
            {"token": token, "lead_id": int(lead_id)},
        )
        return token

    def _sync_token_to_supabase(self, lead_id: int, token: str) -> None:
        del lead_id, token
        return None

    def _build_open_tracking_url(self, lead_id: Optional[int]) -> Optional[str]:
        if not lead_id:
            return None
        base_url = str(self.open_tracking_base_url or "").strip().rstrip("/")
        if not base_url:
            return None
        token = self._get_or_create_tracking_token(int(lead_id))
        return f"{base_url}/api/track/open/{token}"

    def _resolve_public_base_url(self) -> str:
        candidates = [
            os.environ.get("UNSUBSCRIBE_BASE_URL"),
            os.environ.get("APP_BASE_URL"),
            os.environ.get("PUBLIC_APP_URL"),
            os.environ.get("OPEN_TRACKING_BASE_URL"),
            self.config.get("unsubscribe_base_url", ""),
            self.config.get("app_base_url", ""),
            self.config.get("open_tracking_base_url", ""),
        ]
        for candidate in candidates:
            base_url = str(candidate or "").strip().rstrip("/")
            if base_url:
                return base_url
        return "http://localhost:8000"

    def _build_unsubscribe_url(self, recipient_email: Optional[str]) -> Optional[str]:
        email_value = str(recipient_email or "").strip().lower()
        if not email_value or "@" not in email_value:
            return None
        return f"{self._resolve_public_base_url()}/api/unsubscribe/{quote(email_value, safe='')}"

    def ensure_unsubscribe_footer(self, body: str, recipient_email: Optional[str]) -> str:
        clean_body = str(body or "").strip()
        unsubscribe_url = self._build_unsubscribe_url(recipient_email)
        footer = "Unsubscribe from these alerts"
        if unsubscribe_url:
            footer = f"Unsubscribe from these alerts: {unsubscribe_url}"

        if footer.lower() in clean_body.lower():
            return clean_body
        if not clean_body:
            return footer
        return f"{clean_body}\n\n---\n{footer}".strip()

    def _fetch_sendable_leads(self, limit: int, status_allowlist: Optional[list[str]] = None) -> list[dict[str, Any]]:
        base_query = """
            SELECT
                id,
                business_name,
                email,
                website_url,
                search_keyword,
                rating,
                main_shortcoming,
                address,
                status,
                sent_at,
                last_contacted_at,
                COALESCE(follow_up_count, 0) AS follow_up_count,
                COALESCE(ai_score, 0) AS ai_score,
                COALESCE(client_tier, 'standard') AS client_tier,
                COALESCE(competitive_hook, '') AS competitive_hook,
                enrichment_data,
                ai_description,
                generated_email_body
            FROM leads
            WHERE
                email IS NOT NULL
                AND TRIM(email) != ''
        """
        params: dict[str, Any] = {"limit": int(limit)}
        query_text = base_query

        if status_allowlist:
            normalized_statuses = [status.strip().lower() for status in status_allowlist if status and status.strip()]
            if not normalized_statuses:
                return []
            query_text += " AND LOWER(COALESCE(status, '')) IN :status_allowlist"
            params["status_allowlist"] = normalized_statuses
        if self.user_id:
            query_text += " AND COALESCE(NULLIF(user_id, ''), 'legacy') = :user_id"
            params["user_id"] = self.user_id

        if status_allowlist:
            normalized_statuses = [status.strip().lower() for status in status_allowlist if status and status.strip()]
            if not normalized_statuses:
                return []
        else:
            query_text += """
                AND (
                    LOWER(COALESCE(status, 'pending')) NOT IN (
                        'blacklisted',
                        'closed',
                        'emailed',
                        'interested',
                        'invalid_email',
                        'low_priority',
                        'meeting set',
                        'paid',
                        'qualified_not_interested',
                        'qualified not interested',
                        'replied',
                        'skipped (test lead)',
                        'skipped (unsubscribed)',
                        'zoom scheduled',
                        'bounced'
                    )
                    OR (
                        LOWER(COALESCE(status, 'pending')) = 'emailed'
                        AND COALESCE(follow_up_count, 0) < 2
                    )
                )
            """

        query_text += " ORDER BY COALESCE(ai_score, 0) DESC, id ASC LIMIT :limit"
        statement = text(query_text)
        if status_allowlist:
            statement = statement.bindparams(bindparam("status_allowlist", expanding=True))
        return self._fetchall(statement, params)

    def mark_emailed(
        self,
        lead_id: int,
        status: str,
        sender_email: Optional[str],
        generated_email_body: Optional[str],
        is_follow_up: bool = False,
        subject_line: Optional[str] = None,
        ab_variant: Optional[str] = None,
        campaign_sequence_id: Optional[int] = None,
        campaign_step: Optional[int] = None,
        bounce_reason: Optional[str] = None,
    ) -> None:
        normalized_status = str(status or "").strip().lower()
        lead_row = self._fetchone(
            text("SELECT id, email, COALESCE(NULLIF(user_id, ''), 'legacy') AS user_id FROM leads WHERE id = :lead_id LIMIT 1"),
            {"lead_id": int(lead_id)},
        )
        if lead_row is None:
            return

        if normalized_status == "emailed":
            self._execute(
                text(
                    """
                    UPDATE leads
                    SET
                        status = :status,
                        sent_at = COALESCE(sent_at, CURRENT_TIMESTAMP::text),
                        last_contacted_at = CURRENT_TIMESTAMP::text,
                        last_sender_email = :sender_email,
                        generated_email_body = COALESCE(:generated_email_body, generated_email_body),
                        follow_up_count = COALESCE(follow_up_count, 0) + :follow_up_increment,
                        last_subject_line = COALESCE(:subject_line, last_subject_line),
                        ab_variant = COALESCE(:ab_variant, ab_variant),
                        campaign_sequence_id = COALESCE(:campaign_sequence_id, campaign_sequence_id),
                        campaign_step = COALESCE(:campaign_step, campaign_step)
                    WHERE id = :lead_id
                    """
                ),
                {
                    "status": status,
                    "sender_email": sender_email,
                    "generated_email_body": generated_email_body,
                    "follow_up_increment": 1 if is_follow_up else 0,
                    "subject_line": subject_line,
                    "ab_variant": ab_variant,
                    "campaign_sequence_id": campaign_sequence_id,
                    "campaign_step": campaign_step,
                    "lead_id": int(lead_id),
                },
            )
            self._execute(
                text(
                    """
                    INSERT INTO "CampaignEvents" (
                        lead_id, user_id, email, event_type, subject_variant, subject_line, metadata_json, occurred_at
                    ) VALUES (:lead_id, :user_id, :email, 'sent', :subject_variant, :subject_line, :metadata_json, CURRENT_TIMESTAMP::text)
                    """
                ),
                {
                    "lead_id": int(lead_id),
                    "user_id": str(lead_row.get("user_id") or self.user_id or "legacy"),
                    "email": str(lead_row.get("email") or "").strip(),
                    "subject_variant": ab_variant,
                    "subject_line": subject_line,
                    "metadata_json": json.dumps({"campaign_step": campaign_step or (2 if is_follow_up else 1)}),
                },
            )
            return

        self._execute(
            text(
                """
                UPDATE leads
                SET
                    status = :status,
                    last_sender_email = :sender_email,
                    generated_email_body = COALESCE(:generated_email_body, generated_email_body),
                    last_subject_line = COALESCE(:subject_line, last_subject_line),
                    ab_variant = COALESCE(:ab_variant, ab_variant),
                    campaign_sequence_id = COALESCE(:campaign_sequence_id, campaign_sequence_id),
                    campaign_step = COALESCE(:campaign_step, campaign_step),
                    reply_detected_at = CASE WHEN :normalized_status IN ('replied', 'interested', 'meeting set') THEN CURRENT_TIMESTAMP::text ELSE reply_detected_at END,
                    bounced_at = CASE WHEN :normalized_status IN ('bounced', 'invalid_email') THEN CURRENT_TIMESTAMP::text ELSE bounced_at END,
                    bounce_reason = COALESCE(:bounce_reason, bounce_reason)
                WHERE id = :lead_id
                """
            ),
            {
                "status": status,
                "sender_email": sender_email,
                "generated_email_body": generated_email_body,
                "subject_line": subject_line,
                "ab_variant": ab_variant,
                "campaign_sequence_id": campaign_sequence_id,
                "campaign_step": campaign_step,
                "normalized_status": normalized_status,
                "bounce_reason": bounce_reason,
                "lead_id": int(lead_id),
            },
        )

    @staticmethod
    def truncate_to_word_limit(text: str, max_words: int = 100) -> str:
        words = [part for part in str(text or "").split() if part]
        if len(words) <= max_words:
            return " ".join(words).strip()
        return " ".join(words[:max_words]).strip()

    @staticmethod
    def ensure_issue_first_sentence(body: str, issue: str) -> str:
        text = str(body or "").strip()
        issue_text = str(issue or "").strip().rstrip(".")
        if not issue_text:
            return text

        sentences = re.split(r"(?<=[.!?])\s+", text)
        first_sentence = sentences[0].strip().lower() if sentences else ""
        if issue_text.lower() in first_sentence:
            return text

        prefix = f"I noticed {issue_text}."
        if not text:
            return prefix
        return f"{prefix} {text}".strip()

    @staticmethod
    def ensure_competitive_shortcoming_sentence(body: str, shortcoming: str, location: str) -> str:
        text = str(body or "").strip()
        clean_shortcoming = str(shortcoming or "a key conversion gap").strip().rstrip(".!?")
        clean_location = str(location or "your area").strip() or "your area"
        required = (
            f"I noticed that {clean_shortcoming}, which is likely why your competitors in {clean_location} "
            "are getting more calls lately."
        )

        if required.lower() in text.lower():
            return text
        if not text:
            return required
        return f"{required} {text}".strip()

    @staticmethod
    def ensure_two_minute_cta(body: str) -> str:
        required_cta = (
            "If it would help, I can send over a 2-minute video with the exact fixes I'd recommend. "
            "Would you be against me sending it?"
        )
        text = str(body or "").strip()
        if required_cta.lower() in text.lower():
            return text
        if not text:
            return required_cta
        return f"{text} {required_cta}".strip()

    @staticmethod
    def enforce_subject_style(subject: str, business_name: str) -> str:
        safe_business = str(business_name or "your business").strip() or "your business"
        candidate = re.sub(r"\s+", " ", str(subject or "")).strip()
        lowered = candidate.lower()
        if not candidate or "growth" in lowered or "marketing" in lowered:
            return f"{safe_business} // quick observation"

        if "//" in candidate:
            left, right = candidate.split("//", 1)
            return f"{left.strip()} // {right.strip().lower()}"

        return f"{safe_business} // quick observation"

    @staticmethod
    def extract_domain(raw_value: Optional[str]) -> Optional[str]:
        raw = str(raw_value or "").strip().lower()
        if not raw:
            return None
        if "@" in raw and not raw.startswith(("http://", "https://")):
            return raw.split("@", 1)[1]
        parsed = urlparse(raw if raw.startswith(("http://", "https://")) else f"https://{raw}")
        domain = (parsed.netloc or parsed.path).strip().lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain or None

    def is_test_lead(self, lead: dict[str, Any]) -> bool:
        status_value = str(lead["status"] or "").strip().lower()
        if status_value == "qa_test_mail":
            return True

        domain = self.extract_domain(lead["email"])
        if not domain:
            return False

        domain = domain.lower().strip()
        if domain.endswith((".invalid", ".test", ".example")):
            return True

        for blocked in TEST_LEAD_BLOCKED_DOMAINS:
            if domain == blocked or domain.endswith(f".{blocked}"):
                return True

        return False

    def is_blacklisted(self, email: Optional[str], website_url: Optional[str]) -> bool:
        email_value = str(email or "").strip().lower()
        domain_values = {
            self.extract_domain(email_value),
            self.extract_domain(website_url),
        }
        domain_values = {value for value in domain_values if value}
        if not email_value and not domain_values:
            return False

        if email_value:
            row = self._fetchone(
                text("SELECT 1 AS exists_flag FROM lead_blacklist WHERE kind = 'email' AND value = :value LIMIT 1"),
                {"value": email_value},
            )
            if row:
                return True
        for domain_value in domain_values:
            row = self._fetchone(
                text("SELECT 1 AS exists_flag FROM lead_blacklist WHERE kind = 'domain' AND value = :value LIMIT 1"),
                {"value": domain_value},
            )
            if row:
                return True
        return False

    @staticmethod
    def should_send_follow_up(lead: dict[str, Any]) -> bool:
        status = str(lead["status"] or "").strip().lower()
        if status != "emailed":
            return False
        if int(lead["follow_up_count"] or 0) >= 1:
            return False

        last_contacted = lead["last_contacted_at"] or lead["sent_at"]
        if not last_contacted:
            return False

        try:
            contact_date = datetime.fromisoformat(str(last_contacted).replace("Z", "+00:00"))
        except ValueError:
            return False

        return (datetime.now(contact_date.tzinfo or None) - contact_date).days >= FOLLOW_UP_DELAY_DAYS

    def peek_next_account(self) -> SMTPAccount:
        if not self.accounts:
            raise ValueError("Please add your SMTP account in Settings.")
        if self.sending_strategy == "random":
            return random.choice(self.accounts)
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
        row = self._fetchone(
            text(
                """
                SELECT COUNT(*) AS count_value
                FROM leads
                WHERE COALESCE(NULLIF(last_contacted_at, ''), NULLIF(sent_at, '')) IS NOT NULL
                  AND CAST(COALESCE(NULLIF(last_contacted_at, ''), NULLIF(sent_at, '')) AS timestamp) :: date = :today_date
                """
            ),
            {"today_date": today},
        )
        return int((row or {}).get("count_value") or 0)

    def get_or_create_warmup_start_date(self):
        row = self._fetchone(text("SELECT value FROM mailer_meta WHERE key = 'warmup_start_date' LIMIT 1"))
        existing = str((row or {}).get("value") or "").strip()
        if existing:
            return datetime.strptime(existing, "%Y-%m-%d").date()

        today = datetime.now().date().isoformat()
        self._execute(
            text(
                """
                INSERT INTO mailer_meta (key, value)
                VALUES ('warmup_start_date', :value)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """
            ),
            {"value": today},
        )
        return datetime.strptime(today, "%Y-%m-%d").date()

    @staticmethod
    def strip_known_signature_lines(text: str) -> str:
        lines = text.splitlines()
        while lines and not lines[-1].strip():
            lines.pop()
        return "\n".join(lines).strip()

    @staticmethod
    def strip_phone_lines(text: str) -> str:
        cleaned_lines = []
        for line in text.splitlines():
            compact = re.sub(r"\s+", "", line)
            digits = re.sub(r"\D", "", line)
            if compact.startswith("tel:"):
                continue
            if re.search(r"(\+?\d[\d\s().-]{6,}\d)", line) and len(digits) >= 7:
                continue
            cleaned_lines.append(line)
        return "\n".join(cleaned_lines).strip()

    def ensure_signature(self, body: str, signature: Optional[str]) -> str:
        clean_body = self.strip_known_signature_lines(body)
        clean_signature = self.strip_phone_lines((signature or "").strip())
        if not clean_signature:
            return clean_body

        if clean_body.endswith(clean_signature):
            return clean_body

        return f"{clean_body}\n\n{clean_signature}"

    @staticmethod
    def render_mail_template(template: str, replacements: dict[str, str]) -> str:
        rendered = str(template or "")
        for key, value in replacements.items():
            rendered = rendered.replace("{" + key + "}", str(value or ""))
        return rendered.strip()

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

    @staticmethod
    def render_template_placeholders(template: str, values: dict[str, str]) -> str:
        rendered = str(template or "")
        for key, raw_value in values.items():
            value = str(raw_value or "")
            variants = {
                f"{{{key}}}",
                f"{{{key.lower()}}}",
                f"{{{key.upper()}}}",
            }
            spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", key).strip()
            if spaced:
                variants.add(f"{{{spaced}}}")
                variants.add(f"{{{spaced.lower()}}}")

            for token in variants:
                rendered = rendered.replace(token, value)

        return re.sub(r"\n{3,}", "\n\n", rendered).strip()

    def render_subject(self, subject: str, business_name: str) -> str:
        subject_base = self.render_spintax(subject or "Quick idea")
        subject_base = subject_base.replace("{Business Name}", business_name or "your business")

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
    send_cmd.add_argument("--db", default="postgres", help="Deprecated local DB arg; Postgres is used via SUPABASE_DATABASE_URL.")
    send_cmd.add_argument("--config", default="env", help="Optional settings source label for non-SMTP settings.")
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
