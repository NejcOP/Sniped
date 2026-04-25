import argparse
import asyncio
import csv
import json
import logging
import os
import re
import time
import urllib.request
from datetime import datetime, timezone
from html import unescape
from typing import Any, Callable, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import aiohttp
from sqlalchemy import text

try:
    import dns.resolver as _dns_resolver  # dnspython
    _HAS_DNS = True
except ImportError:
    _dns_resolver = None  # type: ignore
    _HAS_DNS = False

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from backend.services.prompt_service import PromptFactory
from playwright.sync_api import sync_playwright

from backend.scraper.anti_bot import MODERN_USER_AGENT, apply_stealth, random_delay, random_mouse_movements
from backend.scraper.db import get_engine, init_db
from backend.scraper.phone_extractor import PhoneExtractor

EMAIL_REGEX = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}\b")
FORCED_AI_MODEL = "gpt-4o-mini"          # never changed — no other models allowed
OPENAI_REQUEST_TIMEOUT_SECONDS = 15.0    # friendly timeout on every AI call
OPENAI_429_MAX_RETRIES = 3
OPENAI_429_BACKOFF_BASE_SECONDS = 1.0
PLAYWRIGHT_DEFAULT_TIMEOUT_MS = 15000
PAGE_CONTENT_RETRY_SECONDS = 1.5
_DOMAIN_SCORE_CACHE_TTL = 30 * 24 * 3600  # 30 days in seconds

# In-memory domain-score cache  {domain: (score, summary, hook, deep_data, timestamp)}
_DOMAIN_SCORE_CACHE: dict[str, Tuple[int, str, str, dict, float]] = {}


KEYWORD_NICHE_HINTS: list[tuple[list[str], str]] = [
    (
        ["roof", "roofer", "roofing", "storm", "hail", "gutter"],
        "For roofing businesses, prioritize storm damage demand, emergency repair intent, insurance/claim readiness, and seasonal urgency signals.",
    ),
    (
        ["dental", "dentist", "orthodont", "implant", "invisalign", "cosmetic"],
        "For dental businesses, prioritize cosmetic treatment offers (implants, veneers, whitening, Invisalign), trust/review proof, and conversion friction in consultation booking.",
    ),
    (
        ["hvac", "air conditioning", "heating", "furnace", "heat pump"],
        "For HVAC businesses, prioritize emergency service availability, financing mentions, maintenance-plan offers, and local seasonality capture.",
    ),
    (
        ["solar", "photovoltaic", "panel"],
        "For solar businesses, prioritize incentive clarity, financing options, proof/case studies, and homeowner trust signals.",
    ),
    (
        ["plumb", "plumber", "drain", "water heater"],
        "For plumbing businesses, prioritize emergency response intent, service-area clarity, and quote friction reduction.",
    ),
]


def _clean_for_ai(text: str) -> str:
    """Regex-only clean: strip HTML tags, <script> blocks, and collapse whitespace."""
    if not text:
        return ""
    # remove <script>...</script> blocks first
    text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    # remove all remaining HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # collapse whitespace / newlines
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


class LeadEnricher:
    def __init__(
        self,
        db_path: str = "runtime-db",
        headless: bool = True,
        max_google_links: int = 3,
        config_path: str = "env",
        user_niche: Optional[str] = None,
        user_id: Optional[str] = None,
        model_name_override: Optional[str] = None,
    ) -> None:
        self.db_path = db_path
        self.headless = headless
        self.max_google_links = max_google_links
        self.config_path = config_path
        self.user_niche = user_niche
        self.user_id = str(user_id or "").strip() or None
        self.model_name_override = str(model_name_override or "").strip() or None

        self.openai_api_key, self.ai_model = self._init_ai_client(
            config_path=config_path,
            model_name_override=self.model_name_override,
        )

        init_db(db_path=self.db_path)
        self._ensure_enrichment_columns()

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

    def run(
        self,
        limit: Optional[int] = None,
        lead_ids: Optional[list[int]] = None,
        progress_callback: Optional[Callable[[int, int, int, Optional[str], Optional[str]], None]] = None,
    ) -> tuple[int, int]:
        leads = self._fetch_leads_for_enrichment(limit=limit, lead_ids=lead_ids)
        if not leads:
            logging.info("No scraped leads available for enrichment.")
            return 0, 0

        processed = 0
        with_email = 0
        total = len(leads)

        def _emit_progress(
            processed_count: int,
            with_email_count: int,
            current_lead: Optional[str],
            phase: Optional[str],
        ) -> None:
            if not progress_callback:
                return
            try:
                progress_callback(processed_count, total, with_email_count, current_lead, phase)
            except Exception:
                logging.exception("Enrichment progress callback failed")

        _emit_progress(0, 0, None, "starting")

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=MODERN_USER_AGENT,
                viewport={"width": 1366, "height": 768},
                locale="en-US",
            )
            page = context.new_page()
            page.set_default_timeout(PLAYWRIGHT_DEFAULT_TIMEOUT_MS)
            page.set_default_navigation_timeout(PLAYWRIGHT_DEFAULT_TIMEOUT_MS)
            apply_stealth(page)

            try:
                for lead in leads:
                    try:
                        self._mark_lead_processing(int(lead["id"]))
                        lead_name = str(lead.get("business_name") or "").strip() or f"lead-{lead.get('id')}"
                        _emit_progress(processed, with_email, lead_name, "checking_website")
                        email = None
                        insecure_site = False
                        website_excerpt = ""
                        website_signals: dict[str, Any] = {}
                        social_profiles: dict[str, str] = {
                            "linkedin": str(lead.get("linkedin_url") or "").strip(),
                            "instagram": str(lead.get("instagram_url") or "").strip(),
                            "facebook": str(lead.get("facebook_url") or "").strip(),
                        }
                        social_metrics: dict[str, dict[str, Any]] = {}

                        website_url = self._normalize_website(lead["website_url"])
                        if website_url:
                            email, insecure_site, website_excerpt, website_signals = self._audit_website(page=page, website_url=website_url)
                            _emit_progress(processed, with_email, lead_name, "discovering_email")
                            social_profiles = self._discover_social_profiles(
                                business_name=str(lead.get("business_name") or ""),
                                website_url=website_url,
                                address=str(lead.get("address") or ""),
                                existing_profiles=social_profiles,
                                website_signals=website_signals,
                            )
                            social_metrics = self._collect_social_metrics(context=context, social_profiles=social_profiles)
                        else:
                            _emit_progress(processed, with_email, lead_name, "discovering_email")
                            city = self._extract_city(lead["address"])
                            email = self._find_email_via_google(
                                page=page,
                                context=context,
                                business_name=lead["business_name"],
                                city=city,
                            )
                            social_profiles = self._discover_social_profiles(
                                business_name=str(lead.get("business_name") or ""),
                                website_url=website_url,
                                address=str(lead.get("address") or ""),
                                existing_profiles=social_profiles,
                                website_signals=website_signals,
                            )
                            social_metrics = self._collect_social_metrics(context=context, social_profiles=social_profiles)

                        shortcoming = self._infer_main_shortcoming(
                            website_url=lead["website_url"],
                            rating=lead["rating"],
                            review_count=lead["review_count"],
                            insecure_site=insecure_site,
                        )

                        # ── Email MX verification ───────────────────────────
                        email_invalid = False
                        if email and not self._verify_email_mx(email):
                            logging.warning(
                                "MX check failed for %s (%s) — marking invalid_email.",
                                lead["business_name"], email,
                            )
                            email = None
                            email_invalid = True

                        _emit_progress(processed, with_email, lead_name, "analyzing_ai")
                        ai_score, ai_summary, competitive_hook, deep_intelligence = self._score_lead_priority(
                            business_name=lead["business_name"],
                            website_url=website_url,
                            rating=lead["rating"],
                            review_count=lead["review_count"],
                            shortcoming=shortcoming,
                            insecure_site=insecure_site,
                            page_excerpt=website_excerpt,
                            has_email=bool(email),
                            address=lead["address"] or "",
                            search_keyword=lead["search_keyword"] or "",
                            google_claimed=lead.get("google_claimed"),
                            website_signals=website_signals,
                            social_profiles=social_profiles,
                            social_metrics=social_metrics,
                        )

                        if not website_url:
                            ai_score = 10
                            ai_summary = "No website detected. Highest-priority website + ads opportunity."
                            if not competitive_hook:
                                competitive_hook = (
                                    "Your top competitors already have a website capturing Google traffic, "
                                    "while you currently have no site to convert visitors or support ads."
                                )
                        client_tier = self._infer_client_tier(ai_score)

                        # ── Golden Lead alert ───────────────────────────────
                        if ai_score >= 9:
                            logging.warning(
                                "\u2605 GOLDEN LEAD FOUND: %s | score=%s | tier=%s | email=%s",
                                lead["business_name"], ai_score, client_tier, email or "no email",
                            )

                        enrichment_data: Optional[str] = None
                        enrichment_payload = dict(deep_intelligence or {})
                        if competitive_hook and not str(enrichment_payload.get("competitive_hook") or "").strip():
                            enrichment_payload["competitive_hook"] = competitive_hook
                        if ai_summary and not str(enrichment_payload.get("enrichment_summary") or "").strip():
                            enrichment_payload["enrichment_summary"] = ai_summary
                        if enrichment_payload:
                            enrichment_data = json.dumps(enrichment_payload, ensure_ascii=False)

                        _emit_progress(processed, with_email, lead_name, "finalizing")
                        self._update_lead_enrichment(
                            lead_id=lead["id"],
                            email=email,
                            insecure_site=insecure_site,
                            main_shortcoming=shortcoming,
                            ai_score=ai_score,
                            ai_description=ai_summary,
                            client_tier=client_tier,
                            enrichment_data=enrichment_data,
                            invalid_email=email_invalid,
                            phone_number=lead["phone_number"],
                            address=lead["address"],
                            linkedin_url=social_profiles.get("linkedin"),
                            instagram_url=social_profiles.get("instagram"),
                            facebook_url=social_profiles.get("facebook"),
                            qualification_score=deep_intelligence.get("qualification_score"),
                        )

                        processed += 1
                        if email:
                            with_email += 1
                        _emit_progress(processed, with_email, lead_name, "completed_lead")
                    except Exception as exc:
                        lead_name = str(lead["business_name"] or "").strip() or f"lead-{lead['id']}"
                        self._mark_lead_failed(int(lead["id"]))
                        logging.exception("Skipping lead after enrichment error: %s (%s)", lead_name, exc)

                    _emit_progress(processed, with_email, str(lead["business_name"] or "").strip() or None, "enriching")

                    random_delay(250, 700)
            finally:
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass

        return processed, with_email

    @staticmethod
    def _build_keyword_niche_guidance(search_keyword: str) -> str:
        keyword_text = str(search_keyword or "").strip().lower()
        if not keyword_text:
            return ""
        for needles, instruction in KEYWORD_NICHE_HINTS:
            if any(needle in keyword_text for needle in needles):
                return instruction
        return (
            "Use the scrape keyword as the primary niche intent signal and prioritize offer-fit, "
            "commercial urgency, and conversion blockers specific to that keyword category."
        )

    def export_ai_mailer_ready(self, output_csv: str = "ai_mailer_ready.csv") -> int:
        rows = self._fetch_ai_mailer_rows()

        with open(output_csv, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["business_name", "email", "main_shortcoming"])
            for row in rows:
                writer.writerow([row["business_name"], row["email"], row["main_shortcoming"]])

        return len(rows)

    def _init_ai_client(self, config_path: str, model_name_override: Optional[str] = None) -> tuple[Optional[str], str]:
        api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
        resolved_model = str(model_name_override or FORCED_AI_MODEL).strip() or FORCED_AI_MODEL

        try:
            with open(config_path, "r", encoding="utf-8") as handle:
                config = json.load(handle)
            if not api_key:
                api_key = str(config.get("openai", {}).get("api_key", "")).strip()
        except Exception:
            pass

        if not api_key or api_key == "YOUR_OPENAI_API_KEY":
            logging.info("OpenAI key not set for enrichment scoring. Falling back to heuristic scoring.")
            return None, resolved_model

        return api_key, resolved_model

    def _ensure_enrichment_columns(self) -> None:
        statements = [
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS email text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS google_claimed bigint',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS linkedin_url text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS instagram_url text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS facebook_url text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS insecure_site bigint DEFAULT 0',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS main_shortcoming text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_at text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS status text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS status_updated_at text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS ai_score double precision',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS qualification_score double precision',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS ai_description text',
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS client_tier text DEFAULT 'standard'",
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS enrichment_data text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS phone_formatted text',
            'ALTER TABLE leads ADD COLUMN IF NOT EXISTS phone_type text',
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS enrichment_status text DEFAULT 'pending'",
        ]
        for statement in statements:
            self._execute(text(statement))

    def _fetch_leads_for_enrichment(self, limit: Optional[int] = None, lead_ids: Optional[list[int]] = None) -> List[dict[str, Any]]:
        query = """
                SELECT
                    id,
                    business_name,
                    website_url,
                    rating,
                    review_count,
                    google_claimed,
                    linkedin_url,
                    instagram_url,
                    facebook_url,
                    address,
                    search_keyword,
                    phone_number
                FROM leads
                WHERE
                    (
                        LOWER(COALESCE(status, '')) = 'scraped'
                        OR (
                            (email IS NULL OR email = '')
                            AND enriched_at IS NULL
                        )
                    )
                    AND LOWER(COALESCE(enrichment_status, 'pending')) IN ('pending', 'failed')
            """
        params: dict[str, Any] = {}

        if self.user_id:
            query += " AND user_id = :user_id"
            params["user_id"] = self.user_id

        normalized_ids = [int(x) for x in (lead_ids or []) if str(x).strip().isdigit()]
        if normalized_ids:
            id_params: list[str] = []
            for idx, lead_id in enumerate(normalized_ids[:500]):
                key = f"lead_id_{idx}"
                id_params.append(f":{key}")
                params[key] = int(lead_id)
            query += f" AND id IN ({', '.join(id_params)})"

        query += " ORDER BY id ASC"

        if limit and limit > 0:
            query += " LIMIT :limit"
            params["limit"] = int(limit)

        return self._fetchall(text(query), params)

    def _mark_lead_processing(self, lead_id: int) -> None:
        self._execute(
            text(
                """
                UPDATE leads
                SET
                    enrichment_status = 'processing',
                    status = 'processing',
                    status_updated_at = CURRENT_TIMESTAMP::text
                WHERE id = :lead_id
                """
            ),
            {"lead_id": int(lead_id)},
        )

    def _mark_lead_failed(self, lead_id: int) -> None:
        self._execute(
            text(
                """
                UPDATE leads
                SET
                    enrichment_status = 'failed',
                    status = 'failed',
                    status_updated_at = CURRENT_TIMESTAMP::text
                WHERE id = :lead_id
                """
            ),
            {"lead_id": int(lead_id)},
        )

    def _update_lead_enrichment(
        self,
        lead_id: int,
        email: Optional[str],
        insecure_site: bool,
        main_shortcoming: str,
        ai_score: int,
        ai_description: str,
        client_tier: str,
        enrichment_data: Optional[str] = None,
        invalid_email: bool = False,
        phone_number: Optional[str] = None,
        address: Optional[str] = None,
        linkedin_url: Optional[str] = None,
        instagram_url: Optional[str] = None,
        facebook_url: Optional[str] = None,
        qualification_score: Optional[float] = None,
    ) -> None:
        new_status = "invalid_email" if invalid_email else "enriched"

        # Normalize phone number during enrichment
        phone_formatted: Optional[str] = None
        phone_type: Optional[str] = None
        if phone_number:
            country_hint = self._country_hint_from_address(address)
            pe = PhoneExtractor()
            result = pe.extract(phone_number, country_hint=country_hint)
            if result["phone_found"]:
                phone_formatted = result["primary_number"]
                phone_type = result["type"]

        self._execute(
            text(
                """
                UPDATE leads
                SET
                    email = COALESCE(:email, email),
                    insecure_site = :insecure_site,
                    main_shortcoming = :main_shortcoming,
                    ai_score = :ai_score,
                    qualification_score = COALESCE(:qualification_score, qualification_score),
                    ai_description = :ai_description,
                    client_tier = :client_tier,
                    enrichment_data = COALESCE(:enrichment_data, enrichment_data),
                    enrichment_status = 'completed',
                    status = :new_status,
                    status_updated_at = CURRENT_TIMESTAMP::text,
                    enriched_at = CURRENT_TIMESTAMP::text,
                    linkedin_url = COALESCE(:linkedin_url, linkedin_url),
                    instagram_url = COALESCE(:instagram_url, instagram_url),
                    facebook_url = COALESCE(:facebook_url, facebook_url),
                    phone_formatted = COALESCE(:phone_formatted, phone_formatted),
                    phone_type = COALESCE(:phone_type, phone_type)
                WHERE id = :lead_id
                """
            ),
            {
                "email": email,
                "insecure_site": 1 if insecure_site else 0,
                "main_shortcoming": main_shortcoming,
                "ai_score": ai_score,
                "qualification_score": float(qualification_score) if qualification_score is not None else None,
                "ai_description": ai_description,
                "client_tier": client_tier,
                "enrichment_data": enrichment_data,
                "new_status": new_status,
                "linkedin_url": linkedin_url,
                "instagram_url": instagram_url,
                "facebook_url": facebook_url,
                "phone_formatted": phone_formatted,
                "phone_type": phone_type,
                "lead_id": int(lead_id),
            },
        )

    def _fetch_ai_mailer_rows(self) -> List[dict[str, Any]]:
        query = """
                SELECT
                    business_name,
                    email,
                    COALESCE(main_shortcoming, 'No clear shortcoming identified') AS main_shortcoming
                FROM leads
                WHERE
                    email IS NOT NULL
                    AND TRIM(email) != ''
                    AND TRIM(email) LIKE '%@%.%'
                    AND LOWER(COALESCE(status, '')) IN ('enriched', 'queued_mail')
                    AND LOWER(COALESCE(enrichment_status, 'completed')) = 'completed'
            """
        params: dict[str, Any] = {}

        if self.user_id:
            query += " AND user_id = :user_id"
            params["user_id"] = self.user_id

        query += " ORDER BY business_name ASC"
        return self._fetchall(text(query), params)

    def _audit_website(self, page, website_url: str) -> tuple[Optional[str], bool, str, dict[str, Any]]:
        try:
            page.goto(website_url, wait_until="domcontentloaded", timeout=15000)
        except PlaywrightTimeoutError:
            logging.warning("Timeout while loading website: %s", website_url)
            return None, not website_url.lower().startswith("https://"), "", {}
        except Exception as exc:
            logging.warning("Could not load website %s (%s)", website_url, exc)
            return None, not website_url.lower().startswith("https://"), "", {}

        random_mouse_movements(page, count=3)
        random_delay(250, 700)

        final_url = page.url or website_url
        insecure_site = not final_url.lower().startswith("https://")
        try:
            page_html = page.content()
        except Exception:
            page.wait_for_timeout(int(PAGE_CONTENT_RETRY_SECONDS * 1000))
            try:
                page_html = page.content()
            except Exception:
                page_html = ""

        website_signals = self._extract_website_signals(page_html=page_html, base_url=final_url, website_url=website_url)

        emails = self._extract_emails(page_html)
        if emails:
            return self._pick_best_email(emails), insecure_site, self._extract_page_excerpt(page_html), website_signals

        contact_page = self._discover_contact_page(page=page, base_url=final_url)
        if contact_page:
            try:
                page.goto(contact_page, wait_until="domcontentloaded", timeout=12000)
                random_delay(250, 600)
                try:
                    contact_html = page.content()
                except Exception:
                    page.wait_for_timeout(int(PAGE_CONTENT_RETRY_SECONDS * 1000))
                    try:
                        contact_html = page.content()
                    except Exception:
                        contact_html = ""
                emails = self._extract_emails(contact_html)
                website_signals = self._merge_website_signals(
                    website_signals,
                    self._extract_website_signals(page_html=contact_html, base_url=contact_page, website_url=website_url),
                )
                if emails:
                    return self._pick_best_email(emails), insecure_site, self._extract_page_excerpt(contact_html), website_signals
            except Exception:
                return None, insecure_site, self._extract_page_excerpt(page_html), website_signals

        return None, insecure_site, self._extract_page_excerpt(page_html), website_signals

    def _extract_website_signals(self, page_html: str, base_url: str, website_url: Optional[str]) -> dict[str, Any]:
        blob = str(page_html or "")
        lower_blob = blob.lower()
        tech_stack = self._detect_tech_stack(website_url, self._extract_page_excerpt(blob))
        social_links = self._extract_social_links_from_html(blob, base_url)
        has_meta_pixel = any(token in lower_blob for token in ["connect.facebook.net/en_us/fbevents", "fbq(", "meta pixel", "facebook pixel"])
        has_google_analytics = any(token in lower_blob for token in ["googletagmanager", "google-analytics", "gtag(", "gtm.js"])
        has_contact_form = (
            ("<form" in lower_blob and ("contact" in lower_blob or 'type="email"' in lower_blob or "textarea" in lower_blob))
            or "contact us" in lower_blob
        )
        modern_design = bool(
            "viewport" in lower_blob
            and any(token in lower_blob for token in ["tailwind", "bootstrap", "react", "next.js", "vite", "webflow", "framer"])
        ) or (lower_blob.count("<section") >= 2 and lower_blob.count("<img") >= 4)
        return {
            "has_meta_pixel": has_meta_pixel,
            "has_google_analytics": has_google_analytics,
            "has_pixel": bool(has_meta_pixel or has_google_analytics),
            "has_contact_form": has_contact_form,
            "modern_design": modern_design,
            "tech_stack": tech_stack,
            "social_links": social_links,
        }

    @staticmethod
    def _merge_website_signals(primary: dict[str, Any], secondary: dict[str, Any]) -> dict[str, Any]:
        merged = dict(primary or {})
        for key in ["has_meta_pixel", "has_google_analytics", "has_pixel", "has_contact_form", "modern_design"]:
            merged[key] = bool(merged.get(key) or secondary.get(key))
        merged["tech_stack"] = list(dict.fromkeys([*(merged.get("tech_stack") or []), *(secondary.get("tech_stack") or [])]))[:5]
        social_links = dict(merged.get("social_links") or {})
        social_links.update({k: v for k, v in (secondary.get("social_links") or {}).items() if v})
        merged["social_links"] = social_links
        return merged

    def _extract_social_links_from_html(self, page_html: str, base_url: str) -> dict[str, str]:
        found: dict[str, str] = {}
        for href in re.findall(r'href=["\']([^"\']+)["\']', page_html or "", flags=re.IGNORECASE):
            absolute = urljoin(base_url, unescape(href.strip()))
            normalized = absolute.lower()
            if "linkedin.com" in normalized and "/share" not in normalized and "/feed" not in normalized:
                found.setdefault("linkedin", absolute)
            elif "instagram.com" in normalized and "/p/" not in normalized and "/reel/" not in normalized:
                found.setdefault("instagram", absolute)
            elif "facebook.com" in normalized and "sharer" not in normalized and "/posts/" not in normalized:
                found.setdefault("facebook", absolute)
        return found

    def _discover_social_profiles(
        self,
        *,
        business_name: str,
        website_url: Optional[str],
        address: str,
        existing_profiles: Optional[dict[str, str]] = None,
        website_signals: Optional[dict[str, Any]] = None,
    ) -> dict[str, str]:
        profiles = {
            "linkedin": str((existing_profiles or {}).get("linkedin") or "").strip(),
            "instagram": str((existing_profiles or {}).get("instagram") or "").strip(),
            "facebook": str((existing_profiles or {}).get("facebook") or "").strip(),
        }
        site_links = dict((website_signals or {}).get("social_links") or {})
        for key in profiles:
            if not profiles[key] and site_links.get(key):
                profiles[key] = str(site_links[key]).strip()

        city = self._extract_city(address)
        domain_hint = ""
        if website_url:
            try:
                domain_hint = urlparse(str(website_url)).netloc.lower().replace("www.", "").split(":", 1)[0]
            except Exception:
                domain_hint = ""

        platform_queries = {
            "linkedin": ["linkedin.com/company", "linkedin.com/in"],
            "instagram": ["instagram.com"],
            "facebook": ["facebook.com"],
        }

        for platform, domains in platform_queries.items():
            if profiles.get(platform):
                continue
            search_parts = [f'site:{domains[0]}', f'"{business_name}"']
            if city:
                search_parts.append(f'"{city}"')
            if domain_hint:
                search_parts.append(f'"{domain_hint.split(".")[0]}"')
            query = " ".join(part for part in search_parts if part)
            match = self._search_social_profile_url(query=query, allowed_domains=domains)
            if match:
                profiles[platform] = match
        return profiles

    def _search_social_profile_url(self, *, query: str, allowed_domains: list[str]) -> Optional[str]:
        results = self._search_engine_links(query=query)
        for result in results:
            normalized = result.lower()
            if any(domain in normalized for domain in allowed_domains):
                return result
        return None

    def _search_engine_links(self, *, query: str) -> list[str]:
        serpapi_key = str(os.environ.get("SERPAPI_API_KEY", "") or "").strip()
        if serpapi_key:
            try:
                url = (
                    "https://serpapi.com/search.json?engine=google"
                    f"&q={quote_plus(query)}&api_key={quote_plus(serpapi_key)}&num=5"
                )
                request = urllib.request.Request(url, headers={"User-Agent": MODERN_USER_AGENT})
                with urllib.request.urlopen(request, timeout=12) as response:
                    payload = json.loads(response.read().decode("utf-8", errors="ignore") or "{}")
                return [
                    str(item.get("link") or "").strip()
                    for item in payload.get("organic_results") or []
                    if str(item.get("link") or "").strip()
                ][:5]
            except Exception as exc:
                logging.debug("SerpApi social discovery failed for %s: %s", query, exc)

        endpoints = [
            f"https://html.duckduckgo.com/html/?q={quote_plus(query)}",
            f"https://www.bing.com/search?q={quote_plus(query)}",
        ]
        for endpoint in endpoints:
            try:
                request = urllib.request.Request(endpoint, headers={"User-Agent": MODERN_USER_AGENT})
                with urllib.request.urlopen(request, timeout=12) as response:
                    html = response.read().decode("utf-8", errors="ignore")
                links = self._extract_search_links_from_html(html)
                if links:
                    return links[:5]
            except Exception as exc:
                logging.debug("Search endpoint failed for %s (%s): %s", query, endpoint, exc)
        return []

    @staticmethod
    def _extract_search_links_from_html(html: str) -> list[str]:
        candidates: list[str] = []
        seen: set[str] = set()
        for href in re.findall(r'href=["\']([^"\']+)["\']', html or "", flags=re.IGNORECASE):
            raw = unescape(href)
            if "uddg=" in raw:
                parsed = urlparse(raw)
                uddg = parse_qs(parsed.query).get("uddg")
                if uddg:
                    raw = unquote(uddg[0])
            if raw.startswith("/") and "bing.com" not in raw:
                continue
            if not raw.startswith("http"):
                continue
            domain = urlparse(raw).netloc.lower()
            if any(blocked in domain for blocked in ["duckduckgo.com", "bing.com", "google.com"]):
                continue
            if raw in seen:
                continue
            seen.add(raw)
            candidates.append(raw)
        return candidates

    def _collect_social_metrics(self, context, social_profiles: dict[str, str]) -> dict[str, dict[str, Any]]:
        metrics: dict[str, dict[str, Any]] = {}
        for platform, url in social_profiles.items():
            if not url:
                continue
            metrics[platform] = self._fetch_social_profile_metrics(context=context, platform=platform, url=url)
        return metrics

    def _fetch_social_profile_metrics(self, context, *, platform: str, url: str) -> dict[str, Any]:
        page = context.new_page()
        apply_stealth(page)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=12000)
            random_delay(250, 700)
            html = page.content()
            text_blob = _clean_for_ai(html)
        except Exception:
            html = ""
            text_blob = ""
        finally:
            try:
                page.close()
            except Exception:
                pass

        follower_count = self._extract_social_follower_count(text_blob)
        last_post_text, last_active_days = self._extract_social_recency(text_blob)
        engagement_count = self._extract_social_engagement_count(text_blob)
        active = bool(last_active_days is not None and last_active_days <= 120)
        if not active and engagement_count >= 25 and follower_count > 0:
            active = True
        return {
            "platform": platform,
            "url": url,
            "follower_count": follower_count,
            "engagement_count": engagement_count,
            "last_post_text": last_post_text,
            "last_active_days": last_active_days,
            "active": active,
        }

    @staticmethod
    def _parse_compact_number(raw_value: str) -> int:
        text_value = str(raw_value or "").strip().upper().replace(",", "")
        if not text_value:
            return 0
        multiplier = 1
        if text_value.endswith("K"):
            multiplier = 1_000
            text_value = text_value[:-1]
        elif text_value.endswith("M"):
            multiplier = 1_000_000
            text_value = text_value[:-1]
        elif text_value.endswith("B"):
            multiplier = 1_000_000_000
            text_value = text_value[:-1]
        try:
            return int(float(text_value) * multiplier)
        except Exception:
            return 0

    def _extract_social_follower_count(self, text_blob: str) -> int:
        match = re.search(r'([\d.,]+\s*[KMB]?)\s+(?:followers|follower|likes)', text_blob or "", re.IGNORECASE)
        if not match:
            return 0
        return self._parse_compact_number(match.group(1))

    def _extract_social_engagement_count(self, text_blob: str) -> int:
        counts = re.findall(r'([\d.,]+\s*[KMB]?)\s+(?:likes|comments|reactions)', text_blob or "", re.IGNORECASE)
        if not counts:
            return 0
        parsed = [self._parse_compact_number(item) for item in counts[:3]]
        return max(parsed or [0])

    def _extract_social_recency(self, text_blob: str) -> tuple[str, Optional[int]]:
        relative = re.search(r'(\d+)\s+(day|days|week|weeks|month|months|year|years)\s+ago', text_blob or "", re.IGNORECASE)
        if relative:
            amount = int(relative.group(1))
            unit = relative.group(2).lower()
            multiplier = 1
            if unit.startswith("week"):
                multiplier = 7
            elif unit.startswith("month"):
                multiplier = 30
            elif unit.startswith("year"):
                multiplier = 365
            return relative.group(0), amount * multiplier

        absolute = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}', text_blob or "", re.IGNORECASE)
        if absolute:
            raw_text = absolute.group(0)
            for fmt in ["%b %d, %Y", "%B %d, %Y"]:
                try:
                    parsed = datetime.strptime(raw_text, fmt).replace(tzinfo=timezone.utc)
                    days = max(0, (datetime.now(timezone.utc) - parsed).days)
                    return raw_text, days
                except Exception:
                    continue
            return raw_text, None
        return "", None

    def _find_email_via_google(self, page, context, business_name: str, city: str) -> Optional[str]:
        query = f"contact {business_name} {city}".strip()
        search_url = f"https://www.google.com/search?q={quote_plus(query)}"

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
        except Exception:
            return None

        self._accept_google_consent(page)
        random_mouse_movements(page, count=2)
        random_delay(300, 900)

        try:
            _google_html = page.content()
        except Exception:
            page.wait_for_timeout(int(PAGE_CONTENT_RETRY_SECONDS * 1000))
            try:
                _google_html = page.content()
            except Exception:
                _google_html = ""
        immediate_emails = self._extract_emails(_google_html)
        if immediate_emails:
            return self._pick_best_email(immediate_emails)

        links = self._extract_google_result_links(page=page)
        for link in links:
            sub_page = context.new_page()
            apply_stealth(sub_page)
            try:
                sub_page.goto(link, wait_until="domcontentloaded", timeout=12000)
                random_delay(250, 700)
                try:
                    _sub_html = sub_page.content()
                except Exception:
                    sub_page.wait_for_timeout(int(PAGE_CONTENT_RETRY_SECONDS * 1000))
                    try:
                        _sub_html = sub_page.content()
                    except Exception:
                        _sub_html = ""
                emails = self._extract_emails(_sub_html)
                if emails:
                    return self._pick_best_email(emails)
            except Exception:
                continue
            finally:
                sub_page.close()

        return None

    def _extract_google_result_links(self, page) -> List[str]:
        blocked_domains = {
            "google.com",
            "webcache.googleusercontent.com",
            "youtube.com",
            "facebook.com",
            "instagram.com",
            "linkedin.com",
        }

        links: List[str] = []
        seen = set()

        anchors = page.locator("a[href]")
        total = min(anchors.count(), 140)

        for idx in range(total):
            href = anchors.nth(idx).get_attribute("href")
            if not href:
                continue

            candidate = self._normalize_google_href(href)
            if not candidate:
                continue

            parsed = urlparse(candidate)
            domain = parsed.netloc.lower().replace("www.", "")
            if not domain or domain in blocked_domains:
                continue

            if candidate in seen:
                continue

            seen.add(candidate)
            links.append(candidate)

            if len(links) >= self.max_google_links:
                break

        return links

    @staticmethod
    def _normalize_google_href(href: str) -> Optional[str]:
        if href.startswith("/url?"):
            parsed = urlparse(href)
            q = parse_qs(parsed.query).get("q")
            if q and q[0].startswith("http"):
                return q[0]
            return None

        if href.startswith("http"):
            return href

        return None

    @staticmethod
    def _accept_google_consent(page) -> None:
        candidates = [
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
        ]

        for selector in candidates:
            try:
                button = page.locator(selector).first
                if button.count() > 0:
                    button.click(timeout=1500)
                    random_delay(200, 500)
                    return
            except Exception:
                continue

    @staticmethod
    def _discover_contact_page(page, base_url: str) -> Optional[str]:
        anchors = page.locator("a[href]")
        total = min(anchors.count(), 100)

        for idx in range(total):
            anchor = anchors.nth(idx)
            text = (anchor.inner_text(timeout=800) or "").strip().lower()
            href = (anchor.get_attribute("href") or "").strip()
            blob = f"{text} {href}".lower()

            if "contact" not in blob:
                continue

            if href.startswith("mailto:"):
                continue

            return urljoin(base_url, href)

        return None

    @staticmethod
    def _extract_emails(text: str) -> List[str]:
        found = set(match.lower() for match in EMAIL_REGEX.findall(text or ""))
        invalid_suffixes = (".png", ".jpg", ".jpeg", ".svg", ".webp", ".gif")

        filtered = []
        for email in found:
            if any(email.endswith(suffix) for suffix in invalid_suffixes):
                continue
            if email.endswith("@example.com"):
                continue
            filtered.append(email)

        return sorted(filtered)

    @staticmethod
    def _pick_best_email(emails: Iterable[str]) -> Optional[str]:
        pool = list(emails)
        if not pool:
            return None

        priorities = ["info", "contact", "hello", "office", "sales", "support"]
        for token in priorities:
            for email in pool:
                local = email.split("@", 1)[0].lower()
                if token in local:
                    return email

        return sorted(pool)[0]

    @staticmethod
    def _normalize_website(website_url: Optional[str]) -> Optional[str]:
        if not website_url:
            return None

        cleaned = website_url.strip()
        if not cleaned or cleaned.lower() == "none":
            return None

        if cleaned.startswith("http://") or cleaned.startswith("https://"):
            return cleaned

        return f"https://{cleaned}"

    @staticmethod
    def _extract_city(address: Optional[str]) -> str:
        if not address:
            return ""

        parts = [part.strip() for part in address.split(",") if part.strip()]
        if not parts:
            return ""

        for part in reversed(parts):
            if re.search(r"\d", part):
                continue
            if len(part) <= 2 and part.isupper():
                continue
            return part

        if len(parts) >= 2:
            return parts[-2]

        return parts[0]

    @staticmethod
    def _country_hint_from_address(address: Optional[str]) -> Optional[str]:
        """Return an ISO-3166 alpha-2 country code inferred from an address string."""
        if not address:
            return None
        addr = address.upper()
        # Explicit country markers
        _MAP = {
            "SLOVENIJA": "SI", "SLOVENIA": "SI",
            "DEUTSCHLAND": "DE", "GERMANY": "DE",
            "ÖSTERREICH": "AT", "AUSTRIA": "AT",
            "HRVATSKA": "HR", "CROATIA": "HR",
            "SCHWEIZ": "CH", "SWITZERLAND": "CH",
            "ITALY": "IT", "ITALIA": "IT",
            "FRANCE": "FR", "FRANKREICH": "FR",
            "UNITED KINGDOM": "GB", "UK": "GB",
            "UNITED STATES": "US", "USA": "US",
        }
        for keyword, code in _MAP.items():
            if keyword in addr:
                return code
        # Postal code heuristics
        if re.search(r"\bSI[-\s]?\d{4}\b", addr):
            return "SI"
        if re.search(r"\bDE[-\s]?\d{5}\b", addr):
            return "DE"
        if re.search(r"\bAT[-\s]?\d{4}\b", addr):
            return "AT"
        return None

    @staticmethod
    def _infer_main_shortcoming(
        website_url: Optional[str],
        rating: Optional[float],
        review_count: Optional[int],
        insecure_site: bool,
    ) -> str:
        if not website_url or str(website_url).strip().lower() == "none":
            return "Missing website"

        if insecure_site:
            return "Website not using HTTPS"

        if rating is None:
            return "No visible Google rating"

        if rating < 3.5:
            return f"Low Google rating ({rating:.1f})"

        if review_count is None or review_count < 15:
            return "Low review count"

        return "No clear shortcoming identified"

    @staticmethod
    def _extract_page_excerpt(content: str) -> str:
        text = _clean_for_ai(content or "")
        return text[:1400]

    @staticmethod
    def _normalize_string_list(value: object, *, limit: int = 3) -> list[str]:
        if isinstance(value, (list, tuple, set)):
            items = [str(item or "").strip() for item in value]
        elif isinstance(value, str):
            parts = re.split(r"\n|\||;|•", value)
            items = [str(item or "").strip() for item in parts]
        else:
            items = []
        normalized: list[str] = []
        seen: set[str] = set()
        for item in items:
            if not item:
                continue
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            normalized.append(item)
            if len(normalized) >= limit:
                break
        return normalized

    @staticmethod
    def _coerce_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _detect_tech_stack(website_url: Optional[str], page_excerpt: str) -> list[str]:
        blob = f"{website_url or ''} {page_excerpt or ''}".lower()
        stack_map = {
            "shopify": ["cdn.shopify.com", "shopify", "myshopify"],
            "WooCommerce": ["woocommerce", "wp-content/plugins/woocommerce"],
            "WordPress": ["wp-content", "wordpress"],
            "Wix": ["wix.com", "wix"],
            "Squarespace": ["squarespace", "static.squarespace"],
            "HubSpot": ["hubspot", "hsforms"],
            "Klaviyo": ["klaviyo"],
            "Google Analytics": ["gtag(", "googletagmanager", "google-analytics"],
            "Meta Pixel": ["facebook pixel", "connect.facebook.net/en_us/fbevents", "meta pixel"],
        }
        detected: list[str] = []
        for label, needles in stack_map.items():
            if any(needle in blob for needle in needles):
                detected.append(label)
        return detected[:5]

    @staticmethod
    def _infer_recent_site_update(page_excerpt: str) -> bool:
        blob = str(page_excerpt or "").lower()
        current_year = time.gmtime().tm_year
        recent_year_tokens = {str(current_year), str(current_year - 1)}
        freshness_tokens = ["latest", "new", "updated", "launch", "case study", "blog", "news"]
        return any(token in blob for token in recent_year_tokens) or any(token in blob for token in freshness_tokens)

    @staticmethod
    def _estimate_employee_count(page_excerpt: str, review_count: Optional[int]) -> int:
        blob = str(page_excerpt or "")
        explicit = re.search(r"(?:team of|over|more than|with)\s+(\d{1,4})\s+(?:people|employees|staff)", blob, re.IGNORECASE)
        if explicit:
            try:
                return max(1, int(explicit.group(1)))
            except Exception:
                pass
        reviews = int(review_count or 0)
        if reviews >= 250:
            return 60
        if reviews >= 100:
            return 35
        if reviews >= 40:
            return 18
        if reviews >= 10:
            return 8
        return 4

    def _derive_competitor_snapshot(self, *, business_name: str, search_keyword: str, address: str) -> list[str]:
        city = self._extract_city(address)
        niche_hint = str(search_keyword or "").strip() or "local service"
        base_city = city or "their market"
        name = str(business_name or "This company").strip()
        return [
            f"Top-ranked {niche_hint} providers in {base_city}",
            f"Google Maps leaders for {niche_hint} near {base_city}",
            f"Faster-moving competitors around {base_city} targeting {name}'s buyers",
        ]

    def _build_deep_intelligence_payload(
        self,
        *,
        business_name: str,
        website_url: Optional[str],
        rating: Optional[float],
        review_count: Optional[int],
        shortcoming: str,
        page_excerpt: str,
        has_email: bool,
        address: str,
        search_keyword: str,
        ai_score: int,
        google_claimed: Optional[bool] = None,
        website_signals: Optional[dict[str, Any]] = None,
        social_profiles: Optional[dict[str, str]] = None,
        social_metrics: Optional[dict[str, dict[str, Any]]] = None,
        parsed_ai: Optional[dict] = None,
    ) -> dict:
        parsed = parsed_ai if isinstance(parsed_ai, dict) else {}
        website_signal_map = dict(website_signals or {})
        social_profile_map = {key: str(value or "").strip() for key, value in dict(social_profiles or {}).items() if str(value or "").strip()}
        social_metric_map = dict(social_metrics or {})
        default_strengths = []
        if website_url:
            default_strengths.append("Website is live and discoverable")
        if isinstance(rating, (int, float)) and float(rating) >= 4.2:
            default_strengths.append(f"Strong review sentiment ({float(rating):.1f}★)")
        if has_email:
            default_strengths.append("Reachable contact channel is available")
        if social_profile_map:
            default_strengths.append("Business has social profiles that can be verified cross-platform")
        if self._infer_recent_site_update(page_excerpt):
            default_strengths.append("Recent content or update signals are visible")
        if not default_strengths:
            default_strengths = ["Clear market presence", "Relevant local service offer", "Some digital footprint exists"]

        default_weaknesses = self._normalize_string_list(parsed.get("weak_points") or parsed.get("weaknesses"), limit=3)
        if not default_weaknesses:
            default_weaknesses = [str(shortcoming or "No obvious shortcoming found").strip() or "No obvious shortcoming found"]
            if not has_email:
                default_weaknesses.append("No contact email found for fast outreach")
            if not self._infer_recent_site_update(page_excerpt):
                default_weaknesses.append("Site appears stale or thin on fresh content")
        while len(default_weaknesses) < 3:
            default_weaknesses.append("Weak local demand capture signals")

        strengths = self._normalize_string_list(parsed.get("strengths"), limit=3) or default_strengths[:3]
        weaknesses = default_weaknesses[:3]
        competitors = self._normalize_string_list(parsed.get("competitor_snapshot") or parsed.get("competitors"), limit=3)
        if not competitors:
            competitors = self._derive_competitor_snapshot(
                business_name=business_name,
                search_keyword=search_keyword,
                address=address,
            )[:3]

        tech_stack = self._normalize_string_list(parsed.get("tech_stack"), limit=5)
        if not tech_stack:
            tech_stack = self._normalize_string_list(website_signal_map.get("tech_stack"), limit=5) or self._detect_tech_stack(website_url, page_excerpt)

        recent_site_update = self._coerce_bool(parsed.get("recent_site_update")) or self._infer_recent_site_update(page_excerpt)
        intent_signals = self._normalize_string_list(parsed.get("intent_signals"), limit=6)
        if recent_site_update and all("recently updated" not in signal.lower() for signal in intent_signals):
            intent_signals.insert(0, "Recently updated site")
        for stack_name in tech_stack[:2]:
            label = f"{stack_name} detected"
            if all(label.lower() != signal.lower() for signal in intent_signals):
                intent_signals.append(label)

        employee_count_raw = parsed.get("employee_count")
        try:
            employee_count = max(0, int(float(str(employee_count_raw).strip()))) if str(employee_count_raw or "").strip() else 0
        except Exception:
            employee_count = 0
        if employee_count <= 0:
            employee_count = self._estimate_employee_count(page_excerpt, review_count)

        ai_sentiment_score_raw = parsed.get("ai_sentiment_score", parsed.get("lead_score_100", ai_score * 10))
        try:
            ai_sentiment_score = float(ai_sentiment_score_raw)
        except Exception:
            ai_sentiment_score = float(ai_score * 10)
        ai_sentiment_score = max(0.0, min(100.0, ai_sentiment_score))
        qualification_score = int(round(ai_sentiment_score))

        social_last_active_days = min(
            [int(item.get("last_active_days")) for item in social_metric_map.values() if item.get("last_active_days") is not None] or [9999]
        )
        social_activity_score = round(min(10.0, sum(
            2.5 if bool(item.get("active")) else 0.5
            for item in social_metric_map.values()
        ) + (1.5 if any(int(item.get("follower_count") or 0) >= 1000 for item in social_metric_map.values()) else 0.0)), 1)

        email_component = 40 if has_email else 8
        if employee_count >= 100:
            size_component = 30
        elif employee_count >= 40:
            size_component = 26
        elif employee_count >= 15:
            size_component = 22
        elif employee_count >= 5:
            size_component = 16
        else:
            size_component = 10
        sentiment_component = round(ai_sentiment_score * 0.3, 1)
        best_lead_score = round(min(100.0, email_component + size_component + sentiment_component), 1)

        return {
            "competitive_hook": str(parsed.get("competitive_hook") or "").strip(),
            "main_offer": str(parsed.get("main_offer") or "").strip(),
            "latest_achievements": self._normalize_string_list(parsed.get("latest_achievements"), limit=3),
            "strengths": strengths[:3],
            "weaknesses": weaknesses[:3],
            "weak_points": weaknesses[:3],
            "company_audit": {
                "strengths": strengths[:3],
                "weaknesses": weaknesses[:3],
            },
            "competitor_snapshot": competitors[:3],
            "tech_stack": tech_stack,
            "intent_signals": intent_signals[:6],
            "recent_site_update": recent_site_update,
            "employee_count": employee_count,
            "ai_sentiment_score": ai_sentiment_score,
            "lead_score_100": int(round(ai_sentiment_score)),
            "qualification_score": qualification_score,
            "best_lead_score": best_lead_score,
            "lead_priority": str(parsed.get("lead_priority") or ("Hot Lead" if best_lead_score >= 80 else "Qualified" if best_lead_score >= 55 else "Low Priority")).strip(),
            "reason": str(parsed.get("reason") or parsed.get("enrichment_summary") or "").strip(),
            "enrichment_summary": str(parsed.get("enrichment_summary") or parsed.get("reason") or "").strip(),
            "google_maps": {
                "claimed": google_claimed,
                "rating": rating,
                "review_count": review_count,
            },
            "social_profiles": social_profile_map,
            "social_metrics": social_metric_map,
            "has_social_links": bool(social_profile_map),
            "social_last_active_days": None if social_last_active_days == 9999 else social_last_active_days,
            "social_activity_score": social_activity_score,
            "website_signals": {
                "has_pixel": bool(website_signal_map.get("has_pixel")),
                "has_meta_pixel": bool(website_signal_map.get("has_meta_pixel")),
                "has_google_analytics": bool(website_signal_map.get("has_google_analytics")),
                "has_contact_form": bool(website_signal_map.get("has_contact_form")),
                "modern_design": bool(website_signal_map.get("modern_design")),
            },
            "has_contact_form": bool(website_signal_map.get("has_contact_form")),
            "has_pixel": bool(website_signal_map.get("has_pixel")),
            "modern_design": bool(website_signal_map.get("modern_design")),
        }

    def _score_lead_priority(
        self,
        business_name: str,
        website_url: Optional[str],
        rating: Optional[float],
        review_count: Optional[int],
        shortcoming: str,
        insecure_site: bool,
        page_excerpt: str,
        has_email: bool,
        address: str = "",
        search_keyword: str = "",
        google_claimed: Optional[bool] = None,
        website_signals: Optional[dict[str, Any]] = None,
        social_profiles: Optional[dict[str, str]] = None,
        social_metrics: Optional[dict[str, dict[str, Any]]] = None,
    ) -> tuple[int, str, str, dict]:
        """Return (score 1-10, ai_summary reason, competitive_hook string, deep_intelligence payload)."""
        if not website_url or str(website_url).strip().lower() == "none":
            no_website_payload = self._build_deep_intelligence_payload(
                business_name=business_name,
                website_url=website_url,
                rating=rating,
                review_count=review_count,
                shortcoming=shortcoming,
                page_excerpt=page_excerpt,
                has_email=has_email,
                address=address,
                search_keyword=search_keyword,
                ai_score=10,
                google_claimed=google_claimed,
                website_signals=website_signals,
                social_profiles=social_profiles,
                social_metrics=social_metrics,
                parsed_ai={
                    "competitive_hook": "Your top competitors already have a website capturing Google traffic, while you currently have no site to convert visitors or support ads.",
                    "strengths": ["Recognizable business name", "Clear local market fit", "Fast upside once online"],
                    "weak_points": ["Missing website", "No owned landing page", "Weak trust signals online"],
                    "lead_priority": "Hot Lead",
                    "enrichment_summary": "No website detected. Highest-priority website + ads opportunity.",
                    "reason": "No website detected. Highest-priority website + ads opportunity.",
                },
            )
            return (
                10,
                "No website detected. Highest-priority website + ads opportunity.",
                "Your top competitors already have a website capturing Google traffic, while you currently have no site to convert visitors or support ads.",
                no_website_payload,
            )

        # Normalise domain for cache lookups
        try:
            parsed_domain = urlparse(str(website_url)).netloc.lower().replace("www.", "").strip()
        except Exception:
            parsed_domain = ""

        # 1. In-memory cache (30 days TTL)
        if parsed_domain:
            mem = _DOMAIN_SCORE_CACHE.get(parsed_domain)
            if mem and (time.time() - mem[4]) < _DOMAIN_SCORE_CACHE_TTL:
                logging.info("Memory cache HIT for %s — skipping AI call.", parsed_domain)
                return mem[0], mem[1], mem[2], dict(mem[3] or {})

        # 2. Supabase cache — check if ai_score already exists for this domain/business
        supabase_hit = self._check_supabase_score_cache(business_name, website_url)
        if supabase_hit:
            score, summary, hook, deep_payload = supabase_hit
            if parsed_domain:
                _DOMAIN_SCORE_CACHE[parsed_domain] = (score, summary, hook, deep_payload, time.time())
            logging.info("Supabase cache HIT for %s — skipping AI call.", business_name)
            return score, summary, hook, deep_payload

        if self.openai_api_key is None:
            heuristic_score = self._heuristic_score(
                website_url=website_url, rating=rating, review_count=review_count,
                shortcoming=shortcoming, insecure_site=insecure_site, has_email=has_email,
            )
            deep_payload = self._build_deep_intelligence_payload(
                business_name=business_name,
                website_url=website_url,
                rating=rating,
                review_count=review_count,
                shortcoming=shortcoming,
                page_excerpt=page_excerpt,
                has_email=has_email,
                address=address,
                search_keyword=search_keyword,
                ai_score=heuristic_score,
                google_claimed=google_claimed,
                website_signals=website_signals,
                social_profiles=social_profiles,
                social_metrics=social_metrics,
            )
            return (
                heuristic_score,
                str(shortcoming or "").strip() or "Heuristic scoring used (OpenAI unavailable).",
                str(deep_payload.get("competitive_hook") or "").strip(),
                deep_payload,
            )

        try:
            factory = PromptFactory()

            # --- STEP 0: Lead Qualification (category match check) ---
            if self.user_niche and page_excerpt:
                try:
                    qual_system, qual_user = factory.get_lead_qualification_prompt(
                        user_category=self.user_niche,
                        business_name=business_name,
                        scraped_content=_clean_for_ai(page_excerpt)[:600],
                    )
                    qual_parsed = asyncio.run(
                        self._score_lead_priority_async(
                            system_prompt=qual_system,
                            payload=qual_user,
                            temperature=0.1,
                        )
                    )
                    if not qual_parsed.get("is_match", True):
                        confidence = int(qual_parsed.get("confidence_score", 0))
                        reason = str(qual_parsed.get("relevance_reason", "Does not match selected category."))
                        logging.info(
                            "Lead REJECTED by qualification: %s (confidence=%d) — %s",
                            business_name, confidence, reason,
                        )
                        heuristic_score = self._heuristic_score(
                            website_url=website_url, rating=rating, review_count=review_count,
                            shortcoming=shortcoming, insecure_site=insecure_site, has_email=has_email,
                        )
                        deep_payload = self._build_deep_intelligence_payload(
                            business_name=business_name, website_url=website_url, rating=rating,
                            review_count=review_count, shortcoming=shortcoming, page_excerpt=page_excerpt,
                            has_email=has_email, address=address, search_keyword=search_keyword,
                            ai_score=heuristic_score,
                        )
                        deep_payload["qualification_rejected"] = True
                        deep_payload["qualification_reason"] = reason
                        return (
                            heuristic_score,
                            f"[Category Mismatch] {reason}",
                            "",
                            deep_payload,
                        )
                except Exception as qexc:
                    logging.warning("Lead qualification check failed for %s: %s", business_name, qexc)

            system_prompt = factory.get_enrichment_system_prompt(user_niche=self.user_niche)
            keyword_guidance = self._build_keyword_niche_guidance(search_keyword)
            if keyword_guidance:
                system_prompt = (
                    f"{system_prompt}\n\n"
                    "KEYWORD-SPECIFIC DIRECTIVE (MANDATORY):\n"
                    f"Scrape keyword: {search_keyword}\n"
                    f"{keyword_guidance}\n"
                )
            social_activity_score = round(
                min(
                    10.0,
                    sum(2.5 if bool(item.get("active")) else 0.5 for item in (social_metrics or {}).values())
                    + (
                        1.5
                        if any(int(item.get("follower_count") or 0) >= 1000 for item in (social_metrics or {}).values())
                        else 0.0
                    ),
                ),
                1,
            )
            payload = {
                "company_name": business_name,
                "location": address,
                "website_url": website_url,
                "rating": rating,
                "reviews": review_count,
                "review_count": review_count,
                "google_maps_claimed": google_claimed,
                "audit_findings": shortcoming,
                "shortcoming": shortcoming,
                "search_keyword": search_keyword,
                "keyword_niche_guidance": keyword_guidance,
                "has_website": bool(website_url),
                "insecure_site": insecure_site,
                "has_email": has_email,
                "website_excerpt": _clean_for_ai(page_excerpt)[:800],
                "website_signals": website_signals or {},
                "social_profiles": social_profiles or {},
                "social_metrics": social_metrics or {},
                "social_activity_score": social_activity_score,
            }
            parsed = asyncio.run(
                self._score_lead_priority_async(
                    system_prompt=system_prompt,
                    payload=payload,
                    temperature=factory.get_temperature("enrichment"),
                )
            )
            score = max(1, min(10, int(float(parsed.get("score", 5)))))
            ai_summary = str(parsed.get("reason", "")).strip()
            if not ai_summary:
                ai_summary = str(parsed.get("enrichment_summary", "")).strip()
            competitive_hook = str(parsed.get("competitive_hook", "")).strip()
            if not ai_summary:
                ai_summary = str(shortcoming or "").strip() or "AI scoring completed."

            deep_payload = self._build_deep_intelligence_payload(
                business_name=business_name,
                website_url=website_url,
                rating=rating,
                review_count=review_count,
                shortcoming=shortcoming,
                page_excerpt=page_excerpt,
                has_email=has_email,
                address=address,
                search_keyword=search_keyword,
                ai_score=score,
                google_claimed=google_claimed,
                website_signals=website_signals,
                social_profiles=social_profiles,
                social_metrics=social_metrics,
                parsed_ai=parsed,
            )
            if competitive_hook and not str(deep_payload.get("competitive_hook") or "").strip():
                deep_payload["competitive_hook"] = competitive_hook

            # --- STEP 3: Niche fit analysis — generates email_opener and signals ---
            if self.user_niche and page_excerpt:
                try:
                    fit_system, fit_user = factory.get_niche_fit_analysis_prompt(
                        user_niche=self.user_niche,
                        business_name=business_name,
                        scraped_content=_clean_for_ai(page_excerpt)[:600],
                    )
                    fit_parsed = asyncio.run(
                        self._score_lead_priority_async(
                            system_prompt=fit_system,
                            payload=fit_user,
                            temperature=0.2,
                        )
                    )
                    email_opener = str(fit_parsed.get("email_opener") or "").strip()
                    niche_signals = fit_parsed.get("signals") or []
                    niche_fit_score = int(fit_parsed.get("niche_fit_score") or 0)
                    if email_opener:
                        deep_payload["email_opener"] = email_opener
                    if niche_signals:
                        deep_payload["niche_signals"] = niche_signals
                    if niche_fit_score:
                        deep_payload["niche_fit_score"] = niche_fit_score
                    # Use fit_reason as ai_summary if stronger than existing one
                    fit_reason = str(fit_parsed.get("fit_reason") or "").strip()
                    if fit_reason and (not ai_summary or ai_summary == "AI scoring completed."):
                        ai_summary = fit_reason
                except Exception as fexc:
                    logging.warning("Niche fit analysis failed for %s: %s", business_name, fexc)

            # Store in both caches
            if parsed_domain:
                _DOMAIN_SCORE_CACHE[parsed_domain] = (score, ai_summary, competitive_hook, deep_payload, time.time())

            return score, ai_summary, competitive_hook, deep_payload

        except (TimeoutError, asyncio.TimeoutError):
            logging.warning("OpenAI timeout for %s — using heuristic score.", business_name)
            heuristic_score = self._heuristic_score(
                website_url=website_url, rating=rating, review_count=review_count,
                shortcoming=shortcoming, insecure_site=insecure_site, has_email=has_email,
            )
            deep_payload = self._build_deep_intelligence_payload(
                business_name=business_name,
                website_url=website_url,
                rating=rating,
                review_count=review_count,
                shortcoming=shortcoming,
                page_excerpt=page_excerpt,
                has_email=has_email,
                address=address,
                search_keyword=search_keyword,
                ai_score=heuristic_score,
                google_claimed=google_claimed,
                website_signals=website_signals,
                social_profiles=social_profiles,
                social_metrics=social_metrics,
            )
            return (
                heuristic_score,
                "AI scoring timed out (15s limit). Heuristic score used.",
                str(deep_payload.get("competitive_hook") or "").strip(),
                deep_payload,
            )
        except Exception as exc:
            logging.warning("AI score generation failed for %s: %s", business_name, exc)
            heuristic_score = self._heuristic_score(
                website_url=website_url, rating=rating, review_count=review_count,
                shortcoming=shortcoming, insecure_site=insecure_site, has_email=has_email,
            )
            deep_payload = self._build_deep_intelligence_payload(
                business_name=business_name,
                website_url=website_url,
                rating=rating,
                review_count=review_count,
                shortcoming=shortcoming,
                page_excerpt=page_excerpt,
                has_email=has_email,
                address=address,
                search_keyword=search_keyword,
                ai_score=heuristic_score,
                google_claimed=google_claimed,
                website_signals=website_signals,
                social_profiles=social_profiles,
                social_metrics=social_metrics,
            )
            return (
                heuristic_score,
                str(shortcoming or "").strip() or "Heuristic scoring used after AI failure.",
                str(deep_payload.get("competitive_hook") or "").strip(),
                deep_payload,
            )

    async def _score_lead_priority_async(
        self,
        system_prompt: str,
        payload: dict | str,
        temperature: float,
    ) -> dict:
        timeout = aiohttp.ClientTimeout(total=OPENAI_REQUEST_TIMEOUT_SECONDS)
        user_content = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
        request_payload = {
            "model": self.ai_model or FORCED_AI_MODEL,
            "temperature": temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.openai_api_key}",
            "Content-Type": "application/json",
        }

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for attempt in range(OPENAI_429_MAX_RETRIES + 1):
                async with session.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=request_payload,
                ) as response:
                    body = await response.text()
                    if response.status == 429:
                        if attempt < OPENAI_429_MAX_RETRIES:
                            backoff_seconds = OPENAI_429_BACKOFF_BASE_SECONDS * (2 ** attempt)
                            logging.warning(
                                "OpenAI returned 429. Retrying in %.1fs (attempt %s/%s).",
                                backoff_seconds,
                                attempt + 1,
                                OPENAI_429_MAX_RETRIES,
                            )
                            await asyncio.sleep(backoff_seconds)
                            continue
                        raise RuntimeError(f"OpenAI HTTP 429: {body[:400]}")

                    if response.status >= 400:
                        raise RuntimeError(f"OpenAI HTTP {response.status}: {body[:400]}")

                    data = json.loads(body or "{}")
                    content = (
                        data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "{}")
                    )
                    return json.loads(content or "{}")

        raise RuntimeError("OpenAI scoring failed after retry attempts.")

    def _check_supabase_score_cache(
        self, business_name: str, website_url: Optional[str]
    ) -> Optional[tuple[int, str, str, dict]]:
        """Query Supabase leads table for an existing ai_score for this business/domain.
        Returns (score, ai_description, competitive_hook, deep_payload) if found, else None."""
        try:
            import importlib as _il
            _sb_mod = _il.import_module("supabase")
            _create = getattr(_sb_mod, "create_client")
        except Exception:
            return None
        try:
            import json as _j
            from pathlib import Path as _P
            cfg = _j.loads(_P(self.config_path).read_text(encoding="utf-8"))
            sb = cfg.get("supabase", {})
            url = str(sb.get("url", "")).strip()
            key = str(sb.get("service_role_key", "") or sb.get("key", "")).strip()
            if not url or not key:
                return None
            client = _create(url, key)
            # search by website domain first, fall back to business name
            domain = ""
            if website_url:
                try:
                    from urllib.parse import urlparse as _up
                    domain = _up(str(website_url)).netloc.lower().replace("www.", "").strip()
                except Exception:
                    pass
            rows = []
            if domain:
                rows = (
                    client.table("leads")
                    .select("ai_score,ai_description,enrichment_data")
                    .ilike("website_url", f"%{domain}%")
                    .not_.is_("ai_score", "null")
                    .order("id", desc=True)
                    .limit(1)
                    .execute()
                    .data or []
                )
            if not rows:
                rows = (
                    client.table("leads")
                    .select("ai_score,ai_description,enrichment_data")
                    .eq("business_name", business_name)
                    .not_.is_("ai_score", "null")
                    .order("id", desc=True)
                    .limit(1)
                    .execute()
                    .data or []
                )
            if not rows:
                return None
            row = rows[0]
            score = int(float(row.get("ai_score") or 0))
            summary = str(row.get("ai_description") or "").strip()
            hook = ""
            deep_payload: dict = {}
            try:
                ed = row.get("enrichment_data")
                if ed:
                    parsed_payload = _j.loads(ed)
                    if isinstance(parsed_payload, dict):
                        deep_payload = parsed_payload
                        hook = str(parsed_payload.get("competitive_hook", "")).strip()
            except Exception:
                pass
            if score < 1:
                return None
            return score, summary, hook, deep_payload
        except Exception as exc:
            logging.debug("Supabase score cache check failed: %s", exc)
            return None

    @staticmethod
    def _heuristic_score(
        website_url: Optional[str],
        rating: Optional[float],
        review_count: Optional[int],
        shortcoming: str,
        insecure_site: bool,
        has_email: bool,
    ) -> int:
        score = 4
        if not website_url or str(website_url).strip().lower() == "none":
            score += 3
        if insecure_site:
            score += 2
        if isinstance(rating, (int, float)) and rating < 4.0:
            score += 1
        if review_count is None or review_count < 20:
            score += 1
        if "missing" in shortcoming.lower():
            score += 1
        if not has_email:
            score -= 1

        return max(1, min(10, score))

    @staticmethod
    def _verify_email_mx(email: str) -> bool:
        """Return True if the email domain resolves at least one MX record."""
        if not _HAS_DNS:
            return True  # skip check when dnspython is not installed
        domain = email.split("@", 1)[-1].lower().strip()
        if not domain:
            return False
        try:
            _dns_resolver.resolve(domain, "MX", lifetime=6.0)  # type: ignore[union-attr]
            return True
        except Exception:
            return False

    @staticmethod
    def _infer_client_tier(ai_score: int) -> str:
        if ai_score >= 9:
            return "premium_ads"
        if ai_score >= 7:
            return "standard"
        return "saas"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enrich leads with email discovery, AI score and website security status."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging verbosity.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    enrich_cmd = subparsers.add_parser("enrich", help="Populate enrichment fields and AI score.")
    enrich_cmd.add_argument("--db", default="postgres", help="Deprecated local DB arg; Postgres is used via SUPABASE_DATABASE_URL.")
    enrich_cmd.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max number of leads to enrich in one run (0 = all).",
    )
    enrich_cmd.add_argument(
        "--headless",
        action="store_true",
        help="Run Playwright in headless mode.",
    )
    enrich_cmd.add_argument(
        "--config",
        default="env",
        help="Optional settings source label for OpenAI settings when env vars are absent.",
    )
    enrich_cmd.add_argument(
        "--output",
        default="ai_mailer_ready.csv",
        help="Output CSV path for AI Mailer format.",
    )
    enrich_cmd.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip exporting AI Mailer CSV after enrichment.",
    )

    export_cmd = subparsers.add_parser("export-ai", help="Export AI Mailer ready CSV.")
    export_cmd.add_argument("--db", default="postgres", help="Deprecated local DB arg; Postgres is used via SUPABASE_DATABASE_URL.")
    export_cmd.add_argument(
        "--output",
        default="ai_mailer_ready.csv",
        help="Output CSV path for AI Mailer format.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    if args.command == "enrich":
        enricher = LeadEnricher(
            db_path=args.db,
            headless=args.headless,
            config_path=args.config,
        )
        limit = args.limit if args.limit and args.limit > 0 else None
        processed, with_email = enricher.run(limit=limit)

        print(f"Processed: {processed}")
        print(f"Found e-mails: {with_email}")

        if not args.skip_export:
            exported = enricher.export_ai_mailer_ready(output_csv=args.output)
            print(f"AI Mailer export rows: {exported} -> {args.output}")

    elif args.command == "export-ai":
        enricher = LeadEnricher(db_path=args.db, headless=True)
        exported = enricher.export_ai_mailer_ready(output_csv=args.output)
        print(f"AI Mailer export rows: {exported} -> {args.output}")


if __name__ == "__main__":
    main()
