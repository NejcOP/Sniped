import hashlib
import hmac
import inspect
import csv
from html import escape as html_escape
import json
import logging
import importlib
import io
import os
import calendar
import random
import re
import secrets
import smtplib
import sqlite3
import urllib.request
import uuid
import base64
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from functools import wraps
from pathlib import Path
from threading import BoundedSemaphore, Event, Lock, Thread
from typing import Any, Callable, List, Optional
from urllib.parse import quote_plus, urlencode, urlparse
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field
from openai import OpenAI

try:
    _supabase_module = importlib.import_module("supabase")
    create_supabase_client = getattr(_supabase_module, "create_client")
    _HAS_SUPABASE = True
except Exception:
    create_supabase_client = None  # type: ignore
    _HAS_SUPABASE = False

from backend.check_access import get_plan_feature_access, normalize_plan_key, require_feature_access
from backend.scraper.db import init_db, upsert_lead, batch_upsert_leads
from backend.scraper.exporter import export_target_leads
from backend.scraper.google_maps import GoogleMapsScraper
from backend.scraper.phone_extractor import PhoneExtractor
from backend.services.ai_mailer_service import (
    AIMailer,
    DEFAULT_COMPETITOR_BODY_TEMPLATE,
    DEFAULT_COMPETITOR_SUBJECT_TEMPLATE,
    DEFAULT_GHOST_BODY_TEMPLATE,
    DEFAULT_GHOST_SUBJECT_TEMPLATE,
    DEFAULT_GOLDEN_BODY_TEMPLATE,
    DEFAULT_GOLDEN_SUBJECT_TEMPLATE,
    DEFAULT_SPEED_BODY_TEMPLATE,
    DEFAULT_SPEED_SUBJECT_TEMPLATE,
)
from backend.services.enrichment_service import LeadEnricher
from backend.services.prompt_service import PromptFactory
from backend.stripe_webhook import extract_payment_refresh_payload

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT_DIR / "leads.db"
DEFAULT_CONFIG_PATH = ROOT_DIR / "config.json"
DEFAULT_PROFILE_DIR = ROOT_DIR / "profiles" / "maps_profile"
DEFAULT_TARGET_EXPORT = ROOT_DIR / "target_leads.csv"
DEFAULT_AI_EXPORT = ROOT_DIR / "ai_mailer_ready.csv"
TASK_TYPES = ("scrape", "enrich", "mailer")
ACTIVE_TASK_STATUSES = {"queued", "running"}
TASK_HISTORY_LIMIT = 25
STALE_QUEUED_TASK_SECONDS = 180
STALE_RUNNING_TASK_SECONDS = 7200
ORPHAN_TASK_GRACE_SECONDS = 15
SMTP_TEST_RECIPIENT = "opnjc06@gmail.com"
TRACKING_PIXEL_GIF = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==")

AUTOPILOT_ENRICH_LIMIT = 150
HIGH_AI_SCORE_THRESHOLD = 7.0
DRIP_MINUTES_MIN = 10
DRIP_MINUTES_MAX = 15
AUTO_DRIP_DISPATCH_ENABLED = False
MRR_GOAL = 16000
SETUP_MILESTONE = 6500
SETUP_FEE_WEBSITE = 1300
MRR_WEBSITE_ONLY = 200
MRR_ADS_ONLY = 1100
MRR_ADS_AND_WEBSITE = 1400
DEFAULT_GOAL_CURRENCY = "EUR"
ALLOWED_GOAL_CURRENCIES = {"EUR", "USD", "GBP"}
REPLY_STATUSES = {"interested", "meeting set"}
DEFAULT_AI_MODEL = "gpt-4o-mini"
SUPABASE_SYNC_TABLES = (
    "leads",
    "workers",
    "revenue_log",
    "delivery_tasks",
    "worker_audit_log",
    "lead_blacklist",
)

# Optional hardcoded proxy pool fallback.
# If config.json does not provide proxy_urls/proxy_url, scraper will use this list.
# Add one full URL per item, e.g. "http://user:pass@host:port".
HARDCODED_PROXY_URLS: List[str] = []

# Per-user AI usage guardrail (units/day). Enrichment consumes units ~= lead limit.
AI_DAILY_USAGE_LIMIT = int(os.environ.get("SNIPED_AI_DAILY_USAGE_LIMIT", os.environ.get("LEADFLOW_AI_DAILY_USAGE_LIMIT", "1000")))
_AI_USAGE_LOCK = Lock()
ENRICH_CONCURRENCY_LIMIT = 5
ENRICH_SEMAPHORE_TIMEOUT_SECONDS = 30
ENRICH_CAPACITY_ERROR_MESSAGE = "Server is currently at capacity. Please try again in a few minutes."

SETUP_FEE_BY_TIER = {
    "standard": SETUP_FEE_WEBSITE,
    "premium_ads": SETUP_FEE_WEBSITE,
    "saas": SETUP_FEE_WEBSITE,
}
MRR_BY_TIER = {
    "standard": MRR_WEBSITE_ONLY,
    "premium_ads": MRR_ADS_AND_WEBSITE,
    "saas": 0,
}

# ---------------------------------------------------------------------------
# Simple in-memory TTL cache for the /api/leads listing.
# Avoids hitting the DB on every 10-second poll when data hasn't changed.
# ---------------------------------------------------------------------------
import time as _time

_LEADS_CACHE: dict = {}
_LEADS_CACHE_TTL = 20  # seconds


def _invalidate_leads_cache() -> None:
    _LEADS_CACHE.clear()


def _get_cached_leads(key: str):
    entry = _LEADS_CACHE.get(key)
    if entry and (_time.monotonic() - entry["ts"]) < _LEADS_CACHE_TTL:
        return entry["data"]
    return None


def _set_cached_leads(key: str, data: dict) -> None:
    _LEADS_CACHE[key] = {"data": data, "ts": _time.monotonic()}


# User-scoped in-memory cache for niche recommendations.
_NICHE_REC_CACHE: dict[str, dict[str, Any]] = {}


class ScrapeRequest(BaseModel):
    keyword: str = Field(..., min_length=2)
    results: int = Field(25, ge=1, le=500)
    headless: bool = False
    country: str = "US"
    country_code: Optional[str] = None
    user_data_dir: Optional[str] = None
    export_targets: bool = False
    output_csv: Optional[str] = None
    min_rating: float = Field(3.5, ge=0.0, le=5.0)
    db_path: Optional[str] = None


class EnrichRequest(BaseModel):
    limit: Optional[int] = Field(default=None, ge=1)
    headless: bool = True
    output_csv: Optional[str] = None
    skip_export: bool = False
    db_path: Optional[str] = None
    token: Optional[str] = None


class MailerRequest(BaseModel):
    limit: int = Field(10, ge=1)
    delay_min: int = Field(400, ge=0)
    delay_max: int = Field(900, ge=0)
    db_path: Optional[str] = None
    config_path: Optional[str] = None
    status_allowlist: Optional[list[str]] = None
    start_after_hour_est: Optional[int] = Field(None, ge=0, le=23)


class MailPreviewRequest(BaseModel):
    regenerate: bool = False
    db_path: Optional[str] = None
    config_path: Optional[str] = None
    mail_signature: Optional[str] = None
    ghost_subject_template: Optional[str] = None
    ghost_body_template: Optional[str] = None
    golden_subject_template: Optional[str] = None
    golden_body_template: Optional[str] = None
    competitor_subject_template: Optional[str] = None
    competitor_body_template: Optional[str] = None
    speed_subject_template: Optional[str] = None
    speed_body_template: Optional[str] = None


class CampaignSequenceRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    step1_subject: Optional[str] = Field(default=None, max_length=200)
    step1_body: Optional[str] = Field(default=None, max_length=12000)
    step2_delay_days: int = Field(3, ge=1, le=30)
    step2_subject: Optional[str] = Field(default=None, max_length=200)
    step2_body: Optional[str] = Field(default=None, max_length=12000)
    step3_delay_days: int = Field(7, ge=1, le=60)
    step3_subject: Optional[str] = Field(default=None, max_length=200)
    step3_body: Optional[str] = Field(default=None, max_length=12000)
    ab_subject_a: Optional[str] = Field(default=None, max_length=200)
    ab_subject_b: Optional[str] = Field(default=None, max_length=200)
    active: bool = True


class SavedTemplateRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    category: str = Field(default="general", min_length=2, max_length=60)
    prompt_text: Optional[str] = Field(default=None, max_length=8000)
    subject_template: Optional[str] = Field(default=None, max_length=300)
    body_template: Optional[str] = Field(default=None, max_length=12000)


class CampaignEventRequest(BaseModel):
    lead_id: Optional[int] = None
    event_type: str = Field(..., min_length=3, max_length=40)
    email: Optional[str] = Field(default=None, max_length=320)
    subject_variant: Optional[str] = Field(default=None, max_length=24)
    subject_line: Optional[str] = Field(default=None, max_length=300)
    reason: Optional[str] = Field(default=None, max_length=400)
    metadata: Optional[dict[str, Any]] = None


class ExportTargetsRequest(BaseModel):
    min_score: float = Field(HIGH_AI_SCORE_THRESHOLD, ge=0.0, le=10.0)
    output_csv: Optional[str] = None
    db_path: Optional[str] = None


class ColdOutreachRequest(BaseModel):
    business_name: str = Field(..., min_length=1, max_length=200)
    city: str = Field(..., min_length=1, max_length=120)
    niche: Optional[str] = Field(default=None, max_length=120)
    pain_point: Optional[str] = Field(default=None, max_length=600)
    competitors: Optional[list[str]] = None
    monthly_loss: Optional[str] = Field(default=None, max_length=80)
    website_content: Optional[str] = Field(default=None, max_length=6000)
    linkedin_data: Optional[str] = Field(default=None, max_length=4000)
    user_defined_icp: Optional[str] = Field(default=None, max_length=500)
    config_path: Optional[str] = None


class DeepOutreachIntelRequest(BaseModel):
    raw_content: str = Field(..., min_length=20, max_length=100000)
    user_niche: Optional[str] = Field(default=None, max_length=120)
    company_name: Optional[str] = Field(default=None, max_length=200)
    location: Optional[str] = Field(default=None, max_length=200)
    token: Optional[str] = ""


class PhoneExtractRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=500)
    country_hint: Optional[str] = Field(default=None, max_length=5)


class ExportAIRequest(BaseModel):
    output_csv: Optional[str] = None
    db_path: Optional[str] = None


class LeadStatusRequest(BaseModel):
    status: str = Field(..., min_length=2, max_length=40)


class LeadTierRequest(BaseModel):
    tier: str = Field(..., min_length=2, max_length=40)


class LeadClientFolderRequest(BaseModel):
    client_folder_id: Optional[int] = None


class ClientFolderRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    color: Optional[str] = Field(default="cyan", max_length=40)
    notes: Optional[str] = Field(default=None, max_length=600)


class SavedSegmentRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    filters: dict[str, Any] = Field(default_factory=dict)


class WebhookExportRequest(BaseModel):
    target: str = Field(..., min_length=2, max_length=40)
    kind: str = Field(default="target", min_length=2, max_length=40)
    min_score: float = Field(HIGH_AI_SCORE_THRESHOLD, ge=0.0, le=10.0)


class MonthlyReportEmailRequest(BaseModel):
    recipient: Optional[str] = Field(default=None, max_length=320)


class ManualLeadRequest(BaseModel):
    contact_name: str = Field(..., min_length=2, max_length=120)
    email: str = Field(..., min_length=3, max_length=200)
    business_name: str = Field(..., min_length=2, max_length=200)


class LeadScoreInput(BaseModel):
    lead_id: Optional[int] = None
    business_name: str = Field(..., min_length=1, max_length=200)
    website_url: Optional[str] = Field(default="", max_length=500)
    location: Optional[str] = Field(default="", max_length=200)
    niche: Optional[str] = Field(default="", max_length=120)
    enrichment_data: Optional[dict] = None


class BulkLeadScoreRequest(BaseModel):
    leads: List[LeadScoreInput] = Field(..., min_length=1, max_length=50)
    token: Optional[str] = ""
    niche_override: Optional[str] = ""


class BlacklistEntryRequest(BaseModel):
    value: str = Field(..., min_length=3, max_length=320)
    kind: str = Field(default="email", min_length=4, max_length=10)
    reason: Optional[str] = Field(default="Manual blacklist", max_length=240)


class SMTPAccountUpdateRequest(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    email: Optional[str] = None
    password: Optional[str] = None
    use_tls: Optional[bool] = None
    use_ssl: Optional[bool] = None
    from_name: Optional[str] = None
    signature: Optional[str] = None


class ConfigUpdateRequest(BaseModel):
    openai_api_key: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_email: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_accounts: Optional[list[SMTPAccountUpdateRequest]] = None
    sending_strategy: Optional[str] = None
    mail_signature: Optional[str] = None
    ghost_subject_template: Optional[str] = None
    ghost_body_template: Optional[str] = None
    golden_subject_template: Optional[str] = None
    golden_body_template: Optional[str] = None
    competitor_subject_template: Optional[str] = None
    competitor_body_template: Optional[str] = None
    speed_subject_template: Optional[str] = None
    speed_body_template: Optional[str] = None
    open_tracking_base_url: Optional[str] = None
    hubspot_webhook_url: Optional[str] = None
    google_sheets_webhook_url: Optional[str] = None
    auto_weekly_report_email: Optional[bool] = None
    auto_monthly_report_email: Optional[bool] = None
    proxy_url: Optional[str] = None
    proxy_urls: Optional[str] = None  # newline-separated list of proxy URLs
    supabase_url: Optional[str] = None
    supabase_publishable_key: Optional[str] = None
    supabase_service_role_key: Optional[str] = None
    supabase_primary_mode: Optional[bool] = None


class RevenueEntryRequest(BaseModel):
    amount: float = Field(..., gt=0)
    service_type: str = Field(..., min_length=1, max_length=100)
    lead_name: Optional[str] = Field(default=None, max_length=200)
    lead_id: Optional[int] = None
    is_recurring: bool = False


class WorkerCreateRequest(BaseModel):
    worker_name: str = Field(..., min_length=2, max_length=120)
    role: str = Field(..., min_length=2, max_length=40)
    monthly_cost: float = Field(..., ge=0)
    status: str = Field(default="Active", min_length=2, max_length=20)
    comms_link: Optional[str] = Field(default=None, max_length=400)


class WorkerUpdateRequest(BaseModel):
    worker_name: Optional[str] = Field(default=None, min_length=2, max_length=120)
    role: Optional[str] = Field(default=None, min_length=2, max_length=40)
    monthly_cost: Optional[float] = Field(default=None, ge=0)
    status: Optional[str] = Field(default=None, min_length=2, max_length=20)
    comms_link: Optional[str] = Field(default=None, max_length=400)


class AssignWorkerRequest(BaseModel):
    worker_id: Optional[int] = None


class DeliveryTaskUpdateRequest(BaseModel):
    status: Optional[str] = Field(default=None, min_length=2, max_length=20)
    notes: Optional[str] = Field(default=None, max_length=1200)
    worker_id: Optional[int] = None


class TaskReorderRequest(BaseModel):
    task_ids: list[int] = Field(default_factory=list)


class SMTPTestRequest(BaseModel):
    account_index: Optional[int] = None
    host: Optional[str] = None
    port: Optional[int] = None
    email: Optional[str] = None
    password: Optional[str] = None
    use_tls: Optional[bool] = None
    use_ssl: Optional[bool] = None
    from_name: Optional[str] = None


NICHES = [
    "Paid Ads Agency",
    "Web Design & Dev",
    "SEO & Content",
    "Lead Gen Agency",
    "B2B Service Provider",
]
ACCOUNT_TYPES = {"entrepreneur", "freelancer", "agency", "company"}

STRIPE_TOP_UP_PACKAGES: dict[str, dict[str, Any]] = {
    "credits_1000": {"credits": 1000, "price_usd": 29.99, "amount_cents": 2999, "price_id": "price_1TJdbYRGcYMcfC8vEmqohR0A"},
    "credits_2500": {"credits": 2500, "price_usd": 59.00, "amount_cents": 5900, "price_id": "price_1TJdfaRGcYMcfC8vM9chdfdl"},
    "credits_5000": {"credits": 5000, "price_usd": 99.00, "amount_cents": 9900, "price_id": "price_1TJdgKRGcYMcfC8vMhO5UcVP"},
    "credits_10000": {"credits": 10000, "price_usd": 169.00, "amount_cents": 16900, "price_id": "price_1TJdgqRGcYMcfC8vJN7fgTME"},
    "credits_25000": {"credits": 25000, "price_usd": 349.00, "amount_cents": 34900, "price_id": "price_1TJdhWRGcYMcfC8vAzIwvHB7"},
    "credits_50000": {"credits": 50000, "price_usd": 699.00, "amount_cents": 69900, "price_id": "price_1TJdhvRGcYMcfC8vk1zZ3MqL"},
    "credits_100000": {"credits": 100000, "price_usd": 1119.00, "amount_cents": 111900, "price_id": "price_1TJdjXRGcYMcfC8vRBHiQYwC"},
    "credits_250000": {"credits": 250000, "price_usd": 2199.00, "amount_cents": 219900, "price_id": "price_1TJdk2RGcYMcfC8vHkN777Rw"},
    "credits_500000": {"credits": 500000, "price_usd": 3499.00, "amount_cents": 349900, "price_id": "price_1TJdkSRGcYMcfC8vHlu1NTxc"},
}
STRIPE_TOP_UP_PRICE_ID_TO_PACKAGE: dict[str, dict[str, Any]] = {
    str(config.get("price_id") or "").strip(): {"package_id": key, **config}
    for key, config in STRIPE_TOP_UP_PACKAGES.items()
    if str(config.get("price_id") or "").strip()
}
STRIPE_SUBSCRIPTION_PLANS: dict[str, dict[str, Any]] = {
    "hustler": {
        "price_id": "price_1TJHdkRGcYMcfC8viZYHscWt",
        "credits": 2000,
        "display_name": "The Hustler",
    },
    "growth": {
        "price_id": "price_1TJHeMRGcYMcfC8vevfcX7LL",
        "credits": 7000,
        "display_name": "The Growth",
    },
    "scale": {
        "price_id": "price_1TJHeiRGcYMcfC8vSribLQSd",
        "credits": 20000,
        "display_name": "The Scale",
    },
    "empire": {
        "price_id": "price_1TJHf7RGcYMcfC8vleT9raNz",
        "credits": 100000,
        "display_name": "The Empire",
    },
}
STRIPE_PRICE_ID_TO_PLAN: dict[str, dict[str, Any]] = {
    str(config.get("price_id") or "").strip(): {"plan_key": key, **config}
    for key, config in STRIPE_SUBSCRIPTION_PLANS.items()
    if str(config.get("price_id") or "").strip()
}
PLAN_DISPLAY_NAMES: dict[str, str] = {
    "free": "The Starter",
    "pro": "The Growth",
    **{
        key: str(config.get("display_name") or key.title())
        for key, config in STRIPE_SUBSCRIPTION_PLANS.items()
    },
}

PLAN_MONTHLY_QUOTAS: dict[str, int] = {
    "free": 50,
    "hustler": 2000,
    "growth": 7000,
    "scale": 20000,
    "empire": 100000,
    "pro": 7000,
}
DEFAULT_PLAN_KEY = "free"
DEFAULT_MONTHLY_CREDIT_LIMIT = int(PLAN_MONTHLY_QUOTAS.get(DEFAULT_PLAN_KEY, 50))
DEFAULT_AVERAGE_DEAL_VALUE = 1000
FREE_PLAN_NICHE_RECOMMENDATIONS_PER_MONTH = 1
FREE_PLAN_NICHE_REFRESH_DAYS = 7
PAID_PLAN_NICHE_REFRESH_HOURS = 1
PAID_PLAN_NICHE_REFRESH_DAYS = PAID_PLAN_NICHE_REFRESH_HOURS / 24


class RegisterRequest(BaseModel):
    email: str
    password: str
    niche: str
    account_type: Optional[str] = "entrepreneur"
    display_name: Optional[str] = ""
    contact_name: Optional[str] = ""


class LoginRequest(BaseModel):
    email: str
    password: str


class SessionTokenRequest(BaseModel):
    token: str


class ProfileUpdateRequest(BaseModel):
    token: str
    display_name: Optional[str] = None
    contact_name: Optional[str] = None
    account_type: Optional[str] = None
    niche: Optional[str] = None
    current_password: Optional[str] = None
    new_password: Optional[str] = None
    quickstart_completed: Optional[bool] = None
    average_deal_value: Optional[float] = Field(default=None, ge=0)


class PersonalGoalUpdateRequest(BaseModel):
    token: Optional[str] = None
    name: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None


class ForgotPasswordRequest(BaseModel):
    email: str
    reset_base_url: Optional[str] = None


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


class DeleteAccountRequest(BaseModel):
    token: str
    current_password: str


class ColdEmailOpenerRequest(BaseModel):
    token: str
    prospect_data: str
    pack_mode: Optional[str] = None  # "local_first" | "aggressive" | None


class OnboardingCompleteRequest(BaseModel):
    email: str
    password: str
    niche: str
    account_type: Optional[str] = "entrepreneur"
    display_name: Optional[str] = ""
    contact_name: Optional[str] = ""
    prospect_data: str


class AppSumoRedeemRequest(BaseModel):
    coupon_code: str


class StripeTopUpSessionRequest(BaseModel):
    package_id: str = Field(..., min_length=1, max_length=64)


class StripeSubscriptionSessionRequest(BaseModel):
    plan_id: str = Field(..., min_length=1, max_length=64)


def generate_cold_email_opener_for_niche(
    niche: str,
    prospect_data: str,
    pack_mode: Optional[str] = None,
    model_name_override: Optional[str] = None,
) -> str:
    """
    Generate a cold email opening line using the centralized Prompt Factory.

    Enforces:
    - ONE SENTENCE ONLY
    - No greetings or introductions
    - Niche-specific psychology (+ optional pack modifier)
    - Professional, no-BS tone

    Args:
        niche: User's selected niche
        prospect_data: Prospect description from the user
        pack_mode: Optional tone modifier — "local_first" or "aggressive"
    """
    client, model_name = load_openai_client(DEFAULT_CONFIG_PATH)
    if client is None:
        raise HTTPException(status_code=503, detail="OpenAI API key not configured.")

    # Get system + user prompts from centralized factory
    factory = PromptFactory()
    system_prompt, user_prompt = factory.generate_opening_line_prompt(
        niche, prospect_data, pack_mode=pack_mode
    )
    temperature = factory.get_temperature("opening_line")
    
    try:
        response = client.chat.completions.create(
            model=str(model_name_override or model_name or DEFAULT_AI_MODEL).strip() or DEFAULT_AI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=150,
            temperature=temperature,
        )
        return response.choices[0].message.content.strip().strip('"').strip("'")
    except Exception as exc:
        logging.error("cold_email_opener OpenAI error: %s", exc)
        raise HTTPException(status_code=502, detail="AI generation failed. Check your OpenAI key.")


def normalize_blacklist_domain(raw: Optional[str]) -> Optional[str]:
    value = str(raw or "").strip().lower()
    if not value:
        return None

    if "@" in value and not value.startswith(("http://", "https://")):
        value = value.split("@", 1)[1]

    parsed = urlparse(value if value.startswith(("http://", "https://")) else f"https://{value}")
    domain = parsed.netloc or parsed.path
    domain = domain.strip().lower().lstrip(".")
    if domain.startswith("www."):
        domain = domain[4:]
    return domain or None


def normalize_blacklist_entry(kind: Optional[str], value: Optional[str]) -> tuple[str, str]:
    normalized_kind = str(kind or "email").strip().lower()
    raw_value = str(value or "").strip().lower()

    if normalized_kind not in {"email", "domain"}:
        raise HTTPException(status_code=400, detail="Blacklist kind must be 'email' or 'domain'.")

    if normalized_kind == "email":
        if not raw_value or "@" not in raw_value or raw_value.startswith("@") or raw_value.endswith("@"):
            raise HTTPException(status_code=400, detail="A valid email is required.")
        return normalized_kind, raw_value

    domain_value = normalize_blacklist_domain(raw_value)
    if not domain_value:
        raise HTTPException(status_code=400, detail="A valid domain is required.")
    return normalized_kind, domain_value


def resolve_path(raw: Optional[str], default_path: Path) -> Path:
    if raw:
        return Path(raw).expanduser().resolve()
    return default_path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_country_value(country: Optional[str], country_code: Optional[str] = None) -> str:
    return (country or country_code or "US").strip().upper()


def is_slovenia_address(address: Optional[str]) -> bool:
    value = str(address or "").strip().lower()
    if not value:
        return False
    if "slovenia" in value or "slovenija" in value:
        return True
    # A lot of Google Maps addresses end with country code only (e.g. ", SI").
    return value.endswith(", si") or value.endswith(" si")


def serialize_json(value: Optional[dict]) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def deserialize_json(value: Optional[str]) -> Optional[dict]:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


PIPELINE_STAGE_VALUES = ("Scraped", "Contacted", "Replied", "Won (Paid)")
PIPELINE_STAGE_RANK = {stage: index for index, stage in enumerate(PIPELINE_STAGE_VALUES)}


def _normalize_pipeline_stage(value: Any) -> str:
    text = str(value or "").strip().lower().replace("_", " ")
    mapping = {
        "scraped": "Scraped",
        "pending": "Scraped",
        "contacted": "Contacted",
        "emailed": "Contacted",
        "replied": "Replied",
        "interested": "Replied",
        "meeting set": "Replied",
        "zoom scheduled": "Replied",
        "won": "Won (Paid)",
        "won (paid)": "Won (Paid)",
        "paid": "Won (Paid)",
        "closed": "Won (Paid)",
    }
    return mapping.get(text, "Scraped")


def _status_from_pipeline_stage(stage: Any, fallback_status: Any = None) -> str:
    normalized_stage = _normalize_pipeline_stage(stage)
    mapping = {
        "Scraped": "Pending",
        "Contacted": "Emailed",
        "Replied": "Replied",
        "Won (Paid)": "Paid",
    }
    return mapping.get(normalized_stage, str(fallback_status or "Pending").strip() or "Pending")


def _derive_pipeline_stage(
    *,
    status: Any = None,
    sent_at: Any = None,
    last_contacted_at: Any = None,
    reply_detected_at: Any = None,
    paid_at: Any = None,
    pipeline_stage: Any = None,
) -> str:
    explicit_stage = _normalize_pipeline_stage(pipeline_stage) if str(pipeline_stage or "").strip() else None

    status_value = str(status or "").strip().lower()
    if paid_at or status_value in {"paid", "closed", "won (paid)", "won"}:
        derived_stage = "Won (Paid)"
    elif reply_detected_at or status_value in {"replied", "interested", "meeting set", "zoom scheduled"}:
        derived_stage = "Replied"
    elif sent_at or last_contacted_at or status_value in {"emailed", "contacted", "failed", "bounced", "invalid_email"}:
        derived_stage = "Contacted"
    else:
        derived_stage = "Scraped"

    if explicit_stage and PIPELINE_STAGE_RANK.get(explicit_stage, 0) > PIPELINE_STAGE_RANK.get(derived_stage, 0):
        return explicit_stage
    return derived_stage


def row_to_task_dict(row: Optional[Any], task_type: str) -> dict:
    if row is None:
        return {
            "id": None,
            "task_type": task_type,
            "status": "idle",
            "running": False,
            "created_at": None,
            "started_at": None,
            "finished_at": None,
            "last_request": None,
            "result": None,
            "error": None,
            "source": None,
        }

    # Compatible with both sqlite3.Row and Supabase dict
    status = str(row.get("status") if isinstance(row, dict) else row["status"] or "idle").lower()
    return {
        "id": int(row.get("id") if isinstance(row, dict) else row["id"]),
        "task_type": row.get("task_type") if isinstance(row, dict) else row["task_type"],
        "status": status,
        "running": status in ACTIVE_TASK_STATUSES,
        "created_at": row.get("created_at") if isinstance(row, dict) else row["created_at"],
        "started_at": row.get("started_at") if isinstance(row, dict) else row["started_at"],
        "finished_at": row.get("finished_at") if isinstance(row, dict) else row["finished_at"],
        "last_request": deserialize_json(row.get("request_payload") if isinstance(row, dict) else row["request_payload"]),
        "result": deserialize_json(row.get("result_payload") if isinstance(row, dict) else row["result_payload"]),
        "error": row.get("error") if isinstance(row, dict) else row["error"],
        "source": row.get("source") if isinstance(row, dict) else (row["source"] if "source" in row.keys() else None),
    }


def parse_task_row(row: sqlite3.Row) -> dict:
    return row_to_task_dict(row, row["task_type"])


def ensure_dashboard_columns(db_path: Path) -> None:
    init_db(db_path=str(db_path))

    optional_columns = {
        "contact_name": "ALTER TABLE leads ADD COLUMN contact_name TEXT",
        "email": "ALTER TABLE leads ADD COLUMN email TEXT",
        "insecure_site": "ALTER TABLE leads ADD COLUMN insecure_site INTEGER DEFAULT 0",
        "main_shortcoming": "ALTER TABLE leads ADD COLUMN main_shortcoming TEXT",
        "ai_description": "ALTER TABLE leads ADD COLUMN ai_description TEXT",
        "enriched_at": "ALTER TABLE leads ADD COLUMN enriched_at TEXT",
        "enrichment_data": "ALTER TABLE leads ADD COLUMN enrichment_data TEXT",
        "status": "ALTER TABLE leads ADD COLUMN status TEXT",
        "enrichment_status": "ALTER TABLE leads ADD COLUMN enrichment_status TEXT DEFAULT 'pending'",
        "sent_at": "ALTER TABLE leads ADD COLUMN sent_at TEXT",
        "generated_email_body": "ALTER TABLE leads ADD COLUMN generated_email_body TEXT",
        "crm_comment": "ALTER TABLE leads ADD COLUMN crm_comment TEXT",
        "status_updated_at": "ALTER TABLE leads ADD COLUMN status_updated_at TEXT",
        "last_sender_email": "ALTER TABLE leads ADD COLUMN last_sender_email TEXT",
        "last_contacted_at": "ALTER TABLE leads ADD COLUMN last_contacted_at TEXT",
        "follow_up_count": "ALTER TABLE leads ADD COLUMN follow_up_count INTEGER DEFAULT 0",
        "ai_score": "ALTER TABLE leads ADD COLUMN ai_score REAL",
        "client_tier": "ALTER TABLE leads ADD COLUMN client_tier TEXT DEFAULT 'standard'",
        "next_mail_at": "ALTER TABLE leads ADD COLUMN next_mail_at TEXT",
        "is_ads_client": "ALTER TABLE leads ADD COLUMN is_ads_client INTEGER DEFAULT 0",
        "is_website_client": "ALTER TABLE leads ADD COLUMN is_website_client INTEGER DEFAULT 0",
        "worker_id": "ALTER TABLE leads ADD COLUMN worker_id INTEGER",
        "assigned_worker_at": "ALTER TABLE leads ADD COLUMN assigned_worker_at TEXT",
        "paid_at": "ALTER TABLE leads ADD COLUMN paid_at TEXT",
        "open_tracking_token": "ALTER TABLE leads ADD COLUMN open_tracking_token TEXT",
        "open_count": "ALTER TABLE leads ADD COLUMN open_count INTEGER DEFAULT 0",
        "first_opened_at": "ALTER TABLE leads ADD COLUMN first_opened_at TEXT",
        "last_opened_at": "ALTER TABLE leads ADD COLUMN last_opened_at TEXT",
        "campaign_sequence_id": "ALTER TABLE leads ADD COLUMN campaign_sequence_id INTEGER",
        "campaign_step": "ALTER TABLE leads ADD COLUMN campaign_step INTEGER DEFAULT 1",
        "ab_variant": "ALTER TABLE leads ADD COLUMN ab_variant TEXT",
        "last_subject_line": "ALTER TABLE leads ADD COLUMN last_subject_line TEXT",
        "reply_detected_at": "ALTER TABLE leads ADD COLUMN reply_detected_at TEXT",
        "bounced_at": "ALTER TABLE leads ADD COLUMN bounced_at TEXT",
        "bounce_reason": "ALTER TABLE leads ADD COLUMN bounce_reason TEXT",
        "phone_formatted": "ALTER TABLE leads ADD COLUMN phone_formatted TEXT",
        "phone_type": "ALTER TABLE leads ADD COLUMN phone_type TEXT",
        "pipeline_stage": "ALTER TABLE leads ADD COLUMN pipeline_stage TEXT DEFAULT 'Scraped'",
        "client_folder_id": "ALTER TABLE leads ADD COLUMN client_folder_id INTEGER",
        "user_id": "ALTER TABLE leads ADD COLUMN user_id TEXT",
        "created_at": "ALTER TABLE leads ADD COLUMN created_at TEXT",
    }

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
        for column_name, statement in optional_columns.items():
            if column_name not in columns:
                conn.execute(statement)
        conn.execute("UPDATE leads SET user_id = 'legacy' WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''")
        conn.execute(
            """
            UPDATE leads
            SET created_at = COALESCE(NULLIF(created_at, ''), NULLIF(scraped_at, ''), CURRENT_TIMESTAMP)
            WHERE created_at IS NULL OR TRIM(COALESCE(created_at, '')) = ''
            """
        )
        conn.execute(
            """
            UPDATE leads
            SET pipeline_stage = CASE
                WHEN paid_at IS NOT NULL OR LOWER(COALESCE(status, '')) IN ('paid', 'closed', 'won (paid)', 'won') THEN 'Won (Paid)'
                WHEN reply_detected_at IS NOT NULL OR LOWER(COALESCE(status, '')) IN ('replied', 'interested', 'meeting set', 'zoom scheduled') THEN 'Replied'
                WHEN sent_at IS NOT NULL OR last_contacted_at IS NOT NULL OR LOWER(COALESCE(status, '')) IN ('emailed', 'contacted', 'failed', 'bounced', 'invalid_email') THEN 'Contacted'
                ELSE 'Scraped'
            END
            WHERE pipeline_stage IS NULL OR TRIM(COALESCE(pipeline_stage, '')) = ''
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_user_id ON leads(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_user_created_at ON leads(user_id, created_at DESC, id DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_user_scraped_at ON leads(user_id, scraped_at DESC, id DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_pipeline_stage ON leads(user_id, pipeline_stage)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_client_folder_id ON leads(user_id, client_folder_id)")
        conn.commit()


def ensure_blacklist_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                value TEXT NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_blacklist_kind_value ON lead_blacklist(kind, value)"
        )
        conn.commit()


def fetch_blacklist_sets(db_path: Path) -> tuple[set[str], set[str]]:
    ensure_blacklist_table(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT kind, value FROM lead_blacklist").fetchall()

    emails: set[str] = set()
    domains: set[str] = set()
    for kind, value in rows:
        normalized_kind = str(kind or "").strip().lower()
        normalized_value = str(value or "").strip().lower()
        if not normalized_value:
            continue
        if normalized_kind == "email":
            emails.add(normalized_value)
        elif normalized_kind == "domain":
            domains.add(normalized_value)
    return emails, domains


def sync_blacklisted_leads(db_path: Path) -> int:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return sync_blacklisted_leads_supabase(DEFAULT_CONFIG_PATH)

    emails, domains = fetch_blacklist_sets(db_path)
    if not emails and not domains:
        return 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, email, website_url, status
            FROM leads
            WHERE LOWER(COALESCE(status, '')) != 'paid'
            """
        ).fetchall()

        lead_ids: list[int] = []
        for row in rows:
            email_value = str(row["email"] or "").strip().lower()
            domain_candidates = {
                normalize_blacklist_domain(row["email"]),
                normalize_blacklist_domain(row["website_url"]),
            }
            if email_value in emails or any(domain and domain in domains for domain in domain_candidates):
                lead_ids.append(int(row["id"]))

        if not lead_ids:
            return 0

        placeholders = ",".join(["?"] * len(lead_ids))
        cursor = conn.execute(
            f"""
            UPDATE leads
            SET status = 'blacklisted', next_mail_at = NULL, status_updated_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            """,
            lead_ids,
        )
        conn.commit()
        affected = int(cursor.rowcount or 0)

    if affected:
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
    return affected


def blacklist_lead_and_matches(db_path: Path, lead_id: int, reason: str = "Manual blacklist") -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return blacklist_lead_and_matches_supabase(lead_id, reason, DEFAULT_CONFIG_PATH)

    ensure_system_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, business_name, email, website_url FROM leads WHERE id = ?",
            (lead_id,),
        ).fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Lead not found")

        email_value = str(row["email"] or "").strip().lower()
        domain_values = {
            normalize_blacklist_domain(row["email"]),
            normalize_blacklist_domain(row["website_url"]),
        }
        domain_values = {value for value in domain_values if value}

        if email_value:
            conn.execute(
                """
                INSERT OR IGNORE INTO lead_blacklist (kind, value, reason, created_at)
                VALUES ('email', ?, ?, ?)
                """,
                (email_value, reason, utc_now_iso()),
            )
        for domain_value in sorted(domain_values):
            conn.execute(
                """
                INSERT OR IGNORE INTO lead_blacklist (kind, value, reason, created_at)
                VALUES ('domain', ?, ?, ?)
                """,
                (domain_value, reason, utc_now_iso()),
            )
        conn.commit()

    affected = sync_blacklisted_leads(db_path)
    maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
    _invalidate_leads_cache()
    return {
        "status": "blacklisted",
        "lead_id": lead_id,
        "business_name": row["business_name"],
        "blacklisted_email": bool(email_value),
        "blacklisted_domains": sorted(domain_values),
        "affected_leads": affected,
    }


def add_blacklist_entry(db_path: Path, *, kind: str, value: str, reason: str = "Manual blacklist") -> dict:
    normalized_kind, normalized_value = normalize_blacklist_entry(kind, value)
    clean_reason = str(reason or "Manual blacklist").strip() or "Manual blacklist"
    ensure_blacklist_table(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO lead_blacklist (kind, value, reason, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (normalized_kind, normalized_value, clean_reason, utc_now_iso()),
        )
        row = conn.execute(
            "SELECT kind, value, reason, created_at FROM lead_blacklist WHERE kind = ? AND value = ? LIMIT 1",
            (normalized_kind, normalized_value),
        ).fetchone()
        conn.commit()

    affected = sync_blacklisted_leads(db_path)
    maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
    _invalidate_leads_cache()
    return {
        "status": "blacklisted",
        "kind": normalized_kind,
        "value": normalized_value,
        "reason": row[2] if row else clean_reason,
        "created_at": row[3] if row else utc_now_iso(),
        "affected_leads": affected,
    }


def _lead_matches_blacklist_entry(kind: str, value: str, email: Optional[str], website_url: Optional[str]) -> bool:
    email_value = str(email or "").strip().lower()
    domain_candidates = {
        normalize_blacklist_domain(email),
        normalize_blacklist_domain(website_url),
    }
    domain_candidates = {domain for domain in domain_candidates if domain}

    if kind == "email":
        return email_value == value
    return value in domain_candidates


def _lead_matches_blacklist_sets(email: Optional[str], website_url: Optional[str], emails: set[str], domains: set[str]) -> bool:
    email_value = str(email or "").strip().lower()
    domain_candidates = {
        normalize_blacklist_domain(email),
        normalize_blacklist_domain(website_url),
    }
    domain_candidates = {domain for domain in domain_candidates if domain}
    return email_value in emails or any(domain in domains for domain in domain_candidates)


def restore_released_blacklisted_leads(db_path: Path, removed_entries: list[tuple[str, str]]) -> int:
    normalized_entries = [normalize_blacklist_entry(kind, value) for kind, value in removed_entries if str(value or "").strip()]
    if not normalized_entries:
        return 0

    emails, domains = fetch_blacklist_sets(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, email, website_url, status
            FROM leads
            WHERE LOWER(COALESCE(status, '')) IN ('blacklisted', 'skipped (unsubscribed)')
            """
        ).fetchall()

        lead_ids: list[int] = []
        for row in rows:
            if not any(
                _lead_matches_blacklist_entry(kind, value, row["email"], row["website_url"])
                for kind, value in normalized_entries
            ):
                continue
            if _lead_matches_blacklist_sets(row["email"], row["website_url"], emails, domains):
                continue
            lead_ids.append(int(row["id"]))

        if not lead_ids:
            return 0

        placeholders = ",".join(["?"] * len(lead_ids))
        cursor = conn.execute(
            f"""
            UPDATE leads
            SET status = 'Pending', next_mail_at = NULL, status_updated_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            """,
            lead_ids,
        )
        conn.commit()
        return int(cursor.rowcount or 0)


def remove_blacklist_entry(db_path: Path, *, kind: str, value: str) -> dict:
    normalized_kind, normalized_value = normalize_blacklist_entry(kind, value)
    ensure_blacklist_table(db_path)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "DELETE FROM lead_blacklist WHERE kind = ? AND value = ?",
            (normalized_kind, normalized_value),
        )
        conn.commit()
        deleted_count = int(cursor.rowcount or 0)

    restored = restore_released_blacklisted_leads(db_path, [(normalized_kind, normalized_value)])
    maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
    _invalidate_leads_cache()
    return {
        "status": "removed" if deleted_count else "not_found",
        "kind": normalized_kind,
        "value": normalized_value,
        "deleted_entries": deleted_count,
        "restored_leads": restored,
    }


def remove_lead_blacklist_and_matches(db_path: Path, lead_id: int) -> dict:
    ensure_system_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, business_name, email, website_url FROM leads WHERE id = ?",
            (lead_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Lead not found")

        removed_entries: list[tuple[str, str]] = []
        email_value = str(row["email"] or "").strip().lower()
        if email_value:
            removed_entries.append(("email", email_value))
        for domain_value in sorted({
            normalize_blacklist_domain(row["email"]),
            normalize_blacklist_domain(row["website_url"]),
        } - {None}):
            removed_entries.append(("domain", str(domain_value)))

        deleted_count = 0
        for entry_kind, entry_value in removed_entries:
            cursor = conn.execute(
                "DELETE FROM lead_blacklist WHERE kind = ? AND value = ?",
                (entry_kind, entry_value),
            )
            deleted_count += int(cursor.rowcount or 0)
        conn.commit()

    restored = restore_released_blacklisted_leads(db_path, removed_entries)
    maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
    _invalidate_leads_cache()
    return {
        "status": "removed" if deleted_count else "not_found",
        "lead_id": lead_id,
        "business_name": row["business_name"],
        "deleted_entries": deleted_count,
        "restored_leads": restored,
    }


def ensure_revenue_log_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS revenue_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                amount REAL NOT NULL,
                service_type TEXT NOT NULL,
                lead_name TEXT,
                lead_id INTEGER,
                is_recurring INTEGER NOT NULL DEFAULT 0,
                date TEXT NOT NULL
            )
            """
        )
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(revenue_log)").fetchall()}
        if "user_id" not in columns:
            conn.execute("ALTER TABLE revenue_log ADD COLUMN user_id TEXT NOT NULL DEFAULT 'legacy'")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_revenue_log_user_id ON revenue_log(user_id)")
        try:
            first_user_row = conn.execute("SELECT id FROM users ORDER BY id ASC LIMIT 1").fetchone()
        except sqlite3.OperationalError:
            first_user_row = None
        if first_user_row is not None:
            conn.execute(
                "UPDATE revenue_log SET user_id = ? WHERE user_id = 'legacy'",
                (str(first_user_row[0]),),
            )
        conn.commit()


def _ensure_jobs_table_sqlite(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'default',
                type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                payload TEXT NOT NULL DEFAULT '{}',
                result TEXT,
                error TEXT,
                worker_id TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                started_at TEXT,
                completed_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, created_at ASC)")
        conn.commit()


def ensure_workers_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                worker_name TEXT NOT NULL,
                role TEXT NOT NULL,
                monthly_cost REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'Active',
                comms_link TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(workers)").fetchall()}
        if "user_id" not in columns:
            conn.execute("ALTER TABLE workers ADD COLUMN user_id TEXT")
            conn.execute("UPDATE workers SET user_id = 'legacy' WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_workers_user_id ON workers(user_id)")
        conn.commit()


def ensure_worker_audit_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER,
                lead_id INTEGER,
                action TEXT NOT NULL,
                message TEXT,
                actor TEXT NOT NULL DEFAULT 'system',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_worker_audit_created ON worker_audit_log(created_at DESC)"
        )
        conn.commit()


def ensure_delivery_tasks_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                lead_id INTEGER NOT NULL,
                worker_id INTEGER,
                business_name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'todo',
                notes TEXT,
                due_at TEXT NOT NULL,
                done_at TEXT,
                position INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(delivery_tasks)").fetchall()}
        if "user_id" not in columns:
            conn.execute("ALTER TABLE delivery_tasks ADD COLUMN user_id TEXT")
            conn.execute("UPDATE delivery_tasks SET user_id = 'legacy' WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''")
        if "position" not in columns:
            conn.execute("ALTER TABLE delivery_tasks ADD COLUMN position INTEGER")
        conn.execute(
            "UPDATE delivery_tasks SET position = id WHERE position IS NULL OR CAST(COALESCE(position, 0) AS INTEGER) <= 0"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_delivery_tasks_status_due ON delivery_tasks(status, due_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_delivery_tasks_lead ON delivery_tasks(lead_id)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_delivery_tasks_user_id ON delivery_tasks(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_delivery_tasks_user_position ON delivery_tasks(user_id, position)")
        conn.commit()


def add_worker_audit(
    db_path: Path,
    *,
    action: str,
    message: str,
    worker_id: Optional[int] = None,
    lead_id: Optional[int] = None,
    actor: str = "system",
) -> None:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        add_worker_audit_supabase(
            DEFAULT_CONFIG_PATH,
            action=action,
            message=message,
            worker_id=worker_id,
            lead_id=lead_id,
            actor=actor,
        )
        return

    ensure_worker_audit_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO worker_audit_log (worker_id, lead_id, action, message, actor, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (worker_id, lead_id, action.strip().lower(), message.strip(), actor.strip() or "system", utc_now_iso()),
        )
        conn.commit()


def infer_worker_role_for_lead(tier_value: str) -> str:
    tier_key = str(tier_value or "standard").strip().lower()
    if tier_key == "premium_ads":
        return "DEV"
    if tier_key == "saas":
        return "DEV"
    return "DEV"


def auto_assign_worker_to_paid_lead(db_path: Path, lead_id: int) -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return auto_assign_worker_to_paid_lead_supabase(lead_id, DEFAULT_CONFIG_PATH)

    ensure_workers_table(db_path)
    ensure_dashboard_columns(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lead = conn.execute(
            """
            SELECT id, business_name, status, client_tier, worker_id, user_id
            FROM leads
            WHERE id = ?
            """,
            (lead_id,),
        ).fetchone()

        if lead is None:
            return {"auto_assigned": False, "reason": "Lead not found"}
        if str(lead["status"] or "").strip().lower() != "paid":
            return {"auto_assigned": False, "reason": "Lead is not paid"}
        if lead["worker_id"] is not None:
            return {"auto_assigned": False, "reason": "Lead already assigned", "worker_id": int(lead["worker_id"])}

        target_role = infer_worker_role_for_lead(str(lead["client_tier"] or "standard"))
        lead_user_id = str(lead["user_id"] or "legacy")
        candidate = conn.execute(
            """
            SELECT
                w.id,
                w.worker_name,
                w.role,
                COALESCE(loads.assigned_count, 0) AS assigned_count
            FROM workers w
            LEFT JOIN (
                SELECT worker_id, COUNT(*) AS assigned_count
                FROM leads
                WHERE worker_id IS NOT NULL AND user_id = ?
                GROUP BY worker_id
            ) loads ON loads.worker_id = w.id
            WHERE LOWER(COALESCE(w.status, '')) = 'active'
              AND UPPER(COALESCE(w.role, '')) = ?
              AND w.user_id = ?
            ORDER BY assigned_count ASC, w.id ASC
            LIMIT 1
            """,
            (lead_user_id, target_role, lead_user_id),
        ).fetchone()

        if candidate is None:
            candidate = conn.execute(
                """
                SELECT
                    w.id,
                    w.worker_name,
                    w.role,
                    COALESCE(loads.assigned_count, 0) AS assigned_count
                FROM workers w
                LEFT JOIN (
                    SELECT worker_id, COUNT(*) AS assigned_count
                    FROM leads
                    WHERE worker_id IS NOT NULL AND user_id = ?
                    GROUP BY worker_id
                ) loads ON loads.worker_id = w.id
                WHERE LOWER(COALESCE(w.status, '')) = 'active'
                  AND w.user_id = ?
                ORDER BY assigned_count ASC, w.id ASC
                LIMIT 1
                """,
                (lead_user_id, lead_user_id),
            ).fetchone()

        if candidate is None:
            return {"auto_assigned": False, "reason": "No active workers available"}

        assigned_at = utc_now_iso()
        conn.execute(
            """
            UPDATE leads
            SET worker_id = ?, assigned_worker_at = ?
            WHERE id = ?
            """,
            (int(candidate["id"]), assigned_at, lead_id),
        )
        conn.execute(
            """
            UPDATE delivery_tasks
            SET worker_id = ?, updated_at = ?
            WHERE lead_id = ?
              AND LOWER(COALESCE(status, '')) IN ('todo', 'in_progress', 'blocked')
            """,
            (int(candidate["id"]), utc_now_iso(), lead_id),
        )
        conn.commit()

    add_worker_audit(
        db_path,
        action="auto_assign",
        worker_id=int(candidate["id"]),
        lead_id=lead_id,
        message=f"Auto-assigned paid client '{lead['business_name']}' to {candidate['worker_name']} ({candidate['role']}).",
    )
    return {
        "auto_assigned": True,
        "worker_id": int(candidate["id"]),
        "worker_name": candidate["worker_name"],
        "worker_role": candidate["role"],
    }


def ensure_delivery_task_for_paid_lead(db_path: Path, lead_id: int) -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return ensure_delivery_task_for_paid_lead_supabase(lead_id, DEFAULT_CONFIG_PATH)

    ensure_delivery_tasks_table(db_path)
    ensure_dashboard_columns(db_path)

    task_map = {
        "premium_ads": "Website + Google Ads Setup",
        "saas": "Dev Onboarding",
        "standard": "Website Setup",
    }

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lead = conn.execute(
            """
            SELECT id, business_name, status, client_tier, worker_id, user_id
            FROM leads
            WHERE id = ?
            """,
            (lead_id,),
        ).fetchone()
        if lead is None:
            return {"task_created": False, "reason": "Lead not found"}
        if str(lead["status"] or "").strip().lower() != "paid":
            return {"task_created": False, "reason": "Lead is not paid"}

        existing = conn.execute(
            """
            SELECT id
            FROM delivery_tasks
            WHERE lead_id = ?
              AND LOWER(COALESCE(status, '')) IN ('todo', 'in_progress', 'blocked')
            ORDER BY id DESC
            LIMIT 1
            """,
            (lead_id,),
        ).fetchone()

        if existing is not None:
            return {"task_created": False, "task_id": int(existing["id"]), "reason": "Open task already exists"}

        tier_key = str(lead["client_tier"] or "standard").strip().lower() or "standard"
        task_type = task_map.get(tier_key, task_map["standard"])
        due_at = (datetime.now(timezone.utc) + timedelta(hours=48)).isoformat()
        now_iso = utc_now_iso()
        lead_user_id = str(lead["user_id"] or "legacy")
        next_position_row = conn.execute(
            "SELECT COALESCE(MAX(position), 0) + 1 AS next_position FROM delivery_tasks WHERE user_id = ?",
            (lead_user_id,),
        ).fetchone()
        next_position = int(next_position_row["next_position"] or 1) if next_position_row is not None else 1

        cursor = conn.execute(
            """
            INSERT INTO delivery_tasks (user_id, lead_id, worker_id, business_name, task_type, status, notes, due_at, done_at, position, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'todo', NULL, ?, NULL, ?, ?, ?)
            """,
            (
                lead_user_id,
                lead_id,
                int(lead["worker_id"]) if lead["worker_id"] is not None else None,
                str(lead["business_name"] or "").strip() or f"Lead #{lead_id}",
                task_type,
                due_at,
                next_position,
                now_iso,
                now_iso,
            ),
        )
        conn.commit()
        task_id = int(cursor.lastrowid)

    add_worker_audit(
        db_path,
        action="delivery_task_created",
        worker_id=int(lead["worker_id"]) if lead["worker_id"] is not None else None,
        lead_id=lead_id,
        message=f"Created delivery task '{task_type}' for paid client '{lead['business_name']}'.",
    )
    return {"task_created": True, "task_id": task_id, "task_type": task_type}


def get_workers_snapshot(db_path: Path, user_id: Optional[str] = None) -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return get_workers_snapshot_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)

    ensure_workers_table(db_path)
    ensure_worker_audit_table(db_path)
    ensure_delivery_tasks_table(db_path)
    ensure_dashboard_columns(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if user_id:
            workers = conn.execute(
                """
                SELECT id, worker_name, role, monthly_cost, status, comms_link, created_at, updated_at
                FROM workers
                WHERE user_id = ?
                ORDER BY id DESC
                """,
                (user_id,),
            ).fetchall()
        else:
            workers = conn.execute(
                """
                SELECT id, worker_name, role, monthly_cost, status, comms_link, created_at, updated_at
                FROM workers
                ORDER BY id DESC
                """
            ).fetchall()

        if user_id:
            assignments = conn.execute(
                """
                SELECT
                    l.worker_id,
                    l.business_name,
                    LOWER(COALESCE(l.client_tier, 'standard')) AS tier,
                    LOWER(COALESCE(l.status, '')) AS status,
                    l.paid_at,
                    l.assigned_worker_at
                FROM leads l
                WHERE l.worker_id IS NOT NULL AND l.user_id = ?
                """,
                (user_id,),
            ).fetchall()
        else:
            assignments = conn.execute(
                """
                SELECT
                    l.worker_id,
                    l.business_name,
                    LOWER(COALESCE(l.client_tier, 'standard')) AS tier,
                    LOWER(COALESCE(l.status, '')) AS status,
                    l.paid_at,
                    l.assigned_worker_at
                FROM leads l
                WHERE l.worker_id IS NOT NULL
                """
            ).fetchall()

        if user_id:
            completed_tasks = conn.execute(
                """
                SELECT dt.worker_id, dt.done_at, l.paid_at
                FROM delivery_tasks dt
                JOIN leads l ON l.id = dt.lead_id
                WHERE LOWER(COALESCE(dt.status, '')) = 'done'
                  AND dt.done_at IS NOT NULL
                  AND l.paid_at IS NOT NULL
                  AND l.user_id = ?
                """,
                (user_id,),
            ).fetchall()
        else:
            completed_tasks = conn.execute(
                """
                SELECT dt.worker_id, dt.done_at, l.paid_at
                FROM delivery_tasks dt
                JOIN leads l ON l.id = dt.lead_id
                WHERE LOWER(COALESCE(dt.status, '')) = 'done'
                  AND dt.done_at IS NOT NULL
                  AND l.paid_at IS NOT NULL
                """
            ).fetchall()

        if user_id:
            audit_rows = conn.execute(
                """
                SELECT wa.id, wa.worker_id, wa.lead_id, wa.action, wa.message, wa.actor, wa.created_at
                FROM worker_audit_log wa
                LEFT JOIN leads l ON l.id = wa.lead_id
                LEFT JOIN workers w ON w.id = wa.worker_id
                WHERE (l.user_id = ?) OR (w.user_id = ?)
                ORDER BY wa.id DESC
                LIMIT 40
                """,
                (user_id, user_id),
            ).fetchall()
        else:
            audit_rows = conn.execute(
                """
                SELECT id, worker_id, lead_id, action, message, actor, created_at
                FROM worker_audit_log
                ORDER BY id DESC
                LIMIT 40
                """
            ).fetchall()

    worker_map: dict[int, dict] = {}
    for row in workers:
        worker_id = int(row["id"])
        worker_map[worker_id] = {
            "id": worker_id,
            "worker_name": row["worker_name"],
            "role": row["role"],
            "monthly_cost": float(row["monthly_cost"] or 0),
            "status": row["status"],
            "comms_link": row["comms_link"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "assigned_clients": [],
            "assigned_clients_count": 0,
            "total_profit_generated": 0.0,
            "profitability_metric": 0.0,
        }

    efficiency_days: list[float] = []
    for row in assignments:
        worker_id_raw = row["worker_id"]
        if worker_id_raw is None:
            continue
        worker_id = int(worker_id_raw)
        worker = worker_map.get(worker_id)
        if worker is None:
            continue

        business_name = str(row["business_name"] or "").strip()
        if business_name:
            worker["assigned_clients"].append(business_name)

        if row["status"] == "paid":
            tier_key = str(row["tier"] or "standard").strip().lower() or "standard"
            setup_fee = float(SETUP_FEE_BY_TIER.get(tier_key, SETUP_FEE_BY_TIER["standard"]))
            monthly_fee = float(MRR_BY_TIER.get(tier_key, MRR_BY_TIER["standard"]))
            worker["total_profit_generated"] += setup_fee + monthly_fee

            paid_at = parse_iso_datetime(row["paid_at"])
            assigned_worker_at = parse_iso_datetime(row["assigned_worker_at"])
            if not completed_tasks and paid_at and assigned_worker_at and assigned_worker_at >= paid_at:
                delta_days = (assigned_worker_at - paid_at).total_seconds() / 86400
                efficiency_days.append(delta_days)

    for row in completed_tasks:
        paid_at = parse_iso_datetime(row["paid_at"])
        done_at = parse_iso_datetime(row["done_at"])
        if paid_at and done_at and done_at >= paid_at:
            efficiency_days.append((done_at - paid_at).total_seconds() / 86400)

    total_team_cost = 0.0
    total_generated = 0.0
    for worker in worker_map.values():
        worker["assigned_clients"] = sorted(set(worker["assigned_clients"]))
        worker["assigned_clients_count"] = len(worker["assigned_clients"])
        worker["profitability_metric"] = worker["total_profit_generated"] - worker["monthly_cost"]
        total_team_cost += float(worker["monthly_cost"])
        total_generated += float(worker["total_profit_generated"])

    delivery_efficiency_days = round(sum(efficiency_days) / len(efficiency_days), 1) if efficiency_days else 0.0
    net_agency_margin = total_generated - total_team_cost

    return {
        "items": list(worker_map.values()),
        "metrics": {
            "total_team_cost": round(total_team_cost, 2),
            "delivery_efficiency_days": delivery_efficiency_days,
            "net_agency_margin": round(net_agency_margin, 2),
        },
        "audit": [dict(row) for row in audit_rows],
    }


def ensure_system_task_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS system_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                task_type TEXT NOT NULL,
                status TEXT NOT NULL,
                source TEXT DEFAULT 'api',
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                request_payload TEXT,
                result_payload TEXT,
                error TEXT
            )
            """
        )

        columns = {row[1] for row in conn.execute("PRAGMA table_info(system_tasks)").fetchall()}
        if "source" not in columns:
            conn.execute("ALTER TABLE system_tasks ADD COLUMN source TEXT DEFAULT 'api'")
        if "user_id" not in columns:
            conn.execute("ALTER TABLE system_tasks ADD COLUMN user_id TEXT")
            conn.execute("UPDATE system_tasks SET user_id = 'legacy' WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''")

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_system_tasks_type_id ON system_tasks(task_type, id DESC)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_system_tasks_user_type_id ON system_tasks(user_id, task_type, id DESC)")
        conn.commit()


def ensure_runtime_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS system_runtime (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def ensure_users_table(db_path: Path) -> None:
    ensure_dashboard_columns(db_path)
    ensure_blacklist_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                email         TEXT    NOT NULL UNIQUE,
                password_hash TEXT    NOT NULL,
                salt          TEXT    NOT NULL,
                niche         TEXT    NOT NULL DEFAULT 'B2B Service Provider',
                account_type  TEXT    NOT NULL DEFAULT 'entrepreneur',
                display_name  TEXT    NOT NULL DEFAULT '',
                contact_name  TEXT    NOT NULL DEFAULT '',
                token         TEXT    UNIQUE,
                credits_balance INTEGER NOT NULL DEFAULT 0,
                monthly_quota INTEGER NOT NULL DEFAULT 50,
                credits_limit INTEGER NOT NULL DEFAULT 50,
                monthly_limit INTEGER NOT NULL DEFAULT 50,
                topup_credits_balance INTEGER NOT NULL DEFAULT 0,
                subscription_start_date TEXT,
                subscription_active INTEGER NOT NULL DEFAULT 0,
                subscription_status TEXT,
                subscription_cancel_at TEXT,
                subscription_cancel_at_period_end INTEGER NOT NULL DEFAULT 0,
                plan_key TEXT NOT NULL DEFAULT 'free',
                stripe_customer_id TEXT,
                quickstart_completed INTEGER NOT NULL DEFAULT 0,
                average_deal_value REAL NOT NULL DEFAULT 1000,
                smtp_accounts_json TEXT,
                reset_token   TEXT,
                reset_token_expires_at TEXT,
                created_at    TEXT    NOT NULL,
                updated_at    TEXT
            )
            """
        )
        # Migrate existing tables that may be missing new columns
        for col, typedef in [
            ("account_type", "TEXT NOT NULL DEFAULT 'entrepreneur'"),
            ("display_name", "TEXT NOT NULL DEFAULT ''"),
            ("contact_name", "TEXT NOT NULL DEFAULT ''"),
            ("credits_balance", "INTEGER NOT NULL DEFAULT 0"),
            ("monthly_quota", f"INTEGER NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT}"),
            ("credits_limit", f"INTEGER NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT}"),
            ("monthly_limit", f"INTEGER NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT}"),
            ("topup_credits_balance", "INTEGER NOT NULL DEFAULT 0"),
            ("subscription_start_date", "TEXT"),
            ("subscription_active", "INTEGER NOT NULL DEFAULT 0"),
            ("subscription_status", "TEXT"),
            ("subscription_cancel_at", "TEXT"),
            ("subscription_cancel_at_period_end", "INTEGER NOT NULL DEFAULT 0"),
            ("plan_key", "TEXT NOT NULL DEFAULT 'free'"),
            ("stripe_customer_id", "TEXT"),
            ("quickstart_completed", "INTEGER NOT NULL DEFAULT 0"),
            ("average_deal_value", f"REAL NOT NULL DEFAULT {DEFAULT_AVERAGE_DEAL_VALUE}"),
            ("smtp_accounts_json", "TEXT"),
            ("reset_token", "TEXT"),
            ("reset_token_expires_at", "TEXT"),
            ("updated_at", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {typedef}")
            except Exception:
                pass
        conn.execute("UPDATE users SET credits_balance = COALESCE(credits_balance, 0)")
        conn.execute(f"UPDATE users SET credits_limit = COALESCE(NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})")
        conn.execute(f"UPDATE users SET monthly_quota = COALESCE(NULLIF(monthly_quota, 0), NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})")
        conn.execute(f"UPDATE users SET monthly_limit = COALESCE(NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})")
        conn.execute("UPDATE users SET monthly_limit = monthly_quota WHERE COALESCE(NULLIF(monthly_quota, 0), 0) > 0")
        conn.execute("UPDATE users SET credits_limit = monthly_quota WHERE COALESCE(NULLIF(monthly_quota, 0), 0) > 0")
        conn.execute("UPDATE users SET topup_credits_balance = COALESCE(topup_credits_balance, 0)")
        conn.execute("UPDATE users SET subscription_active = COALESCE(subscription_active, 0)")
        conn.execute("UPDATE users SET subscription_cancel_at_period_end = COALESCE(subscription_cancel_at_period_end, 0)")
        conn.execute("UPDATE users SET quickstart_completed = COALESCE(quickstart_completed, 0)")
        conn.execute(
            f"UPDATE users SET average_deal_value = CASE WHEN COALESCE(average_deal_value, 0) <= 0 THEN {DEFAULT_AVERAGE_DEAL_VALUE} ELSE average_deal_value END"
        )
        conn.execute(
            """
            UPDATE users
            SET plan_key = CASE
                WHEN LOWER(TRIM(COALESCE(plan_key, ''))) IN ('free', 'hustler', 'growth', 'scale', 'empire', 'pro')
                    THEN LOWER(TRIM(COALESCE(plan_key, '')))
                WHEN COALESCE(subscription_active, 0) IN (1, '1', 'true', 'TRUE')
                    THEN 'pro'
                ELSE 'free'
            END
            """
        )
        conn.execute(
            f"""
            UPDATE users
            SET monthly_quota = {DEFAULT_MONTHLY_CREDIT_LIMIT},
                monthly_limit = {DEFAULT_MONTHLY_CREDIT_LIMIT},
                credits_limit = {DEFAULT_MONTHLY_CREDIT_LIMIT},
                credits_balance = MAX(COALESCE(credits_balance, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT} + COALESCE(topup_credits_balance, 0))
            WHERE LOWER(COALESCE(NULLIF(plan_key, ''), 'free')) = 'free'
              AND COALESCE(subscription_active, 0) IN (0, '0', 'false', 'FALSE', '')
              AND (
                    COALESCE(NULLIF(monthly_quota, 0), 0) != {DEFAULT_MONTHLY_CREDIT_LIMIT}
                 OR COALESCE(NULLIF(monthly_limit, 0), 0) != {DEFAULT_MONTHLY_CREDIT_LIMIT}
                 OR COALESCE(NULLIF(credits_limit, 0), 0) != {DEFAULT_MONTHLY_CREDIT_LIMIT}
              )
            """
        )
        conn.execute(
            """
            UPDATE users
            SET updated_at = COALESCE(NULLIF(updated_at, ''), created_at, CURRENT_TIMESTAMP)
            WHERE updated_at IS NULL OR TRIM(COALESCE(updated_at, '')) = ''
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_reset_token ON users(reset_token)")
        conn.commit()


def _hash_password(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), 260_000
    ).hex()


def ensure_system_tables(db_path: Path) -> None:
    ensure_dashboard_columns(db_path)
    ensure_system_task_table(db_path)
    ensure_runtime_table(db_path)
    ensure_blacklist_table(db_path)
    ensure_revenue_log_table(db_path)
    ensure_workers_table(db_path)
    ensure_worker_audit_table(db_path)
    ensure_delivery_tasks_table(db_path)
    ensure_users_table(db_path)
    ensure_mailer_campaign_tables(db_path)
    ensure_client_success_tables(db_path)


def ensure_client_success_tables(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS client_folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                name TEXT NOT NULL,
                color TEXT DEFAULT 'cyan',
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                name TEXT NOT NULL,
                filters_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_client_folders_user_id ON client_folders(user_id, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_saved_segments_user_updated ON saved_segments(user_id, updated_at)")
        conn.execute(
            """
            UPDATE leads
            SET pipeline_stage = CASE
                WHEN paid_at IS NOT NULL OR LOWER(COALESCE(status, '')) IN ('paid', 'closed', 'won (paid)', 'won') THEN 'Won (Paid)'
                WHEN reply_detected_at IS NOT NULL OR LOWER(COALESCE(status, '')) IN ('replied', 'interested', 'meeting set', 'zoom scheduled') THEN 'Replied'
                WHEN sent_at IS NOT NULL OR last_contacted_at IS NOT NULL OR LOWER(COALESCE(status, '')) IN ('emailed', 'contacted', 'failed', 'bounced', 'invalid_email') THEN 'Contacted'
                ELSE 'Scraped'
            END
            WHERE pipeline_stage IS NULL OR TRIM(COALESCE(pipeline_stage, '')) = ''
            """
        )
        conn.commit()


def ensure_mailer_campaign_tables(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
        optional_columns = {
            "campaign_sequence_id": "ALTER TABLE leads ADD COLUMN campaign_sequence_id INTEGER",
            "campaign_step": "ALTER TABLE leads ADD COLUMN campaign_step INTEGER DEFAULT 1",
            "ab_variant": "ALTER TABLE leads ADD COLUMN ab_variant TEXT",
            "last_subject_line": "ALTER TABLE leads ADD COLUMN last_subject_line TEXT",
            "reply_detected_at": "ALTER TABLE leads ADD COLUMN reply_detected_at TEXT",
            "bounced_at": "ALTER TABLE leads ADD COLUMN bounced_at TEXT",
            "bounce_reason": "ALTER TABLE leads ADD COLUMN bounce_reason TEXT",
        }
        for column_name, statement in optional_columns.items():
            if column_name not in columns:
                conn.execute(statement)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mailer_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS CampaignSequences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                name TEXT NOT NULL,
                step1_subject TEXT,
                step1_body TEXT,
                step2_delay_days INTEGER DEFAULT 3,
                step2_subject TEXT,
                step2_body TEXT,
                step3_delay_days INTEGER DEFAULT 7,
                step3_subject TEXT,
                step3_body TEXT,
                ab_subject_a TEXT,
                ab_subject_b TEXT,
                active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS SavedTemplates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                name TEXT NOT NULL,
                category TEXT DEFAULT 'general',
                prompt_text TEXT,
                subject_template TEXT,
                body_template TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS CampaignEvents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                email TEXT,
                event_type TEXT NOT NULL,
                subject_variant TEXT,
                subject_line TEXT,
                metadata_json TEXT,
                occurred_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_sequences_user_active ON CampaignSequences(user_id, active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_saved_templates_user_category ON SavedTemplates(user_id, category)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_events_user_type ON CampaignEvents(user_id, event_type)")
        conn.execute("UPDATE leads SET campaign_step = 1 WHERE campaign_step IS NULL")
        conn.commit()


def _normalize_campaign_sequence_row(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    item = dict(row)
    item["active"] = bool(int(item.get("active") or 0))
    return item


def _normalize_saved_template_row(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return dict(row)


def _normalize_campaign_event_row(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    item = dict(row)
    item["metadata"] = deserialize_json(item.get("metadata_json")) or {}
    return item


def _normalize_campaign_event_type(raw_event_type: Any) -> str:
    value = str(raw_event_type or "").strip().lower().replace("_", " ")
    aliases = {
        "opened": "open",
        "reply detected": "reply",
        "replied": "reply",
        "bounced": "bounce",
    }
    return aliases.get(value, value.replace(" ", "_"))


def create_mailer_campaign_sequence(db_path: Path, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    now_iso = utc_now_iso()
    is_active = bool(payload.get("active", True))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if is_active:
            conn.execute(
                "UPDATE CampaignSequences SET active = 0, updated_at = ? WHERE user_id = ?",
                (now_iso, user_id),
            )
        cursor = conn.execute(
            """
            INSERT INTO CampaignSequences (
                user_id, name, step1_subject, step1_body, step2_delay_days, step2_subject,
                step2_body, step3_delay_days, step3_subject, step3_body, ab_subject_a,
                ab_subject_b, active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                str(payload.get("name") or "").strip(),
                str(payload.get("step1_subject") or "").strip() or None,
                str(payload.get("step1_body") or "").strip() or None,
                int(payload.get("step2_delay_days") or 3),
                str(payload.get("step2_subject") or "").strip() or None,
                str(payload.get("step2_body") or "").strip() or None,
                int(payload.get("step3_delay_days") or 7),
                str(payload.get("step3_subject") or "").strip() or None,
                str(payload.get("step3_body") or "").strip() or None,
                str(payload.get("ab_subject_a") or "").strip() or None,
                str(payload.get("ab_subject_b") or "").strip() or None,
                1 if is_active else 0,
                now_iso,
                now_iso,
            ),
        )
        row = conn.execute(
            "SELECT * FROM CampaignSequences WHERE id = ? LIMIT 1",
            (cursor.lastrowid,),
        ).fetchone()
        conn.commit()
    return _normalize_campaign_sequence_row(row)


def list_mailer_campaign_sequences(db_path: Path, user_id: str, limit: int = 25) -> list[dict[str, Any]]:
    ensure_mailer_campaign_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM CampaignSequences
            WHERE user_id = ?
            ORDER BY COALESCE(active, 0) DESC, datetime(updated_at) DESC, id DESC
            LIMIT ?
            """,
            (user_id, int(limit)),
        ).fetchall()
    return [_normalize_campaign_sequence_row(row) for row in rows]


def create_saved_mail_template(db_path: Path, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    now_iso = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            INSERT INTO SavedTemplates (
                user_id, name, category, prompt_text, subject_template, body_template, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                str(payload.get("name") or "").strip(),
                str(payload.get("category") or "general").strip() or "general",
                str(payload.get("prompt_text") or "").strip() or None,
                str(payload.get("subject_template") or "").strip() or None,
                str(payload.get("body_template") or "").strip() or None,
                now_iso,
                now_iso,
            ),
        )
        row = conn.execute(
            "SELECT * FROM SavedTemplates WHERE id = ? LIMIT 1",
            (cursor.lastrowid,),
        ).fetchone()
        conn.commit()
    return _normalize_saved_template_row(row)


def list_saved_mail_templates(db_path: Path, user_id: str, limit: int = 50) -> list[dict[str, Any]]:
    ensure_mailer_campaign_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM SavedTemplates
            WHERE user_id = ?
            ORDER BY datetime(updated_at) DESC, id DESC
            LIMIT ?
            """,
            (user_id, int(limit)),
        ).fetchall()
    return [_normalize_saved_template_row(row) for row in rows]


def record_mailer_campaign_event(db_path: Path, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    event_type = _normalize_campaign_event_type(payload.get("event_type"))
    allowed_event_types = {"sent", "open", "reply", "bounce"}
    if event_type not in allowed_event_types:
        raise HTTPException(status_code=422, detail=f"Unsupported event_type '{event_type}'")

    raw_lead_id = payload.get("lead_id")
    lead_id = int(raw_lead_id) if raw_lead_id is not None else None
    now_iso = utc_now_iso()
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    reason = str(payload.get("reason") or "").strip()
    if reason and not metadata.get("reason"):
        metadata = {**metadata, "reason": reason}

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lead_row = None
        email = str(payload.get("email") or "").strip()
        if lead_id is not None:
            lead_row = conn.execute(
                "SELECT id, email, COALESCE(NULLIF(user_id, ''), 'legacy') AS user_id, status FROM leads WHERE id = ? LIMIT 1",
                (lead_id,),
            ).fetchone()
            if lead_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_row["user_id"] or "legacy") != str(user_id or "legacy"):
                raise HTTPException(status_code=403, detail="Forbidden")
            if not email:
                email = str(lead_row["email"] or "").strip()

        cursor = conn.execute(
            """
            INSERT INTO CampaignEvents (
                lead_id, user_id, email, event_type, subject_variant, subject_line, metadata_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lead_id,
                str(user_id or "legacy"),
                email or None,
                event_type,
                str(payload.get("subject_variant") or "").strip() or None,
                str(payload.get("subject_line") or "").strip() or None,
                serialize_json(metadata) if metadata else None,
                now_iso,
            ),
        )

        if lead_row is not None:
            conn.execute(
                """
                UPDATE leads
                SET
                    last_subject_line = COALESCE(?, last_subject_line),
                    ab_variant = COALESCE(?, ab_variant)
                WHERE id = ?
                """,
                (
                    str(payload.get("subject_line") or "").strip() or None,
                    str(payload.get("subject_variant") or "").strip() or None,
                    lead_id,
                ),
            )

            if event_type == "sent":
                conn.execute(
                    """
                    UPDATE leads
                    SET
                        status = CASE
                            WHEN LOWER(COALESCE(status, '')) IN ('paid', 'closed', 'replied', 'interested', 'meeting set') THEN status
                            ELSE 'Emailed'
                        END,
                        pipeline_stage = CASE
                            WHEN LOWER(COALESCE(status, '')) IN ('paid', 'closed') THEN 'Won (Paid)'
                            WHEN LOWER(COALESCE(status, '')) IN ('replied', 'interested', 'meeting set') THEN 'Replied'
                            ELSE 'Contacted'
                        END,
                        sent_at = COALESCE(sent_at, ?),
                        last_contacted_at = COALESCE(last_contacted_at, ?)
                    WHERE id = ?
                    """,
                    (now_iso, now_iso, lead_id),
                )
            elif event_type == "open":
                conn.execute(
                    """
                    UPDATE leads
                    SET
                        open_count = COALESCE(open_count, 0) + 1,
                        first_opened_at = COALESCE(first_opened_at, ?),
                        last_opened_at = ?
                    WHERE id = ?
                    """,
                    (now_iso, now_iso, lead_id),
                )
            elif event_type == "reply":
                conn.execute(
                    """
                    UPDATE leads
                    SET
                        reply_detected_at = COALESCE(reply_detected_at, ?),
                        pipeline_stage = CASE
                            WHEN LOWER(COALESCE(status, '')) IN ('paid', 'closed') THEN 'Won (Paid)'
                            ELSE 'Replied'
                        END,
                        status = CASE
                            WHEN LOWER(COALESCE(status, '')) IN ('paid', 'closed') THEN status
                            ELSE 'Replied'
                        END
                    WHERE id = ?
                    """,
                    (now_iso, lead_id),
                )
            elif event_type == "bounce":
                conn.execute(
                    """
                    UPDATE leads
                    SET
                        bounced_at = COALESCE(bounced_at, ?),
                        bounce_reason = COALESCE(?, bounce_reason),
                        pipeline_stage = CASE
                            WHEN LOWER(COALESCE(status, '')) IN ('paid', 'closed') THEN 'Won (Paid)'
                            WHEN sent_at IS NOT NULL OR last_contacted_at IS NOT NULL THEN 'Contacted'
                            ELSE pipeline_stage
                        END,
                        status = CASE
                            WHEN LOWER(COALESCE(status, '')) IN ('paid', 'closed') THEN status
                            ELSE 'Failed'
                        END
                    WHERE id = ?
                    """,
                    (now_iso, reason or None, lead_id),
                )

        row = conn.execute(
            "SELECT * FROM CampaignEvents WHERE id = ? LIMIT 1",
            (cursor.lastrowid,),
        ).fetchone()
        conn.commit()

    return _normalize_campaign_event_row(row)


def get_mailer_campaign_stats(db_path: Path, user_id: str) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        sent = int(conn.execute(
            "SELECT COUNT(*) FROM leads WHERE COALESCE(NULLIF(user_id, ''), 'legacy') = ? AND sent_at IS NOT NULL",
            (user_id,),
        ).fetchone()[0] or 0)
        opened = int(conn.execute(
            "SELECT COUNT(*) FROM leads WHERE COALESCE(NULLIF(user_id, ''), 'legacy') = ? AND COALESCE(open_count, 0) > 0",
            (user_id,),
        ).fetchone()[0] or 0)
        replied = int(conn.execute(
            """
            SELECT COUNT(*) FROM leads
            WHERE COALESCE(NULLIF(user_id, ''), 'legacy') = ?
              AND (
                    reply_detected_at IS NOT NULL
                 OR LOWER(COALESCE(status, '')) IN ('replied', 'interested', 'meeting set')
              )
            """,
            (user_id,),
        ).fetchone()[0] or 0)
        bounced = int(conn.execute(
            """
            SELECT COUNT(*) FROM leads
            WHERE COALESCE(NULLIF(user_id, ''), 'legacy') = ?
              AND (
                    bounced_at IS NOT NULL
                 OR LOWER(COALESCE(status, '')) IN ('bounced', 'invalid_email')
              )
            """,
            (user_id,),
        ).fetchone()[0] or 0)
        opens_total = int(conn.execute(
            "SELECT COALESCE(SUM(open_count), 0) FROM leads WHERE COALESCE(NULLIF(user_id, ''), 'legacy') = ?",
            (user_id,),
        ).fetchone()[0] or 0)
        recent_rows = conn.execute(
            """
            SELECT e.*, l.business_name
            FROM CampaignEvents e
            LEFT JOIN leads l ON l.id = e.lead_id
            WHERE e.user_id = ?
            ORDER BY datetime(e.occurred_at) DESC, e.id DESC
            LIMIT 15
            """,
            (user_id,),
        ).fetchall()
        ab_rows = conn.execute(
            """
            SELECT COALESCE(subject_variant, '') AS variant, COUNT(*) AS total
            FROM CampaignEvents
            WHERE user_id = ? AND event_type = 'sent'
            GROUP BY COALESCE(subject_variant, '')
            """,
            (user_id,),
        ).fetchall()

    ab_breakdown = {"A": 0, "B": 0}
    for row in ab_rows:
        variant = str(row["variant"] or "").strip().upper()
        if variant in ab_breakdown:
            ab_breakdown[variant] = int(row["total"] or 0)

    return {
        "sent": sent,
        "opened": opened,
        "replied": replied,
        "bounced": bounced,
        "opens_total": opens_total,
        "open_rate": round((opened / sent) * 100, 2) if sent else 0.0,
        "reply_rate": round((replied / sent) * 100, 2) if sent else 0.0,
        "bounce_rate": round((bounced / sent) * 100, 2) if sent else 0.0,
        "ab_breakdown": ab_breakdown,
        "sequences": list_mailer_campaign_sequences(db_path, user_id=user_id),
        "saved_templates": list_saved_mail_templates(db_path, user_id=user_id),
        "recent_events": [_normalize_campaign_event_row(row) for row in recent_rows],
    }


def _coerce_subscription_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "active", "paid", "trialing"}


def _add_one_month(dt: datetime) -> datetime:
    year = dt.year
    month = dt.month + 1
    if month > 12:
        month = 1
        year += 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=timezone.utc)


def _is_monthly_renewal_due(subscription_start_date_raw: Optional[str], now_dt: Optional[datetime] = None) -> bool:
    start_dt = parse_iso_datetime(subscription_start_date_raw)
    if start_dt is None:
        return False
    now = now_dt or datetime.now(timezone.utc)
    due_at = _add_one_month(start_dt)
    return now >= due_at


def _is_subscription_cancel_expired(subscription_cancel_at_raw: Optional[str], now_dt: Optional[datetime] = None) -> bool:
    cancel_at_dt = parse_iso_datetime(subscription_cancel_at_raw)
    if cancel_at_dt is None:
        return False
    now = now_dt or datetime.now(timezone.utc)
    return now >= cancel_at_dt


def _normalize_plan_key(plan_key_raw: Any, fallback: str = DEFAULT_PLAN_KEY) -> str:
    value = str(plan_key_raw or "").strip().lower()
    if value in PLAN_MONTHLY_QUOTAS:
        return value
    return fallback if fallback in PLAN_MONTHLY_QUOTAS else DEFAULT_PLAN_KEY


def reset_due_monthly_credits(db_path: Path, config_path: Path) -> dict[str, int]:
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    scanned = 0
    reset = 0
    downgraded = 0
    free_quota = int(PLAN_MONTHLY_QUOTAS.get("free", DEFAULT_MONTHLY_CREDIT_LIMIT))

    if is_supabase_auth_enabled(config_path):
        if not ensure_supabase_users_table(config_path):
            return {"scanned": 0, "reset": 0}
        sb_client = get_supabase_client(config_path)
        if sb_client is None:
            return {"scanned": 0, "reset": 0}

        try:
            rows = (
                sb_client.table("users")
                .select("id,subscription_active,subscription_status,subscription_start_date,subscription_cancel_at,subscription_cancel_at_period_end,plan_key,monthly_quota,monthly_limit,credits_limit,topup_credits_balance")
                .execute()
                .data
                or []
            )
        except Exception as exc:
            logging.warning("Monthly reset scan failed (Supabase): %s", exc)
            return {"scanned": 0, "reset": 0}

        for row in rows:
            scanned += 1
            is_active = _coerce_subscription_flag(row.get("subscription_active"))
            plan_key = _normalize_plan_key(row.get("plan_key"), fallback=DEFAULT_PLAN_KEY)
            subscription_start_date = str(row.get("subscription_start_date") or "")
            cancel_at_raw = str(row.get("subscription_cancel_at") or "").strip() or None
            cancel_at_period_end = bool(row.get("subscription_cancel_at_period_end"))
            topup_balance = max(0, int(row.get("topup_credits_balance") or 0))

            # Downgrade exactly when paid period is over, but keep all purchased top-up credits.
            if (not is_active or cancel_at_period_end) and _is_subscription_cancel_expired(cancel_at_raw, now_dt=now):
                free_balance = free_quota + topup_balance
                try:
                    sb_client.table("users").update(
                        {
                            "plan_key": "free",
                            "subscription_active": False,
                            "subscription_status": "expired",
                            "subscription_cancel_at_period_end": False,
                            "monthly_quota": free_quota,
                            "monthly_limit": free_quota,
                            "credits_limit": free_quota,
                            "credits_balance": free_balance,
                            "subscription_start_date": now_iso,
                            "updated_at": now_iso,
                        }
                    ).eq("id", row.get("id")).execute()
                    downgraded += 1
                except Exception as exc:
                    logging.warning("Subscription expiry downgrade failed for user %s: %s", row.get("id"), exc)
                continue

            # Paid plans keep their balance; only free plan gets monthly top-up reset.
            if plan_key != "free":
                continue
            if not _is_monthly_renewal_due(subscription_start_date, now_dt=now):
                continue

            free_balance = free_quota + topup_balance
            try:
                sb_client.table("users").update(
                    {
                        "monthly_quota": free_quota,
                        "monthly_limit": free_quota,
                        "credits_limit": free_quota,
                        "credits_balance": free_balance,
                        "subscription_start_date": now_iso,
                        "plan_key": "free",
                        "subscription_active": False,
                        "subscription_status": "free_active",
                        "updated_at": now_iso,
                    }
                ).eq("id", row.get("id")).execute()
                reset += 1
            except Exception as exc:
                logging.warning("Free-plan monthly reset failed for user %s: %s", row.get("id"), exc)

        return {"scanned": scanned, "reset": reset, "downgraded": downgraded}

    ensure_users_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                id,
                COALESCE(subscription_active, 0) AS subscription_active,
                COALESCE(subscription_status, '') AS subscription_status,
                COALESCE(subscription_start_date, '') AS subscription_start_date,
                COALESCE(subscription_cancel_at, '') AS subscription_cancel_at,
                COALESCE(subscription_cancel_at_period_end, 0) AS subscription_cancel_at_period_end,
                COALESCE(NULLIF(plan_key, ''), 'free') AS plan_key,
                COALESCE(NULLIF(monthly_quota, 0), NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), ?) AS monthly_quota,
                COALESCE(topup_credits_balance, 0) AS topup_credits_balance
            FROM users
            """,
            (DEFAULT_MONTHLY_CREDIT_LIMIT,),
        ).fetchall()

        for row in rows:
            scanned += 1
            is_active = _coerce_subscription_flag(row["subscription_active"])
            plan_key = _normalize_plan_key(row["plan_key"], fallback=DEFAULT_PLAN_KEY)
            subscription_start_date = str(row["subscription_start_date"] or "")
            cancel_at_raw = str(row["subscription_cancel_at"] or "").strip() or None
            cancel_at_period_end = bool(int(row["subscription_cancel_at_period_end"] or 0))
            topup_balance = max(0, int(row["topup_credits_balance"] or 0))

            if (not is_active or cancel_at_period_end) and _is_subscription_cancel_expired(cancel_at_raw, now_dt=now):
                free_balance = free_quota + topup_balance
                conn.execute(
                    """
                    UPDATE users
                    SET plan_key = 'free',
                        subscription_active = 0,
                        subscription_status = 'expired',
                        subscription_cancel_at_period_end = 0,
                        monthly_quota = ?,
                        monthly_limit = ?,
                        credits_limit = ?,
                        credits_balance = ?,
                        subscription_start_date = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        free_quota,
                        free_quota,
                        free_quota,
                        free_balance,
                        now_iso,
                        now_iso,
                        row["id"],
                    ),
                )
                downgraded += 1
                continue

            # Paid plans keep their balance; only free plan gets monthly top-up reset.
            if plan_key != "free":
                continue
            if not _is_monthly_renewal_due(subscription_start_date, now_dt=now):
                continue

            free_balance = free_quota + topup_balance
            conn.execute(
                """
                UPDATE users
                SET monthly_quota = ?,
                    monthly_limit = ?,
                    credits_limit = ?,
                    credits_balance = ?,
                    subscription_start_date = ?,
                    plan_key = 'free',
                    subscription_active = 0,
                    subscription_status = 'free_active',
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    free_quota,
                    free_quota,
                    free_quota,
                    free_balance,
                    now_iso,
                    now_iso,
                    row["id"],
                ),
            )
            reset += 1

        conn.commit()
    return {"scanned": scanned, "reset": reset, "downgraded": downgraded}


def run_monthly_credit_reset_cycle(app_instance: FastAPI | None = None) -> None:
    target_app = app_instance or app
    try:
        result = reset_due_monthly_credits(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)
        logging.info(
            "Monthly credit reset cycle complete: scanned=%s reset=%s downgraded=%s",
            result.get("scanned", 0),
            result.get("reset", 0),
            result.get("downgraded", 0),
        )
    except Exception as exc:
        logging.exception("Monthly credit reset cycle failed: %s", exc)
    finally:
        if target_app is not None:
            setattr(target_app.state, "last_monthly_credit_reset_check", utc_now_iso())


def create_task_record(
    db_path: Path,
    user_id: str,
    task_type: str,
    status: str,
    request_payload: dict,
    source: str = "api",
) -> int:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                created_at = utc_now_iso()
                response = client.table("system_tasks").insert(
                    {
                        "user_id": user_id,
                        "task_type": task_type,
                        "status": status,
                        "source": source,
                        "created_at": created_at,
                        "request_payload": serialize_json(request_payload),
                    }
                ).execute()
                rows = list(getattr(response, "data", None) or [])
                if rows and rows[0].get("id") is not None:
                    return int(rows[0].get("id"))
            except Exception as exc:
                logging.warning("Supabase create_task_record fallback to SQLite: %s", exc)

    ensure_system_task_table(db_path)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO system_tasks (user_id, task_type, status, source, created_at, request_payload)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, task_type, status, source, utc_now_iso(), serialize_json(request_payload)),
        )
        conn.commit()
        return int(cursor.lastrowid)


def mark_task_running(db_path: Path, task_id: int) -> None:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                client.table("system_tasks").update(
                    {
                        "status": "running",
                        "started_at": utc_now_iso(),
                        "error": None,
                    }
                ).eq("id", task_id).execute()
                return
            except Exception as exc:
                logging.warning("Supabase mark_task_running fallback to SQLite: %s", exc)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE system_tasks
            SET status = ?, started_at = COALESCE(started_at, ?), error = NULL
            WHERE id = ?
            """,
            ("running", utc_now_iso(), task_id),
        )
        conn.commit()


def update_task_progress(db_path: Path, task_id: int, result_payload: dict) -> None:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                client.table("system_tasks").update(
                    {
                        "result_payload": serialize_json(result_payload),
                    }
                ).eq("id", task_id).execute()
                return
            except Exception as exc:
                logging.warning("Supabase update_task_progress fallback to SQLite: %s", exc)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE system_tasks
            SET result_payload = ?
            WHERE id = ?
            """,
            (serialize_json(result_payload), task_id),
        )
        conn.commit()


def finish_task_record(
    db_path: Path,
    task_id: int,
    *,
    status: str,
    result_payload: Optional[dict] = None,
    error: Optional[str] = None,
) -> None:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                client.table("system_tasks").update(
                    {
                        "status": status,
                        "result_payload": serialize_json(result_payload),
                        "error": error,
                        "finished_at": utc_now_iso(),
                    }
                ).eq("id", task_id).execute()
                return
            except Exception as exc:
                logging.warning("Supabase finish_task_record fallback to SQLite: %s", exc)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE system_tasks
            SET status = ?, result_payload = ?, error = ?, finished_at = ?
            WHERE id = ?
            """,
            (status, serialize_json(result_payload), error, utc_now_iso(), task_id),
        )
        conn.commit()


def fetch_latest_task(db_path: Path, task_type: str, user_id: Optional[str] = None) -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                rows = client.table("system_tasks").select(
                    "id,task_type,status,source,created_at,started_at,finished_at,request_payload,result_payload,error"
                ).eq("task_type", task_type)
                if user_id:
                    rows = rows.eq("user_id", user_id)
                rows = rows.order("id", desc=True).limit(1).execute().data or []
                row = rows[0] if rows else None
                return row_to_task_dict(row, task_type)
            except Exception as exc:
                logging.warning("Supabase fetch_latest_task fallback to SQLite: %s", exc)

    ensure_system_task_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if user_id:
            row = conn.execute(
                """
                SELECT
                    id,
                    task_type,
                    status,
                    source,
                    created_at,
                    started_at,
                    finished_at,
                    request_payload,
                    result_payload,
                    error
                FROM system_tasks
                WHERE task_type = ? AND user_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (task_type, user_id),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT
                    id,
                    task_type,
                    status,
                    source,
                    created_at,
                    started_at,
                    finished_at,
                    request_payload,
                    result_payload,
                    error
                FROM system_tasks
                WHERE task_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (task_type,),
            ).fetchone()
    return row_to_task_dict(row, task_type)


def fetch_all_latest_tasks(db_path: Path, user_id: Optional[str] = None) -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                row_map: dict[str, Any] = {}
                for task_type in TASK_TYPES:
                    query = client.table("system_tasks").select(
                        "id,task_type,status,source,created_at,started_at,finished_at,request_payload,result_payload,error"
                    ).eq("task_type", task_type)
                    if user_id:
                        query = query.eq("user_id", user_id)
                    rows = query.order("id", desc=True).limit(1).execute().data or []
                    if rows:
                        row_map[task_type] = rows[0]
                return {task_type: row_to_task_dict(row_map.get(task_type), task_type) for task_type in TASK_TYPES}
            except Exception as exc:
                logging.warning("Supabase fetch_all_latest_tasks fallback to SQLite: %s", exc)

    ensure_system_task_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if user_id:
            rows = conn.execute(
                """
                SELECT st.*
                FROM system_tasks st
                INNER JOIN (
                    SELECT task_type, MAX(id) AS max_id
                    FROM system_tasks
                    WHERE user_id = ?
                    GROUP BY task_type
                ) latest ON latest.max_id = st.id
                ORDER BY st.id DESC
                """,
                (user_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT st.*
                FROM system_tasks st
                INNER JOIN (
                    SELECT task_type, MAX(id) AS max_id
                    FROM system_tasks
                    GROUP BY task_type
                ) latest ON latest.max_id = st.id
                ORDER BY st.id DESC
                """
            ).fetchall()

    row_map = {row["task_type"]: row for row in rows}
    return {task_type: row_to_task_dict(row_map.get(task_type), task_type) for task_type in TASK_TYPES}


def fetch_task_history(db_path: Path, limit: int = TASK_HISTORY_LIMIT, user_id: Optional[str] = None) -> list[dict]:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                query = client.table("system_tasks").select(
                    "id,task_type,status,source,created_at,started_at,finished_at,request_payload,result_payload,error"
                )
                if user_id:
                    query = query.eq("user_id", user_id)
                rows = query.order("id", desc=True).limit(limit).execute().data or []
                return [parse_task_row(row) for row in rows]
            except Exception as exc:
                logging.warning("Supabase fetch_task_history fallback to SQLite: %s", exc)

    ensure_system_task_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if user_id:
            rows = conn.execute(
                """
                SELECT
                    id,
                    task_type,
                    status,
                    source,
                    created_at,
                    started_at,
                    finished_at,
                    request_payload,
                    result_payload,
                    error
                FROM system_tasks
                WHERE user_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    id,
                    task_type,
                    status,
                    source,
                    created_at,
                    started_at,
                    finished_at,
                    request_payload,
                    result_payload,
                    error
                FROM system_tasks
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    return [parse_task_row(row) for row in rows]


def fetch_task_by_id(db_path: Path, task_id: int, user_id: Optional[str] = None) -> Optional[dict]:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                query = client.table("system_tasks").select(
                    "id,task_type,status,source,created_at,started_at,finished_at,request_payload,result_payload,error"
                ).eq("id", task_id)
                if user_id:
                    query = query.eq("user_id", user_id)
                rows = query.limit(1).execute().data or []
                if not rows:
                    return None
                return parse_task_row(rows[0])
            except Exception as exc:
                logging.warning("Supabase fetch_task_by_id fallback to SQLite: %s", exc)

    ensure_system_task_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if user_id:
            row = conn.execute(
                """
                SELECT
                    id,
                    task_type,
                    status,
                    source,
                    created_at,
                    started_at,
                    finished_at,
                    request_payload,
                    result_payload,
                    error
                FROM system_tasks
                WHERE id = ? AND user_id = ?
                """,
                (task_id, user_id),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT
                    id,
                    task_type,
                    status,
                    source,
                    created_at,
                    started_at,
                    finished_at,
                    request_payload,
                    result_payload,
                    error
                FROM system_tasks
                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()
    if row is None:
        return None
    return parse_task_row(row)


def task_is_active(db_path: Path, task_type: str, user_id: Optional[str] = None) -> bool:
    latest = fetch_latest_task(db_path, task_type, user_id=user_id)
    if not latest.get("running"):
        return False

    status = str(latest.get("status") or "").strip().lower()
    if status == "running":
        started_at = parse_iso_datetime(latest.get("started_at"))
        if started_at is None:
            return True

        age_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
        if age_seconds <= STALE_RUNNING_TASK_SECONDS:
            return True

        task_id = latest.get("id")
        if task_id is not None:
            try:
                finish_task_record(
                    db_path,
                    int(task_id),
                    status="failed",
                    error="Task auto-reset: stale running job exceeded timeout.",
                )
                logging.warning(
                    "Auto-reset stale running task: id=%s type=%s age=%ss",
                    task_id,
                    task_type,
                    int(age_seconds),
                )
            except Exception as exc:
                logging.warning("Could not auto-reset stale running task %s: %s", task_id, exc)
        return False

    if status != "queued":
        return False

    # queued + no started_at can happen if process died before worker picked the job.
    started_at = parse_iso_datetime(latest.get("started_at"))
    if started_at is not None:
        return True

    created_at = parse_iso_datetime(latest.get("created_at"))
    if created_at is None:
        return True

    age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
    if age_seconds <= STALE_QUEUED_TASK_SECONDS:
        return True

    task_id = latest.get("id")
    if task_id is not None:
        try:
            finish_task_record(
                db_path,
                int(task_id),
                status="failed",
                error="Task auto-reset: stale queued job (worker did not start).",
            )
            logging.warning(
                "Auto-reset stale queued task: id=%s type=%s age=%ss",
                task_id,
                task_type,
                int(age_seconds),
            )
        except Exception as exc:
            logging.warning("Could not auto-reset stale queued task %s: %s", task_id, exc)

    return False


def get_runtime_value(db_path: Path, key: str) -> Optional[str]:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_runtime"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                rows = client.table("system_runtime").select("value").eq("key", key).limit(1).execute().data or []
                if not rows:
                    return None
                return rows[0].get("value")
            except Exception as exc:
                logging.warning("Supabase get_runtime_value fallback to SQLite: %s", exc)

    ensure_runtime_table(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT value FROM system_runtime WHERE key = ?", (key,)).fetchone()
    if not row:
        return None
    return row[0]


def set_runtime_value(db_path: Path, key: str, value: str) -> None:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_runtime"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                client.table("system_runtime").upsert(
                    {
                        "key": key,
                        "value": value,
                        "updated_at": utc_now_iso(),
                    },
                    on_conflict="key",
                ).execute()
                return
            except Exception as exc:
                logging.warning("Supabase set_runtime_value fallback to SQLite: %s", exc)

    ensure_runtime_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO system_runtime (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key)
            DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, utc_now_iso()),
        )
        conn.commit()


def _billing_runtime_keys(user_id: Optional[str] = None, user_email: Optional[str] = None) -> list[str]:
    keys: list[str] = []
    normalized_user_id = str(user_id or "").strip()
    normalized_email = str(user_email or "").strip().lower()
    if normalized_user_id:
        keys.append(f"billing_snapshot:{normalized_user_id}")
    if normalized_email:
        keys.append(f"billing_snapshot_email:{normalized_email}")
    return keys


def load_runtime_billing_snapshot(user_id: Optional[str] = None, user_email: Optional[str] = None) -> dict[str, Any]:
    for key in _billing_runtime_keys(user_id=user_id, user_email=user_email):
        raw = get_runtime_value(DEFAULT_DB_PATH, key)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if isinstance(payload, dict):
            return dict(payload)
    return {}


def store_runtime_billing_snapshot(user_id: Optional[str] = None, user_email: Optional[str] = None, snapshot: Optional[dict[str, Any]] = None) -> None:
    if not isinstance(snapshot, dict) or not snapshot:
        return
    payload = json.dumps(snapshot, ensure_ascii=False)
    for key in _billing_runtime_keys(user_id=user_id, user_email=user_email):
        set_runtime_value(DEFAULT_DB_PATH, key, payload)


def compute_next_drip_at() -> datetime:
    minutes = random.randint(DRIP_MINUTES_MIN, DRIP_MINUTES_MAX)
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def _read_json_config(config_path: Path) -> dict[str, Any]:
    try:
        with Path(config_path).open("r", encoding="utf-8") as handle:
            config = json.load(handle)
    except Exception:
        return {}
    return config if isinstance(config, dict) else {}


def get_stripe_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    config = _read_json_config(config_path)
    stripe_cfg = config.get("stripe", {}) if isinstance(config, dict) else {}
    return stripe_cfg if isinstance(stripe_cfg, dict) else {}


def get_stripe_secret_key(config_path: Path = DEFAULT_CONFIG_PATH) -> str:
    stripe_cfg = get_stripe_config(config_path)
    return str(
        os.environ.get("STRIPE_SECRET_KEY")
        or os.environ.get("SNIPED_STRIPE_SECRET_KEY")
        or stripe_cfg.get("secret_key", "")
        or stripe_cfg.get("secretKey", "")
        or stripe_cfg.get("api_key", "")
        or ""
    ).strip()


def get_stripe_webhook_secret(config_path: Path = DEFAULT_CONFIG_PATH) -> str:
    stripe_cfg = get_stripe_config(config_path)
    return str(
        os.environ.get("STRIPE_WEBHOOK_SECRET")
        or os.environ.get("SNIPED_STRIPE_WEBHOOK_SECRET")
        or stripe_cfg.get("webhook_secret", "")
        or stripe_cfg.get("webhookSecret", "")
        or ""
    ).strip()


def get_dashboard_base_url(config_path: Path = DEFAULT_CONFIG_PATH, request: Optional[Request] = None) -> str:
    config = _read_json_config(config_path)
    configured = str(
        os.environ.get("SNIPED_DASHBOARD_URL")
        or os.environ.get("LEADFLOW_DASHBOARD_URL")
        or os.environ.get("FRONTEND_URL")
        or config.get("dashboard_url", "")
        or ""
    ).strip().rstrip("/")
    if configured:
        return configured

    if request is not None:
        request_base = str(request.base_url).strip().rstrip("/")
        parsed = urlparse(request_base)
        host = str(parsed.hostname or "").strip().lower()
        port = parsed.port
        scheme = str(parsed.scheme or "http").strip() or "http"
        if host in {"localhost", "127.0.0.1"} and port == 8000:
            return f"{scheme}://{host}:5173"
        if request_base:
            return request_base

    return "http://localhost:5173"


def _stripe_api_get_json(api_path: str, *, params: Optional[dict[str, Any]] = None, config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    secret_key = get_stripe_secret_key(config_path)
    if not secret_key:
        return {}

    url = f"https://api.stripe.com/v1/{str(api_path or '').lstrip('/')}"
    if params:
        filtered_params = {key: value for key, value in params.items() if value is not None and str(value) != ''}
        if filtered_params:
            url = f"{url}?{urlencode(filtered_params)}"

    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {secret_key}"},
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logging.warning("Stripe API lookup failed for %s: %s", api_path, exc)
        return {}

    return data if isinstance(data, dict) else {}


def recover_billing_snapshot_from_stripe(
    *,
    user_email: Optional[str] = None,
    stripe_customer_id: Optional[str] = None,
    fallback_plan_key: str = "free",
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> dict[str, Any]:
    normalized_email = str(user_email or "").strip().lower()
    customer_id = str(stripe_customer_id or "").strip()

    if not get_stripe_secret_key(config_path):
        return {}

    if not customer_id and normalized_email:
        customer_payload = _stripe_api_get_json(
            "customers",
            params={"email": normalized_email, "limit": 1},
            config_path=config_path,
        )
        customer_rows = customer_payload.get("data") if isinstance(customer_payload.get("data"), list) else []
        if customer_rows:
            customer_id = str((customer_rows[0] or {}).get("id") or "").strip()

    if not customer_id:
        return {}

    subscriptions_payload = _stripe_api_get_json(
        "subscriptions",
        params={"customer": customer_id, "status": "all", "limit": 10},
        config_path=config_path,
    )
    subscription_rows = subscriptions_payload.get("data") if isinstance(subscriptions_payload.get("data"), list) else []
    if not subscription_rows:
        return {"stripe_customer_id": customer_id}

    def _subscription_rank(item: dict[str, Any]) -> tuple[int, int]:
        status = str(item.get("status") or "").strip().lower()
        status_rank = {
            "active": 0,
            "trialing": 1,
            "past_due": 2,
            "unpaid": 3,
            "canceled": 4,
            "incomplete_expired": 5,
        }.get(status, 9)
        try:
            period_end = int(item.get("current_period_end") or 0)
        except Exception:
            period_end = 0
        return (status_rank, -period_end)

    candidate = sorted(
        [row for row in subscription_rows if isinstance(row, dict)],
        key=_subscription_rank,
    )[0]

    status = str(candidate.get("status") or "").strip().lower()
    cancel_at_period_end = bool(candidate.get("cancel_at_period_end"))
    try:
        current_period_end = int(candidate.get("current_period_end") or 0)
    except Exception:
        current_period_end = 0
    try:
        cancel_at = int(candidate.get("cancel_at") or 0)
    except Exception:
        cancel_at = 0

    now_ts = int(datetime.now(timezone.utc).timestamp())
    has_paid_access = status in {"active", "trialing", "past_due"} or (cancel_at_period_end and (cancel_at or current_period_end) > now_ts)

    items = candidate.get("items") if isinstance(candidate.get("items"), dict) else {}
    item_rows = items.get("data") if isinstance(items.get("data"), list) else []
    first_item = item_rows[0] if item_rows and isinstance(item_rows[0], dict) else {}
    price = first_item.get("price") if isinstance(first_item.get("price"), dict) else {}
    price_id = str(price.get("id") or "").strip()
    mapped_plan = STRIPE_PRICE_ID_TO_PLAN.get(price_id, {}) if price_id else {}

    plan_key = _normalize_plan_key(
        mapped_plan.get("plan_key") if isinstance(mapped_plan, dict) else fallback_plan_key,
        fallback=fallback_plan_key if fallback_plan_key in PLAN_MONTHLY_QUOTAS else DEFAULT_PLAN_KEY,
    )
    monthly_limit = int(
        (mapped_plan.get("credits") if isinstance(mapped_plan, dict) else 0)
        or PLAN_MONTHLY_QUOTAS.get(plan_key, PLAN_MONTHLY_QUOTAS.get("pro", DEFAULT_MONTHLY_CREDIT_LIMIT))
    )

    cancel_timestamp = cancel_at or current_period_end
    cancel_iso = None
    if cancel_timestamp > 0:
        try:
            cancel_iso = datetime.fromtimestamp(cancel_timestamp, tz=timezone.utc).isoformat()
        except Exception:
            cancel_iso = None

    if not has_paid_access:
        return {
            "stripe_customer_id": customer_id,
            "subscription_active": False,
            "subscription_status": status or "expired",
            "plan_key": "free",
        }

    return {
        "stripe_customer_id": customer_id,
        "subscription_active": True,
        "subscription_status": "cancelled_pending" if cancel_at_period_end else (status or "active"),
        "subscription_cancel_at": cancel_iso if cancel_at_period_end else None,
        "subscription_cancel_at_period_end": bool(cancel_at_period_end),
        "plan_key": plan_key,
        "monthly_quota": monthly_limit,
        "monthly_limit": monthly_limit,
        "credits_limit": monthly_limit,
        "credits_balance": monthly_limit,
        "subscription_start_date": utc_now_iso(),
    }


def _stripe_topup_marker_key(payment_id: str) -> str:
    return f"stripe_topup_applied:{str(payment_id or '').strip()}"


def mark_stripe_topup_payments_applied(payment_ids: list[str], *, user_id: Optional[str] = None, credits_delta: int = 0) -> None:
    cleaned_ids = [str(item or "").strip() for item in payment_ids if str(item or "").strip()]
    if not cleaned_ids:
        return

    payload = json.dumps(
        {
            "user_id": str(user_id or "").strip(),
            "credits_delta": max(0, int(credits_delta or 0)),
            "applied_at": utc_now_iso(),
        },
        ensure_ascii=False,
    )
    for payment_id in cleaned_ids:
        try:
            set_runtime_value(DEFAULT_DB_PATH, _stripe_topup_marker_key(payment_id), payload)
        except Exception:
            logging.debug("Could not persist Stripe top-up marker for %s", payment_id)


def recover_pending_topup_credits_from_stripe(
    *,
    user_id: Optional[str] = None,
    user_email: Optional[str] = None,
    stripe_customer_id: Optional[str] = None,
    updated_at_raw: Optional[str] = None,
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> dict[str, Any]:
    normalized_user_id = str(user_id or "").strip()
    normalized_email = str(user_email or "").strip().lower()
    customer_id = str(stripe_customer_id or "").strip()

    if not get_stripe_secret_key(config_path):
        return {}

    if not customer_id and normalized_email:
        customer_payload = _stripe_api_get_json(
            "customers",
            params={"email": normalized_email, "limit": 1},
            config_path=config_path,
        )
        customer_rows = customer_payload.get("data") if isinstance(customer_payload.get("data"), list) else []
        if customer_rows:
            customer_id = str((customer_rows[0] or {}).get("id") or "").strip()

    if not customer_id:
        return {}

    updated_at_dt = parse_iso_datetime(str(updated_at_raw or "").strip())
    payment_payload = _stripe_api_get_json(
        "payment_intents",
        params={"customer": customer_id, "limit": 100},
        config_path=config_path,
    )
    payment_rows = payment_payload.get("data") if isinstance(payment_payload.get("data"), list) else []
    if not payment_rows:
        return {"stripe_customer_id": customer_id}

    credits_delta = 0
    payment_ids: list[str] = []

    for item in payment_rows:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "").strip().lower() != "succeeded":
            continue

        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        payment_kind = str(metadata.get("payment_kind") or "").strip().lower()
        if payment_kind and payment_kind != "topup":
            continue

        raw_credits = metadata.get("credits_added") or metadata.get("credits")
        try:
            credits = int(raw_credits or 0)
        except Exception:
            credits = 0

        if credits <= 0:
            mapped_package = STRIPE_TOP_UP_PRICE_ID_TO_PACKAGE.get(str(metadata.get("stripe_price_id") or "").strip(), {})
            credits = int(mapped_package.get("credits") or 0) if isinstance(mapped_package, dict) else 0
        if credits <= 0:
            continue

        meta_user_id = str(metadata.get("user_id") or "").strip()
        meta_email = str(metadata.get("email") or "").strip().lower()
        if normalized_user_id and meta_user_id and meta_user_id != normalized_user_id:
            continue
        if normalized_email and meta_email and meta_email != normalized_email:
            continue

        payment_id = str(item.get("id") or "").strip()
        if not payment_id:
            continue
        if get_runtime_value(DEFAULT_DB_PATH, _stripe_topup_marker_key(payment_id)):
            continue

        created_ts = int(item.get("created") or 0)
        if updated_at_dt is not None and created_ts > 0:
            created_dt = datetime.fromtimestamp(created_ts, tz=timezone.utc)
            if created_dt <= updated_at_dt:
                continue

        credits_delta += credits
        payment_ids.append(payment_id)

    if credits_delta <= 0:
        return {"stripe_customer_id": customer_id}

    return {
        "stripe_customer_id": customer_id,
        "credits_delta": credits_delta,
        "payment_ids": payment_ids,
    }


def load_config_health(config_path: Path) -> dict:
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
    except Exception as exc:
        return {
            "ok": False,
            "openai_ok": False,
            "smtp_ok": False,
            "error": f"Could not read config.json: {exc}",
        }

    openai_cfg = config.get("openai", {}) if isinstance(config, dict) else {}
    api_key = str(openai_cfg.get("api_key", "") or "").strip()
    openai_ok = bool(api_key and api_key != "YOUR_OPENAI_API_KEY")

    smtp_accounts = config.get("smtp_accounts", []) if isinstance(config, dict) else []
    smtp_ok = False
    for item in smtp_accounts:
        email = str(item.get("email", "") or "").strip()
        password = str(item.get("password", "") or "").strip()
        host = str(item.get("host", "") or "").strip()
        if email and password and host and "your-" not in email.lower() and "your-" not in password.lower():
            smtp_ok = True
            break

    supabase_cfg = config.get("supabase", {}) if isinstance(config, dict) else {}
    supabase_url = str(os.environ.get("SUPABASE_URL") or supabase_cfg.get("url", "") or "").strip()
    supabase_key = str(
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_PUBLISHABLE_KEY")
        or supabase_cfg.get("service_role_key", "")
        or supabase_cfg.get("publishable_key", "")
        or ""
    ).strip()
    supabase_ok = bool(_HAS_SUPABASE and supabase_url and supabase_key)

    return {
        "ok": openai_ok and smtp_ok,
        "openai_ok": openai_ok,
        "smtp_ok": smtp_ok,
        "supabase_ok": supabase_ok,
        "error": None,
    }


def normalize_sending_strategy(raw: Optional[str]) -> str:
    value = str(raw or "round_robin").strip().lower().replace("-", "_")
    if value not in {"round_robin", "random"}:
        return "round_robin"
    return value


def format_from_header_dict(account: dict) -> str:
    from_name = str(account.get("from_name", "") or "").strip()
    email = str(account.get("email", "") or "").strip()
    return f"{from_name} <{email}>" if from_name else email


def classify_smtp_error(exc: Exception) -> str:
    msg = str(exc or "").lower()
    if isinstance(exc, smtplib.SMTPAuthenticationError) or "auth" in msg or "username" in msg or "password" in msg:
        return "Wrong Password"
    if isinstance(exc, TimeoutError) or "timed out" in msg:
        return "Port Blocked"
    if "connection refused" in msg or "cannot connect" in msg:
        return "Port Blocked"
    if "name or service not known" in msg or "nodename nor servname provided" in msg:
        return "Host Not Found"
    return "Connection Failed"


def send_smtp_test_message(account: dict, recipient: str) -> None:
    host = str(account.get("host", "") or "").strip()
    port = int(account.get("port", 587) or 587)
    email = str(account.get("email", "") or "").strip()
    password = str(account.get("password", "") or "").strip()
    use_tls = bool(account.get("use_tls", True))
    use_ssl = bool(account.get("use_ssl", False))

    if not host or not email or not password:
        raise ValueError("Missing SMTP host/email/password")

    message = EmailMessage()
    message["To"] = recipient
    message["From"] = format_from_header_dict(account)
    message["Subject"] = "SMTP test connection"
    message.set_content("SMTP connection test successful.")

    if use_ssl or port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=12) as smtp:
            smtp.login(email, password)
            smtp.send_message(message)
        return

    with smtplib.SMTP(host, port, timeout=12) as smtp:
        smtp.ehlo()
        if use_tls or port == 587:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(email, password)
        smtp.send_message(message)


def get_primary_smtp_account(config_path: Path) -> dict:
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Could not read config.json: {exc}")

    smtp_accounts = cfg.get("smtp_accounts", []) if isinstance(cfg, dict) else []
    if not smtp_accounts:
        raise HTTPException(status_code=503, detail="SMTP is not configured.")

    account = dict(smtp_accounts[0] or {})
    host = str(account.get("host", "") or "").strip()
    email = str(account.get("email", "") or "").strip()
    password = str(account.get("password", "") or "").strip()
    if not host or not email or not password:
        raise HTTPException(status_code=503, detail="SMTP is not fully configured.")
    return account


def _normalize_single_smtp_account(raw: dict[str, Any], existing: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    existing = dict(existing or {})
    account = {
        "host": str(raw.get("host") if raw.get("host") is not None else existing.get("host", "")).strip(),
        "port": int(raw.get("port") if raw.get("port") is not None else existing.get("port", 587) or 587),
        "email": str(raw.get("email") if raw.get("email") is not None else existing.get("email", "")).strip(),
        "password": str(existing.get("password", "") or ""),
        "use_tls": bool(raw.get("use_tls") if raw.get("use_tls") is not None else existing.get("use_tls", True)),
        "use_ssl": bool(raw.get("use_ssl") if raw.get("use_ssl") is not None else existing.get("use_ssl", False)),
        "from_name": str(raw.get("from_name") if raw.get("from_name") is not None else existing.get("from_name", "")).strip(),
        "signature": str(raw.get("signature") if raw.get("signature") is not None else existing.get("signature", "")).strip(),
    }
    if raw.get("password") is not None and str(raw.get("password") or "").strip():
        account["password"] = str(raw.get("password") or "").strip()
    return account


def _normalize_smtp_accounts(raw_accounts: list[dict[str, Any]], existing_accounts: Optional[list[dict[str, Any]]] = None) -> list[dict[str, Any]]:
    existing_accounts = list(existing_accounts or [])
    normalized_accounts: list[dict[str, Any]] = []
    for idx, raw in enumerate(raw_accounts):
        existing = existing_accounts[idx] if idx < len(existing_accounts) else {}
        next_account = _normalize_single_smtp_account(raw, existing)
        if next_account["host"] and next_account["email"]:
            normalized_accounts.append(next_account)
    return normalized_accounts


def _safe_smtp_accounts(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    safe_accounts: list[dict[str, Any]] = []
    for item in accounts:
        safe_accounts.append(
            {
                "host": str(item.get("host", "") or ""),
                "port": int(item.get("port", 587) or 587),
                "email": str(item.get("email", "") or ""),
                "use_tls": bool(item.get("use_tls", True)),
                "use_ssl": bool(item.get("use_ssl", False)),
                "from_name": str(item.get("from_name", "") or ""),
                "signature": str(item.get("signature", "") or ""),
                "password_set": bool(str(item.get("password", "") or "").strip()),
            }
        )
    return safe_accounts


def _parse_smtp_accounts_json(raw_value: Any) -> list[dict[str, Any]]:
    if not raw_value:
        return []
    try:
        parsed = raw_value if isinstance(raw_value, list) else json.loads(str(raw_value))
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [dict(item) for item in parsed if isinstance(item, dict)]


def load_user_smtp_accounts(*, session_token: Optional[str] = None, user_id: Optional[str] = None, db_path: Path = DEFAULT_DB_PATH) -> list[dict[str, Any]]:
    session_token = str(session_token or "").strip()
    user_id = str(user_id or "").strip()
    if not session_token and not user_id:
        return []

    if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
        if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
            return []
        sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if sb_client is None:
            return []
        try:
            query = sb_client.table("users").select("smtp_accounts_json")
            if session_token:
                query = query.eq("token", session_token)
            else:
                try:
                    query = query.eq("id", int(user_id))
                except Exception:
                    query = query.eq("id", user_id)
            response = query.limit(1).execute()
            rows = list(getattr(response, "data", None) or [])
        except Exception as exc:
            if "does not exist" in str(exc):
                return []
            raise HTTPException(status_code=502, detail=f"SMTP lookup failed: {exc}")
        if not rows:
            return []
        return _parse_smtp_accounts_json(rows[0].get("smtp_accounts_json"))

    ensure_users_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if session_token:
            row = conn.execute("SELECT smtp_accounts_json FROM users WHERE token = ?", (session_token,)).fetchone()
        else:
            row = conn.execute("SELECT smtp_accounts_json FROM users WHERE id = ?", (user_id,)).fetchone()
    if row is None:
        return []
    return _parse_smtp_accounts_json(row["smtp_accounts_json"])


def save_user_smtp_accounts(session_token: str, accounts: list[dict[str, Any]], db_path: Path = DEFAULT_DB_PATH) -> None:
    token = str(session_token or "").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid or expired session token.")
    existing_accounts = load_user_smtp_accounts(session_token=token, db_path=db_path)
    normalized_accounts = _normalize_smtp_accounts(accounts, existing_accounts)
    payload = json.dumps(normalized_accounts, ensure_ascii=False)

    if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
        if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
            raise HTTPException(status_code=503, detail="Supabase users table is missing.")
        sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if sb_client is None:
            raise HTTPException(status_code=503, detail="Supabase is not reachable.")
        try:
            response = sb_client.table("users").select("id").eq("token", token).limit(1).execute()
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            execute_supabase_update_with_retry(
                sb_client,
                "users",
                {"smtp_accounts_json": payload, "updated_at": utc_now_iso()},
                eq_filters={"id": rows[0].get("id")},
                operation_name="smtp settings update",
            )
            return
        except HTTPException:
            raise
        except Exception as exc:
            if "does not exist" in str(exc):
                raise HTTPException(status_code=503, detail="Supabase users schema is missing smtp_accounts_json column.")
            raise HTTPException(status_code=502, detail=f"SMTP save failed: {exc}")

    ensure_users_table(db_path)
    with sqlite3.connect(db_path) as conn:
        updated = conn.execute(
            "UPDATE users SET smtp_accounts_json = ?, updated_at = ? WHERE token = ?",
            (payload, utc_now_iso(), token),
        )
        if int(updated.rowcount or 0) <= 0:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")
        conn.commit()


def get_primary_user_smtp_account(*, session_token: Optional[str] = None, user_id: Optional[str] = None, db_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    accounts = load_user_smtp_accounts(session_token=session_token, user_id=user_id, db_path=db_path)
    if not accounts:
        raise HTTPException(status_code=503, detail="SMTP is not configured for this account.")
    account = dict(accounts[0] or {})
    host = str(account.get("host", "") or "").strip()
    email = str(account.get("email", "") or "").strip()
    password = str(account.get("password", "") or "").strip()
    if not host or not email or not password:
        raise HTTPException(status_code=503, detail="SMTP is not fully configured for this account.")
    return account


def send_auth_email(account: dict, recipient: str, subject: str, text_body: str, html_body: Optional[str] = None) -> None:
    host = str(account.get("host", "") or "").strip()
    port = int(account.get("port", 587) or 587)
    email = str(account.get("email", "") or "").strip()
    password = str(account.get("password", "") or "").strip()
    use_tls = bool(account.get("use_tls", True))
    use_ssl = bool(account.get("use_ssl", False))

    if not host or not email or not password:
        raise ValueError("Missing SMTP host/email/password")

    message = EmailMessage()
    message["To"] = recipient
    message["From"] = format_from_header_dict(account)
    message["Subject"] = subject
    message.set_content(text_body)
    if html_body:
        message.add_alternative(html_body, subtype="html")

    if use_ssl or port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=20) as smtp:
            smtp.login(email, password)
            smtp.send_message(message)
        return

    with smtplib.SMTP(host, port, timeout=20) as smtp:
        smtp.ehlo()
        if use_tls or port == 587:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(email, password)
        smtp.send_message(message)


def load_supabase_settings(config_path: Path) -> dict:
    cfg: dict = {}
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
    except Exception:
        cfg = {}

    supabase_cfg = cfg.get("supabase", {}) if isinstance(cfg, dict) else {}
    url = str(os.environ.get("SUPABASE_URL") or supabase_cfg.get("url", "") or "").strip()
    service_role_key = str(os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or supabase_cfg.get("service_role_key", "") or "").strip()
    publishable_key = str(os.environ.get("SUPABASE_PUBLISHABLE_KEY") or supabase_cfg.get("publishable_key", "") or "").strip()
    primary_mode_raw = os.environ.get("SUPABASE_PRIMARY_DB")

    if primary_mode_raw is None:
        # Default to True when credentials are present, unless explicitly disabled
        has_credentials = bool(url and (service_role_key or publishable_key))
        primary_mode = bool(supabase_cfg.get("primary_mode", has_credentials))
    else:
        primary_mode = str(primary_mode_raw).strip().lower() in {"1", "true", "yes", "on"}

    key = service_role_key or publishable_key
    return {
        "enabled": bool(url and key),
        "url": url,
        "key": key,
        "has_service_role": bool(service_role_key),
        "has_publishable": bool(publishable_key),
        "primary_mode": primary_mode,
    }


def is_supabase_primary_enabled(config_path: Path) -> bool:
    settings = load_supabase_settings(config_path)
    return bool(settings["enabled"] and settings.get("primary_mode"))


def get_supabase_client(config_path: Path) -> Optional[Any]:
    if not _HAS_SUPABASE or create_supabase_client is None:
        return None
    settings = load_supabase_settings(config_path)
    if not settings["enabled"]:
        return None
    try:
        return create_supabase_client(settings["url"], settings["key"])
    except Exception as exc:
        logging.warning("Supabase init failed: %s", exc)
        return None


def set_supabase_primary_mode(config_path: Path, enabled: bool) -> None:
    cfg: dict = {}
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
    except Exception:
        cfg = {}

    supabase_cfg = dict(cfg.get("supabase", {}))
    supabase_cfg["primary_mode"] = bool(enabled)
    cfg["supabase"] = supabase_cfg

    with config_path.open("w", encoding="utf-8") as handle:
        json.dump(cfg, handle, indent=2)


def supabase_select_rows(
    client: Any,
    table_name: str,
    *,
    columns: str = "*",
    filters: Optional[dict[str, Any]] = None,
    order_by: Optional[str] = None,
    desc: bool = False,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> list[dict]:
    query = client.table(table_name).select(columns)
    if filters:
        for key, value in filters.items():
            query = query.eq(key, value)
    if order_by:
        query = query.order(order_by, desc=desc)
    if offset is not None and limit is not None:
        query = query.range(offset, max(offset, offset + limit - 1))
    elif limit is not None:
        query = query.limit(limit)
    response = query.execute()
    return list(getattr(response, "data", None) or [])


def supabase_table_available(config_path: Path, table_name: str) -> bool:
    client = get_supabase_client(config_path)
    if client is None:
        return False
    try:
        client.table(table_name).select("*").limit(1).execute()
        return True
    except Exception:
        return False


def is_supabase_auth_enabled(config_path: Path) -> bool:
    settings = load_supabase_settings(config_path)
    return bool(settings.get("enabled"))


def ensure_supabase_users_table(config_path: Path) -> bool:
    client = get_supabase_client(config_path)
    if client is None:
        return False

    users_sql = f"""
    CREATE TABLE IF NOT EXISTS public.users (
        id BIGSERIAL PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        salt TEXT NOT NULL,
        niche TEXT NOT NULL DEFAULT 'B2B Service Provider',
        account_type TEXT NOT NULL DEFAULT 'entrepreneur',
        display_name TEXT NOT NULL DEFAULT '',
        contact_name TEXT NOT NULL DEFAULT '',
        token TEXT UNIQUE,
        credits_balance BIGINT NOT NULL DEFAULT 0,
        monthly_quota BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT},
        credits_limit BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT},
        monthly_limit BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT},
        topup_credits_balance BIGINT NOT NULL DEFAULT 0,
        subscription_start_date TEXT,
        subscription_active BOOLEAN NOT NULL DEFAULT FALSE,
        subscription_status TEXT,
        subscription_cancel_at TEXT,
        subscription_cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE,
        plan_key TEXT NOT NULL DEFAULT 'free',
        stripe_customer_id TEXT,
        quickstart_completed BOOLEAN NOT NULL DEFAULT FALSE,
        average_deal_value DOUBLE PRECISION NOT NULL DEFAULT 1000,
        smtp_accounts_json TEXT,
        reset_token TEXT,
        reset_token_expires_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT
    );

    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'entrepreneur';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS contact_name TEXT NOT NULL DEFAULT '';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS niche TEXT NOT NULL DEFAULT 'B2B Service Provider';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS token TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS credits_balance BIGINT NOT NULL DEFAULT 0;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS monthly_quota BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS credits_limit BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS monthly_limit BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS topup_credits_balance BIGINT NOT NULL DEFAULT 0;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS subscription_start_date TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS subscription_active BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS subscription_status TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS subscription_cancel_at TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS subscription_cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS plan_key TEXT NOT NULL DEFAULT 'free';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS quickstart_completed BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS average_deal_value DOUBLE PRECISION NOT NULL DEFAULT 1000;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS smtp_accounts_json TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS reset_token TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS reset_token_expires_at TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS created_at TEXT NOT NULL DEFAULT NOW()::text;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS updated_at TEXT;

    UPDATE public.users
    SET monthly_quota = COALESCE(NULLIF(monthly_quota, 0), NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})
    WHERE monthly_quota IS NULL OR monthly_quota <= 0;

    UPDATE public.users
    SET monthly_limit = COALESCE(NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})
    WHERE monthly_limit IS NULL OR monthly_limit <= 0;

    UPDATE public.users
    SET monthly_limit = monthly_quota,
        credits_limit = monthly_quota
    WHERE monthly_quota > 0;

    UPDATE public.users
    SET topup_credits_balance = COALESCE(topup_credits_balance, 0)
    WHERE topup_credits_balance IS NULL;

    UPDATE public.users
    SET quickstart_completed = COALESCE(quickstart_completed, FALSE)
    WHERE quickstart_completed IS NULL;

    UPDATE public.users
    SET average_deal_value = CASE WHEN COALESCE(average_deal_value, 0) <= 0 THEN {DEFAULT_AVERAGE_DEAL_VALUE} ELSE average_deal_value END
    WHERE average_deal_value IS NULL OR average_deal_value <= 0;

    UPDATE public.users
    SET monthly_quota = {DEFAULT_MONTHLY_CREDIT_LIMIT},
        monthly_limit = {DEFAULT_MONTHLY_CREDIT_LIMIT},
        credits_limit = {DEFAULT_MONTHLY_CREDIT_LIMIT},
        credits_balance = GREATEST(COALESCE(credits_balance, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT} + COALESCE(topup_credits_balance, 0))
    WHERE LOWER(COALESCE(NULLIF(plan_key, ''), 'free')) = 'free'
      AND COALESCE(subscription_active, FALSE) = FALSE
      AND (
            COALESCE(NULLIF(monthly_quota, 0), 0) != {DEFAULT_MONTHLY_CREDIT_LIMIT}
         OR COALESCE(NULLIF(monthly_limit, 0), 0) != {DEFAULT_MONTHLY_CREDIT_LIMIT}
         OR COALESCE(NULLIF(credits_limit, 0), 0) != {DEFAULT_MONTHLY_CREDIT_LIMIT}
      );

    UPDATE public.users
    SET subscription_cancel_at_period_end = COALESCE(subscription_cancel_at_period_end, FALSE)
    WHERE subscription_cancel_at_period_end IS NULL;

    UPDATE public.users
    SET plan_key = CASE
        WHEN LOWER(TRIM(COALESCE(plan_key, ''))) IN ('free', 'hustler', 'growth', 'scale', 'empire', 'pro')
            THEN LOWER(TRIM(COALESCE(plan_key, '')))
        WHEN COALESCE(subscription_active, FALSE) = TRUE THEN 'pro'
        ELSE 'free'
    END;

    CREATE INDEX IF NOT EXISTS idx_users_token
        ON public.users(token);
    CREATE INDEX IF NOT EXISTS idx_users_reset_token
        ON public.users(reset_token);
    """

    try:
        client.rpc("exec_sql", {"sql": users_sql}).execute()
    except Exception as exc:
        logging.warning("Supabase users table auto-create skipped (rpc exec_sql unavailable): %s", exc)

    return supabase_table_available(config_path, "users")


def add_worker_audit_supabase(
    config_path: Path,
    *,
    action: str,
    message: str,
    worker_id: Optional[int] = None,
    lead_id: Optional[int] = None,
    actor: str = "system",
) -> None:
    client = get_supabase_client(config_path)
    if client is None:
        return

    payload = {
        "worker_id": worker_id,
        "lead_id": lead_id,
        "action": action.strip().lower(),
        "message": message.strip(),
        "actor": actor.strip() or "system",
        "created_at": utc_now_iso(),
    }
    try:
        client.table("worker_audit_log").insert(payload).execute()
    except Exception as exc:
        logging.warning("Supabase worker audit insert failed: %s", exc)


def fetch_blacklist_sets_supabase(config_path: Path) -> tuple[set[str], set[str]]:
    client = get_supabase_client(config_path)
    if client is None:
        return set(), set()

    emails: set[str] = set()
    domains: set[str] = set()
    try:
        rows = supabase_select_rows(client, "lead_blacklist", columns="kind,value")
    except Exception:
        return set(), set()

    for row in rows:
        normalized_kind = str(row.get("kind") or "").strip().lower()
        normalized_value = str(row.get("value") or "").strip().lower()
        if not normalized_value:
            continue
        if normalized_kind == "email":
            emails.add(normalized_value)
        elif normalized_kind == "domain":
            domains.add(normalized_value)
    return emails, domains


def sync_blacklisted_leads_supabase(config_path: Path) -> int:
    client = get_supabase_client(config_path)
    if client is None:
        return 0

    emails, domains = fetch_blacklist_sets_supabase(config_path)
    if not emails and not domains:
        return 0

    rows = supabase_select_rows(client, "leads", columns="id,email,website_url,status")
    lead_ids: list[int] = []
    for row in rows:
        status_value = str(row.get("status") or "").strip().lower()
        if status_value == "paid":
            continue

        email_value = str(row.get("email") or "").strip().lower()
        domain_candidates = {
            normalize_blacklist_domain(row.get("email")),
            normalize_blacklist_domain(row.get("website_url")),
        }
        if email_value in emails or any(domain and domain in domains for domain in domain_candidates):
            lead_ids.append(int(row.get("id")))

    if not lead_ids:
        return 0

    try:
        client.table("leads").update(
            {
                "status": "blacklisted",
                "next_mail_at": None,
                "status_updated_at": utc_now_iso(),
            }
        ).in_("id", lead_ids).execute()
    except Exception as exc:
        logging.warning("Supabase blacklist sync failed: %s", exc)
        return 0

    return len(lead_ids)


def blacklist_lead_and_matches_supabase(lead_id: int, reason: str, config_path: Path) -> dict:
    client = get_supabase_client(config_path)
    if client is None:
        raise HTTPException(status_code=500, detail="Supabase not configured")

    lead_rows = client.table("leads").select("id,business_name,email,website_url").eq("id", lead_id).limit(1).execute().data or []
    if not lead_rows:
        raise HTTPException(status_code=404, detail="Lead not found")

    row = lead_rows[0]
    email_value = str(row.get("email") or "").strip().lower()
    domain_values = {
        normalize_blacklist_domain(row.get("email")),
        normalize_blacklist_domain(row.get("website_url")),
    }
    domain_values = {value for value in domain_values if value}

    existing_rows = client.table("lead_blacklist").select("kind,value").execute().data or []
    existing = {(str(item.get("kind") or "").lower(), str(item.get("value") or "").lower()) for item in existing_rows}

    to_insert: list[dict] = []
    if email_value and ("email", email_value) not in existing:
        to_insert.append({"kind": "email", "value": email_value, "reason": reason, "created_at": utc_now_iso()})
    for domain_value in sorted(domain_values):
        if ("domain", domain_value) in existing:
            continue
        to_insert.append({"kind": "domain", "value": domain_value, "reason": reason, "created_at": utc_now_iso()})

    if to_insert:
        client.table("lead_blacklist").insert(to_insert).execute()

    affected = sync_blacklisted_leads_supabase(config_path)
    _invalidate_leads_cache()
    return {
        "status": "blacklisted",
        "lead_id": lead_id,
        "business_name": row.get("business_name"),
        "blacklisted_email": bool(email_value),
        "blacklisted_domains": sorted(domain_values),
        "affected_leads": affected,
    }


def add_blacklist_entry_supabase(config_path: Path, *, kind: str, value: str, reason: str = "Manual blacklist") -> dict:
    normalized_kind, normalized_value = normalize_blacklist_entry(kind, value)
    clean_reason = str(reason or "Manual blacklist").strip() or "Manual blacklist"
    client = get_supabase_client(config_path)
    if client is None:
        raise HTTPException(status_code=500, detail="Supabase not configured")

    existing_rows = (
        client.table("lead_blacklist")
        .select("kind,value,reason,created_at")
        .eq("kind", normalized_kind)
        .eq("value", normalized_value)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not existing_rows:
        client.table("lead_blacklist").insert(
            {
                "kind": normalized_kind,
                "value": normalized_value,
                "reason": clean_reason,
                "created_at": utc_now_iso(),
            }
        ).execute()
        existing_rows = (
            client.table("lead_blacklist")
            .select("kind,value,reason,created_at")
            .eq("kind", normalized_kind)
            .eq("value", normalized_value)
            .limit(1)
            .execute()
            .data
            or []
        )

    row = existing_rows[0] if existing_rows else {
        "kind": normalized_kind,
        "value": normalized_value,
        "reason": clean_reason,
        "created_at": utc_now_iso(),
    }
    affected = sync_blacklisted_leads_supabase(config_path)
    _invalidate_leads_cache()
    return {
        "status": "blacklisted",
        "kind": normalized_kind,
        "value": normalized_value,
        "reason": row.get("reason") or clean_reason,
        "created_at": row.get("created_at") or utc_now_iso(),
        "affected_leads": affected,
    }


def restore_released_blacklisted_leads_supabase(config_path: Path, removed_entries: list[tuple[str, str]]) -> int:
    normalized_entries = [normalize_blacklist_entry(kind, value) for kind, value in removed_entries if str(value or "").strip()]
    if not normalized_entries:
        return 0

    client = get_supabase_client(config_path)
    if client is None:
        return 0

    emails, domains = fetch_blacklist_sets_supabase(config_path)
    rows = supabase_select_rows(client, "leads", columns="id,email,website_url,status")
    lead_ids: list[int] = []
    for row in rows:
        status_value = str(row.get("status") or "").strip().lower()
        if status_value not in {"blacklisted", "skipped (unsubscribed)"}:
            continue
        if not any(
            _lead_matches_blacklist_entry(kind, value, row.get("email"), row.get("website_url"))
            for kind, value in normalized_entries
        ):
            continue
        if _lead_matches_blacklist_sets(row.get("email"), row.get("website_url"), emails, domains):
            continue
        lead_ids.append(int(row.get("id")))

    if not lead_ids:
        return 0

    try:
        client.table("leads").update(
            {
                "status": "Pending",
                "next_mail_at": None,
                "status_updated_at": utc_now_iso(),
            }
        ).in_("id", lead_ids).execute()
    except Exception as exc:
        logging.warning("Supabase blacklist removal sync failed: %s", exc)
        return 0

    return len(lead_ids)


def remove_blacklist_entry_supabase(config_path: Path, *, kind: str, value: str) -> dict:
    normalized_kind, normalized_value = normalize_blacklist_entry(kind, value)
    client = get_supabase_client(config_path)
    if client is None:
        raise HTTPException(status_code=500, detail="Supabase not configured")

    existing_rows = (
        client.table("lead_blacklist")
        .select("id")
        .eq("kind", normalized_kind)
        .eq("value", normalized_value)
        .execute()
        .data
        or []
    )
    if existing_rows:
        client.table("lead_blacklist").delete().eq("kind", normalized_kind).eq("value", normalized_value).execute()
    deleted_count = len(existing_rows)

    restored = restore_released_blacklisted_leads_supabase(config_path, [(normalized_kind, normalized_value)])
    _invalidate_leads_cache()
    return {
        "status": "removed" if deleted_count else "not_found",
        "kind": normalized_kind,
        "value": normalized_value,
        "deleted_entries": deleted_count,
        "restored_leads": restored,
    }


def remove_lead_blacklist_and_matches_supabase(lead_id: int, config_path: Path) -> dict:
    client = get_supabase_client(config_path)
    if client is None:
        raise HTTPException(status_code=500, detail="Supabase not configured")

    lead_rows = client.table("leads").select("id,business_name,email,website_url").eq("id", lead_id).limit(1).execute().data or []
    if not lead_rows:
        raise HTTPException(status_code=404, detail="Lead not found")

    row = lead_rows[0]
    removed_entries: list[tuple[str, str]] = []
    email_value = str(row.get("email") or "").strip().lower()
    if email_value:
        removed_entries.append(("email", email_value))
    for domain_value in sorted({
        normalize_blacklist_domain(row.get("email")),
        normalize_blacklist_domain(row.get("website_url")),
    } - {None}):
        removed_entries.append(("domain", str(domain_value)))

    deleted_count = 0
    for entry_kind, entry_value in removed_entries:
        existing_rows = (
            client.table("lead_blacklist")
            .select("id")
            .eq("kind", entry_kind)
            .eq("value", entry_value)
            .execute()
            .data
            or []
        )
        if existing_rows:
            client.table("lead_blacklist").delete().eq("kind", entry_kind).eq("value", entry_value).execute()
            deleted_count += len(existing_rows)

    restored = restore_released_blacklisted_leads_supabase(config_path, removed_entries)
    _invalidate_leads_cache()
    return {
        "status": "removed" if deleted_count else "not_found",
        "lead_id": lead_id,
        "business_name": row.get("business_name"),
        "deleted_entries": deleted_count,
        "restored_leads": restored,
    }


def auto_assign_worker_to_paid_lead_supabase(lead_id: int, config_path: Path) -> dict:
    client = get_supabase_client(config_path)
    if client is None:
        return {"auto_assigned": False, "reason": "Supabase not configured"}

    lead_rows = client.table("leads").select("id,business_name,status,client_tier,worker_id,user_id").eq("id", lead_id).limit(1).execute().data or []
    if not lead_rows:
        return {"auto_assigned": False, "reason": "Lead not found"}

    lead = lead_rows[0]
    if str(lead.get("status") or "").strip().lower() != "paid":
        return {"auto_assigned": False, "reason": "Lead is not paid"}
    if lead.get("worker_id") is not None:
        return {
            "auto_assigned": False,
            "reason": "Lead already assigned",
            "worker_id": int(lead.get("worker_id")),
        }

    lead_user_id = str(lead.get("user_id") or "legacy")
    target_role = infer_worker_role_for_lead(str(lead.get("client_tier") or "standard"))
    # Scope workers and load counts to the same tenant as the lead
    workers = supabase_select_rows(client, "workers", columns="id,worker_name,role,status", filters={"user_id": lead_user_id}, order_by="id", desc=False)
    leads = supabase_select_rows(client, "leads", columns="id,worker_id", filters={"user_id": lead_user_id})

    load_by_worker: dict[int, int] = {}
    for lrow in leads:
        wid = lrow.get("worker_id")
        if wid is None:
            continue
        worker_id = int(wid)
        load_by_worker[worker_id] = load_by_worker.get(worker_id, 0) + 1

    active_workers = [
        w
        for w in workers
        if str(w.get("status") or "").strip().lower() == "active"
    ]
    role_workers = [w for w in active_workers if str(w.get("role") or "").strip().upper() == target_role]
    candidates = role_workers or active_workers
    if not candidates:
        return {"auto_assigned": False, "reason": "No active workers available"}

    candidates.sort(key=lambda w: (load_by_worker.get(int(w.get("id")), 0), int(w.get("id"))))
    candidate = candidates[0]
    assigned_worker_id = int(candidate.get("id"))

    assigned_at = utc_now_iso()
    client.table("leads").update({"worker_id": assigned_worker_id, "assigned_worker_at": assigned_at}).eq("id", lead_id).execute()
    client.table("delivery_tasks").update({"worker_id": assigned_worker_id, "updated_at": utc_now_iso()}).eq("lead_id", lead_id).in_("status", ["todo", "in_progress", "blocked"]).execute()

    add_worker_audit_supabase(
        config_path,
        action="auto_assign",
        worker_id=assigned_worker_id,
        lead_id=lead_id,
        message=(
            f"Auto-assigned paid client '{lead.get('business_name')}' to "
            f"{candidate.get('worker_name')} ({candidate.get('role')})."
        ),
    )
    return {
        "auto_assigned": True,
        "worker_id": assigned_worker_id,
        "worker_name": candidate.get("worker_name"),
        "worker_role": candidate.get("role"),
    }


def ensure_delivery_task_for_paid_lead_supabase(lead_id: int, config_path: Path) -> dict:
    client = get_supabase_client(config_path)
    if client is None:
        return {"task_created": False, "reason": "Supabase not configured"}

    task_map = {
        "premium_ads": "Website + Google Ads Setup",
        "saas": "Dev Onboarding",
        "standard": "Website Setup",
    }

    lead_rows = client.table("leads").select("id,business_name,status,client_tier,worker_id,user_id").eq("id", lead_id).limit(1).execute().data or []
    if not lead_rows:
        return {"task_created": False, "reason": "Lead not found"}

    lead = lead_rows[0]
    if str(lead.get("status") or "").strip().lower() != "paid":
        return {"task_created": False, "reason": "Lead is not paid"}

    existing = client.table("delivery_tasks").select("id,status").eq("lead_id", lead_id).in_("status", ["todo", "in_progress", "blocked"]).order("id", desc=True).limit(1).execute().data or []
    if existing:
        return {
            "task_created": False,
            "task_id": int(existing[0].get("id")),
            "reason": "Open task already exists",
        }

    tier_key = str(lead.get("client_tier") or "standard").strip().lower() or "standard"
    task_type = task_map.get(tier_key, task_map["standard"])
    due_at = (datetime.now(timezone.utc) + timedelta(hours=48)).isoformat()
    now_iso = utc_now_iso()
    lead_user_id = str(lead.get("user_id") or "legacy")
    max_position_rows = (
        client.table("delivery_tasks")
        .select("position")
        .eq("user_id", lead_user_id)
        .order("position", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    next_position = int(max_position_rows[0].get("position") or 0) + 1 if max_position_rows else 1

    payload = {
        "user_id": lead_user_id,
        "lead_id": lead_id,
        "worker_id": int(lead.get("worker_id")) if lead.get("worker_id") is not None else None,
        "business_name": str(lead.get("business_name") or "").strip() or f"Lead #{lead_id}",
        "task_type": task_type,
        "status": "todo",
        "notes": None,
        "due_at": due_at,
        "done_at": None,
        "position": next_position,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    inserted = client.table("delivery_tasks").insert(payload).execute().data or []
    task_id = int(inserted[0].get("id")) if inserted else 0

    add_worker_audit_supabase(
        config_path,
        action="delivery_task_created",
        worker_id=int(lead.get("worker_id")) if lead.get("worker_id") is not None else None,
        lead_id=lead_id,
        message=f"Created delivery task '{task_type}' for paid client '{lead.get('business_name')}'.",
    )
    return {"task_created": True, "task_id": task_id, "task_type": task_type}


def get_workers_snapshot_supabase(config_path: Path, user_id: Optional[str] = None) -> dict:
    client = get_supabase_client(config_path)
    if client is None:
        raise HTTPException(status_code=500, detail="Supabase not configured")

    workers = supabase_select_rows(client, "workers", columns="id,worker_name,role,monthly_cost,status,comms_link,created_at,updated_at", filters={"user_id": user_id} if user_id else None, order_by="id", desc=True)
    assignments = supabase_select_rows(client, "leads", columns="worker_id,business_name,client_tier,status,paid_at,assigned_worker_at", filters={"user_id": user_id} if user_id else None)
    completed_tasks = [
        row
        for row in supabase_select_rows(client, "delivery_tasks", columns="worker_id,done_at,lead_id,status", filters={"user_id": user_id} if user_id else None)
        if str(row.get("status") or "").strip().lower() == "done" and row.get("done_at")
    ]
    paid_lookup = {
        int(row.get("id")): row
        for row in supabase_select_rows(client, "leads", columns="id,paid_at", filters={"user_id": user_id} if user_id else None)
        if row.get("id") is not None
    }
    audit_rows = supabase_select_rows(client, "worker_audit_log", columns="id,worker_id,lead_id,action,message,actor,created_at", order_by="id", desc=True, limit=40)

    worker_map: dict[int, dict] = {}
    for row in workers:
        worker_id = int(row.get("id"))
        worker_map[worker_id] = {
            "id": worker_id,
            "worker_name": row.get("worker_name"),
            "role": row.get("role"),
            "monthly_cost": float(row.get("monthly_cost") or 0),
            "status": row.get("status"),
            "comms_link": row.get("comms_link"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "assigned_clients": [],
            "assigned_clients_count": 0,
            "total_profit_generated": 0.0,
            "profitability_metric": 0.0,
        }

    efficiency_days: list[float] = []
    for row in assignments:
        worker_id_raw = row.get("worker_id")
        if worker_id_raw is None:
            continue
        worker_id = int(worker_id_raw)
        worker = worker_map.get(worker_id)
        if worker is None:
            continue

        business_name = str(row.get("business_name") or "").strip()
        if business_name:
            worker["assigned_clients"].append(business_name)

        if str(row.get("status") or "").strip().lower() == "paid":
            tier_key = str(row.get("client_tier") or "standard").strip().lower() or "standard"
            setup_fee = float(SETUP_FEE_BY_TIER.get(tier_key, SETUP_FEE_BY_TIER["standard"]))
            monthly_fee = float(MRR_BY_TIER.get(tier_key, MRR_BY_TIER["standard"]))
            worker["total_profit_generated"] += setup_fee + monthly_fee

            paid_at = parse_iso_datetime(row.get("paid_at"))
            assigned_worker_at = parse_iso_datetime(row.get("assigned_worker_at"))
            if paid_at and assigned_worker_at and assigned_worker_at >= paid_at:
                delta_days = (assigned_worker_at - paid_at).total_seconds() / 86400
                efficiency_days.append(delta_days)

    for row in completed_tasks:
        lead_id = row.get("lead_id")
        if lead_id is None:
            continue
        paid_row = paid_lookup.get(int(lead_id))
        if not paid_row:
            continue
        paid_at = parse_iso_datetime(paid_row.get("paid_at"))
        done_at = parse_iso_datetime(row.get("done_at"))
        if paid_at and done_at and done_at >= paid_at:
            efficiency_days.append((done_at - paid_at).total_seconds() / 86400)

    total_team_cost = 0.0
    total_generated = 0.0
    for worker in worker_map.values():
        worker["assigned_clients"] = sorted(set(worker["assigned_clients"]))
        worker["assigned_clients_count"] = len(worker["assigned_clients"])
        worker["profitability_metric"] = worker["total_profit_generated"] - worker["monthly_cost"]
        total_team_cost += float(worker["monthly_cost"])
        total_generated += float(worker["total_profit_generated"])

    delivery_efficiency_days = round(sum(efficiency_days) / len(efficiency_days), 1) if efficiency_days else 0.0
    net_agency_margin = total_generated - total_team_cost

    return {
        "items": list(worker_map.values()),
        "metrics": {
            "total_team_cost": round(total_team_cost, 2),
            "delivery_efficiency_days": delivery_efficiency_days,
            "net_agency_margin": round(net_agency_margin, 2),
        },
        "audit": audit_rows,
    }


def get_queued_mail_count_supabase(config_path: Path, user_id: Optional[str] = None) -> int:
    client = get_supabase_client(config_path)
    if client is None:
        return 0
    rows = supabase_select_rows(
        client,
        "leads",
        columns="status,email,next_mail_at",
        filters={"user_id": user_id} if user_id else None,
    )
    now_utc = datetime.now(timezone.utc)
    count = 0
    for row in rows:
        status_value = str(row.get("status") or "").strip().lower()
        email_value = str(row.get("email") or "").strip()
        if status_value != "queued_mail" or not email_value:
            continue
        next_mail_at = parse_iso_datetime(row.get("next_mail_at"))
        if next_mail_at is None or next_mail_at <= now_utc:
            count += 1
    return count


def get_dashboard_stats_supabase(config_path: Path, user_id: Optional[str] = None) -> dict:
    client = get_supabase_client(config_path)
    if client is None:
        raise HTTPException(status_code=500, detail="Supabase not configured")

    uid_filter = {"user_id": user_id} if user_id else None
    try:
        leads = supabase_select_rows(
            client,
            "leads",
            columns="id,status,sent_at,last_contacted_at,reply_detected_at,paid_at,pipeline_stage,scraped_at,status_updated_at,open_count,client_tier,is_ads_client,is_website_client,client_folder_id",
            filters=uid_filter,
        )
    except Exception as exc:
        logging.warning("Supabase stats query fallback to legacy leads columns: %s", exc)
        leads = supabase_select_rows(client, "leads", columns="id,status,sent_at,client_tier", filters=uid_filter)
        for row in leads:
            row.setdefault("open_count", 0)
            row.setdefault("is_ads_client", 0)
            row.setdefault("is_website_client", 0)
            row.setdefault("last_contacted_at", None)
            row.setdefault("reply_detected_at", None)
            row.setdefault("paid_at", None)
            row.setdefault("pipeline_stage", None)
            row.setdefault("scraped_at", None)
            row.setdefault("status_updated_at", None)
            row.setdefault("client_folder_id", None)
    revenue_log = supabase_select_rows(
        client,
        "revenue_log",
        columns="amount,is_recurring",
        filters=uid_filter,
    )

    total_leads = len(leads)
    emails_sent = sum(1 for row in leads if row.get("sent_at") is not None)
    opened_count = sum(1 for row in leads if int(row.get("open_count") or 0) > 0)
    opens_total = sum(int(row.get("open_count") or 0) for row in leads)
    replies_count = sum(1 for row in leads if str(row.get("status") or "").strip().lower() in REPLY_STATUSES)
    paid_rows = [row for row in leads if str(row.get("status") or "").strip().lower() == "paid"]
    paid_count = len(paid_rows)
    now_utc = datetime.now(timezone.utc)
    month_prefix = now_utc.strftime("%Y-%m")
    week_cutoff = now_utc - timedelta(days=7)
    found_this_month = sum(1 for row in leads if str(row.get("scraped_at") or "").startswith(month_prefix))
    contacted_this_month = sum(
        1
        for row in leads
        if str(row.get("sent_at") or "").startswith(month_prefix) or str(row.get("last_contacted_at") or "").startswith(month_prefix)
    )
    replied_this_month = sum(
        1
        for row in leads
        if str(row.get("reply_detected_at") or "").startswith(month_prefix)
        or (
            str(row.get("status_updated_at") or "").startswith(month_prefix)
            and str(row.get("status") or "").strip().lower() in REPLY_STATUSES
        )
    )
    won_this_month = sum(
        1
        for row in leads
        if str(row.get("paid_at") or "").startswith(month_prefix)
        or (
            str(row.get("status_updated_at") or "").startswith(month_prefix)
            and str(row.get("status") or "").strip().lower() == "paid"
        )
    )
    found_this_week = sum(1 for row in leads if (parse_iso_datetime(str(row.get("scraped_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)) >= week_cutoff)
    contacted_this_week = sum(
        1
        for row in leads
        if (parse_iso_datetime(str(row.get("sent_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)) >= week_cutoff
        or (parse_iso_datetime(str(row.get("last_contacted_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)) >= week_cutoff
    )
    replied_this_week = sum(
        1
        for row in leads
        if (parse_iso_datetime(str(row.get("reply_detected_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)) >= week_cutoff
        or (
            (parse_iso_datetime(str(row.get("status_updated_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)) >= week_cutoff
            and str(row.get("status") or "").strip().lower() in REPLY_STATUSES
        )
    )
    won_this_week = sum(
        1
        for row in leads
        if (parse_iso_datetime(str(row.get("paid_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)) >= week_cutoff
        or (
            (parse_iso_datetime(str(row.get("status_updated_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)) >= week_cutoff
            and str(row.get("status") or "").strip().lower() == "paid"
        )
    )

    setup_revenue = 0.0
    monthly_recurring_revenue = 0.0
    paid_by_tier: dict[str, int] = {}
    website_clients = 0
    ads_clients = 0
    ads_and_website_clients = 0

    for row in paid_rows:
        tier_key = str(row.get("client_tier") or "standard").strip().lower() or "standard"
        paid_by_tier[tier_key] = paid_by_tier.get(tier_key, 0) + 1
        is_ads = int(row.get("is_ads_client") or 0)
        is_web = int(row.get("is_website_client") or 0)

        if is_web:
            setup_revenue += SETUP_FEE_WEBSITE
            website_clients += 1

        if is_web and is_ads:
            monthly_recurring_revenue += MRR_ADS_AND_WEBSITE
            ads_and_website_clients += 1
        elif is_ads:
            monthly_recurring_revenue += MRR_ADS_ONLY
            ads_clients += 1
        elif is_web:
            monthly_recurring_revenue += MRR_WEBSITE_ONLY
        else:
            monthly_recurring_revenue += float(MRR_BY_TIER.get(tier_key, MRR_BY_TIER["standard"]))

    setup_revenue += sum(float(row.get("amount") or 0) for row in revenue_log)
    monthly_recurring_revenue += sum(
        float(row.get("amount") or 0)
        for row in revenue_log
        if int(row.get("is_recurring") or 0) == 1
    )

    mrr_progress_pct = 0
    if MRR_GOAL > 0:
        mrr_progress_pct = min(100, round((monthly_recurring_revenue / MRR_GOAL) * 100))

    milestone_progress_pct = min(100, round((setup_revenue / SETUP_MILESTONE) * 100))

    queued_mail_count = get_queued_mail_count_supabase(config_path, user_id=user_id)
    next_drip_at = get_runtime_value(DEFAULT_DB_PATH, "next_drip_at") if AUTO_DRIP_DISPATCH_ENABLED else None
    reply_rate = round((replies_count / emails_sent) * 100, 1) if emails_sent > 0 else 0.0
    open_rate = round((opened_count / emails_sent) * 100, 1) if emails_sent > 0 else 0.0
    pipeline = {"scraped": 0, "contacted": 0, "replied": 0, "won_paid": 0}
    for row in leads:
        stage = _derive_pipeline_stage(
            status=row.get("status"),
            sent_at=row.get("sent_at"),
            last_contacted_at=row.get("last_contacted_at"),
            reply_detected_at=row.get("reply_detected_at"),
            paid_at=row.get("paid_at"),
            pipeline_stage=row.get("pipeline_stage"),
        )
        if stage == "Won (Paid)":
            pipeline["won_paid"] += 1
        elif stage == "Replied":
            pipeline["replied"] += 1
        elif stage == "Contacted":
            pipeline["contacted"] += 1
        else:
            pipeline["scraped"] += 1

    try:
        client_folder_rows = supabase_select_rows(
            client,
            "ClientFolders",
            columns="id",
            filters=uid_filter,
            limit=1000,
        )
        client_folder_count = len(client_folder_rows)
    except Exception:
        client_folder_count = 0

    return {
        "total_leads": total_leads,
        "emails_sent": emails_sent,
        "opened_count": opened_count,
        "opens_total": opens_total,
        "open_rate": open_rate,
        "replies_count": replies_count,
        "reply_rate": reply_rate,
        "paid_count": paid_count,
        "total_revenue": setup_revenue,
        "setup_revenue": setup_revenue,
        "monthly_recurring_revenue": monthly_recurring_revenue,
        "paid_by_tier": paid_by_tier,
        "mrr_goal": MRR_GOAL,
        "mrr_progress_pct": mrr_progress_pct,
        "potential_revenue": setup_revenue,
        "setup_milestone": SETUP_MILESTONE,
        "milestone_progress_pct": milestone_progress_pct,
        "website_clients": website_clients,
        "ads_clients": ads_clients,
        "ads_and_website_clients": ads_and_website_clients,
        "queued_mail_count": queued_mail_count,
        "next_drip_at": next_drip_at,
        "found_this_month": found_this_month,
        "contacted_this_month": contacted_this_month,
        "replied_this_month": replied_this_month,
        "won_this_month": won_this_month,
        "found_this_week": found_this_week,
        "contacted_this_week": contacted_this_week,
        "replied_this_week": replied_this_week,
        "won_this_week": won_this_week,
        "pipeline": pipeline,
        "client_folder_count": client_folder_count,
    }


def get_sqlite_table_rows(db_path: Path, table_name: str) -> list[dict]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    return [dict(row) for row in rows]


def get_sqlite_table_columns(db_path: Path, table_name: str) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def replace_sqlite_table_rows(db_path: Path, table_name: str, rows: list[dict], columns: list[str]) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DELETE FROM {table_name}")
        if rows and columns:
            cols_sql = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))
            conn.executemany(
                f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders})",
                [[row.get(col) for col in columns] for row in rows],
            )
        conn.commit()


def sync_table_from_supabase(db_path: Path, table_name: str, config_path: Path) -> tuple[int, Optional[str]]:
    client = get_supabase_client(config_path)
    if client is None:
        return 0, "Supabase not configured"

    try:
        local_columns = get_sqlite_table_columns(db_path, table_name)
        if not local_columns:
            return 0, f"Local table '{table_name}' not found"

        remote_rows = supabase_select_rows(
            client,
            table_name,
            columns="*",
            order_by="id" if "id" in local_columns else None,
            desc=False,
        )

        remote_keys: set[str] = set()
        for row in remote_rows:
            remote_keys.update(row.keys())

        write_columns = [col for col in local_columns if col in remote_keys]
        normalized_rows = [{col: row.get(col) for col in write_columns} for row in remote_rows]
        replace_sqlite_table_rows(db_path, table_name, normalized_rows, write_columns)
        return len(remote_rows), None
    except Exception as exc:
        return 0, str(exc)


def sync_table_to_supabase(db_path: Path, table_name: str, config_path: Path) -> tuple[int, Optional[str]]:
    import re as _re
    client = get_supabase_client(config_path)
    if client is None:
        return 0, "Supabase not configured"

    try:
        rows = get_sqlite_table_rows(db_path, table_name)
        if not rows:
            return 0, None

        excluded_cols: set[str] = set()
        last_error: Optional[str] = None
        for _attempt in range(15):
            try:
                filtered = [{k: v for k, v in row.items() if k not in excluded_cols} for row in rows]
                client.table(table_name).upsert(filtered).execute()
                if excluded_cols:
                    logging.info(
                        "Supabase sync '%s': skipped unmapped columns %s",
                        table_name, sorted(excluded_cols),
                    )
                return len(rows), None
            except Exception as exc:
                msg = str(exc)
                m = _re.search(r"Could not find the '(\w+)' column", msg)
                if m and "PGRST204" in msg:
                    col = m.group(1)
                    excluded_cols.add(col)
                    logging.warning(
                        "Supabase sync '%s': column '%s' missing in remote, will skip it.",
                        table_name, col,
                    )
                    last_error = msg
                    continue
                return 0, msg
        return 0, f"Too many missing columns: {sorted(excluded_cols)}. Last error: {last_error}"
    except Exception as exc:
        return 0, str(exc)


def execute_supabase_update_with_retry(
    client: Any,
    table_name: str,
    payload: dict[str, Any],
    *,
    eq_filters: Optional[dict[str, Any]] = None,
    operation_name: str = "update",
):
    attempt_payload = dict(payload or {})
    stripped_columns: set[str] = set()

    while attempt_payload:
        try:
            query = client.table(table_name).update(attempt_payload)
            for key, value in (eq_filters or {}).items():
                query = query.eq(key, value)
            response = query.execute()
            return response, attempt_payload
        except Exception as exc:
            msg = str(exc)
            match = re.search(r"Could not find the '(\w+)' column", msg)
            missing_col = str(match.group(1) or "").strip() if match else ""
            if "PGRST204" not in msg or not missing_col or missing_col not in attempt_payload:
                raise
            stripped_columns.add(missing_col)
            attempt_payload.pop(missing_col, None)
            logging.warning(
                "Supabase %s: column '%s' missing in remote schema, retrying without it.",
                operation_name,
                missing_col,
            )

    return None, {}


def delete_supabase_row(table_name: str, row_id: int, config_path: Path) -> Optional[str]:
    client = get_supabase_client(config_path)
    if client is None:
        return "Supabase not configured"
    try:
        client.table(table_name).delete().eq("id", row_id).execute()
        return None
    except Exception as exc:
        return str(exc)


def sync_all_to_supabase(db_path: Path, config_path: Path) -> dict:
    synced: dict[str, int] = {}
    errors: dict[str, str] = {}

    for table_name in SUPABASE_SYNC_TABLES:
        try:
            count, err = sync_table_to_supabase(db_path, table_name, config_path)
        except Exception as exc:
            count, err = 0, str(exc)
        synced[table_name] = count
        if err:
            errors[table_name] = err

    return {
        "ok": len(errors) == 0,
        "synced": synced,
        "errors": errors,
        "generated_at": utc_now_iso(),
    }


def maybe_sync_supabase(db_path: Path, config_path: Path) -> None:
    _invalidate_leads_cache()
    settings = load_supabase_settings(config_path)
    if not settings["enabled"]:
        return
    result = sync_all_to_supabase(db_path, config_path)
    if not result["ok"]:
        logging.warning("Supabase sync had errors: %s", result["errors"])


def insert_manual_lead_supabase(client: Any, payload: ManualLeadRequest) -> dict:
    row_data = {
        "business_name": payload.business_name.strip(),
        "website_url": None,
        "phone_number": None,
        "rating": None,
        "review_count": None,
        "address": f"Manual Entry | {payload.email.strip().lower()}",
        "search_keyword": "manual-entry",
        "contact_name": payload.contact_name.strip(),
        "email": payload.email.strip(),
        "status": "Pending",
        "client_tier": "standard",
        "status_updated_at": utc_now_iso(),
        "crm_comment": "Manually added lead",
    }

    inserted = None
    last_error: Optional[Exception] = None

    for attempt in range(3):
        try:
            payload_to_insert = dict(row_data)
            if attempt > 0:
                max_row = supabase_select_rows(client, "leads", columns="id", order_by="id", desc=True, limit=1)
                next_id = int((max_row[0] if max_row else {}).get("id") or 0) + 1
                payload_to_insert["id"] = next_id
            inserted = client.table("leads").insert(payload_to_insert).execute()
            break
        except Exception as exc:
            last_error = exc
            msg = str(exc)
            if "duplicate key value violates unique constraint" in msg and "leads_pkey" in msg:
                continue
            raise

    if inserted is None:
        raise RuntimeError(f"Failed to insert lead after retries: {last_error}")

    data = inserted.data or []
    lead_id = int(data[0].get("id")) if data else None
    return {"lead_id": lead_id}


def load_openai_client(config_path: Path) -> tuple[Optional[OpenAI], str]:
    model_name = DEFAULT_AI_MODEL
    api_key = str(os.environ.get("OPENAI_API_KEY", "") or "").strip()

    try:
        with config_path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
        openai_cfg = cfg.get("openai", {}) if isinstance(cfg, dict) else {}
        model_name = str(openai_cfg.get("model", DEFAULT_AI_MODEL) or DEFAULT_AI_MODEL).strip()
        if not api_key:
            api_key = str(openai_cfg.get("api_key", "") or "").strip()
    except Exception:
        pass

    if not api_key or api_key == "YOUR_OPENAI_API_KEY":
        return None, model_name

    try:
        return OpenAI(api_key=api_key), model_name
    except Exception as exc:
        logging.warning("Could not initialize OpenAI client for niche recommendation: %s", exc)
        return None, model_name


def extract_keyword_performance(db_path: Path, limit: int = 8) -> list[dict]:
    # Use Supabase primary when enabled so niche advisor reflects live cloud data
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                rows = supabase_select_rows(
                    client,
                    "leads",
                    columns="search_keyword,sent_at,status",
                )
                perf: dict[str, dict] = {}
                for row in rows:
                    kw = str(row.get("search_keyword") or "").strip()
                    if not kw:
                        continue
                    entry = perf.setdefault(kw, {"total": 0, "sent": 0, "replies": 0})
                    entry["total"] += 1
                    if row.get("sent_at") is not None:
                        entry["sent"] += 1
                    if str(row.get("status") or "").strip().lower() in REPLY_STATUSES:
                        entry["replies"] += 1
                result: list[dict] = []
                for kw, e in perf.items():
                    if e["sent"] <= 0:
                        continue
                    result.append({
                        "keyword": kw,
                        "total_leads": e["total"],
                        "sent_count": e["sent"],
                        "replies": e["replies"],
                        "reply_rate": round((e["replies"] / e["sent"]) * 100, 1),
                    })
                result.sort(key=lambda x: (-x["reply_rate"], -x["sent_count"], -x["total_leads"]))
                return result[:limit]
            except Exception as exc:
                logging.warning("Supabase keyword performance fallback to SQLite: %s", exc)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                TRIM(search_keyword) AS keyword,
                COUNT(*) AS total_leads,
                SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END) AS sent_count,
                SUM(CASE WHEN LOWER(COALESCE(status, '')) IN ('interested', 'meeting set') THEN 1 ELSE 0 END) AS replies
            FROM leads
            WHERE search_keyword IS NOT NULL AND TRIM(search_keyword) != ''
            GROUP BY TRIM(search_keyword)
            HAVING SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END) > 0
            ORDER BY
                (CAST(SUM(CASE WHEN LOWER(COALESCE(status, '')) IN ('interested', 'meeting set') THEN 1 ELSE 0 END) AS REAL)
                 / SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END)) DESC,
                SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END) DESC,
                COUNT(*) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    result = []
    for row in rows:
        sent_count = int(row["sent_count"] or 0)
        replies = int(row["replies"] or 0)
        if sent_count <= 0:
            continue
        reply_rate = round((replies / sent_count) * 100, 1)
        result.append(
            {
                "keyword": str(row["keyword"] or "").strip(),
                "total_leads": int(row["total_leads"] or 0),
                "sent_count": sent_count,
                "replies": replies,
                "reply_rate": reply_rate,
            }
        )
    return result


def default_market_recommendations(country_code: str = "US") -> list[dict]:
    selected_country = normalize_country_value(country_code)
    presets = {
        "US": [
            {
                "keyword": "Roofers in Miami, FL",
                "location": "Miami, FL",
                "country_code": "US",
                "reason": "Storm repair, insurance jobs, and high-ticket roofing demand keep reply quality strong.",
                "expected_reply_rate": 6.8,
            },
            {
                "keyword": "HVAC Repair in Phoenix, AZ",
                "location": "Phoenix, AZ",
                "country_code": "US",
                "reason": "Seasonal urgency and high local intent make service businesses responsive here.",
                "expected_reply_rate": 6.2,
            },
            {
                "keyword": "Dental Implants in Austin, TX",
                "location": "Austin, TX",
                "country_code": "US",
                "reason": "Strong lifetime value and premium treatment margins support aggressive acquisition.",
                "expected_reply_rate": 5.4,
            },
        ],
        "DE": [
            {
                "keyword": "Solar Installation in Berlin, DE",
                "location": "Berlin, DE",
                "country_code": "DE",
                "reason": "Energy-efficiency demand and strong contract values make this niche attractive in Germany.",
                "expected_reply_rate": 5.6,
            },
            {
                "keyword": "Dental Implants in Munich, DE",
                "location": "Munich, DE",
                "country_code": "DE",
                "reason": "Premium local treatment demand and long customer value create room for ROI-positive outreach.",
                "expected_reply_rate": 4.9,
            },
            {
                "keyword": "Commercial Cleaning in Hamburg, DE",
                "location": "Hamburg, DE",
                "country_code": "DE",
                "reason": "Recurring contracts and facility demand make this market consistently monetizable.",
                "expected_reply_rate": 4.7,
            },
        ],
        "AT": [
            {
                "keyword": "Roof Repair in Vienna, AT",
                "location": "Vienna, AT",
                "country_code": "AT",
                "reason": "Property upkeep and premium urban jobs make roofing and repairs commercially attractive.",
                "expected_reply_rate": 5.1,
            },
            {
                "keyword": "Physio Clinic in Graz, AT",
                "location": "Graz, AT",
                "country_code": "AT",
                "reason": "High-value recurring visits and strong local search intent help conversion economics.",
                "expected_reply_rate": 4.6,
            },
            {
                "keyword": "Solar Installation in Linz, AT",
                "location": "Linz, AT",
                "country_code": "AT",
                "reason": "Austrian energy-upgrade demand keeps decision intent relatively high.",
                "expected_reply_rate": 4.8,
            },
        ],
        "SI": [
            {
                "keyword": "Toplotne črpalke v Ljubljani, SI",
                "location": "Ljubljana, SI",
                "country_code": "SI",
                "reason": "High-ticket home-efficiency projects and local trust-based buying make this a strong signal niche.",
                "expected_reply_rate": 5.0,
            },
            {
                "keyword": "Krovstvo v Mariboru, SI",
                "location": "Maribor, SI",
                "country_code": "SI",
                "reason": "Roofing projects carry urgent need and solid contract value for outbound campaigns.",
                "expected_reply_rate": 4.7,
            },
            {
                "keyword": "Zobni implantati v Celju, SI",
                "location": "Celje, SI",
                "country_code": "SI",
                "reason": "Premium treatments and long client value support higher-margin lead generation.",
                "expected_reply_rate": 4.5,
            },
        ],
    }
    return list(presets.get(selected_country) or presets["US"])


def heuristic_recommendations_from_performance(performance: list[dict], country_code: str = "US") -> list[dict]:
    selected_country = normalize_country_value(country_code)
    defaults = default_market_recommendations(selected_country)
    if not performance:
        return defaults

    default_location = str((defaults[0] if defaults else {}).get("location") or selected_country).strip() or selected_country
    recs: list[dict] = []
    for item in performance[:3]:
        raw_keyword = str(item.get("keyword") or "").strip()
        if not raw_keyword:
            continue
        service_label = raw_keyword.split(" in ", 1)[0].strip() if " in " in raw_keyword.lower() else raw_keyword
        localized_keyword = f"{service_label} in {default_location}" if service_label and default_location else raw_keyword

        recs.append(
            {
                "keyword": localized_keyword,
                "location": default_location,
                "country_code": selected_country,
                "reason": (
                    f"Your own data shows this service angle already performs with {item['reply_rate']}% reply rate "
                    f"across {item['sent_count']} sent emails, adapted for {selected_country}."
                ),
                "expected_reply_rate": float(item["reply_rate"]),
            }
        )

    seen = {str(rec["keyword"]).lower() for rec in recs}
    for fallback in defaults:
        if len(recs) >= 3:
            break
        if str(fallback["keyword"]).lower() in seen:
            continue
        recs.append(fallback)
        seen.add(str(fallback["keyword"]).lower())

    return recs[:3]


def get_niche_recommendation(db_path: Path, config_path: Path, country_code: str = "US") -> dict:
    ensure_system_tables(db_path)
    selected_country = normalize_country_value(country_code)
    country_labels = {
        "US": "United States",
        "DE": "Germany",
        "AT": "Austria",
        "SI": "Slovenia",
    }
    selected_country_label = country_labels.get(selected_country, selected_country)
    performance = extract_keyword_performance(db_path)
    heuristic = heuristic_recommendations_from_performance(performance, selected_country)

    client, model_name = load_openai_client(config_path)
    if client is None:
        return {
            "source": "heuristic",
            "generated_at": utc_now_iso(),
            "recommendations": heuristic,
            "top_pick": heuristic[0],
            "performance_snapshot": performance,
            "selected_country_code": selected_country,
            "selected_country_label": selected_country_label,
        }

    perf_lines = "\n".join(
        [
            f"- {p['keyword']} | sent={p['sent_count']} replies={p['replies']} reply_rate={p['reply_rate']}%"
            for p in performance
        ]
    )
    if not perf_lines:
        perf_lines = "- No historical keyword performance yet."

    country_examples = "\n".join(
        [f"- {item['keyword']} ({item['reason']})" for item in heuristic[:3]]
    ) or "- Use the selected country only."

    system_prompt = (
        "You are a revenue strategist for lead generation campaigns. "
        "Always return valid JSON only."
    )
    user_prompt = f"""
Glede na današnji datum (april 2026), trenutne ekonomske trende in dejstvo,
da uporabnik prodaja Google Ads / SEO storitve, predlagaj 3 najbolj donosne niše
SAMO za izbrano državo: {selected_country_label} ({selected_country}).

Obvezno upoštevaj našo zgodovino reply-ratov po ključnih besedah:
{perf_lines}

Primeri, ki se ujemajo z izbrano državo:
{country_examples}

Vrni JSON objekt s točno strukturo:
{{
  "recommendations": [
    {{
      "keyword": "Roofers in Miami, FL",
      "location": "Miami, FL",
      "country_code": "US",
      "reason": "Kratek razlog (sezonskost / marža / povpraševanje)",
      "expected_reply_rate": 6.2
    }}
  ],
  "top_pick_index": 0
}}

Pravila:
- recommendations mora imeti točno 3 elemente.
- Vse 3 lokacije morajo biti v državi {selected_country}.
- country_code mora biti vedno '{selected_country}'.
- expected_reply_rate naj bo realna številka med 1.0 in 15.0.
""".strip()

    try:
        response = client.chat.completions.create(
            model=model_name,
            temperature=0.55,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )

        content = response.choices[0].message.content or "{}"
        payload = json.loads(content)
        raw_recs = payload.get("recommendations") if isinstance(payload, dict) else []
        if not isinstance(raw_recs, list):
            raw_recs = []

        normalized: list[dict] = []
        for idx, rec in enumerate(raw_recs):
            if not isinstance(rec, dict):
                continue
            keyword = str(rec.get("keyword", "") or "").strip()
            location = str(rec.get("location", "") or "").strip()
            reason = str(rec.get("reason", "") or "").strip()
            rec_country_code = selected_country
            try:
                expected_reply_rate = float(rec.get("expected_reply_rate", 0) or 0)
            except (TypeError, ValueError):
                expected_reply_rate = 0.0

            if not keyword and idx < len(heuristic):
                keyword = str(heuristic[idx].get("keyword") or "").strip()
            if not location and idx < len(heuristic):
                location = str(heuristic[idx].get("location") or "").strip()
            if keyword and " in " not in keyword.lower() and location:
                keyword = f"{keyword} in {location}"
            if not keyword:
                continue
            if expected_reply_rate <= 0 and idx < len(heuristic):
                expected_reply_rate = float(heuristic[idx].get("expected_reply_rate", 5.0))
            if not reason and idx < len(heuristic):
                reason = str(heuristic[idx].get("reason", f"High intent demand in {selected_country_label}."))

            normalized.append(
                {
                    "keyword": keyword,
                    "location": location or selected_country_label,
                    "country_code": rec_country_code,
                    "reason": reason,
                    "expected_reply_rate": round(max(1.0, min(expected_reply_rate, 15.0)), 1),
                }
            )

        existing = {str(item["keyword"]).lower() for item in normalized}
        for fallback in heuristic:
            if len(normalized) >= 3:
                break
            key = str(fallback["keyword"]).lower()
            if key in existing:
                continue
            normalized.append(fallback)
            existing.add(key)

        if not normalized:
            normalized = heuristic

        top_pick_index = 0
        if isinstance(payload, dict):
            try:
                top_pick_index = int(payload.get("top_pick_index", 0))
            except (TypeError, ValueError):
                top_pick_index = 0
        if top_pick_index < 0 or top_pick_index >= len(normalized):
            top_pick_index = 0

        return {
            "source": "openai",
            "generated_at": utc_now_iso(),
            "recommendations": normalized[:3],
            "top_pick": normalized[top_pick_index],
            "performance_snapshot": performance,
            "selected_country_code": selected_country,
            "selected_country_label": selected_country_label,
        }
    except Exception as exc:
        logging.warning("Niche recommendation fallback to heuristic: %s", exc)
        return {
            "source": "heuristic",
            "generated_at": utc_now_iso(),
            "recommendations": heuristic,
            "top_pick": heuristic[0],
            "performance_snapshot": performance,
            "selected_country_code": selected_country,
            "selected_country_label": selected_country_label,
        }


def queue_high_score_enriched_leads(
    db_path: Path,
    threshold: float = HIGH_AI_SCORE_THRESHOLD,
    user_id: Optional[str] = None,
) -> int:
    ensure_system_tables(db_path)

    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                filters = {"user_id": user_id} if user_id else None
                rows = supabase_select_rows(
                    client,
                    "leads",
                    columns="id,status,email,ai_score,next_mail_at",
                    filters=filters,
                )
                now_iso = utc_now_iso()
                queued_count = 0
                with sqlite3.connect(db_path) as conn:
                    for row in rows:
                        status_value = str(row.get("status") or "").strip().lower()
                        email_value = str(row.get("email") or "").strip()
                        ai_score = float(row.get("ai_score") or 0)
                        if status_value != "enriched" or not email_value or ai_score < threshold:
                            continue

                        lead_id = int(row.get("id") or 0)
                        if lead_id <= 0:
                            continue

                        update_payload = {
                            "status": "queued_mail",
                            "status_updated_at": now_iso,
                        }
                        if row.get("next_mail_at") in (None, ""):
                            update_payload["next_mail_at"] = now_iso

                        client.table("leads").update(update_payload).eq("id", lead_id).execute()

                        # Keep local mirror aligned so later syncs do not revert status back to enriched.
                        conn.execute(
                            """
                            UPDATE leads
                            SET
                                status = 'queued_mail',
                                next_mail_at = COALESCE(next_mail_at, ?),
                                status_updated_at = ?
                            WHERE id = ?
                            """,
                            (now_iso, now_iso, lead_id),
                        )
                        queued_count += 1

                    conn.commit()
                return queued_count
            except Exception as exc:
                logging.warning("Supabase queue_high_score_enriched_leads fallback to SQLite: %s", exc)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE leads
            SET
                status = 'queued_mail',
                next_mail_at = COALESCE(next_mail_at, CURRENT_TIMESTAMP),
                status_updated_at = CURRENT_TIMESTAMP
            WHERE
                LOWER(COALESCE(status, '')) = 'enriched'
                AND email IS NOT NULL
                AND TRIM(email) != ''
                AND COALESCE(ai_score, 0) >= ?
                AND (? IS NULL OR user_id = ?)
            """,
            (threshold, user_id, user_id),
        )
        conn.commit()
        queued_count = int(cursor.rowcount or 0)

    if queued_count:
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
    return queued_count


def get_scraped_lead_count(db_path: Path) -> int:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                rows = supabase_select_rows(client, "leads", columns="status")
                return sum(1 for row in rows if str(row.get("status") or "").strip().lower() == "scraped")
            except Exception as exc:
                logging.warning("Supabase get_scraped_lead_count fallback to SQLite: %s", exc)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status, '')) = 'scraped'"
        ).fetchone()
    return int(row[0] if row else 0)


def get_queued_mail_count(db_path: Path, user_id: Optional[str] = None) -> int:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return get_queued_mail_count_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)

    uid_clause = "AND user_id = ?" if user_id else ""
    uid_params = [user_id] if user_id else []

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM leads
            WHERE
                LOWER(COALESCE(status, '')) = 'queued_mail'
                AND email IS NOT NULL
                AND TRIM(email) != ''
                {uid_clause}
                AND (
                    next_mail_at IS NULL
                    OR datetime(next_mail_at) <= datetime('now')
                )
            """
            ,
            uid_params,
        ).fetchone()
    return int(row[0] if row else 0)


def _looks_like_email(value: Any) -> bool:
    email = str(value or "").strip()
    if not email or " " in email or "@" not in email:
        return False
    local, _, domain = email.partition("@")
    return bool(local and domain and "." in domain and not domain.startswith(".") and not domain.endswith("."))


def _to_export_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return default


def _normalize_export_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _collect_lead_export_rows(
    db_path: Path,
    *,
    kind: str,
    user_id: Optional[str] = None,
    min_score: float = HIGH_AI_SCORE_THRESHOLD,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    normalized_kind = _normalize_export_text(kind).lower()
    if normalized_kind not in {"target", "ai_mailer"}:
        raise HTTPException(status_code=400, detail="Unsupported export kind.")

    columns = (
        "business_name,website_url,phone_number,email,rating,review_count,address,"
        "search_keyword,main_shortcoming,ai_score,status,enrichment_status,client_tier,"
        "scraped_at,enriched_at"
    )

    source_rows: Optional[list[dict[str, Any]]] = None
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                filters = {"user_id": user_id} if user_id else None
                source_rows = supabase_select_rows(
                    client,
                    "leads",
                    columns=columns,
                    filters=filters,
                    order_by="ai_score",
                    desc=True,
                    limit=5000,
                )
            except Exception as exc:
                logging.warning("Supabase lead export fallback to SQLite: %s", exc)

    if source_rows is None:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT {columns}
                FROM leads
                WHERE (? IS NULL OR user_id = ?)
                ORDER BY COALESCE(ai_score, 0) DESC, business_name ASC
                """,
                (user_id, user_id),
            ).fetchall()
        source_rows = [dict(row) for row in rows]

    export_rows: list[dict[str, Any]] = []

    if normalized_kind == "target":
        fieldnames = [
            "business_name",
            "ai_score",
            "email",
            "phone_number",
            "website_url",
            "rating",
            "review_count",
            "main_shortcoming",
            "address",
            "search_keyword",
            "status",
            "enriched_at",
            "scraped_at",
        ]
        for row in source_rows:
            score_value = _to_export_float(row.get("ai_score"))
            status_value = _normalize_export_text(row.get("status")).lower()
            if score_value < float(min_score):
                continue
            if status_value == "blacklisted":
                continue
            export_rows.append(
                {
                    "business_name": _normalize_export_text(row.get("business_name")),
                    "ai_score": score_value,
                    "email": _normalize_export_text(row.get("email")),
                    "phone_number": _normalize_export_text(row.get("phone_number")),
                    "website_url": _normalize_export_text(row.get("website_url")),
                    "rating": row.get("rating") or "",
                    "review_count": row.get("review_count") or "",
                    "main_shortcoming": _normalize_export_text(row.get("main_shortcoming")),
                    "address": _normalize_export_text(row.get("address")),
                    "search_keyword": _normalize_export_text(row.get("search_keyword")),
                    "status": _normalize_export_text(row.get("status")),
                    "enriched_at": _normalize_export_text(row.get("enriched_at")),
                    "scraped_at": _normalize_export_text(row.get("scraped_at")),
                }
            )
        return "target_leads.csv", fieldnames, export_rows

    fieldnames = [
        "business_name",
        "email",
        "main_shortcoming",
        "ai_score",
        "client_tier",
        "phone_number",
        "website_url",
        "address",
        "search_keyword",
    ]
    for row in source_rows:
        email_value = _normalize_export_text(row.get("email"))
        status_value = _normalize_export_text(row.get("status")).lower()
        enrichment_value = _normalize_export_text(row.get("enrichment_status")).lower()
        if status_value not in {"enriched", "queued_mail"}:
            continue
        if enrichment_value not in {"", "completed"}:
            continue
        if not _looks_like_email(email_value):
            continue
        export_rows.append(
            {
                "business_name": _normalize_export_text(row.get("business_name")),
                "email": email_value,
                "main_shortcoming": _normalize_export_text(row.get("main_shortcoming")) or "No clear shortcoming identified",
                "ai_score": _to_export_float(row.get("ai_score")),
                "client_tier": _normalize_export_text(row.get("client_tier")),
                "phone_number": _normalize_export_text(row.get("phone_number")),
                "website_url": _normalize_export_text(row.get("website_url")),
                "address": _normalize_export_text(row.get("address")),
                "search_keyword": _normalize_export_text(row.get("search_keyword")),
            }
        )
    return "ai_mailer_ready.csv", fieldnames, export_rows


def _render_csv_download(filename: str, fieldnames: list[str], rows: list[dict[str, Any]]) -> Response:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in fieldnames})

    csv_text = "\ufeff" + buffer.getvalue()
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store",
        "X-Exported-Count": str(len(rows)),
        "Access-Control-Expose-Headers": "Content-Disposition, X-Exported-Count",
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


def create_client_folder(db_path: Path, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_client_success_tables(db_path)
    now_iso = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            INSERT INTO client_folders (user_id, name, color, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(user_id or "legacy"),
                str(payload.get("name") or "").strip(),
                str(payload.get("color") or "cyan").strip() or "cyan",
                str(payload.get("notes") or "").strip() or None,
                now_iso,
                now_iso,
            ),
        )
        row = conn.execute("SELECT * FROM client_folders WHERE id = ? LIMIT 1", (cursor.lastrowid,)).fetchone()
        conn.commit()
    return dict(row) if row else {}


def _summarize_client_folders(folder_rows: list[dict[str, Any]], lead_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[int, dict[str, Any]] = {
        int(folder["id"]): {**dict(folder), "lead_count": 0, "contacted_count": 0, "replied_count": 0, "won_paid_count": 0, "top_leads": []}
        for folder in folder_rows
        if folder.get("id") is not None
    }
    for lead in lead_rows:
        folder_id = lead.get("client_folder_id")
        if folder_id is None:
            continue
        folder = grouped.get(int(folder_id))
        if folder is None:
            continue
        folder["lead_count"] += 1
        stage = _derive_pipeline_stage(
            status=lead.get("status"),
            sent_at=lead.get("sent_at"),
            last_contacted_at=lead.get("last_contacted_at"),
            reply_detected_at=lead.get("reply_detected_at"),
            paid_at=lead.get("paid_at"),
            pipeline_stage=lead.get("pipeline_stage"),
        )
        if stage == "Contacted":
            folder["contacted_count"] += 1
        elif stage == "Replied":
            folder["replied_count"] += 1
        elif stage == "Won (Paid)":
            folder["won_paid_count"] += 1
        if len(folder["top_leads"]) < 4:
            folder["top_leads"].append(
                {
                    "id": lead.get("id"),
                    "business_name": lead.get("business_name"),
                    "status": lead.get("status"),
                    "pipeline_stage": stage,
                    "ai_score": lead.get("ai_score"),
                }
            )
    return list(grouped.values())


def list_client_folders(db_path: Path, user_id: str) -> list[dict[str, Any]]:
    ensure_client_success_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        folders = [dict(row) for row in conn.execute(
            """
            SELECT id, user_id, name, color, notes, created_at, updated_at
            FROM client_folders
            WHERE user_id = ?
            ORDER BY datetime(updated_at) DESC, id DESC
            """,
            (user_id,),
        ).fetchall()]
        leads = [dict(row) for row in conn.execute(
            """
            SELECT id, business_name, status, sent_at, last_contacted_at, reply_detected_at, paid_at, pipeline_stage, client_folder_id, ai_score
            FROM leads
            WHERE user_id = ?
            ORDER BY COALESCE(ai_score, 0) DESC, id DESC
            """,
            (user_id,),
        ).fetchall()]

    return _summarize_client_folders(folders, leads)


def _normalize_saved_segment_row(row: sqlite3.Row | dict | None) -> dict[str, Any]:
    if row is None:
        return {}
    item = dict(row)
    item["filters"] = deserialize_json(item.get("filters_json")) or {}
    return item


def create_saved_segment(db_path: Path, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_client_success_tables(db_path)
    now_iso = utc_now_iso()
    name = str(payload.get("name") or "").strip()
    filters_json = json.dumps(payload.get("filters") or {}, ensure_ascii=False)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        existing = conn.execute(
            "SELECT id FROM saved_segments WHERE user_id = ? AND LOWER(name) = LOWER(?) LIMIT 1",
            (str(user_id or "legacy"), name),
        ).fetchone()
        if existing is not None:
            conn.execute(
                "UPDATE saved_segments SET filters_json = ?, updated_at = ? WHERE id = ?",
                (filters_json, now_iso, int(existing["id"])),
            )
            row = conn.execute("SELECT * FROM saved_segments WHERE id = ? LIMIT 1", (int(existing["id"]),)).fetchone()
        else:
            cursor = conn.execute(
                """
                INSERT INTO saved_segments (user_id, name, filters_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(user_id or "legacy"), name, filters_json, now_iso, now_iso),
            )
            row = conn.execute("SELECT * FROM saved_segments WHERE id = ? LIMIT 1", (cursor.lastrowid,)).fetchone()
        conn.commit()
    return _normalize_saved_segment_row(row)


def list_saved_segments(db_path: Path, user_id: str) -> list[dict[str, Any]]:
    ensure_client_success_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, user_id, name, filters_json, created_at, updated_at
            FROM saved_segments
            WHERE user_id = ?
            ORDER BY datetime(updated_at) DESC, id DESC
            """,
            (user_id,),
        ).fetchall()
    return [_normalize_saved_segment_row(row) for row in rows]


def delete_saved_segment(db_path: Path, user_id: str, segment_id: int) -> dict[str, Any]:
    ensure_client_success_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("DELETE FROM saved_segments WHERE id = ? AND user_id = ?", (int(segment_id), user_id))
        conn.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Saved segment not found")
    return {"status": "deleted", "id": int(segment_id)}


def assign_lead_to_client_folder(db_path: Path, user_id: str, lead_id: int, client_folder_id: Optional[int]) -> dict[str, Any]:
    ensure_client_success_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lead_row = conn.execute(
            "SELECT id, user_id, business_name FROM leads WHERE id = ? LIMIT 1",
            (lead_id,),
        ).fetchone()
        if lead_row is None:
            raise HTTPException(status_code=404, detail="Lead not found")
        if str(lead_row["user_id"] or "") != str(user_id or ""):
            raise HTTPException(status_code=403, detail="Forbidden")

        folder_name = None
        normalized_folder_id = int(client_folder_id) if client_folder_id not in (None, 0, "0", "") else None
        if normalized_folder_id is not None:
            folder_row = conn.execute(
                "SELECT id, name FROM client_folders WHERE id = ? AND user_id = ? LIMIT 1",
                (normalized_folder_id, user_id),
            ).fetchone()
            if folder_row is None:
                raise HTTPException(status_code=404, detail="Client folder not found")
            folder_name = str(folder_row["name"] or "").strip() or None

        conn.execute(
            "UPDATE leads SET client_folder_id = ?, status_updated_at = COALESCE(status_updated_at, ?) WHERE id = ?",
            (normalized_folder_id, utc_now_iso(), lead_id),
        )
        conn.commit()

    _invalidate_leads_cache()
    return {
        "status": "updated",
        "lead_id": lead_id,
        "client_folder_id": normalized_folder_id,
        "client_folder_name": folder_name,
        "business_name": str(lead_row["business_name"] or "").strip(),
    }


def _get_export_webhook_url(config_path: Path, target: str) -> str:
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
    except Exception:
        cfg = {}

    normalized_target = str(target or "").strip().lower().replace(" ", "_")
    if normalized_target == "hubspot":
        url = str(cfg.get("hubspot_webhook_url", "") or "").strip()
        if not url:
            raise HTTPException(status_code=503, detail="HubSpot webhook is not configured. Add it in Platform Settings.")
        return url

    if normalized_target in {"google_sheets", "sheets", "google-sheet", "google_sheet"}:
        url = str(cfg.get("google_sheets_webhook_url", "") or "").strip()
        if not url:
            raise HTTPException(status_code=503, detail="Google Sheets webhook is not configured. Add it in Platform Settings.")
        return url

    raise HTTPException(status_code=400, detail="Unsupported webhook target.")


def deliver_export_webhook(url: str, payload: dict) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read().decode("utf-8", errors="ignore")
        parsed = None
        if raw:
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = raw[:1000]
        return {
            "ok": True,
            "status": int(getattr(response, "status", 200) or 200),
            "response": parsed,
        }


def _escape_pdf_text(value: Any) -> str:
    return str(value or "").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_simple_pdf(title: str, lines: list[str]) -> bytes:
    stream_lines = [
        "BT",
        "/F1 18 Tf",
        "50 770 Td",
        f"({_escape_pdf_text(title)}) Tj",
        "/F1 11 Tf",
    ]
    for line in lines[:42]:
        stream_lines.append("0 -16 Td")
        stream_lines.append(f"({_escape_pdf_text(line[:120])}) Tj")
    stream_lines.append("ET")
    content_stream = "\n".join(stream_lines).encode("latin-1", errors="replace")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        f"<< /Length {len(content_stream)} >>\nstream\n".encode("latin-1") + content_stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{idx} 0 obj\n".encode("latin-1"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    startxref = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("latin-1"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("latin-1"))
    pdf.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{startxref}\n%%EOF".encode("latin-1")
    )
    return bytes(pdf)


def _load_user_email_for_reports(db_path: Path, user_id: str) -> str:
    ensure_users_table(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT email FROM users WHERE id = ? LIMIT 1", (user_id,)).fetchone()
    return str(row[0] or "").strip() if row else ""


def _build_report_pdf(title: str, summary: dict[str, Any], *, period_key: str) -> bytes:
    pipeline = summary.get("pipeline") if isinstance(summary.get("pipeline"), dict) else {}
    if period_key == "weekly":
        lines = [
            f"Leads found this week: {summary.get('found_this_week', 0)}",
            f"Leads contacted this week: {summary.get('contacted_this_week', 0)}",
            f"Replies this week: {summary.get('replied_this_week', 0)}",
            f"Won (Paid) this week: {summary.get('won_this_week', 0)}",
        ]
    else:
        lines = [
            f"Leads found this month: {summary.get('found_this_month', 0)}",
            f"Leads contacted this month: {summary.get('contacted_this_month', 0)}",
            f"Replies this month: {summary.get('replied_this_month', 0)}",
            f"Won (Paid) this month: {summary.get('won_this_month', 0)}",
        ]
    lines.extend(
        [
            f"Open rate: {float(summary.get('open_rate') or 0):.1f}%",
            f"Reply rate: {float(summary.get('reply_rate') or 0):.1f}%",
            "",
            "Pipeline overview:",
            f"- Scraped: {int(pipeline.get('scraped') or 0)}",
            f"- Contacted: {int(pipeline.get('contacted') or 0)}",
            f"- Replied: {int(pipeline.get('replied') or 0)}",
            f"- Won (Paid): {int(pipeline.get('won_paid') or 0)}",
            "",
            "Client folders:",
        ]
    )
    for folder in (summary.get("client_folders") or [])[:8]:
        lines.append(
            f"- {folder.get('name')}: {int(folder.get('lead_count') or 0)} leads, {int(folder.get('won_paid_count') or 0)} won"
        )
    return _build_simple_pdf(title, lines)


def _build_report_email_content(summary: dict[str, Any], *, period_key: str) -> tuple[str, str, str]:
    is_weekly = period_key == "weekly"
    label = str(summary.get("period_label") or summary.get("month_label") or "this period").strip() or "this period"
    found = int(summary.get("found_this_week" if is_weekly else "found_this_month") or 0)
    contacted = int(summary.get("contacted_this_week" if is_weekly else "contacted_this_month") or 0)
    replied = int(summary.get("replied_this_week" if is_weekly else "replied_this_month") or 0)
    won = int(summary.get("won_this_week" if is_weekly else "won_this_month") or 0)
    open_rate = float(summary.get("open_rate") or 0)
    reply_rate = float(summary.get("reply_rate") or 0)
    pipeline = summary.get("pipeline") if isinstance(summary.get("pipeline"), dict) else {}
    folder_count = int(summary.get("folder_count") or len(summary.get("client_folders") or []))
    subject = f"Sniped {'Weekly' if is_weekly else 'Monthly'} Summary — {label}"
    intro = "Here is your polished weekly lead snapshot." if is_weekly else "Here is your polished monthly lead summary."

    text_body = (
        f"Sniped {'weekly' if is_weekly else 'monthly'} summary for {label}\n\n"
        f"Leads found: {found}\n"
        f"Leads contacted: {contacted}\n"
        f"Replies: {replied}\n"
        f"Won (Paid): {won}\n"
        f"Open rate: {open_rate:.1f}%\n"
        f"Reply rate: {reply_rate:.1f}%\n"
        f"Client folders: {folder_count}\n"
    )

    cards = [
        ("Leads Found", found, "#60a5fa"),
        ("Contacted", contacted, "#22d3ee"),
        ("Replies", replied, "#34d399"),
        ("Won (Paid)", won, "#fbbf24"),
    ]
    card_html = "".join(
        f"""
        <div style=\"flex:1 1 180px;min-width:160px;background:#111827;border:1px solid rgba(148,163,184,.18);border-radius:16px;padding:16px;\">
          <div style=\"font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;\">{html_escape(title)}</div>
          <div style=\"margin-top:8px;font-size:28px;font-weight:700;color:{accent};\">{value}</div>
        </div>
        """
        for title, value, accent in cards
    )
    folder_items = "".join(
        f"<li style=\"margin:0 0 8px;\"><strong>{html_escape(str(folder.get('name') or 'Client'))}</strong> · {int(folder.get('lead_count') or 0)} leads · {int(folder.get('won_paid_count') or 0)} won</li>"
        for folder in (summary.get("client_folders") or [])[:4]
    ) or "<li style=\"margin:0;\">No client folders yet.</li>"

    html_body = f"""
    <div style=\"margin:0;padding:24px;background:#020617;font-family:Arial,Helvetica,sans-serif;color:#e2e8f0;\">
      <div style=\"max-width:720px;margin:0 auto;background:linear-gradient(180deg,#0f172a 0%,#111827 100%);border:1px solid rgba(148,163,184,.16);border-radius:24px;overflow:hidden;box-shadow:0 24px 60px rgba(15,23,42,.45);\">
        <div style=\"padding:28px 28px 18px;background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 100%);\">
          <div style=\"display:inline-block;padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.12);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:#bfdbfe;\">Sniped {html_escape('Weekly' if is_weekly else 'Monthly')} Report</div>
          <h1 style=\"margin:14px 0 6px;font-size:28px;line-height:1.2;color:#ffffff;\">{html_escape(label)}</h1>
          <p style=\"margin:0;font-size:14px;color:#dbeafe;\">{html_escape(intro)} Delivered to your account email automatically.</p>
        </div>
        <div style=\"padding:24px 28px;\">
          <div style=\"display:flex;flex-wrap:wrap;gap:12px;margin-bottom:18px;\">{card_html}</div>
          <div style=\"background:#0b1220;border:1px solid rgba(148,163,184,.16);border-radius:18px;padding:18px;margin-bottom:16px;\">
            <div style=\"font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:#93c5fd;margin-bottom:10px;\">Performance</div>
            <p style=\"margin:0 0 6px;font-size:14px;color:#e5e7eb;\"><strong>Open rate:</strong> {open_rate:.1f}%</p>
            <p style=\"margin:0 0 6px;font-size:14px;color:#e5e7eb;\"><strong>Reply rate:</strong> {reply_rate:.1f}%</p>
            <p style=\"margin:0;font-size:14px;color:#e5e7eb;\"><strong>Pipeline:</strong> Scraped {int(pipeline.get('scraped') or 0)} · Contacted {int(pipeline.get('contacted') or 0)} · Replied {int(pipeline.get('replied') or 0)} · Won {int(pipeline.get('won_paid') or 0)}</p>
          </div>
          <div style=\"background:#0b1220;border:1px solid rgba(148,163,184,.16);border-radius:18px;padding:18px;\">
            <div style=\"font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:#93c5fd;margin-bottom:10px;\">Client folders</div>
            <ul style=\"padding-left:18px;margin:0;color:#cbd5e1;font-size:14px;\">{folder_items}</ul>
          </div>
        </div>
      </div>
    </div>
    """
    return subject, text_body, html_body


def send_report_email(
    account: dict,
    recipient: str,
    subject: str,
    text_body: str,
    html_body: str,
    *,
    attachment_bytes: Optional[bytes] = None,
    attachment_filename: Optional[str] = None,
) -> None:
    host = str(account.get("host", "") or "").strip()
    port = int(account.get("port", 587) or 587)
    email = str(account.get("email", "") or "").strip()
    password = str(account.get("password", "") or "").strip()
    use_tls = bool(account.get("use_tls", True))
    use_ssl = bool(account.get("use_ssl", False))
    if not host or not email or not password:
        raise ValueError("Missing SMTP host/email/password")

    message = EmailMessage()
    message["To"] = recipient
    message["From"] = format_from_header_dict(account)
    message["Subject"] = subject
    message.set_content(text_body)
    if html_body:
        message.add_alternative(html_body, subtype="html")
    if attachment_bytes and attachment_filename:
        message.add_attachment(attachment_bytes, maintype="application", subtype="pdf", filename=attachment_filename)

    if use_ssl or port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=20) as smtp:
            smtp.login(email, password)
            smtp.send_message(message)
        return

    with smtplib.SMTP(host, port, timeout=20) as smtp:
        smtp.ehlo()
        if use_tls or port == 587:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(email, password)
        smtp.send_message(message)


def send_weekly_report_email(account: dict, recipient: str, summary: dict[str, Any], pdf_bytes: bytes | None = None) -> None:
    subject, text_body, html_body = _build_report_email_content(summary, period_key="weekly")
    send_report_email(account, recipient, subject, text_body, html_body, attachment_bytes=pdf_bytes, attachment_filename="sniped-weekly-summary.pdf" if pdf_bytes else None)


def send_monthly_report_email(account: dict, recipient: str, summary: dict[str, Any], pdf_bytes: bytes) -> None:
    subject, text_body, html_body = _build_report_email_content(summary, period_key="monthly")
    send_report_email(account, recipient, subject, text_body, html_body, attachment_bytes=pdf_bytes, attachment_filename="sniped-monthly-report.pdf")


def build_weekly_report_summary(db_path: Path, user_id: str) -> dict[str, Any]:
    stats = get_dashboard_stats(db_path, user_id=user_id)
    folders = list_client_folders(db_path, user_id)
    return {
        "generated_at": utc_now_iso(),
        "period_key": "weekly",
        "period_label": f"Week ending {datetime.now(timezone.utc).strftime('%d %b %Y')}",
        "found_this_week": int(stats.get("found_this_week") or 0),
        "contacted_this_week": int(stats.get("contacted_this_week") or 0),
        "replied_this_week": int(stats.get("replied_this_week") or 0),
        "won_this_week": int(stats.get("won_this_week") or 0),
        "open_rate": float(stats.get("open_rate") or 0),
        "reply_rate": float(stats.get("reply_rate") or 0),
        "pipeline": dict(stats.get("pipeline") or {}),
        "folder_count": len(folders),
        "client_folders": folders,
    }


def build_monthly_report_summary(db_path: Path, user_id: str) -> dict[str, Any]:
    stats = get_dashboard_stats(db_path, user_id=user_id)
    folders = list_client_folders(db_path, user_id)
    month_label = datetime.now(timezone.utc).strftime("%B %Y")
    return {
        "generated_at": utc_now_iso(),
        "period_key": "monthly",
        "period_label": month_label,
        "month_label": month_label,
        "found_this_month": int(stats.get("found_this_month") or 0),
        "contacted_this_month": int(stats.get("contacted_this_month") or 0),
        "replied_this_month": int(stats.get("replied_this_month") or 0),
        "won_this_month": int(stats.get("won_this_month") or 0),
        "open_rate": float(stats.get("open_rate") or 0),
        "reply_rate": float(stats.get("reply_rate") or 0),
        "pipeline": dict(stats.get("pipeline") or {}),
        "folder_count": len(folders),
        "client_folders": folders,
    }


def build_client_dashboard_snapshot(db_path: Path, user_id: str) -> dict[str, Any]:
    stats = get_dashboard_stats(db_path, user_id=user_id)
    folders = list_client_folders(db_path, user_id)
    with sqlite3.connect(db_path) as conn:
        unassigned_count = int(conn.execute(
            "SELECT COUNT(*) FROM leads WHERE user_id = ? AND client_folder_id IS NULL",
            (user_id,),
        ).fetchone()[0] or 0)
    return {
        "folder_count": len(folders),
        "unassigned_count": unassigned_count,
        "pipeline": dict(stats.get("pipeline") or {}),
        "found_this_month": int(stats.get("found_this_month") or 0),
        "contacted_this_month": int(stats.get("contacted_this_month") or 0),
        "won_this_month": int(stats.get("won_this_month") or 0),
        "folders": folders,
    }


def get_task_executor(task_type: str) -> Callable[[FastAPI, dict], None]:
    if task_type == "scrape":
        return execute_scrape_task
    if task_type == "enrich":
        return execute_enrich_task
    if task_type == "mailer":
        return execute_mailer_task
    raise ValueError(f"Unsupported task type: {task_type}")


def launch_detached_task(executor: Callable[[FastAPI, dict], None], app: FastAPI, payload_data: dict) -> None:
    task_id = payload_data.get("task_id")

    def _run() -> None:
        registry: dict[int, Thread] = getattr(app.state, "active_task_threads", {})
        try:
            if isinstance(task_id, int):
                registry[task_id] = thread
            executor(app, payload_data)
        finally:
            if isinstance(task_id, int):
                registry.pop(task_id, None)

    thread = Thread(target=_run, daemon=True)
    thread.start()


def _task_reference_time(task: dict) -> Optional[datetime]:
    return parse_iso_datetime(task.get("started_at")) or parse_iso_datetime(task.get("created_at"))


def _is_task_thread_alive(app: FastAPI, task_id: Optional[int]) -> bool:
    if task_id is None:
        return False
    registry: dict[int, Thread] = getattr(app.state, "active_task_threads", {})
    thread = registry.get(int(task_id))
    if thread is None:
        return False
    if thread.is_alive():
        return True
    registry.pop(int(task_id), None)
    return False


def reconcile_orphaned_active_tasks(app: FastAPI, db_path: Path) -> None:
    for task_type in TASK_TYPES:
        latest = fetch_latest_task(db_path, task_type)
        if not latest.get("running"):
            continue

        task_id = latest.get("id")
        if task_id is None or _is_task_thread_alive(app, task_id):
            continue

        reference_time = _task_reference_time(latest)
        if reference_time is not None:
            age_seconds = (datetime.now(timezone.utc) - reference_time).total_seconds()
            if age_seconds <= ORPHAN_TASK_GRACE_SECONDS:
                continue

        result_payload = latest.get("result") if isinstance(latest.get("result"), dict) else None
        finish_task_record(
            db_path,
            int(task_id),
            status="stopped",
            result_payload=result_payload,
            error="Task auto-reset: worker not active in current process.",
        )
        logging.warning("Auto-reset orphaned task: id=%s type=%s", task_id, task_type)


def enqueue_task(
    app: FastAPI,
    background_tasks: Optional[BackgroundTasks],
    db_path: Path,
    user_id: str,
    task_type: str,
    request_payload: dict,
    source: str = "api",
) -> dict:
    reconcile_orphaned_active_tasks(app, db_path)
    task_lock: Lock = app.state.task_lock
    with task_lock:
        if task_is_active(db_path, task_type, user_id=user_id):
            running_task = fetch_latest_task(db_path, task_type, user_id=user_id)
            return {"status": "running", "task_id": running_task.get("id")}

        task_id = create_task_record(db_path, user_id, task_type, "queued", request_payload, source=source)

    payload_data = dict(request_payload)
    payload_data["task_id"] = task_id
    payload_data["task_type"] = task_type
    payload_data["user_id"] = user_id

    executor = get_task_executor(task_type)
    if background_tasks is not None:
        # FastAPI-managed background kickoff (returns response immediately).
        background_tasks.add_task(launch_detached_task, executor, app, payload_data)
    else:
        launch_detached_task(executor, app, payload_data)

    return {"status": "started", "task_id": task_id, "job_status": "processing"}


def get_dashboard_stats(db_path: Path, user_id: Optional[str] = None) -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return get_dashboard_stats_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)

    ensure_system_tables(db_path)
    uid_clause = "AND user_id = ?" if user_id else ""
    uid_params = [user_id] if user_id else []
    now_utc = datetime.now(timezone.utc)
    month_prefix = now_utc.strftime("%Y-%m")
    week_cutoff = (now_utc - timedelta(days=7)).isoformat()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total_leads = int(conn.execute(
            f"SELECT COUNT(*) FROM leads WHERE 1=1 {uid_clause}", uid_params
        ).fetchone()[0] or 0)
        emails_sent = int(
            conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE sent_at IS NOT NULL {uid_clause}",
                uid_params,
            ).fetchone()[0] or 0
        )
        opened_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE COALESCE(open_count, 0) > 0 {uid_clause}",
                uid_params,
            ).fetchone()[0] or 0
        )
        opens_total = int(
            conn.execute(
                f"SELECT COALESCE(SUM(open_count), 0) FROM leads WHERE 1=1 {uid_clause}",
                uid_params,
            ).fetchone()[0] or 0
        )
        replies_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status, '')) IN ('replied', 'interested', 'meeting set', 'zoom scheduled') {uid_clause}",
                uid_params,
            ).fetchone()[0] or 0
        )
        paid_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status, '')) = 'paid' {uid_clause}",
                uid_params,
            ).fetchone()[0] or 0
        )
        found_this_month = int(
            conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE COALESCE(scraped_at, '') LIKE ? {uid_clause}",
                [f"{month_prefix}%", *uid_params],
            ).fetchone()[0] or 0
        )
        contacted_this_month = int(
            conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE (COALESCE(sent_at, '') LIKE ? OR COALESCE(last_contacted_at, '') LIKE ?) {uid_clause}",
                [f"{month_prefix}%", f"{month_prefix}%", *uid_params],
            ).fetchone()[0] or 0
        )
        replied_this_month = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM leads
                WHERE (
                    COALESCE(reply_detected_at, '') LIKE ?
                    OR (
                        COALESCE(status_updated_at, '') LIKE ?
                        AND LOWER(COALESCE(status, '')) IN ('replied', 'interested', 'meeting set', 'zoom scheduled')
                    )
                ) {uid_clause}
                """,
                [f"{month_prefix}%", f"{month_prefix}%", *uid_params],
            ).fetchone()[0] or 0
        )
        won_this_month = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM leads
                WHERE (
                    COALESCE(paid_at, '') LIKE ?
                    OR (
                        COALESCE(status_updated_at, '') LIKE ?
                        AND LOWER(COALESCE(status, '')) = 'paid'
                    )
                ) {uid_clause}
                """,
                [f"{month_prefix}%", f"{month_prefix}%", *uid_params],
            ).fetchone()[0] or 0
        )
        found_this_week = int(
            conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE COALESCE(scraped_at, '') >= ? {uid_clause}",
                [week_cutoff, *uid_params],
            ).fetchone()[0] or 0
        )
        contacted_this_week = int(
            conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE (COALESCE(sent_at, '') >= ? OR COALESCE(last_contacted_at, '') >= ?) {uid_clause}",
                [week_cutoff, week_cutoff, *uid_params],
            ).fetchone()[0] or 0
        )
        replied_this_week = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM leads
                WHERE (
                    COALESCE(reply_detected_at, '') >= ?
                    OR (
                        COALESCE(status_updated_at, '') >= ?
                        AND LOWER(COALESCE(status, '')) IN ('replied', 'interested', 'meeting set', 'zoom scheduled')
                    )
                ) {uid_clause}
                """,
                [week_cutoff, week_cutoff, *uid_params],
            ).fetchone()[0] or 0
        )
        won_this_week = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM leads
                WHERE (
                    COALESCE(paid_at, '') >= ?
                    OR (
                        COALESCE(status_updated_at, '') >= ?
                        AND LOWER(COALESCE(status, '')) = 'paid'
                    )
                ) {uid_clause}
                """,
                [week_cutoff, week_cutoff, *uid_params],
            ).fetchone()[0] or 0
        )

        paid_rows = conn.execute(
            f"""
            SELECT
                LOWER(COALESCE(client_tier, 'standard')) AS tier,
                COALESCE(is_ads_client, 0) AS is_ads_client,
                COALESCE(is_website_client, 0) AS is_website_client
            FROM leads
            WHERE LOWER(COALESCE(status, '')) = 'paid' {uid_clause}
            """,
            uid_params,
        ).fetchall()
        pipeline_rows = conn.execute(
            f"""
            SELECT status, sent_at, last_contacted_at, reply_detected_at, paid_at, pipeline_stage
            FROM leads
            WHERE 1=1 {uid_clause}
            """,
            uid_params,
        ).fetchall()
        client_folder_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM client_folders WHERE (? IS NULL OR user_id = ?)",
                (user_id, user_id),
            ).fetchone()[0] or 0
        )

    setup_revenue = 0.0
    monthly_recurring_revenue = 0
    paid_by_tier: dict[str, int] = {}
    website_clients = 0
    ads_clients = 0
    ads_and_website_clients = 0

    for tier, is_ads_client, is_website_client in paid_rows:
        tier_key = str(tier or "standard").strip().lower() or "standard"
        paid_by_tier[tier_key] = paid_by_tier.get(tier_key, 0) + 1
        is_ads = int(is_ads_client or 0)
        is_web = int(is_website_client or 0)

        if is_web:
            setup_revenue += SETUP_FEE_WEBSITE
            website_clients += 1

        if is_web and is_ads:
            monthly_recurring_revenue += MRR_ADS_AND_WEBSITE
            ads_and_website_clients += 1
        elif is_ads:
            monthly_recurring_revenue += MRR_ADS_ONLY
            ads_clients += 1
        elif is_web:
            monthly_recurring_revenue += MRR_WEBSITE_ONLY
        else:
            monthly_recurring_revenue += MRR_BY_TIER.get(tier_key, MRR_BY_TIER["standard"])

    # Add manual revenue entries from revenue_log
    ensure_revenue_log_table(db_path)
    with sqlite3.connect(db_path) as conn:
        rev_total_row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM revenue_log WHERE (? IS NULL OR user_id = ?)",
            (user_id, user_id),
        ).fetchone()
        rev_mrr_row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM revenue_log WHERE is_recurring = 1 AND (? IS NULL OR user_id = ?)",
            (user_id, user_id),
        ).fetchone()
    setup_revenue += float(rev_total_row[0] if rev_total_row else 0)
    monthly_recurring_revenue += float(rev_mrr_row[0] if rev_mrr_row else 0)

    mrr_progress_pct = 0
    if MRR_GOAL > 0:
        mrr_progress_pct = min(100, round((monthly_recurring_revenue / MRR_GOAL) * 100))

    milestone_progress_pct = min(100, round((setup_revenue / SETUP_MILESTONE) * 100))

    queued_mail_count = get_queued_mail_count(db_path, user_id=user_id)
    next_drip_at = get_runtime_value(db_path, "next_drip_at") if AUTO_DRIP_DISPATCH_ENABLED else None
    reply_rate = round((replies_count / emails_sent) * 100, 1) if emails_sent > 0 else 0.0
    open_rate = round((opened_count / emails_sent) * 100, 1) if emails_sent > 0 else 0.0

    pipeline = {"scraped": 0, "contacted": 0, "replied": 0, "won_paid": 0}
    for row in pipeline_rows:
        stage = _derive_pipeline_stage(
            status=row["status"],
            sent_at=row["sent_at"],
            last_contacted_at=row["last_contacted_at"],
            reply_detected_at=row["reply_detected_at"],
            paid_at=row["paid_at"],
            pipeline_stage=row["pipeline_stage"],
        )
        if stage == "Won (Paid)":
            pipeline["won_paid"] += 1
        elif stage == "Replied":
            pipeline["replied"] += 1
        elif stage == "Contacted":
            pipeline["contacted"] += 1
        else:
            pipeline["scraped"] += 1

    return {
        "total_leads": total_leads,
        "emails_sent": emails_sent,
        "opened_count": opened_count,
        "opens_total": opens_total,
        "open_rate": open_rate,
        "replies_count": replies_count,
        "reply_rate": reply_rate,
        "paid_count": paid_count,
        "total_revenue": setup_revenue,
        "setup_revenue": setup_revenue,
        "monthly_recurring_revenue": monthly_recurring_revenue,
        "paid_by_tier": paid_by_tier,
        "mrr_goal": MRR_GOAL,
        "mrr_progress_pct": mrr_progress_pct,
        "potential_revenue": setup_revenue,
        "setup_milestone": SETUP_MILESTONE,
        "milestone_progress_pct": milestone_progress_pct,
        "website_clients": website_clients,
        "ads_clients": ads_clients,
        "ads_and_website_clients": ads_and_website_clients,
        "queued_mail_count": queued_mail_count,
        "next_drip_at": next_drip_at,
        "found_this_month": found_this_month,
        "contacted_this_month": contacted_this_month,
        "replied_this_month": replied_this_month,
        "won_this_month": won_this_month,
        "found_this_week": found_this_week,
        "contacted_this_week": contacted_this_week,
        "replied_this_week": replied_this_week,
        "won_this_week": won_this_week,
        "pipeline": pipeline,
        "client_folder_count": client_folder_count,
    }


def _load_user_credit_snapshot(user_id: str, db_path: Optional[Path] = None) -> dict:
    target_user_id = str(user_id or "").strip()
    if not target_user_id:
        raise HTTPException(status_code=401, detail="Missing authenticated user.")

    if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
        if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
            raise HTTPException(
                status_code=503,
                detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
            )
        sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if sb_client is None:
            raise HTTPException(status_code=503, detail="Supabase is not reachable.")
        try:
            response = (
                sb_client.table("users")
                .select("id,credits_balance,monthly_quota,monthly_limit,credits_limit,topup_credits_balance")
                .eq("id", target_user_id)
                .limit(1)
                .execute()
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Supabase credit lookup failed: {exc}")

        rows = list(getattr(response, "data", None) or [])
        if not rows:
            raise HTTPException(status_code=401, detail="Authenticated user does not exist.")

        row = rows[0]
        return {
            "user_id": target_user_id,
            "credits_balance": int(row.get("credits_balance") or 0),
            "credits_limit": max(1, int(row.get("monthly_quota") or row.get("monthly_limit") or row.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT)),
            "topup_credits_balance": max(0, int(row.get("topup_credits_balance") or 0)),
        }

    sqlite_db_path = db_path or DEFAULT_DB_PATH
    ensure_users_table(sqlite_db_path)
    with sqlite3.connect(sqlite_db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                COALESCE(credits_balance, 0) AS credits_balance,
                COALESCE(NULLIF(monthly_quota, 0), NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), 50) AS credits_limit,
                COALESCE(topup_credits_balance, 0) AS topup_credits_balance
            FROM users
            WHERE id = ?
            LIMIT 1
            """,
            (target_user_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=401, detail="Authenticated user does not exist.")

    return {
        "user_id": target_user_id,
        "credits_balance": int(row["credits_balance"] or 0),
        "credits_limit": max(1, int(row["credits_limit"] or DEFAULT_MONTHLY_CREDIT_LIMIT)),
        "topup_credits_balance": max(0, int(row["topup_credits_balance"] or 0)),
    }


def deduct_credits_on_success(user_id: str, credits_to_deduct: int = 1, db_path: Optional[Path] = None) -> dict:
    target_user_id = str(user_id or "").strip()
    if not target_user_id:
        raise HTTPException(status_code=401, detail="Missing authenticated user.")

    amount = max(0, int(credits_to_deduct or 0))
    if amount <= 0:
        snapshot = _load_user_credit_snapshot(target_user_id, db_path=db_path)
        return {
            "user_id": target_user_id,
            "credits_charged": 0,
            "credits_balance": int(snapshot.get("credits_balance") or 0),
            "credits_limit": int(snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
        }

    now_iso = utc_now_iso()

    if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
        snapshot = _load_user_credit_snapshot(target_user_id, db_path=db_path)
        current_balance = int(snapshot.get("credits_balance") or 0)
        credits_limit = int(snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT)
        topup_balance = max(0, int(snapshot.get("topup_credits_balance") or 0))
        if current_balance < amount:
            if current_balance <= 0:
                raise HTTPException(status_code=403, detail="Out of credits. Please upgrade.")
            raise HTTPException(status_code=403, detail="Insufficient credits. Please top up.")

        monthly_portion = max(0, current_balance - topup_balance)
        topup_consumed = max(0, amount - monthly_portion)
        next_topup_balance = max(0, topup_balance - topup_consumed)
        next_balance = current_balance - amount
        sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if sb_client is None:
            raise HTTPException(status_code=503, detail="Supabase is not reachable.")

        try:
            (
                sb_client.table("users")
                .update({
                    "credits_balance": next_balance,
                    "topup_credits_balance": next_topup_balance,
                    "updated_at": now_iso,
                })
                .eq("id", target_user_id)
                .eq("credits_balance", current_balance)
                .eq("topup_credits_balance", topup_balance)
                .execute()
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Supabase credit deduction failed: {exc}")

        verified = _load_user_credit_snapshot(target_user_id, db_path=db_path)
        verified_balance = int(verified.get("credits_balance") or 0)
        verified_limit = int(verified.get("credits_limit") or credits_limit)
        if verified_balance != next_balance:
            raise HTTPException(status_code=409, detail="Credit balance changed concurrently. Please retry.")

        return {
            "user_id": target_user_id,
            "credits_charged": amount,
            "credits_balance": verified_balance,
            "credits_limit": verified_limit,
        }

    sqlite_db_path = db_path or DEFAULT_DB_PATH
    ensure_users_table(sqlite_db_path)
    with sqlite3.connect(sqlite_db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT
                id,
                COALESCE(credits_balance, 0) AS credits_balance,
                COALESCE(NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), 50) AS credits_limit,
                COALESCE(topup_credits_balance, 0) AS topup_credits_balance
            FROM users
            WHERE id = ?
            LIMIT 1
            """,
            (target_user_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            raise HTTPException(status_code=401, detail="Authenticated user does not exist.")

        current_balance = int(row["credits_balance"] or 0)
        credits_limit = max(1, int(row["credits_limit"] or DEFAULT_MONTHLY_CREDIT_LIMIT))
        topup_balance = max(0, int(row["topup_credits_balance"] or 0))
        if current_balance < amount:
            conn.rollback()
            if current_balance <= 0:
                raise HTTPException(status_code=403, detail="Out of credits. Please upgrade.")
            raise HTTPException(status_code=403, detail="Insufficient credits. Please top up.")

        monthly_portion = max(0, current_balance - topup_balance)
        topup_consumed = max(0, amount - monthly_portion)
        next_topup_balance = max(0, topup_balance - topup_consumed)
        next_balance = current_balance - amount

        updated = conn.execute(
            """
            UPDATE users
            SET credits_balance = ?,
                topup_credits_balance = ?,
                updated_at = ?
            WHERE id = ? AND COALESCE(credits_balance, 0) >= ? AND COALESCE(topup_credits_balance, 0) = ?
            """,
            (next_balance, next_topup_balance, now_iso, target_user_id, amount, topup_balance),
        )
        if int(updated.rowcount or 0) != 1:
            conn.rollback()
            raise HTTPException(status_code=409, detail="Credit deduction conflict. Please retry.")

        next_row = conn.execute(
            "SELECT COALESCE(credits_balance, 0) AS credits_balance FROM users WHERE id = ?",
            (target_user_id,),
        ).fetchone()
        conn.commit()

    next_balance = int(next_row["credits_balance"] or 0) if next_row else max(0, current_balance - amount)
    return {
        "user_id": target_user_id,
        "credits_charged": amount,
        "credits_balance": next_balance,
        "credits_limit": credits_limit,
    }


AI_CREDIT_COSTS: dict[str, int] = {
    "recommend_niche": 0,
    "lead_search": 1,
    "enrich": 1,
    "mail_preview": 1,
    "cold_outreach": 1,
    "cold_email_opener": 1,
}


def get_ai_credit_cost(feature_key: str) -> int:
    key = str(feature_key or "").strip().lower()
    if key in AI_CREDIT_COSTS:
        return max(0, int(AI_CREDIT_COSTS.get(key) or 0))
    return 1


def has_enough_credits(user_id: str, required_credits: int = 1, db_path: Optional[Path] = None) -> dict:
    snapshot = _load_user_credit_snapshot(user_id, db_path=db_path)
    needed = max(0, int(required_credits or 0))
    balance = int(snapshot.get("credits_balance") or 0)
    if needed > 0 and balance < needed:
        if balance <= 0:
            raise HTTPException(status_code=403, detail="Out of credits. Please upgrade.")
        raise HTTPException(status_code=403, detail="Insufficient credits. Please top up.")
    return {
        "user_id": str(snapshot.get("user_id") or user_id),
        "credits_balance": balance,
        "credits_limit": int(snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
        "required_credits": needed,
    }


def reserve_ai_credits_or_raise(user_id: str, feature_key: str, db_path: Optional[Path] = None) -> dict:
    return has_enough_credits(
        user_id,
        required_credits=get_ai_credit_cost(feature_key),
        db_path=db_path,
    )


def charge_ai_credits_after_success(user_id: str, feature_key: str, db_path: Optional[Path] = None) -> dict:
    return deduct_credits_on_success(
        user_id,
        credits_to_deduct=get_ai_credit_cost(feature_key),
        db_path=db_path,
    )


def run_ai_with_credit_policy(
    user_id: str,
    feature_key: str,
    generate_fn: Callable[[], Any],
    db_path: Optional[Path] = None,
) -> tuple[Any, dict]:
    reserve_ai_credits_or_raise(user_id, feature_key=feature_key, db_path=db_path)
    result = generate_fn()
    billing = charge_ai_credits_after_success(user_id, feature_key=feature_key, db_path=db_path)
    return result, billing


def _niche_recommendation_cache_key(user_id: str, country_code: Optional[str] = None) -> str:
    normalized_country = normalize_country_value(country_code, None)
    return f"{str(user_id or '').strip()}::{normalized_country}"


def _get_cached_niche_recommendation(user_id: str, country_code: Optional[str] = None) -> Optional[dict[str, Any]]:
    cache_key = _niche_recommendation_cache_key(user_id, country_code)
    if not str(user_id or "").strip():
        return None

    entry = _NICHE_REC_CACHE.get(cache_key)
    if not isinstance(entry, dict):
        return None

    expires_at = float(entry.get("expires_at") or 0)
    if expires_at <= 0 or _time.monotonic() >= expires_at:
        _NICHE_REC_CACHE.pop(cache_key, None)
        return None

    data = entry.get("data")
    return dict(data) if isinstance(data, dict) else None


def _set_cached_niche_recommendation(user_id: str, data: dict[str, Any], ttl_seconds: int = 86400, country_code: Optional[str] = None) -> None:
    cache_key = _niche_recommendation_cache_key(user_id, country_code)
    if not str(user_id or "").strip() or not isinstance(data, dict):
        return

    _NICHE_REC_CACHE[cache_key] = {
        "data": dict(data),
        "expires_at": _time.monotonic() + max(60, int(ttl_seconds or 86400)),
    }


def _free_plan_niche_runtime_key(user_id: str, country_code: Optional[str] = None, now_dt: Optional[datetime] = None) -> str:
    _ = now_dt
    normalized_country = normalize_country_value(country_code, None).lower()
    return f"niche_recommendation:free:{normalized_country}:{str(user_id or '').strip()}"


def _paid_plan_niche_runtime_key(user_id: str, country_code: Optional[str] = None) -> str:
    normalized_country = normalize_country_value(country_code, None).lower()
    return f"niche_recommendation:paid:{normalized_country}:{str(user_id or '').strip()}"


def _niche_recommendation_refresh_window_days(is_free_plan: bool) -> float:
    return float(FREE_PLAN_NICHE_REFRESH_DAYS if is_free_plan else PAID_PLAN_NICHE_REFRESH_DAYS)


def _niche_recommendation_refresh_window_seconds(is_free_plan: bool) -> int:
    if is_free_plan:
        return max(60, int(FREE_PLAN_NICHE_REFRESH_DAYS * 86400))
    return max(60, int(PAID_PLAN_NICHE_REFRESH_HOURS * 3600))


def _load_runtime_niche_recommendation(
    user_id: str,
    *,
    is_free_plan: bool,
    country_code: Optional[str] = None,
    max_age_seconds: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    runtime_key = _free_plan_niche_runtime_key(user_id, country_code) if is_free_plan else _paid_plan_niche_runtime_key(user_id, country_code)
    stored_raw = get_runtime_value(DEFAULT_DB_PATH, runtime_key)
    if not stored_raw:
        return None

    try:
        parsed = json.loads(stored_raw)
    except Exception:
        return None

    if isinstance(parsed, dict) and isinstance(parsed.get("result"), dict):
        result = dict(parsed.get("result") or {})
        generated_at_raw = str(parsed.get("generated_at") or result.get("generated_at") or "").strip()
    elif isinstance(parsed, dict):
        result = dict(parsed)
        generated_at_raw = str(result.get("generated_at") or "").strip()
    else:
        return None

    if not result:
        return None

    if max_age_seconds is not None:
        generated_at = parse_iso_datetime(generated_at_raw)
        if generated_at is None:
            return None
        age_seconds = (datetime.now(timezone.utc) - generated_at).total_seconds()
        if age_seconds > max(60, int(max_age_seconds or 0)):
            return None

    return result


def _store_runtime_niche_recommendation(user_id: str, *, is_free_plan: bool, result: dict[str, Any], country_code: Optional[str] = None) -> None:
    runtime_key = _free_plan_niche_runtime_key(user_id, country_code) if is_free_plan else _paid_plan_niche_runtime_key(user_id, country_code)
    payload = {
        "result": dict(result),
        "generated_at": str(result.get("generated_at") or utc_now_iso()),
    }
    set_runtime_value(DEFAULT_DB_PATH, runtime_key, json.dumps(payload, ensure_ascii=False))


def _promote_alternate_niche_choice(result: dict[str, Any], previous_result: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if not isinstance(result, dict):
        return result

    previous_keyword = ""
    if isinstance(previous_result, dict):
        previous_keyword = str(
            previous_result.get("keyword")
            or (previous_result.get("top_pick") or {}).get("keyword")
            or ""
        ).strip().lower()

    recommendations = [item for item in (result.get("recommendations") or []) if isinstance(item, dict)]
    if not recommendations:
        return result

    current_top = result.get("top_pick") if isinstance(result.get("top_pick"), dict) else recommendations[0]
    current_keyword = str(current_top.get("keyword") or result.get("keyword") or "").strip().lower()
    if previous_keyword and current_keyword and current_keyword != previous_keyword:
        return result

    alternate = None
    for candidate in recommendations:
        candidate_keyword = str(candidate.get("keyword") or "").strip().lower()
        if candidate_keyword and candidate_keyword != previous_keyword:
            alternate = dict(candidate)
            break

    if alternate is None:
        return result

    boosted_rate = max(float(item.get("expected_reply_rate") or 0) for item in recommendations) + 0.1
    alternate["expected_reply_rate"] = round(boosted_rate, 1)
    result["top_pick"] = alternate
    result["keyword"] = alternate.get("keyword") or result.get("keyword")
    result["location"] = alternate.get("location") or result.get("location")
    result["reason"] = alternate.get("reason") or result.get("reason")
    result["expected_reply_rate"] = alternate.get("expected_reply_rate") or result.get("expected_reply_rate")

    reordered = [alternate]
    for candidate in recommendations:
        candidate_keyword = str(candidate.get("keyword") or "").strip().lower()
        candidate_location = str(candidate.get("location") or "").strip().lower()
        if candidate_keyword == str(alternate.get("keyword") or "").strip().lower() and candidate_location == str(alternate.get("location") or "").strip().lower():
            continue
        reordered.append(candidate)
    result["recommendations"] = reordered
    return result


def execute_scrape_task(_app: FastAPI, payload_data: dict) -> None:
    country_value = normalize_country_value(payload_data.get("country"), payload_data.get("country_code"))
    db_path = resolve_path(payload_data.get("db_path"), DEFAULT_DB_PATH)
    ensure_system_tables(db_path)
    task_id = int(payload_data["task_id"])

    default_profile = f"{DEFAULT_PROFILE_DIR}_{country_value.lower()}"
    user_data_dir = (
        Path(payload_data["user_data_dir"]).expanduser().resolve()
        if payload_data.get("user_data_dir")
        else Path(default_profile)
    )

    try:
        mark_task_running(db_path, task_id)
        requested_total = int(payload_data.get("results", 25))
        progress_state = {
            "phase": "scraping",
            "total_to_find": requested_total,
            "current_found": 0,
            "scanned_count": 0,
            "inserted": 0,
        }
        update_task_progress(db_path, task_id, progress_state)

        requested_headless = bool(payload_data.get("headless", False))

        # Read proxy config — supports single proxy_url or a list proxy_urls
        try:
            with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as _cfg_fh:
                _scrape_cfg = json.load(_cfg_fh)
        except Exception:
            _scrape_cfg = {}
        _proxy_url = str(_scrape_cfg.get("proxy_url", "") or "").strip() or None
        _proxy_urls_raw = _scrape_cfg.get("proxy_urls") or []
        _proxy_urls: List[str] = [
            p.strip()
            for p in (
                _proxy_urls_raw
                if isinstance(_proxy_urls_raw, list)
                else str(_proxy_urls_raw).splitlines()
            )
            if str(p or "").strip()
        ]
        if not _proxy_urls and HARDCODED_PROXY_URLS:
            _proxy_urls = [str(p).strip() for p in HARDCODED_PROXY_URLS if str(p).strip()]
        if _proxy_urls:
            _proxy_url = None

        def _on_progress(current_found: int, total_to_find: int, scanned_count: int, _lead: Any) -> None:
            progress_state["phase"] = "scraping"
            progress_state["current_found"] = int(current_found)
            progress_state["total_to_find"] = int(total_to_find or requested_total)
            progress_state["scanned_count"] = int(scanned_count)
            update_task_progress(db_path, task_id, progress_state)

        def _scrape_once(headless_value: bool):
            with GoogleMapsScraper(
                headless=headless_value,
                country=country_value,
                user_data_dir=str(user_data_dir),
                proxy_url=_proxy_url,
                proxy_urls=_proxy_urls or None,
            ) as scraper:
                return scraper.scrape(
                    keyword=str(payload_data.get("keyword", "")),
                    max_results=int(payload_data.get("results", 25)),
                    progress_callback=_on_progress,
                )

        try:
            leads = _scrape_once(requested_headless)
        except Exception as scrape_exc:
            # Non-headless sessions can be interrupted by consent/ad popups or manual window close.
            msg = str(scrape_exc).lower()
            if (not requested_headless) and ("has been closed" in msg or "target page" in msg):
                logging.warning("Scrape interrupted in visible browser; retrying once in headless mode.")
                leads = _scrape_once(True)
            else:
                raise

        # Do not import Slovenian leads unless Slovenia was explicitly selected.
        if country_value != "SI":
            leads = [lead for lead in leads if not is_slovenia_address(getattr(lead, "address", None))]

        total_scraped_from_maps = len(leads)

        # When Supabase is the primary DB, deduplicate against Supabase too.
        # The local SQLite may be missing leads from previous sessions, so we
        # must check the remote before inserting.
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            try:
                sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
                if sb_client is not None:
                    existing_rows = supabase_select_rows(
                        sb_client, "leads",
                        columns="business_name,address",
                    )
                    existing_keys: set[tuple[str, str]] = {
                        (str(r.get("business_name", "") or "").strip().lower(),
                         str(r.get("address", "") or "").strip().lower())
                        for r in existing_rows
                    }
                    leads = [
                        lead for lead in leads
                        if (
                            str(lead.business_name or "").strip().lower(),
                            str(lead.address or "").strip().lower(),
                        ) not in existing_keys
                    ]
            except Exception as dedup_exc:
                logging.warning("Supabase dedup check failed (will rely on DB constraints): %s", dedup_exc)

        inserted = batch_upsert_leads(leads, db_path=str(db_path), user_id=str(payload_data.get("user_id") or "legacy"))

        progress_state["phase"] = "post_process"
        progress_state["inserted"] = inserted
        progress_state["current_found"] = total_scraped_from_maps
        progress_state["total_to_find"] = requested_total
        update_task_progress(db_path, task_id, progress_state)

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                UPDATE leads
                SET status = 'scraped', status_updated_at = CURRENT_TIMESTAMP
                WHERE
                    search_keyword = ?
                    AND user_id = ?
                    AND LOWER(COALESCE(status, '')) IN ('', 'pending')
                """,
                (
                    str(payload_data.get("keyword", "")).strip(),
                    str(payload_data.get("user_id") or "legacy"),
                ),
            )
            conn.commit()

        blacklisted_synced = sync_blacklisted_leads(db_path)
        if inserted or blacklisted_synced:
            maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        exported = 0
        output_csv = None
        if payload_data.get("export_targets"):
            output_path = resolve_path(payload_data.get("output_csv"), DEFAULT_TARGET_EXPORT)
            exported = export_target_leads(
                db_path=str(db_path),
                output_csv=str(output_path),
                min_score=HIGH_AI_SCORE_THRESHOLD,
                user_id=str(payload_data.get("user_id") or "legacy"),
            )
            output_csv = str(output_path)

        credits_charged = 0
        credits_balance: Optional[int] = None
        credits_limit: Optional[int] = None
        billing_warning: Optional[str] = None
        try:
            billing = deduct_credits_on_success(
                str(payload_data.get("user_id") or ""),
                credits_to_deduct=max(0, int(inserted or 0)),
                db_path=db_path,
            )
            credits_charged = int(billing.get("credits_charged") or 0)
            credits_balance = int(billing.get("credits_balance") or 0)
            credits_limit = int(billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT)
        except HTTPException as exc:
            billing_warning = str(exc.detail)
            logging.warning("Credit deduction skipped after scrape success: %s", billing_warning)
        except Exception as exc:
            billing_warning = "Credits were not deducted due to a billing error."
            logging.warning("Credit deduction failed after scrape success: %s", exc)

        finish_task_record(
            db_path,
            task_id,
            status="completed",
            result_payload={
                "phase": "completed",
                "total_to_find": requested_total,
                "current_found": total_scraped_from_maps,
                "scanned_count": int(progress_state.get("scanned_count") or 0),
                "scraped": total_scraped_from_maps,
                "inserted": inserted,
                "duplicates": len(leads) - inserted,
                "blacklisted_synced": blacklisted_synced,
                "exported": exported,
                "output_csv": output_csv,
                "credits_charged": credits_charged,
                "credits_balance": credits_balance,
                "credits_limit": credits_limit,
                "billing_warning": billing_warning,
            },
        )
    except Exception as exc:
        logging.exception("Background scrape failed")
        requested_total = int(payload_data.get("results", 25))
        fail_payload = {
            "phase": "failed",
            "total_to_find": requested_total,
            "current_found": 0,
            "scanned_count": 0,
            "inserted": 0,
        }
        try:
            latest = fetch_task_by_id(db_path, task_id)
            latest_result = latest.get("result") if latest else None
            if isinstance(latest_result, dict):
                fail_payload["current_found"] = int(latest_result.get("current_found") or 0)
                fail_payload["scanned_count"] = int(latest_result.get("scanned_count") or 0)
                fail_payload["inserted"] = int(latest_result.get("inserted") or 0)
                if latest_result.get("total_to_find") is not None:
                    fail_payload["total_to_find"] = int(latest_result.get("total_to_find") or requested_total)
        except Exception:
            pass
        finish_task_record(db_path, task_id, status="failed", result_payload=fail_payload, error=str(exc))


def execute_enrich_task(_app: FastAPI, payload_data: dict) -> None:
    db_path = resolve_path(payload_data.get("db_path"), DEFAULT_DB_PATH)
    ensure_system_tables(db_path)
    task_id = int(payload_data["task_id"])

    try:
        mark_task_running(db_path, task_id)
        config_path = resolve_path(payload_data.get("config_path"), DEFAULT_CONFIG_PATH)
        requested_limit = int(payload_data.get("limit") or 0)
        progress_state = {
            "phase": "enriching",
            "processed": 0,
            "with_email": 0,
            "total": requested_limit,
            "current_lead": None,
        }
        update_task_progress(db_path, task_id, progress_state)

        # In Supabase-primary mode, push any locally enriched data to Supabase first,
        # so that the enricher doesn't re-process leads that were already enriched in
        # a previous crashed/cancelled task and whose enrichment data is only in SQLite.
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        enricher = LeadEnricher(
            db_path=str(db_path),
            headless=bool(payload_data.get("headless", True)),
            config_path=str(config_path),
            user_niche=payload_data.get("user_niche"),
            user_id=payload_data.get("user_id"),
            model_name_override=str(payload_data.get("_ai_model") or DEFAULT_AI_MODEL),
        )

        def _on_enrich_progress(processed_count: int, total_count: int, with_email_count: int, current_lead: Optional[str]) -> None:
            progress_state["phase"] = "enriching"
            progress_state["processed"] = int(processed_count)
            progress_state["with_email"] = int(with_email_count)
            progress_state["total"] = int(total_count)
            progress_state["current_lead"] = current_lead
            update_task_progress(db_path, task_id, progress_state)

        enrich_semaphore = getattr(_app.state, "enrich_semaphore", None)
        if enrich_semaphore is None:
            enrich_semaphore = BoundedSemaphore(value=ENRICH_CONCURRENCY_LIMIT)
            _app.state.enrich_semaphore = enrich_semaphore

        reserved_slot = bool(payload_data.get("_enrich_slot_reserved"))
        acquired_slot = False
        if reserved_slot:
            acquired_slot = True
        else:
            progress_state["phase"] = "waiting_for_enrich_slot"
            update_task_progress(db_path, task_id, progress_state)
            wait_timeout = ENRICH_SEMAPHORE_TIMEOUT_SECONDS * (2 if bool(payload_data.get("_queue_priority")) else 1)
            acquired_slot = bool(enrich_semaphore.acquire(timeout=wait_timeout))
            if not acquired_slot:
                raise TimeoutError("Timed out waiting for enrichment capacity slot.")

        try:
            progress_state["phase"] = "enriching"
            update_task_progress(db_path, task_id, progress_state)
            processed, with_email = enricher.run(
                limit=payload_data.get("limit"),
                progress_callback=_on_enrich_progress,
            )
        finally:
            if acquired_slot:
                enrich_semaphore.release()

        # In Supabase-primary mode, queueing reads from Supabase. Sync first so
        # freshly enriched rows are visible to the queue step in this same task.
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and processed:
            maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        queued_for_mail = queue_high_score_enriched_leads(
            db_path,
            user_id=str(payload_data.get("user_id") or "legacy"),
        )
        if processed or queued_for_mail:
            maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        exported = 0
        output_csv = None
        if not payload_data.get("skip_export"):
            output_path = resolve_path(payload_data.get("output_csv"), DEFAULT_AI_EXPORT)
            exported = enricher.export_ai_mailer_ready(output_csv=str(output_path))
            output_csv = str(output_path)

        credits_charged = 0
        credits_balance: Optional[int] = None
        credits_limit: Optional[int] = None
        billing_warning: Optional[str] = None
        if processed > 0:
            try:
                billing = deduct_credits_on_success(
                    str(payload_data.get("user_id") or ""),
                    credits_to_deduct=int(payload_data.get("_credits_per_success") or 1),
                    db_path=db_path,
                )
                credits_charged = int(billing.get("credits_charged") or 0)
                credits_balance = int(billing.get("credits_balance") or 0)
                credits_limit = int(billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT)
            except HTTPException as exc:
                billing_warning = str(exc.detail)
                logging.warning("Credit deduction skipped after enrichment success: %s", billing_warning)
            except Exception as exc:
                billing_warning = "Credits were not deducted due to a billing error."
                logging.warning("Credit deduction failed after enrichment success: %s", exc)

        finish_task_record(
            db_path,
            task_id,
            status="completed",
            result_payload={
                "processed": processed,
                "with_email": with_email,
                "queued_for_mail": queued_for_mail,
                "exported": exported,
                "output_csv": output_csv,
                "credits_charged": credits_charged,
                "credits_balance": credits_balance,
                "credits_limit": credits_limit,
                "billing_warning": billing_warning,
            },
        )
    except TimeoutError:
        logging.warning("Background enrichment timed out waiting for capacity slot")
        finish_task_record(db_path, task_id, status="failed", error=ENRICH_CAPACITY_ERROR_MESSAGE)
    except Exception as exc:
        logging.exception("Background enrichment failed")
        finish_task_record(db_path, task_id, status="failed", error=str(exc))


def execute_mailer_task(_app: FastAPI, payload_data: dict) -> None:
    db_path = resolve_path(payload_data.get("db_path"), DEFAULT_DB_PATH)
    ensure_system_tables(db_path)
    task_id = int(payload_data["task_id"])

    # Grab (and clear) the shared stop event so Emergency Stop from a previous run is reset.
    stop_event: Event = _app.state.mailer_stop_event
    stop_event.clear()

    try:
        mark_task_running(db_path, task_id)
        config_path = resolve_path(payload_data.get("config_path"), DEFAULT_CONFIG_PATH)

        # ── Scheduled start: wait until the requested New York (ET) hour ────
        start_after_hour_est = payload_data.get("start_after_hour_est")
        if start_after_hour_est is not None:
            target_hour = int(start_after_hour_est)
            if target_hour < 0 or target_hour > 23:
                raise ValueError("start_after_hour_est must be between 0 and 23")
            et_tz = ZoneInfo("America/New_York")
            while True:
                now_et = datetime.now(tz=et_tz)
                if now_et.hour >= target_hour:
                    break
                logging.info(
                    "Mailer scheduled for %02d:00 ET — current ET time is %02d:%02d. Waiting…",
                    target_hour, now_et.hour, now_et.minute,
                )
                # Sleep 60 s at a time so we can respond to an Emergency Stop.
                if stop_event.wait(timeout=60):
                    logging.info("Mailer stop requested during scheduled wait. Aborting.")
                    finish_task_record(db_path, task_id, status="stopped", error="Stopped by user during scheduled wait.")
                    return

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            for table_name in ("leads", "lead_blacklist"):
                synced, err = sync_table_from_supabase(db_path, table_name, DEFAULT_CONFIG_PATH)
                if err:
                    logging.warning("Primary mode mirror failed for %s: %s", table_name, err)
                else:
                    logging.info("Primary mode mirror complete for %s: %s rows", table_name, synced)

        status_allowlist = payload_data.get("status_allowlist")
        if status_allowlist is not None:
            status_allowlist = [str(item).strip() for item in status_allowlist if str(item).strip()]
            if not status_allowlist:
                status_allowlist = None

        mailer = AIMailer(
            db_path=str(db_path),
            config_path=str(config_path),
            model_name_override=str(payload_data.get("_ai_model") or DEFAULT_AI_MODEL),
            user_id=str(payload_data.get("user_id") or "legacy"),
            smtp_accounts_override=load_user_smtp_accounts(user_id=str(payload_data.get("user_id") or "legacy"), db_path=db_path),
        )
        # Use auto-detected base URL for tracking pixel if not manually configured in config
        if not mailer.open_tracking_base_url:
            auto_base = str(payload_data.get("_auto_base_url") or "").strip().rstrip("/")
            if auto_base:
                mailer.open_tracking_base_url = auto_base
        progress_state = {"sent": 0, "skipped": 0, "failed": 0}
        update_task_progress(db_path, task_id, progress_state)

        def _on_mailer_progress(sent_count: int, skipped_count: int, failed_count: int) -> None:
            progress_state["sent"] = sent_count
            progress_state["skipped"] = skipped_count
            progress_state["failed"] = failed_count
            update_task_progress(db_path, task_id, progress_state)

        sent, skipped, failed = mailer.send(
            limit=int(payload_data.get("limit", 10)),
            delay_min=int(payload_data.get("delay_min", 400)),
            delay_max=int(payload_data.get("delay_max", 900)),
            status_allowlist=status_allowlist,
            stop_event=stop_event,
            progress_callback=_on_mailer_progress,
        )

        send_summary = dict(getattr(mailer, "last_send_summary", {}) or {})
        credits_charged = 0
        credits_balance: Optional[int] = None
        credits_limit: Optional[int] = None
        billing_warning: Optional[str] = None
        if sent > 0:
            try:
                billing = deduct_credits_on_success(
                    str(payload_data.get("user_id") or ""),
                    credits_to_deduct=int(sent),
                    db_path=db_path,
                )
                credits_charged = int(billing.get("credits_charged") or 0)
                credits_balance = int(billing.get("credits_balance") or 0)
                credits_limit = int(billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT)
            except HTTPException as exc:
                billing_warning = str(exc.detail)
                logging.warning("Credit deduction skipped after mailer success: %s", billing_warning)
            except Exception as exc:
                billing_warning = "Credits were not deducted due to a billing error."
                logging.warning("Credit deduction failed after mailer success: %s", exc)

        result_payload = {
            "sent": sent,
            "skipped": skipped,
            "failed": failed,
            "requested_limit": int(send_summary.get("requested_limit", int(payload_data.get("limit", 10)))),
            "effective_limit": int(send_summary.get("effective_limit", int(payload_data.get("limit", 10)))),
            "daily_cap": int(send_summary.get("daily_cap", 0)),
            "sent_today_before": int(send_summary.get("sent_today", 0)),
            "remaining_today_before": int(send_summary.get("remaining_today", 0)),
            "candidate_count": int(send_summary.get("candidate_count", 0)),
            "stopped_by_user": bool(stop_event.is_set()),
            "credits_charged": credits_charged,
            "credits_balance": credits_balance,
            "credits_limit": credits_limit,
            "billing_warning": billing_warning,
        }
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        if payload_data.get("drip_feed"):
            result_payload["drip_feed"] = True
            result_payload["next_drip_at"] = get_runtime_value(db_path, "next_drip_at")

        if stop_event.is_set():
            finish_task_record(db_path, task_id, status="stopped", result_payload=result_payload, error="Stopped by user.")
        else:
            finish_task_record(db_path, task_id, status="completed", result_payload=result_payload)
    except OSError as exc:
        # SMTP / network failure — mark leads for retry instead of hard-failed
        logging.warning("Mailer SMTP/network error (retry_later): %s", exc)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "UPDATE leads SET status = 'retry_later' WHERE LOWER(COALESCE(status,'')) = 'queued_mail'"
            )
            conn.commit()
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        finish_task_record(db_path, task_id, status="failed", error=f"SMTP error (retry_later): {exc}")
    except Exception as exc:
        logging.exception("Background mailer failed")
        finish_task_record(db_path, task_id, status="failed", error=str(exc))


def _list_reporting_users(db_path: Path) -> list[dict[str, str]]:
    ensure_users_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT id, email, plan_key FROM users ORDER BY id ASC").fetchall()

    recipients: list[dict[str, str]] = []
    for row in rows:
        email = str(row["email"] or "").strip()
        plan_key = normalize_plan_key(row["plan_key"] or DEFAULT_PLAN_KEY)
        if not email or not bool(get_plan_feature_access(plan_key).get("advanced_reporting")):
            continue
        recipients.append({"user_id": str(row["id"]), "email": email, "plan_key": plan_key})
    return recipients


def run_weekly_report_digest(_app: FastAPI) -> None:
    db_path = DEFAULT_DB_PATH
    config_path = DEFAULT_CONFIG_PATH
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except Exception as exc:
        logging.warning("Weekly report: could not read config.json — %s", exc)
        return

    if not bool(cfg.get("auto_weekly_report_email", True)):
        logging.info("Weekly report: disabled in config.")
        return

    for recipient_info in _list_reporting_users(db_path):
        recipient = str(recipient_info.get("email") or _load_user_email_for_reports(db_path, recipient_info["user_id"]) or "").strip()
        if not recipient:
            continue
        try:
            account = get_primary_user_smtp_account(user_id=str(recipient_info["user_id"]), db_path=db_path)
            summary = build_weekly_report_summary(db_path, recipient_info["user_id"])
            send_weekly_report_email(account, recipient, summary)
            logging.info("Weekly report sent to %s for %s.", recipient, recipient_info["user_id"])
        except HTTPException as exc:
            logging.warning("Weekly report: SMTP is not ready for %s — %s", recipient_info["user_id"], exc.detail)
        except Exception as exc:
            logging.warning("Weekly report SMTP send failed for %s: %s", recipient, exc)


def run_monthly_report_digest(_app: FastAPI) -> None:
    db_path = DEFAULT_DB_PATH
    config_path = DEFAULT_CONFIG_PATH
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except Exception as exc:
        logging.warning("Monthly report: could not read config.json — %s", exc)
        return

    if not bool(cfg.get("auto_monthly_report_email", True)):
        logging.info("Monthly report: disabled in config.")
        return

    for recipient_info in _list_reporting_users(db_path):
        recipient = str(recipient_info.get("email") or _load_user_email_for_reports(db_path, recipient_info["user_id"]) or "").strip()
        if not recipient:
            continue
        try:
            account = get_primary_user_smtp_account(user_id=str(recipient_info["user_id"]), db_path=db_path)
            summary = build_monthly_report_summary(db_path, recipient_info["user_id"])
            pdf_bytes = _build_report_pdf(f"Sniped Monthly Report — {summary.get('month_label', 'Current Month')}", summary, period_key="monthly")
            send_monthly_report_email(account, recipient, summary, pdf_bytes)
            logging.info("Monthly report sent to %s for %s.", recipient, recipient_info["user_id"])
        except HTTPException as exc:
            logging.warning("Monthly report: SMTP is not ready for %s — %s", recipient_info["user_id"], exc.detail)
        except Exception as exc:
            logging.warning("Monthly report SMTP send failed for %s: %s", recipient, exc)


def run_daily_digest(_app: FastAPI) -> None:
    """Send the morning Profit Digest email at 08:00 UTC."""
    db_path = DEFAULT_DB_PATH
    config_path = DEFAULT_CONFIG_PATH

    # ── Load config & SMTP ────────────────────────────────────────────────────
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except Exception as exc:
        logging.warning("Daily digest: could not read config.json — %s", exc)
        return

    if not bool(cfg.get("auto_daily_digest_email", False)):
        logging.info("Daily digest: disabled in config.")
        return

    smtp_accounts = cfg.get("smtp_accounts", [])
    if not smtp_accounts:
        logging.warning("Daily digest: no SMTP accounts configured — skipping.")
        return

    acct = smtp_accounts[0]
    smtp_host = str(acct.get("host", "")).strip()
    smtp_port = int(acct.get("port", 587))
    smtp_email = str(acct.get("email", "")).strip()
    smtp_password = str(acct.get("password", "")).strip()
    smtp_use_tls = bool(acct.get("use_tls", True))
    smtp_use_ssl = bool(acct.get("use_ssl", False))
    from_name = str(acct.get("from_name", "") or "").strip()
    from_header = f"{from_name} <{smtp_email}>" if from_name else smtp_email

    # digest_email in config (optional, defaults to SMTP sender)
    recipient = str(cfg.get("digest_email", "") or "").strip() or smtp_email
    if not recipient or not smtp_host or not smtp_password:
        logging.warning("Daily digest: incomplete SMTP config — skipping.")
        return

    # ── Gather data ───────────────────────────────────────────────────────
    try:
        stats = get_dashboard_stats(db_path)
    except Exception as exc:
        logging.warning("Daily digest: could not gather stats — %s", exc)
        return

    mrr = stats.get("monthly_recurring_revenue", 0)
    mrr_goal = stats.get("mrr_goal", MRR_GOAL)
    mrr_progress = stats.get("mrr_progress_pct", 0)
    paid_count = stats.get("paid_count", 0)
    total_leads = stats.get("total_leads", 0)
    emails_sent = stats.get("emails_sent", 0)

    # AI Niche recommendation for today's campaign focus
    recommendation = get_niche_recommendation(db_path, config_path)
    top_pick = recommendation.get("top_pick", {}) if isinstance(recommendation, dict) else {}
    niche_keyword = str(top_pick.get("keyword", "AC Repair in Phoenix, AZ") or "AC Repair in Phoenix, AZ")
    expected_reply = float(top_pick.get("expected_reply_rate", 5.0) or 5.0)
    campaign_base = str(cfg.get("dashboard_url", "http://localhost:5173") or "http://localhost:5173").rstrip("/")
    campaign_link = f"{campaign_base}/?niche={quote_plus(niche_keyword)}"

    # Golden leads in the last 24 h
    golden_count = 0
    uptime_alerts: list[dict] = []
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT COUNT(*) FROM leads
                WHERE ai_score >= 9
                  AND datetime(enriched_at) >= datetime('now', '-1 day')
                """
            ).fetchone()
            golden_count = int(row[0] if row else 0)

            alert_rows = conn.execute(
                """
                SELECT request_payload FROM system_tasks
                WHERE task_type = 'uptime_alert'
                  AND datetime(created_at) >= datetime('now', '-1 day')
                ORDER BY id DESC
                LIMIT 20
                """
            ).fetchall()
            for ar in alert_rows:
                try:
                    payload = json.loads(ar["request_payload"] or "{}")
                    uptime_alerts.append(payload)
                except json.JSONDecodeError:
                    pass
    except Exception as exc:
        logging.warning("Daily digest: DB read failed — %s", exc)

    # ── Build email ─────────────────────────────────────────────────────────
    today_str = datetime.now(timezone.utc).strftime("%A, %d %b %Y")
    progress_bar = ("\u2588" * (mrr_progress // 10)).ljust(10, "\u2591")

    alert_lines = ""
    if uptime_alerts:
        lines = []
        for a in uptime_alerts:
            name = a.get("business_name", "?")
            url = a.get("website_url", "?")
            code = a.get("http_status", 0) or "unreachable"
            lines.append(f"  ⚠  {name} ({url}) — HTTP {code}")
        alert_lines = "\n\nUptime Alerts (last 24h):\n" + "\n".join(lines)
    else:
        alert_lines = "\n\nUptime Alerts: none ✅"

    body = (
        f"Good morning! Here is your Daily Profit Digest for {today_str}.\n"
        f"{'=' * 50}\n\n"
        f"MRR:             \u20ac{mrr:,.0f} / \u20ac{mrr_goal:,.0f}\n"
        f"Goal progress:   [{progress_bar}] {mrr_progress}%\n"
        f"Paid clients:    {paid_count}\n"
        f"Total leads:     {total_leads}\n"
        f"Emails sent:     {emails_sent}\n"
        f"Golden Leads:    {golden_count} found in last 24h"
        f"\nDanašnja priložnost: {niche_keyword}. Pričakovan Reply Rate: {expected_reply:.1f}%. "
        f"Kliknite tukaj, da zaženete kampanjo: {campaign_link}"
        f"{alert_lines}\n\n"
        f"{'=' * 50}\n"
        f"Keep pushing. You're {100 - mrr_progress}% from the finish line.\n"
    )
    subject = f"[Digest] MRR \u20ac{mrr:,.0f} | {mrr_progress}% to goal | {today_str}"

    # ── Send ───────────────────────────────────────────────────────────────
    import smtplib
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["From"] = from_header
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        if smtp_use_ssl or smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as smtp:
                smtp.login(smtp_email, smtp_password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
                smtp.ehlo()
                if smtp_use_tls or smtp_port == 587:
                    smtp.starttls()
                    smtp.ehlo()
                smtp.login(smtp_email, smtp_password)
                smtp.send_message(msg)
        logging.info(
            "Daily digest sent to %s (MRR=\u20ac%s, golden=%s, alerts=%s).",
            recipient, mrr, golden_count, len(uptime_alerts),
        )
    except Exception as exc:
        logging.warning("Daily digest SMTP send failed: %s", exc)


def run_uptime_check(_app: FastAPI) -> None:
    """Check HTTP reachability of every paid client's website every 2 hours."""
    db_path = DEFAULT_DB_PATH
    ensure_system_tables(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, business_name, website_url
            FROM leads
            WHERE LOWER(COALESCE(status, '')) = 'paid'
              AND website_url IS NOT NULL
              AND TRIM(website_url) != ''
            """
        ).fetchall()

    if not rows:
        logging.debug("Uptime check: no paid clients to monitor.")
        return

    logging.info("Uptime check: monitoring %s paid client(s).", len(rows))
    for row in rows:
        url = str(row["website_url"]).strip()
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"

        status_code = 0
        error_msg: Optional[str] = None
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0 (Uptime-Monitor/1.0)"}
            )
            with urllib.request.urlopen(req, timeout=12) as resp:
                status_code = resp.getcode()
        except Exception as exc:
            error_msg = str(exc)

        if status_code == 200:
            logging.debug("Uptime OK: %s (%s)", row["business_name"], url)
        else:
            logging.warning(
                "UPTIME ALERT: %s (%s) returned HTTP %s. %s",
                row["business_name"], url, status_code or "unreachable", error_msg or "",
            )
            create_task_record(
                db_path,
                "system",
                task_type="uptime_alert",
                status="failed",
                request_payload={
                    "lead_id": int(row["id"]),
                    "business_name": row["business_name"],
                    "website_url": url,
                    "http_status": status_code,
                    "error": error_msg,
                },
                source="uptime_monitor",
            )


def run_autopilot_cycle(app: FastAPI) -> None:
    db_path = DEFAULT_DB_PATH
    ensure_system_tables(db_path)

    queued_for_mail = queue_high_score_enriched_leads(db_path)
    if queued_for_mail:
        logging.info("Autopilot queued %s high-score leads for drip mail.", queued_for_mail)

    scraped_count = get_scraped_lead_count(db_path)
    if scraped_count <= 0:
        return

    if task_is_active(db_path, "enrich", user_id="system"):
        logging.info("Autopilot skipped enrichment: enrich task already active.")
        return

    enrich_limit = min(scraped_count, AUTOPILOT_ENRICH_LIMIT)
    response = enqueue_task(
        app,
        background_tasks=None,
        db_path=db_path,
        user_id="system",
        task_type="enrich",
        request_payload={
            "limit": enrich_limit,
            "headless": True,
            "skip_export": True,
            "db_path": str(db_path),
            "autopilot": True,
        },
        source="scheduler",
    )
    logging.info("Autopilot enrichment trigger: %s", response)


def run_drip_dispatch_cycle(app: FastAPI) -> None:
    if not AUTO_DRIP_DISPATCH_ENABLED:
        return

    db_path = DEFAULT_DB_PATH
    ensure_system_tables(db_path)

    queue_high_score_enriched_leads(db_path)

    if task_is_active(db_path, "mailer", user_id="system"):
        return

    next_drip_at = parse_iso_datetime(get_runtime_value(db_path, "next_drip_at"))
    if next_drip_at and datetime.now(timezone.utc) < next_drip_at:
        return

    if get_queued_mail_count(db_path) <= 0:
        return

    response = enqueue_task(
        app,
        background_tasks=None,
        db_path=db_path,
        user_id="system",
        task_type="mailer",
        request_payload={
            "limit": 1,
            "delay_min": 0,
            "delay_max": 0,
            "status_allowlist": ["queued_mail"],
            "drip_feed": True,
            "db_path": str(db_path),
        },
        source="scheduler",
    )

    if response.get("status") == "started":
        next_at = compute_next_drip_at()
        set_runtime_value(db_path, "next_drip_at", next_at.isoformat())
        logging.info("Drip dispatch sent one lead. Next drip at %s.", next_at.isoformat())


def start_scheduler(app: FastAPI) -> None:
    existing = getattr(app.state, "scheduler", None)
    if existing is not None:
        return

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        lambda: run_autopilot_cycle(app),
        trigger=IntervalTrigger(minutes=30),
        id="autopilot-cycle",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    if AUTO_DRIP_DISPATCH_ENABLED:
        scheduler.add_job(
            lambda: run_drip_dispatch_cycle(app),
            trigger=IntervalTrigger(minutes=1),
            id="drip-feed-dispatch",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
    scheduler.add_job(
        lambda: run_uptime_check(app),
        trigger=IntervalTrigger(hours=2),
        id="uptime-monitor",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.add_job(
        lambda: run_daily_digest(app),
        trigger=CronTrigger(hour=8, minute=0, timezone="UTC"),
        id="daily-digest",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.add_job(
        lambda: run_weekly_report_digest(app),
        trigger=CronTrigger(day_of_week="mon", hour=7, minute=30, timezone="UTC"),
        id="weekly-report-digest",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.add_job(
        lambda: run_monthly_report_digest(app),
        trigger=CronTrigger(day=1, hour=7, minute=40, timezone="UTC"),
        id="monthly-report-digest",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.add_job(
        lambda: run_monthly_credit_reset_cycle(app),
        trigger=CronTrigger(hour=0, minute=15, timezone="UTC"),
        id="monthly-credit-reset",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.start()
    app.state.scheduler = scheduler

    launch_detached_task(lambda _app, _payload: run_autopilot_cycle(_app), app, {})
    launch_detached_task(lambda _app, _payload: run_monthly_credit_reset_cycle(_app), app, {})
    if AUTO_DRIP_DISPATCH_ENABLED:
        launch_detached_task(lambda _app, _payload: run_drip_dispatch_cycle(_app), app, {})


def stop_scheduler(app: FastAPI) -> None:
    scheduler = getattr(app.state, "scheduler", None)
    if scheduler is None:
        return
    scheduler.shutdown(wait=False)
    app.state.scheduler = None


# ── Lead Qualifier helpers ─────────────────────────────────────────────────────

def _qualifier_extract_city(address: Optional[str]) -> str:
    """Return the most likely city name from a raw address string."""
    import re as _re
    if not address:
        return ""
    parts = [p.strip() for p in str(address).split(",") if p.strip()]
    # Walk from end backwards, skip postal codes and country tokens
    for part in reversed(parts):
        if _re.search(r"\d{3,}", part):
            continue
        cleaned = part.strip()
        if len(cleaned) <= 2 and cleaned.isupper():
            continue
        if cleaned.lower() in ("us", "usa", "united states", "deutschland", "germany",
                                "austria", "schweiz", "switzerland", "slovenija", "slovenia",
                                "si", "de", "at", "ch"):
            continue
        return cleaned
    return parts[0] if parts else ""


def _qualifier_to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        text = str(value).strip().replace(",", ".")
        if text == "":
            return default
        return float(text)
    except Exception:
        return default


def _qualifier_to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return 1 if value else 0
        text = str(value).strip()
        if text == "":
            return default
        return int(float(text))
    except Exception:
        return default


def _qualifier_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _qualifier_parse_enrichment(raw_enrichment: Any) -> dict:
    if not raw_enrichment:
        return {}
    try:
        obj = json.loads(raw_enrichment)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _lead_normalize_string_list(value: Any, *, limit: int = 3) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        raw_items = [str(item or "").strip() for item in value]
    elif isinstance(value, str):
        raw_items = [segment.strip() for segment in re.split(r"\n|\||;|•", value)]
    else:
        raw_items = []

    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
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


def _lead_compute_best_score(lead: dict, enrichment: dict) -> float:
    direct_score = enrichment.get("best_lead_score")
    try:
        if direct_score is not None and str(direct_score).strip() != "":
            return round(max(0.0, min(100.0, float(direct_score))), 1)
    except Exception:
        pass

    has_email = bool(str(lead.get("email") or "").strip())
    employee_count = _qualifier_to_int(
        enrichment.get("employee_count", enrichment.get("linkedin_employee_count", 0)),
        default=0,
    )
    ai_score = _qualifier_to_float(lead.get("ai_score"), default=0.0)
    ai_sentiment_score = _qualifier_to_float(
        enrichment.get("ai_sentiment_score", enrichment.get("lead_score_100", ai_score * 10)),
        default=ai_score * 10,
    )
    ai_sentiment_score = max(0.0, min(100.0, ai_sentiment_score))

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
    return round(min(100.0, email_component + size_component + sentiment_component), 1)


def _augment_lead_with_deep_intelligence(lead: dict) -> dict:
    enrichment = _qualifier_parse_enrichment(lead.get("enrichment_data"))
    company_audit = enrichment.get("company_audit") if isinstance(enrichment.get("company_audit"), dict) else {}

    strengths = (
        _lead_normalize_string_list(company_audit.get("strengths"), limit=3)
        or _lead_normalize_string_list(enrichment.get("strengths"), limit=3)
    )
    weaknesses = (
        _lead_normalize_string_list(company_audit.get("weaknesses"), limit=3)
        or _lead_normalize_string_list(enrichment.get("weak_points") or enrichment.get("weaknesses"), limit=3)
    )
    competitors = _lead_normalize_string_list(
        enrichment.get("competitor_snapshot") or enrichment.get("competitors"),
        limit=3,
    )
    tech_stack = _lead_normalize_string_list(enrichment.get("tech_stack"), limit=5)
    intent_signals = _lead_normalize_string_list(enrichment.get("intent_signals"), limit=6)
    recent_site_update = _qualifier_to_bool(enrichment.get("recent_site_update"))
    if recent_site_update and all("recently updated" not in signal.lower() for signal in intent_signals):
        intent_signals.insert(0, "Recently updated site")

    employee_count = _qualifier_to_int(
        enrichment.get("employee_count", enrichment.get("linkedin_employee_count", 0)),
        default=0,
    )
    ai_score = _qualifier_to_float(lead.get("ai_score"), default=0.0)
    ai_sentiment_score = _qualifier_to_float(
        enrichment.get("ai_sentiment_score", enrichment.get("lead_score_100", ai_score * 10)),
        default=ai_score * 10,
    )
    ai_sentiment_score = max(0.0, min(100.0, ai_sentiment_score))
    best_lead_score = _lead_compute_best_score(lead, enrichment)

    pipeline_stage = _derive_pipeline_stage(
        status=lead.get("status"),
        sent_at=lead.get("sent_at"),
        last_contacted_at=lead.get("last_contacted_at"),
        reply_detected_at=lead.get("reply_detected_at"),
        paid_at=lead.get("paid_at"),
        pipeline_stage=lead.get("pipeline_stage"),
    )

    lead["company_audit"] = {
        "strengths": strengths,
        "weaknesses": weaknesses,
    }
    lead["competitor_snapshot"] = competitors
    lead["tech_stack"] = tech_stack
    lead["intent_signals"] = intent_signals[:6]
    lead["recent_site_update"] = recent_site_update
    lead["employee_count"] = employee_count
    lead["ai_sentiment_score"] = round(ai_sentiment_score, 1)
    lead["best_lead_score"] = best_lead_score
    lead["lead_priority"] = str(enrichment.get("lead_priority") or "").strip() or (
        "Hot Lead" if best_lead_score >= 80 else "Qualified" if best_lead_score >= 55 else "Low Priority"
    )
    lead["competitive_hook"] = str(enrichment.get("competitive_hook") or "").strip()
    lead["main_offer"] = str(enrichment.get("main_offer") or "").strip()
    lead["latest_achievements"] = _lead_normalize_string_list(enrichment.get("latest_achievements"), limit=3)
    lead["pipeline_stage"] = pipeline_stage
    lead["client_folder_id"] = lead.get("client_folder_id")
    return lead


def _lead_is_blacklisted_status(status: Any) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {"blacklisted", "skipped (unsubscribed)"}


def _lead_has_sent_mail(lead: dict) -> bool:
    status = str(lead.get("status") or "").strip().lower()
    return bool(
        lead.get("sent_at")
        or lead.get("last_contacted_at")
        or lead.get("last_sender_email")
        or status in {"emailed", "contacted", "interested", "meeting set", "zoom scheduled", "paid", "closed"}
    )


def _lead_has_opened_mail(lead: dict) -> bool:
    return bool(
        _qualifier_to_int(lead.get("open_count"), default=0) > 0
        or lead.get("first_opened_at")
        or lead.get("last_opened_at")
    )


def _lead_has_reply(lead: dict) -> bool:
    status = str(lead.get("status") or "").strip().lower()
    return bool(lead.get("reply_detected_at") or status in {"replied", "interested", "meeting set", "zoom scheduled"})


def _lead_matches_quick_filter(lead: dict, quick_filter: str) -> bool:
    normalized = str(quick_filter or "all").strip().lower()
    if normalized in {"", "all"}:
        return True
    if normalized == "qualified":
        return _qualifier_to_float(lead.get("ai_score"), default=0.0) >= 7 or _lead_has_reply(lead)
    if normalized == "not_qualified":
        return not (_qualifier_to_float(lead.get("ai_score"), default=0.0) >= 7 or _lead_has_reply(lead))
    if normalized == "mailed":
        return _lead_has_sent_mail(lead)
    if normalized == "opened":
        return _lead_has_opened_mail(lead)
    if normalized == "replied":
        return _lead_has_reply(lead)
    return True


def _lead_dashboard_sort_key(lead: dict, sort_mode: str) -> Any:
    normalized = str(sort_mode or "recent").strip().lower()
    if normalized == "name":
        return (str(lead.get("business_name") or "").lower(), -int(lead.get("id") or 0))
    if normalized == "score":
        return (
            -_qualifier_to_float(lead.get("ai_score"), default=0.0),
            -_qualifier_to_int(lead.get("id"), default=0),
        )
    if normalized == "best":
        return (
            -_qualifier_to_float(lead.get("best_lead_score"), default=0.0),
            -int(bool(str(lead.get("email") or "").strip())),
            -_qualifier_to_int(lead.get("employee_count"), default=0),
            -_qualifier_to_float(lead.get("ai_sentiment_score") or lead.get("ai_score"), default=0.0),
            -_qualifier_to_int(lead.get("id"), default=0),
        )
    return (
        str(lead.get("created_at") or lead.get("scraped_at") or ""),
        _qualifier_to_int(lead.get("id"), default=0),
    )


def _qualifier_extract_metrics(lead: dict) -> dict:
    enrichment = _qualifier_parse_enrichment(lead.get("enrichment_data"))

    has_meta_pixel = _qualifier_to_bool(
        enrichment.get("has_meta_pixel", enrichment.get("meta_pixel", enrichment.get("pixel_installed", False)))
    )
    has_google_pixel = _qualifier_to_bool(
        enrichment.get("has_google_pixel", enrichment.get("google_pixel", enrichment.get("gtag_installed", False)))
    )
    backlink_count = _qualifier_to_int(
        enrichment.get("backlink_count", enrichment.get("backlinks", enrichment.get("ref_domains", 0))),
        default=0,
    )
    organic_traffic = _qualifier_to_float(
        enrichment.get("organic_traffic", enrichment.get("organic_visits", 0.0)),
        default=0.0,
    )
    employee_count = _qualifier_to_int(
        enrichment.get("employee_count", enrichment.get("linkedin_employee_count", 0)),
        default=0,
    )
    social_activity_score = _qualifier_to_float(
        enrichment.get("social_activity_score", enrichment.get("content_activity_score", 0.0)),
        default=0.0,
    )
    pagespeed_score = _qualifier_to_float(
        enrichment.get("pagespeed_score", enrichment.get("page_speed", 100.0)),
        default=100.0,
    )
    tech_stack_score = _qualifier_to_float(
        enrichment.get("tech_stack_score", enrichment.get("stack_health_score", 10.0)),
        default=10.0,
    )
    authority = _qualifier_to_float(
        enrichment.get("authority", enrichment.get("domain_authority", enrichment.get("authority_score", 0.0))),
        default=0.0,
    )
    competitor_avg = _qualifier_to_float(
        enrichment.get("competitor_avg", enrichment.get("competitor_authority_avg", 0.0)),
        default=0.0,
    )

    return {
        "has_meta_pixel": has_meta_pixel,
        "has_google_pixel": has_google_pixel,
        "backlink_count": backlink_count,
        "organic_traffic": organic_traffic,
        "employee_count": employee_count,
        "social_activity_score": social_activity_score,
        "pagespeed_score": pagespeed_score,
        "tech_stack_score": tech_stack_score,
        "authority": authority,
        "competitor_avg": competitor_avg,
    }


def _qualifier_dynamic_pain_point(
    bucket_name: str,
    business_name: str,
    selected_niche: str,
    city: str,
    keyword: str,
    metrics: dict,
    ai_score: float,
    niche_avg_score: float,
) -> str:
    name = business_name or "This business"
    city_str = f" in {city}" if city else ""
    kw = keyword or selected_niche or "their service"

    if bucket_name == "ghost":
        if selected_niche == "Paid Ads Agency":
            return (
                f"{name} has a website but conversion tracking is effectively blind: Meta/Google Pixel is missing. "
                f"That means ad spend for '{kw}'{city_str} cannot be measured correctly, so budget leaks with no reliable ROAS signal."
            )
        if selected_niche == "SEO & Content":
            return (
                f"{name} exists online, but authority is near zero ({metrics.get('backlink_count', 0)} backlinks, "
                f"organic traffic {int(metrics.get('organic_traffic', 0))}). "
                f"For '{kw}'{city_str}, this behaves like digital invisibility and leaves demand to competitors."
            )
        return (
            f"{name} has weak digital presence relative to niche expectations. "
            f"Under-optimized signals for '{kw}'{city_str} reduce discoverability and conversion-ready traffic."
        )

    if bucket_name == "invisible_giant":
        return (
            f"{name} looks bigger offline than online ({int(metrics.get('employee_count', 0))} employees, "
            f"social activity score {metrics.get('social_activity_score', 0):.1f}/10). "
            f"This authority gap makes enterprise-scale credibility invisible in content channels."
        )

    if bucket_name == "tech_debt":
        return (
            f"{name} is losing performance to technical debt: pagespeed {int(metrics.get('pagespeed_score', 100))}/100, "
            f"tech stack {metrics.get('tech_stack_score', 10):.1f}/10, AI quality score {ai_score:.1f} vs niche avg {niche_avg_score:.1f}. "
            f"This suppresses trust, rankings, and conversion rate for '{kw}'."
        )

    return (
        f"{name} is under-performing benchmark signals for '{kw}'{city_str}. "
        f"Niche-average score is {niche_avg_score:.1f} while this lead is {ai_score:.1f}."
    )


def _qualifier_suggested_hook(
    selected_niche: str,
    business_name: str,
    keyword: str,
    city: str,
    metrics: dict,
) -> str:
    """Build a short 2-sentence follow-up preview hook in English."""
    name = business_name or "your company"
    niche_keyword = keyword.strip() or selected_niche or "your core service"
    city_str = f" in {city}" if city else ""

    if selected_niche == "Web Design & Dev":
        pagespeed_score = _qualifier_to_float(metrics.get("pagespeed_score"), default=100.0)
        speed_seconds = max(2.0, min(12.0, round((100.0 - pagespeed_score) / 10.0 + 2.0, 1)))
        target_speed = 2.0
        return (
            f"Hi, I noticed {name}'s website takes around {speed_seconds:.1f}s to load on mobile{city_str}, which likely causes visitors to drop before converting. "
            f"I mapped a quick fix path to bring it closer to {target_speed:.1f}s without a full redesign."
        )

    if selected_niche == "Paid Ads Agency":
        missing_pixel = (not bool(metrics.get("has_meta_pixel"))) or (not bool(metrics.get("has_google_pixel")))
        pixel_part = (
            "I also noticed your Meta or Google tracking pixel is missing, so retargeting and attribution stay blind."
            if missing_pixel
            else "Your tracking setup can still be tightened to improve retargeting and conversion attribution."
        )
        return (
            f"Your competitors are actively buying attention for '{niche_keyword}'{city_str}, while {name} has low paid visibility in the same window. "
            f"{pixel_part}"
        )

    if selected_niche == "SEO & Content":
        return (
            f"{name} is not yet winning page-one visibility for '{niche_keyword}'{city_str}, which is where high-intent traffic is captured. "
            "I prepared a quick 3-keyword gap snapshot that shows exactly where competitors are outranking you and what to fix first."
        )

    if selected_niche == "Lead Gen Agency":
        tech_stack_score = _qualifier_to_float(metrics.get("tech_stack_score"), default=10.0)
        uplift_pct = max(12, min(45, int(round((10.0 - tech_stack_score) * 5))))
        return (
            f"{name} likely gets decent visitor intent, but weak capture paths and unclear CTAs are leaking qualified inquiries. "
            f"I can show a practical funnel tweak that can increase lead capture by about {uplift_pct}% without increasing traffic."
        )

    if selected_niche == "B2B Service Provider":
        target_client_type = "operations and commercial teams"
        return (
            f"I came across {name} in the {niche_keyword} segment and saw strong potential for direct LinkedIn partner sourcing. "
            f"I can map a lightweight outbound system that automates partner discovery and first-touch outreach to {target_client_type}."
        )

    return (
        f"{name} shows clear room to improve its acquisition performance for '{niche_keyword}'{city_str}. "
        "I can share a short, practical action plan tailored to the weakest conversion signals."
    )


def _qualifier_pain_point(
    business_name: str,
    has_website: bool,
    rating: Optional[float],
    review_count: int,
    city: str,
    city_max_reviews: int,
    keyword: str,
    insecure: bool,
    main_shortcoming: str,
    ai_description: str,
    competitive_hook: str,
) -> str:
    """Generate a specific, money-focused pain point sentence for a lead."""
    name = business_name or "This business"
    city_str = f"in {city}" if city else ""
    niche_str = keyword.strip() or "their service"

    if not has_website:
        rating_str = f"{rating:.1f}★" if isinstance(rating, (int, float)) else "good"
        return (
            f"{name} has {rating_str} reviews but ZERO online presence. "
            f"Every day, customers {city_str} search Google for '{niche_str}' — "
            f"they can't find {name} and click the first competitor that shows up instead. "
            f"Estimated impact: dozens of lost high-value jobs every month, going straight to competitors."
        )

    if review_count < 5 and city_max_reviews >= 100:
        return (
            f"{name} has only {review_count} review{'s' if review_count != 1 else ''} "
            f"while leading businesses {city_str} have {city_max_reviews}+. "
            f"Google's local algorithm buries low-review businesses below the fold — "
            f"new customers searching for '{niche_str}' never even see {name}. "
            f"They are effectively invisible in this market."
        )

    if insecure:
        return (
            f"{name}'s website runs on HTTP (not HTTPS). "
            f"Browsers display a 'Not Secure' warning, Google penalises the ranking, "
            f"and potential customers bounce before reading a single word. "
            f"Every ad click and every organic visitor is wasted due to this trust issue."
        )

    # Use existing AI-generated content when available
    if competitive_hook:
        return competitive_hook
    if ai_description and ai_description.lower() not in (
        "heuristic scoring used (openai unavailable).",
        "heuristic scoring used after ai failure.",
        "ai scoring completed.",
    ):
        return ai_description

    if review_count < 15:
        return (
            f"{name} has only {review_count} reviews. "
            f"Google's local pack prioritises businesses with strong review velocity — "
            f"without at least 20–30 reviews, {name} is consistently outranked "
            f"by competitors in the '{niche_str}' category {city_str}."
        )

    shortcoming_lower = main_shortcoming.lower()
    if "missing" in shortcoming_lower or "no website" in shortcoming_lower:
        return (
            f"{name} lacks a properly optimised website. "
            f"Competitors {city_str} are capturing high-intent Google traffic "
            f"while {name} relies on referrals alone — that's a shrinking pipeline."
        )

    return (
        f"{name} has a digital presence but is missing key authority signals. "
        f"Competing businesses {city_str} rank higher in the '{niche_str}' category, "
        f"meaning {name} loses qualified leads every week to more visible rivals."
    )


def create_app() -> FastAPI:
    from concurrent.futures import ThreadPoolExecutor as _TPE
    # Concurrency limit: max 10 worker threads for AI/scrape calls
    _thread_pool = _TPE(max_workers=10, thread_name_prefix="lf-worker")

    app = FastAPI(title="LeadGen Full Stack API", version="1.1.0")
    app.state.task_lock = Lock()
    app.state.active_task_threads = {}
    app.state.scheduler = None
    app.state.mailer_stop_event = Event()
    app.state.thread_pool = _thread_pool  # available to any endpoint that needs it
    app.state.enrich_semaphore = BoundedSemaphore(value=ENRICH_CONCURRENCY_LIMIT)

    # Trust proxy headers (X-Forwarded-Proto, X-Forwarded-For) from any upstream.
    # This ensures request.base_url returns https://your-domain.com behind nginx/Caddy/Railway.
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    def startup_tasks() -> None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
        # ── Env-var check ─────────────────────────────────────────────────────
        _required_env = {
            "SUPABASE_URL": "Supabase project URL (required for auth & DB)",
            "SUPABASE_SERVICE_ROLE_KEY": "Supabase service-role key (required for auth & DB)",
            "OPENAI_API_KEY": "OpenAI key (required for enrichment & mail)",
        }
        _optional_env = {
            "BACKEND_URL": "Public URL of this server (used by Vercel proxy)",
            "STRIPE_SECRET_KEY": "Stripe secret key (required for billing)",
            "SMTP_HOST": "Default SMTP host (optional, can be set per-user)",
        }
        for var, desc in _required_env.items():
            if not os.environ.get(var):
                print(f"[startup] ERROR: Missing required env var {var} — {desc}")
                logging.error("[startup] Missing required env var %s — %s", var, desc)
        for var, desc in _optional_env.items():
            if not os.environ.get(var):
                print(f"[startup] WARNING: Optional env var {var} not set — {desc}")
        # ── DB init ───────────────────────────────────────────────────────────
        print("[startup] Initialising database tables...")
        try:
            ensure_system_tables(DEFAULT_DB_PATH)
            print("[startup] SQLite system tables OK")
        except Exception as exc:
            logging.error("[startup] SQLite table init failed (non-fatal): %s", exc)
            print(f"[startup] WARNING: SQLite table init failed: {exc}")
        try:
            if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
                ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
                print("[startup] Supabase users table OK")
        except Exception as exc:
            logging.error("[startup] Supabase table init failed (non-fatal): %s", exc)
            print(f"[startup] WARNING: Supabase table init failed: {exc}")
        start_scheduler(app)
        print("[startup] Scheduler started — app ready")

    @app.on_event("shutdown")
    def shutdown_tasks() -> None:
        stop_scheduler(app)
        _thread_pool.shutdown(wait=False)

    @app.get("/api/health")
    def health() -> dict:
        return {"ok": True}

    @app.get("/api/system-status")
    def system_status() -> dict:
        health_ok = True
        enrichment_ok = bool(load_config_health(DEFAULT_CONFIG_PATH).get("openai_ok", False))

        db_ok = False
        try:
            if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
                client = get_supabase_client(DEFAULT_CONFIG_PATH)
                if client is not None:
                    client.table("leads").select("id", count="exact").limit(1).execute()
                    db_ok = True
            else:
                with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                    conn.execute("SELECT 1 FROM leads LIMIT 1").fetchone()
                db_ok = True
        except Exception:
            db_ok = False

        services = [
            {
                "key": "lead_search_api",
                "label": "Lead Search API",
                "status": "Operational" if health_ok else "Degraded",
                "operational": health_ok,
            },
            {
                "key": "enrichment_engine",
                "label": "Enrichment Engine",
                "status": "Operational" if enrichment_ok else "Degraded",
                "operational": enrichment_ok,
            },
            {
                "key": "database",
                "label": "Database",
                "status": "Operational" if db_ok else "Degraded",
                "operational": db_ok,
            },
        ]
        return {
            "ok": all(bool(item["operational"]) for item in services),
            "services": services,
            "updated_at": utc_now_iso(),
        }

    @app.get("/api/config-health")
    def config_health() -> dict:
        return load_config_health(DEFAULT_CONFIG_PATH)

    @app.get("/api/config")
    def get_config(request: Request) -> dict:
        session_token = require_authenticated_session(request)
        try:
            with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
        openai_key = str(cfg.get("openai", {}).get("api_key", "") or "")
        smtp_accounts = load_user_smtp_accounts(session_token=session_token, db_path=DEFAULT_DB_PATH)
        mailer_cfg = cfg.get("mailer", {}) if isinstance(cfg, dict) else {}
        safe_accounts = _safe_smtp_accounts(smtp_accounts)
        first_smtp = smtp_accounts[0] if smtp_accounts else {}
        supabase_cfg = cfg.get("supabase", {}) if isinstance(cfg, dict) else {}
        return {
            "openai_api_key": "***" if openai_key and openai_key != "YOUR_OPENAI_API_KEY" else "",
            "smtp_host": first_smtp.get("host", ""),
            "smtp_port": first_smtp.get("port", 587),
            "smtp_email": first_smtp.get("email", ""),
            "smtp_password_set": bool(str(first_smtp.get("password", "") or "").strip()),
            "smtp_accounts": safe_accounts,
            "sending_strategy": normalize_sending_strategy(mailer_cfg.get("sending_strategy", "round_robin")),
            "mail_signature": str(cfg.get("mail_signature", "") or ""),
            "ghost_subject_template": str(cfg.get("ghost_subject_template", DEFAULT_GHOST_SUBJECT_TEMPLATE) or DEFAULT_GHOST_SUBJECT_TEMPLATE),
            "ghost_body_template": str(cfg.get("ghost_body_template", DEFAULT_GHOST_BODY_TEMPLATE) or DEFAULT_GHOST_BODY_TEMPLATE),
            "golden_subject_template": str(cfg.get("golden_subject_template", DEFAULT_GOLDEN_SUBJECT_TEMPLATE) or DEFAULT_GOLDEN_SUBJECT_TEMPLATE),
            "golden_body_template": str(cfg.get("golden_body_template", DEFAULT_GOLDEN_BODY_TEMPLATE) or DEFAULT_GOLDEN_BODY_TEMPLATE),
            "competitor_subject_template": str(cfg.get("competitor_subject_template", DEFAULT_COMPETITOR_SUBJECT_TEMPLATE) or DEFAULT_COMPETITOR_SUBJECT_TEMPLATE),
            "competitor_body_template": str(cfg.get("competitor_body_template", DEFAULT_COMPETITOR_BODY_TEMPLATE) or DEFAULT_COMPETITOR_BODY_TEMPLATE),
            "speed_subject_template": str(cfg.get("speed_subject_template", DEFAULT_SPEED_SUBJECT_TEMPLATE) or DEFAULT_SPEED_SUBJECT_TEMPLATE),
            "speed_body_template": str(cfg.get("speed_body_template", DEFAULT_SPEED_BODY_TEMPLATE) or DEFAULT_SPEED_BODY_TEMPLATE),
            "open_tracking_base_url": str(cfg.get("open_tracking_base_url", "") or ""),
            "hubspot_webhook_url": str(cfg.get("hubspot_webhook_url", "") or ""),
            "google_sheets_webhook_url": str(cfg.get("google_sheets_webhook_url", "") or ""),
            "auto_weekly_report_email": bool(cfg.get("auto_weekly_report_email", True)),
            "auto_monthly_report_email": bool(cfg.get("auto_monthly_report_email", True)),
            "proxy_url": str(cfg.get("proxy_url", "") or ""),
            "proxy_urls": "\n".join(cfg.get("proxy_urls") or []),
            "supabase_url": str(supabase_cfg.get("url", "") or ""),
            "supabase_publishable_key": str(supabase_cfg.get("publishable_key", "") or ""),
            "supabase_service_role_key_set": bool(str(supabase_cfg.get("service_role_key", "") or "").strip()),
            "supabase_primary_mode": bool(supabase_cfg.get("primary_mode", False)),
        }

    @app.put("/api/config")
    def update_config(payload: ConfigUpdateRequest, request: Request) -> dict:
        session_token = require_authenticated_session(request)
        try:
            with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}

        if payload.openai_api_key is not None:
            cfg.setdefault("openai", {})["api_key"] = payload.openai_api_key.strip()

        if payload.sending_strategy is not None:
            mailer_cfg = dict(cfg.get("mailer", {}))
            mailer_cfg["sending_strategy"] = normalize_sending_strategy(payload.sending_strategy)
            cfg["mailer"] = mailer_cfg

        for deprecated_key in ("subject_spintax_template", "initial_email_template", "follow_up_template", "mailer_style_notes"):
            cfg.pop(deprecated_key, None)

        if payload.smtp_accounts is not None:
            save_user_smtp_accounts(
                session_token,
                [account_payload.model_dump() for account_payload in payload.smtp_accounts],
                db_path=DEFAULT_DB_PATH,
            )
        elif any(v is not None for v in [payload.smtp_host, payload.smtp_port, payload.smtp_email, payload.smtp_password]):
            existing_accounts = load_user_smtp_accounts(session_token=session_token, db_path=DEFAULT_DB_PATH)
            existing_first = existing_accounts[0] if existing_accounts else {}
            merged_first = _normalize_single_smtp_account(
                {
                    "host": payload.smtp_host,
                    "port": payload.smtp_port,
                    "email": payload.smtp_email,
                    "password": payload.smtp_password,
                },
                existing_first,
            )
            save_user_smtp_accounts(session_token, [merged_first, *existing_accounts[1:]], db_path=DEFAULT_DB_PATH)

        cfg.pop("smtp_accounts", None)

        if payload.mail_signature is not None:
            cfg["mail_signature"] = payload.mail_signature.strip()

        if payload.ghost_subject_template is not None:
            cfg["ghost_subject_template"] = payload.ghost_subject_template.strip()

        if payload.ghost_body_template is not None:
            cfg["ghost_body_template"] = payload.ghost_body_template.strip()

        if payload.golden_subject_template is not None:
            cfg["golden_subject_template"] = payload.golden_subject_template.strip()

        if payload.golden_body_template is not None:
            cfg["golden_body_template"] = payload.golden_body_template.strip()

        if payload.competitor_subject_template is not None:
            cfg["competitor_subject_template"] = payload.competitor_subject_template.strip()

        if payload.competitor_body_template is not None:
            cfg["competitor_body_template"] = payload.competitor_body_template.strip()

        if payload.speed_subject_template is not None:
            cfg["speed_subject_template"] = payload.speed_subject_template.strip()

        if payload.speed_body_template is not None:
            cfg["speed_body_template"] = payload.speed_body_template.strip()

        if payload.open_tracking_base_url is not None:
            cfg["open_tracking_base_url"] = payload.open_tracking_base_url.strip()

        if payload.hubspot_webhook_url is not None:
            cfg["hubspot_webhook_url"] = payload.hubspot_webhook_url.strip()

        if payload.google_sheets_webhook_url is not None:
            cfg["google_sheets_webhook_url"] = payload.google_sheets_webhook_url.strip()

        if payload.auto_weekly_report_email is not None:
            cfg["auto_weekly_report_email"] = bool(payload.auto_weekly_report_email)

        if payload.auto_monthly_report_email is not None:
            cfg["auto_monthly_report_email"] = bool(payload.auto_monthly_report_email)

        if payload.proxy_url is not None:
            cfg["proxy_url"] = payload.proxy_url.strip()

        if payload.proxy_urls is not None:
            parsed = [p.strip() for p in payload.proxy_urls.splitlines() if p.strip()]
            cfg["proxy_urls"] = parsed

        if any(v is not None for v in [payload.supabase_url, payload.supabase_publishable_key, payload.supabase_service_role_key, payload.supabase_primary_mode]):
            supabase_cfg = dict(cfg.get("supabase", {}))
            if payload.supabase_url is not None:
                supabase_cfg["url"] = payload.supabase_url.strip()
            if payload.supabase_publishable_key is not None:
                supabase_cfg["publishable_key"] = payload.supabase_publishable_key.strip()
            if payload.supabase_service_role_key is not None and payload.supabase_service_role_key.strip():
                supabase_cfg["service_role_key"] = payload.supabase_service_role_key.strip()
            if payload.supabase_primary_mode is not None:
                supabase_cfg["primary_mode"] = bool(payload.supabase_primary_mode)
            cfg["supabase"] = supabase_cfg

        with DEFAULT_CONFIG_PATH.open("w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)

        return {"status": "saved", **load_config_health(DEFAULT_CONFIG_PATH)}

    @app.post("/api/config/test-smtp")
    def test_smtp_connection(payload: SMTPTestRequest, request: Request) -> dict:
        session_token = require_authenticated_session(request)
        try:
            with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}

        existing_accounts = load_user_smtp_accounts(session_token=session_token, db_path=DEFAULT_DB_PATH)
        existing = {}
        if payload.account_index is not None and 0 <= int(payload.account_index) < len(existing_accounts):
            existing = dict(existing_accounts[int(payload.account_index)])

        account = {
            "host": str(payload.host if payload.host is not None else existing.get("host", "")).strip(),
            "port": int(payload.port if payload.port is not None else existing.get("port", 587) or 587),
            "email": str(payload.email if payload.email is not None else existing.get("email", "")).strip(),
            "password": str(existing.get("password", "") or ""),
            "use_tls": bool(payload.use_tls if payload.use_tls is not None else existing.get("use_tls", True)),
            "use_ssl": bool(payload.use_ssl if payload.use_ssl is not None else existing.get("use_ssl", False)),
            "from_name": str(payload.from_name if payload.from_name is not None else existing.get("from_name", "")).strip(),
        }
        if payload.password is not None and payload.password.strip():
            account["password"] = payload.password.strip()

        try:
            send_smtp_test_message(account, SMTP_TEST_RECIPIENT)
            return {"ok": True, "message": f"Test mail sent to {SMTP_TEST_RECIPIENT}"}
        except Exception as exc:
            return {
                "ok": False,
                "message": classify_smtp_error(exc),
                "error": str(exc),
            }

    @app.get("/api/track/open/{token}")
    def track_mail_open(token: str) -> Response:
        safe_token = str(token or "").strip()
        if safe_token:
            now_iso = utc_now_iso()
            try:
                ensure_mailer_campaign_tables(DEFAULT_DB_PATH)
                with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                    conn.row_factory = sqlite3.Row
                    lead_row = conn.execute(
                        """
                        SELECT id, email, COALESCE(NULLIF(user_id, ''), 'legacy') AS user_id
                        FROM leads
                        WHERE open_tracking_token = ?
                        LIMIT 1
                        """,
                        (safe_token,),
                    ).fetchone()
                if lead_row is not None:
                    record_mailer_campaign_event(
                        DEFAULT_DB_PATH,
                        str(lead_row["user_id"] or "legacy"),
                        {
                            "lead_id": int(lead_row["id"]),
                            "email": str(lead_row["email"] or "").strip(),
                            "event_type": "open",
                            "metadata": {"source": "tracking_pixel", "token": safe_token},
                        },
                    )
            except HTTPException:
                logging.debug("Open tracking token did not map to an eligible local lead: %s", safe_token)
            except Exception:
                logging.debug("Failed to update local open tracking for token=%s", safe_token)

            if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
                client = get_supabase_client(DEFAULT_CONFIG_PATH)
                if client is not None:
                    try:
                        rows = client.table("leads").select("id,open_count,first_opened_at").eq("open_tracking_token", safe_token).limit(1).execute().data or []
                        if rows:
                            row = rows[0]
                            payload = {
                                "open_count": int(row.get("open_count") or 0) + 1,
                                "last_opened_at": now_iso,
                            }
                            if not row.get("first_opened_at"):
                                payload["first_opened_at"] = now_iso
                            client.table("leads").update(payload).eq("id", row.get("id")).execute()
                    except Exception:
                        logging.debug("Failed to update Supabase open tracking for token=%s", safe_token)

        return Response(
            content=TRACKING_PIXEL_GIF,
            media_type="image/gif",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.get("/api/supabase-health")
    def supabase_health() -> dict:
        settings = load_supabase_settings(DEFAULT_CONFIG_PATH)
        if not _HAS_SUPABASE:
            return {
                "ok": False,
                "configured": settings["enabled"],
                "error": "Python package 'supabase' is not installed.",
            }

        if not settings["enabled"]:
            return {
                "ok": False,
                "configured": False,
                "error": "Set supabase.url and supabase key (service role or publishable) in config.json or env.",
            }

        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is None:
            return {
                "ok": False,
                "configured": True,
                "error": "Failed to initialize Supabase client.",
            }

        probe_errors: list[str] = []
        for table_name in ["leads", "workers", "delivery_tasks"]:
            try:
                client.table(table_name).select("id").limit(1).execute()
            except Exception as exc:
                probe_errors.append(f"{table_name}: {exc}")

        return {
            "ok": len(probe_errors) == 0,
            "configured": True,
            "has_service_role": settings["has_service_role"],
            "has_publishable": settings["has_publishable"],
            "errors": probe_errors,
        }

    @app.post("/api/supabase/sync-all")
    def supabase_sync_all() -> dict:
        result = sync_all_to_supabase(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)
        if not result["ok"]:
            raise HTTPException(status_code=500, detail=result)
        return result

    @app.post("/api/supabase/migrate-primary")
    def supabase_migrate_primary() -> dict:
        result = sync_all_to_supabase(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)
        if not result["ok"]:
            raise HTTPException(status_code=500, detail=result)

        set_supabase_primary_mode(DEFAULT_CONFIG_PATH, True)
        return {
            "status": "migrated",
            "primary_mode": True,
            **result,
        }

    @app.get("/api/stats")
    def stats(request: Request) -> dict:
        user_id = require_current_user_id(request)
        return get_dashboard_stats(DEFAULT_DB_PATH, user_id=user_id)

    @app.get("/api/reporting/weekly-summary")
    def get_weekly_reporting_summary(request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="advanced_reporting")
        user_id = require_current_user_id(request)
        return build_weekly_report_summary(DEFAULT_DB_PATH, user_id)

    @app.post("/api/reporting/weekly-summary/email")
    def email_weekly_reporting_summary(payload: MonthlyReportEmailRequest, request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="advanced_reporting")
        user_id = require_current_user_id(request)
        summary = build_weekly_report_summary(DEFAULT_DB_PATH, user_id)
        recipient = _load_user_email_for_reports(DEFAULT_DB_PATH, user_id)
        if not recipient:
            raise HTTPException(status_code=422, detail="No signup email is available for this user.")
        account = get_primary_user_smtp_account(user_id=user_id, db_path=DEFAULT_DB_PATH)
        send_weekly_report_email(account, recipient, summary)
        return {"status": "sent", "recipient": recipient, "period_label": summary.get("period_label")}

    @app.get("/api/reporting/monthly-summary")
    def get_monthly_reporting_summary(request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="advanced_reporting")
        user_id = require_current_user_id(request)
        return build_monthly_report_summary(DEFAULT_DB_PATH, user_id)

    @app.get("/api/reporting/monthly-summary.pdf")
    def download_monthly_reporting_pdf(request: Request) -> Response:
        resolve_plan_access_context(request, feature_key="advanced_reporting")
        user_id = require_current_user_id(request)
        summary = build_monthly_report_summary(DEFAULT_DB_PATH, user_id)
        pdf_bytes = _build_report_pdf(
            f"Sniped Monthly Report — {summary.get('month_label', 'Current Month')}",
            summary,
            period_key="monthly",
        )
        headers = {
            "Content-Disposition": 'attachment; filename="sniped-monthly-report.pdf"',
            "Cache-Control": "no-store",
        }
        return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)

    @app.post("/api/reporting/monthly-summary/email")
    def email_monthly_reporting_summary(payload: MonthlyReportEmailRequest, request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="advanced_reporting")
        user_id = require_current_user_id(request)
        summary = build_monthly_report_summary(DEFAULT_DB_PATH, user_id)
        recipient = _load_user_email_for_reports(DEFAULT_DB_PATH, user_id)
        if not recipient:
            raise HTTPException(status_code=422, detail="No signup email is available for this user.")
        account = get_primary_user_smtp_account(user_id=user_id, db_path=DEFAULT_DB_PATH)
        pdf_response = download_monthly_reporting_pdf(request)
        send_monthly_report_email(account, recipient, summary, pdf_response.body)
        return {"status": "sent", "recipient": recipient, "month_label": summary.get("month_label")}

    @app.get("/api/recommend-niche")
    def recommend_niche(request: Request) -> dict:
        session_token = require_authenticated_session(request)
        user_id = resolve_user_id_from_session_token(session_token)
        billing_context = load_user_billing_context(session_token)
        credits_balance = int(billing_context.get("credits_balance") or 0)
        credits_limit = int(
            billing_context.get("monthly_quota")
            or billing_context.get("monthly_limit")
            or billing_context.get("credits_limit")
            or DEFAULT_MONTHLY_CREDIT_LIMIT
        )
        plan_key = str(billing_context.get("plan_key") or DEFAULT_PLAN_KEY).strip().lower() or DEFAULT_PLAN_KEY
        is_free_plan = plan_key == "free" and not bool(billing_context.get("subscription_active"))
        selected_country_code = normalize_country_value(
            request.query_params.get("country") or request.query_params.get("country_code"),
            None,
        )
        force_refresh = str(request.query_params.get("refresh") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
        refresh_window_days = _niche_recommendation_refresh_window_days(is_free_plan)
        refresh_window_seconds = _niche_recommendation_refresh_window_seconds(is_free_plan)
        refresh_window_hours = round(refresh_window_seconds / 3600, 2)

        if not force_refresh:
            cached_result = _get_cached_niche_recommendation(user_id, selected_country_code)
            if cached_result:
                response = {
                    **cached_result,
                    "cached": True,
                    "credits_charged": 0,
                    "credits_balance": credits_balance,
                    "credits_limit": credits_limit,
                    "refresh_window_days": refresh_window_days,
                    "refresh_window_hours": refresh_window_hours,
                    "selected_country_code": str(cached_result.get("selected_country_code") or selected_country_code).upper(),
                }
                if is_free_plan:
                    response.update({
                        "monthly_limited": True,
                        "free_plan_niche_limit": FREE_PLAN_NICHE_RECOMMENDATIONS_PER_MONTH,
                    })
                return response

        stored_result = _load_runtime_niche_recommendation(
            user_id,
            is_free_plan=is_free_plan,
            country_code=selected_country_code,
            max_age_seconds=refresh_window_seconds,
        )
        if stored_result and (is_free_plan or not force_refresh):
            _set_cached_niche_recommendation(user_id, stored_result, ttl_seconds=refresh_window_seconds, country_code=selected_country_code)
            response = {
                **stored_result,
                "cached": True,
                "credits_charged": 0,
                "credits_balance": credits_balance,
                "credits_limit": credits_limit,
                "refresh_window_days": refresh_window_days,
                "refresh_window_hours": refresh_window_hours,
                "selected_country_code": str(stored_result.get("selected_country_code") or selected_country_code).upper(),
            }
            if is_free_plan:
                response.update({
                    "monthly_limited": True,
                    "free_plan_niche_limit": FREE_PLAN_NICHE_RECOMMENDATIONS_PER_MONTH,
                })
            return response

        result = get_niche_recommendation(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH, country_code=selected_country_code)
        if not isinstance(result, dict):
            result = {
                "generated_at": utc_now_iso(),
                "recommendations": [],
                "top_pick": {},
                "selected_country_code": selected_country_code,
            }
        if force_refresh and not is_free_plan and stored_result:
            result = _promote_alternate_niche_choice(result, stored_result)
        if not str(result.get("generated_at") or "").strip():
            result["generated_at"] = utc_now_iso()
        result.setdefault("selected_country_code", selected_country_code)

        _set_cached_niche_recommendation(user_id, result, ttl_seconds=refresh_window_seconds, country_code=selected_country_code)
        _store_runtime_niche_recommendation(user_id, is_free_plan=is_free_plan, result=result, country_code=selected_country_code)

        response = {
            **result,
            "cached": False,
            "credits_charged": 0,
            "credits_balance": credits_balance,
            "credits_limit": credits_limit,
            "refresh_window_days": refresh_window_days,
            "refresh_window_hours": refresh_window_hours,
        }
        if is_free_plan:
            response.update({
                "monthly_limited": True,
                "free_plan_niche_limit": FREE_PLAN_NICHE_RECOMMENDATIONS_PER_MONTH,
            })
        return response

    def auth_required(func: Callable) -> Callable:
        signature = inspect.signature(func)

        def _resolve_request(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Request:
            bound = signature.bind_partial(*args, **kwargs)
            request = bound.arguments.get("request")
            if isinstance(request, Request):
                return request
            for value in bound.arguments.values():
                if isinstance(value, Request):
                    return value
            raise RuntimeError("@auth_required requires a Request parameter.")

        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                request = _resolve_request(args, kwargs)
                request.state.current_user_id = require_current_user_id(request)
                return await func(*args, **kwargs)

            async_wrapper.__signature__ = signature  # type: ignore[attr-defined]
            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            request = _resolve_request(args, kwargs)
            request.state.current_user_id = require_current_user_id(request)
            return func(*args, **kwargs)

        sync_wrapper.__signature__ = signature  # type: ignore[attr-defined]
        return sync_wrapper

    @app.post("/api/revenue")
    @auth_required
    def add_revenue(payload: RevenueEntryRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            inserted = client.table("revenue_log").insert(
                {
                    "user_id": user_id,
                    "amount": payload.amount,
                    "service_type": payload.service_type.strip(),
                    "lead_name": payload.lead_name.strip() if payload.lead_name else None,
                    "lead_id": payload.lead_id,
                    "is_recurring": 1 if payload.is_recurring else 0,
                    "date": utc_now_iso(),
                }
            ).execute().data or []

            revenue_id = int(inserted[0].get("id")) if inserted else None
            return {
                "id": revenue_id,
                "amount": payload.amount,
                "service_type": payload.service_type,
                "lead_name": payload.lead_name,
                "is_recurring": payload.is_recurring,
            }

        db_path = DEFAULT_DB_PATH
        ensure_revenue_log_table(db_path)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO revenue_log (user_id, amount, service_type, lead_name, lead_id, is_recurring, date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    payload.amount,
                    payload.service_type.strip(),
                    payload.lead_name.strip() if payload.lead_name else None,
                    payload.lead_id,
                    1 if payload.is_recurring else 0,
                    utc_now_iso(),
                ),
            )
            conn.commit()
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        return {
            "id": cursor.lastrowid,
            "amount": payload.amount,
            "service_type": payload.service_type,
            "lead_name": payload.lead_name,
            "is_recurring": payload.is_recurring,
        }

    @app.get("/api/revenue")
    @auth_required
    def get_revenue(request: Request, limit: int = Query(10, ge=1, le=100)) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            rows = supabase_select_rows(
                client,
                "revenue_log",
                columns="id,amount,service_type,lead_name,lead_id,is_recurring,date",
                filters={"user_id": user_id},
                order_by="id",
                desc=True,
                limit=limit,
            )
            return {"items": rows}

        db_path = DEFAULT_DB_PATH
        ensure_revenue_log_table(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, amount, service_type, lead_name, lead_id, is_recurring, date
                FROM revenue_log
                WHERE user_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        return {"items": [dict(row) for row in rows]}

    @app.get("/api/tasks")
    def tasks(request: Request) -> dict:
        user_id = require_current_user_id(request)
        reconcile_orphaned_active_tasks(app, DEFAULT_DB_PATH)
        return {
            "tasks": fetch_all_latest_tasks(DEFAULT_DB_PATH, user_id=user_id),
            "history": fetch_task_history(DEFAULT_DB_PATH, limit=TASK_HISTORY_LIMIT, user_id=user_id),
            "autopilot": {
                "next_drip_at": get_runtime_value(DEFAULT_DB_PATH, "next_drip_at"),
                "high_score_threshold": HIGH_AI_SCORE_THRESHOLD,
            },
        }

    @app.get("/api/task")
    def task_alias(request: Request, task_type: str = Query("scrape")) -> dict:
        user_id = require_current_user_id(request)
        reconcile_orphaned_active_tasks(app, DEFAULT_DB_PATH)
        normalized_task_type = str(task_type or "scrape")
        latest_task = fetch_latest_task(DEFAULT_DB_PATH, normalized_task_type, user_id=user_id)
        if not latest_task:
            return {"running": False, "task_type": normalized_task_type, "result": {}, "status": "idle"}
        status = str(latest_task.get("status") or "").lower()
        return {
            **latest_task,
            "running": status in ACTIVE_TASK_STATUSES,
            "task_type": latest_task.get("task_type") or normalized_task_type,
            "result": latest_task.get("result") if isinstance(latest_task.get("result"), dict) else {},
            "status": latest_task.get("status") or "idle",
        }

    @app.post("/api/tasks/reorder")
    def reorder_tasks(payload: TaskReorderRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        task_ids = [int(task_id) for task_id in payload.task_ids if int(task_id) > 0]
        if not task_ids:
            return {"status": "ok", "updated": 0}

        seen: set[int] = set()
        unique_task_ids: list[int] = []
        for task_id in task_ids:
            if task_id in seen:
                continue
            seen.add(task_id)
            unique_task_ids.append(task_id)

        now_iso = utc_now_iso()

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            owned_rows = (
                client.table("delivery_tasks")
                .select("id")
                .eq("user_id", user_id)
                .in_("id", unique_task_ids)
                .execute()
                .data
                or []
            )
            owned_ids = {int(row.get("id")) for row in owned_rows if row.get("id") is not None}
            if len(owned_ids) != len(unique_task_ids):
                raise HTTPException(status_code=404, detail="One or more tasks were not found")

            for index, task_id in enumerate(unique_task_ids, start=1):
                client.table("delivery_tasks").update({"position": index, "updated_at": now_iso}).eq("id", task_id).eq("user_id", user_id).execute()
            return {"status": "ok", "updated": len(unique_task_ids)}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            existing_rows = conn.execute(
                f"SELECT id FROM delivery_tasks WHERE user_id = ? AND id IN ({','.join(['?'] * len(unique_task_ids))})",
                [user_id, *unique_task_ids],
            ).fetchall()
            existing_ids = {int(row["id"]) for row in existing_rows}
            if len(existing_ids) != len(unique_task_ids):
                raise HTTPException(status_code=404, detail="One or more tasks were not found")

            for index, task_id in enumerate(unique_task_ids, start=1):
                conn.execute(
                    "UPDATE delivery_tasks SET position = ?, updated_at = ? WHERE id = ? AND user_id = ?",
                    (index, now_iso, task_id, user_id),
                )
            conn.commit()

        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        return {"status": "ok", "updated": len(unique_task_ids)}

    @app.get("/api/leads")
    def get_leads(
        request: Request,
        limit: int = Query(50, ge=1, le=5000),
        page: int = Query(1, ge=1),
        status: Optional[str] = Query(default=None),
        search: Optional[str] = Query(default=None),
        sort: str = Query("recent"),
        quick_filter: str = Query("all"),
        include_blacklisted: bool = Query(default=False),
    ) -> dict:
        user_id = require_current_user_id(request)
        page_size = max(1, min(int(limit or 50), 200))
        page_number = max(1, int(page or 1))
        offset = max(0, (page_number - 1) * page_size)
        normalized_status = str(status or "").strip().lower()
        normalized_search = str(search or "").strip().lower()
        normalized_sort = str(sort or "recent").strip().lower() or "recent"
        normalized_quick_filter = str(quick_filter or "all").strip().lower() or "all"

        cache_scope = f"{str(DEFAULT_DB_PATH)}:{int(is_supabase_primary_enabled(DEFAULT_CONFIG_PATH))}"
        cache_key = f"{cache_scope}:{user_id}:{page_size}:{page_number}:{normalized_status}:{normalized_search}:{normalized_sort}:{normalized_quick_filter}:{int(include_blacklisted)}"
        cached = _get_cached_leads(cache_key)
        if cached is not None:
            return cached

        def _post_process_rows(raw_rows: list[dict]) -> list[dict]:
            normalized_rows: list[dict] = []
            for raw in raw_rows:
                item = dict(raw)
                item["insecure_site"] = bool(item.get("insecure_site"))
                item.setdefault("enrichment_status", "pending")
                normalized_rows.append(_augment_lead_with_deep_intelligence(item))

            if normalized_status:
                normalized_rows = [
                    row for row in normalized_rows
                    if str(row.get("status") or "pending").strip().lower() == normalized_status
                ]

            if not include_blacklisted:
                normalized_rows = [row for row in normalized_rows if not _lead_is_blacklisted_status(row.get("status"))]

            if normalized_search:
                normalized_rows = [
                    row
                    for row in normalized_rows
                    if normalized_search in " ".join(
                        [
                            str(row.get("business_name") or ""),
                            str(row.get("contact_name") or ""),
                            str(row.get("email") or ""),
                            str(row.get("website_url") or ""),
                            str(row.get("address") or ""),
                            str(row.get("search_keyword") or ""),
                            " ".join(_lead_normalize_string_list(row.get("tech_stack"), limit=5)),
                        ]
                    ).lower()
                ]

            if normalized_quick_filter not in {"", "all"}:
                normalized_rows = [row for row in normalized_rows if _lead_matches_quick_filter(row, normalized_quick_filter)]

            normalized_rows.sort(
                key=lambda row: _lead_dashboard_sort_key(row, normalized_sort),
                reverse=normalized_sort != "name",
            )
            return normalized_rows

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            supabase_columns = (
                "id,business_name,contact_name,email,website_url,phone_number,rating,review_count,address,"
                "search_keyword,insecure_site,main_shortcoming,ai_description,ai_score,client_tier,status,enrichment_status,scraped_at,enriched_at,"
                "sent_at,last_contacted_at,follow_up_count,generated_email_body,crm_comment,status_updated_at,last_sender_email,"
                "is_ads_client,is_website_client,worker_id,assigned_worker_at,paid_at,enrichment_data,pipeline_stage,client_folder_id,"
                "open_tracking_token,open_count,first_opened_at,last_opened_at,"
                "phone_formatted,phone_type,created_at"
            )
            legacy_columns = (
                "id,business_name,contact_name,email,website_url,phone_number,rating,review_count,address,"
                "search_keyword,insecure_site,main_shortcoming,ai_score,client_tier,status,scraped_at,enriched_at,"
                "sent_at,last_contacted_at,follow_up_count,crm_comment,status_updated_at,last_sender_email,enrichment_data,pipeline_stage,client_folder_id,"
                "worker_id,assigned_worker_at,paid_at"
            )
            filters = {"user_id": user_id}
            try:
                rows = supabase_select_rows(
                    client,
                    "leads",
                    columns=supabase_columns,
                    filters=filters,
                    order_by="created_at" if normalized_sort in {"recent", "best"} else ("business_name" if normalized_sort == "name" else "ai_score"),
                    desc=normalized_sort != "name",
                )
            except Exception as exc:
                logging.warning("Supabase leads query fallback to legacy columns: %s", exc)
                try:
                    rows = supabase_select_rows(
                        client,
                        "leads",
                        columns=legacy_columns,
                        filters=filters,
                        order_by="id",
                        desc=True,
                    )
                except Exception as exc2:
                    logging.warning("Supabase leads user_id filter failed (column missing?), returning unfiltered: %s", exc2)
                    rows = supabase_select_rows(
                        client,
                        "leads",
                        columns=legacy_columns,
                        order_by="id",
                        desc=True,
                    )
                for row in rows:
                    row.setdefault("ai_description", None)
                    row.setdefault("enrichment_status", "pending")
                    row.setdefault("generated_email_body", None)
                    row.setdefault("is_ads_client", 0)
                    row.setdefault("is_website_client", 0)
                    row.setdefault("open_tracking_token", None)
                    row.setdefault("open_count", 0)
                    row.setdefault("first_opened_at", None)
                    row.setdefault("last_opened_at", None)
                    row.setdefault("phone_formatted", None)
                    row.setdefault("phone_type", None)
                    row.setdefault("created_at", row.get("scraped_at"))

            filtered_rows = _post_process_rows(rows)
            total = len(filtered_rows)
            page_items = filtered_rows[offset: offset + page_size]
            result = {
                "count": len(page_items),
                "total": total,
                "page": page_number,
                "page_size": page_size,
                "has_more": offset + len(page_items) < total,
                "items": page_items,
            }
            _set_cached_leads(cache_key, result)
            return result

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        select_clause = """
            SELECT
                id,
                business_name,
                contact_name,
                email,
                website_url,
                phone_number,
                rating,
                review_count,
                address,
                search_keyword,
                insecure_site,
                main_shortcoming,
                ai_description,
                ai_score,
                client_tier,
                status,
                enrichment_status,
                scraped_at,
                created_at,
                enriched_at,
                sent_at,
                last_contacted_at,
                follow_up_count,
                open_count,
                first_opened_at,
                last_opened_at,
                generated_email_body,
                crm_comment,
                status_updated_at,
                last_sender_email,
                is_ads_client,
                is_website_client,
                worker_id,
                assigned_worker_at,
                paid_at,
                enrichment_data,
                pipeline_stage,
                client_folder_id
            FROM leads
        """
        where_clauses = ["user_id = ?"]
        params: list[Any] = [user_id]

        if normalized_status:
            where_clauses.append("LOWER(COALESCE(status, 'pending')) = ?")
            params.append(normalized_status)

        if not include_blacklisted:
            where_clauses.append("LOWER(COALESCE(status, 'pending')) NOT IN ('blacklisted', 'skipped (unsubscribed)')")

        if normalized_search:
            search_like = f"%{normalized_search}%"
            where_clauses.append(
                "(" 
                "LOWER(COALESCE(business_name, '')) LIKE ? OR "
                "LOWER(COALESCE(contact_name, '')) LIKE ? OR "
                "LOWER(COALESCE(email, '')) LIKE ? OR "
                "LOWER(COALESCE(website_url, '')) LIKE ? OR "
                "LOWER(COALESCE(address, '')) LIKE ? OR "
                "LOWER(COALESCE(search_keyword, '')) LIKE ?"
                ")"
            )
            params.extend([search_like] * 6)

        order_clause = "datetime(COALESCE(created_at, scraped_at)) DESC, id DESC"
        if normalized_sort == "name":
            order_clause = "LOWER(COALESCE(business_name, '')) ASC, id DESC"
        elif normalized_sort in {"score", "best"}:
            order_clause = "COALESCE(ai_score, 0) DESC, datetime(COALESCE(created_at, scraped_at)) DESC, id DESC"

        where_sql = " AND ".join(where_clauses)
        count_query = f"SELECT COUNT(*) FROM leads WHERE {where_sql}"
        query = f"{select_clause} WHERE {where_sql} ORDER BY {order_clause} LIMIT ? OFFSET ?"

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            total = int(conn.execute(count_query, params).fetchone()[0] or 0)
            rows = conn.execute(query, [*params, page_size, offset]).fetchall()

        page_items = _post_process_rows([dict(row) for row in rows])
        result = {
            "count": len(page_items),
            "total": total,
            "page": page_number,
            "page_size": page_size,
            "has_more": offset + len(page_items) < total,
            "items": page_items,
        }
        _set_cached_leads(cache_key, result)
        return result

    @app.post("/api/leads/manual")
    def create_manual_lead(payload: ManualLeadRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            try:
                insert_result = insert_manual_lead_supabase(client, payload)
                if insert_result.get("lead_id") is not None:
                    client.table("leads").update({"user_id": user_id}).eq("id", int(insert_result.get("lead_id"))).execute()
            except Exception as e:
                logging.error(f"Supabase insert error: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Failed to insert lead: {str(e)}")

            lead_id = insert_result.get("lead_id")
            blacklisted_synced = sync_blacklisted_leads_supabase(DEFAULT_CONFIG_PATH)
            _invalidate_leads_cache()
            return {"status": "created", "lead_id": lead_id, "blacklisted_synced": blacklisted_synced}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO leads (
                    user_id,
                    business_name,
                    website_url,
                    phone_number,
                    rating,
                    review_count,
                    address,
                    search_keyword,
                    contact_name,
                    email,
                    status,
                    client_tier,
                    status_updated_at,
                    crm_comment
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                (
                    user_id,
                    payload.business_name.strip(),
                    None,
                    None,
                    None,
                    None,
                    f"Manual Entry | {payload.email.strip().lower()}",
                    "manual-entry",
                    payload.contact_name.strip(),
                    payload.email.strip(),
                    "Pending",
                    "standard",
                    "Manually added lead",
                ),
            )
            conn.commit()
            lead_id = int(cursor.lastrowid)

        blacklisted_synced = sync_blacklisted_leads(db_path)
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        return {"status": "created", "lead_id": lead_id, "blacklisted_synced": blacklisted_synced}

    @app.post("/api/leads/score")
    async def score_leads_endpoint(payload: BulkLeadScoreRequest, request: Request):
        payload_dict = payload.dict()
        session_token = require_authenticated_session(request, fallback_token=payload_dict.get("token", ""))
        require_feature_access(session_token, "ai_lead_scoring", db_path=str(DEFAULT_DB_PATH))

        client, model_name = load_openai_client(DEFAULT_CONFIG_PATH)
        if client is None:
            raise HTTPException(status_code=503, detail="OpenAI API key not configured.")

        user_niche = str(payload.niche_override or "").strip()

        factory = PromptFactory()
        system_prompt = factory.get_lead_score_system_prompt()

        results = []
        scored_count = 0
        shown_count = 0

        for lead in payload.leads:
            lead_dict = lead.dict()
            effective_niche = str(lead.niche or user_niche or "B2B Service Provider").strip()
            user_prompt = factory.get_lead_score_user_prompt(lead_dict, effective_niche)

            try:
                response = client.chat.completions.create(
                    model=str(model_name or DEFAULT_AI_MODEL).strip() or DEFAULT_AI_MODEL,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    max_tokens=400,
                    temperature=0.3,
                )
                raw_text = response.choices[0].message.content.strip()
                import re as _re
                json_match = _re.search(r'\{.*\}', raw_text, _re.DOTALL)
                scored_data = json.loads(json_match.group() if json_match else raw_text)

                lead_score = max(0, min(100, int(scored_data.get("lead_score", 0))))

                if lead_score >= 90:
                    priority_tier = "Hot"
                elif lead_score >= 70:
                    priority_tier = "Warm"
                else:
                    priority_tier = "Cold"

                if lead_score >= 70:
                    estimated_value = "High"
                elif lead_score >= 40:
                    estimated_value = "Medium"
                else:
                    estimated_value = "Low"

                identified_problems = scored_data.get("identified_problems", [])
                if not isinstance(identified_problems, list):
                    identified_problems = []

                ai_high_priority = bool(scored_data.get("high_priority", False))
                conversion_leak = any(
                    "contact" in str(problem).lower() or "call" in str(problem).lower()
                    for problem in identified_problems
                )
                high_priority = ai_high_priority or conversion_leak

                competitor_name = str(scored_data.get("competitor_name", "") or "").strip()
                location_name = str(scored_data.get("location", "") or lead.location or "").strip()
                market_takeover_message = str(scored_data.get("market_takeover_message", "") or "").strip()
                if not market_takeover_message:
                    market_takeover_message = (
                        "Tole podjetje nima zavarovane strani (SSL) in nima nastavljenih oglasov, "
                        f"ceprav njihova konkurenca [{competitor_name or 'Tekmec'}] trenutno zaseda ves trg v [{location_name or 'Kraj'}]."
                    )

                # Hide Cold leads from user-facing output.
                if priority_tier != "Cold":
                    shown_count += 1
                    results.append({
                        "lead_id": lead.lead_id,
                        "company_name": scored_data.get("company_name", lead.business_name),
                        "lead_score": lead_score,
                        "priority_tier": priority_tier,
                        "high_priority": high_priority,
                        "identified_problems": identified_problems,
                        "insider_hook": scored_data.get("insider_hook", ""),
                        "estimated_value": estimated_value,
                        "competitor_name": competitor_name,
                        "location": location_name,
                        "market_takeover_message": market_takeover_message,
                        "error": None,
                    })
                scored_count += 1
            except Exception as exc:
                logging.warning("Lead scoring failed for %s: %s", lead.business_name, exc)
                results.append({
                    "lead_id": lead.lead_id,
                    "company_name": lead.business_name,
                    "lead_score": 0,
                    "priority_tier": "Cold",
                    "high_priority": False,
                    "identified_problems": [],
                    "insider_hook": "",
                    "estimated_value": "Low",
                    "competitor_name": "",
                    "location": str(lead.location or "").strip(),
                    "market_takeover_message": (
                        "Tole podjetje nima zavarovane strani (SSL) in nima nastavljenih oglasov, "
                        "ceprav njihova konkurenca [Tekmec] trenutno zaseda ves trg v [Kraj]."
                    ),
                    "error": str(exc),
                })

        if scored_count > 0:
            try:
                ensure_users_table(DEFAULT_DB_PATH)
                with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                    conn.execute(
                        "UPDATE users SET credits_balance = MAX(0, credits_balance - ?) WHERE token = ?",
                        (scored_count, session_token),
                    )
                    conn.commit()
            except Exception as exc:
                logging.warning("Credit deduction failed for lead scoring: %s", exc)

        return {
            "results": results,
            "scored": scored_count,
            "shown": shown_count,
            "total": len(payload.leads),
        }

    @app.get("/api/unsubscribe/{email}", response_class=HTMLResponse)
    def unsubscribe_email(email: str) -> HTMLResponse:
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            add_blacklist_entry_supabase(
                DEFAULT_CONFIG_PATH,
                kind="email",
                value=email,
                reason="Unsubscribe link",
            )
        else:
            add_blacklist_entry(
                db_path,
                kind="email",
                value=email,
                reason="Unsubscribe link",
            )

        _invalidate_leads_cache()
        return HTMLResponse(
            content=(
                "<!doctype html><html><head><meta charset='utf-8'><title>Unsubscribed</title>"
                "<meta name='viewport' content='width=device-width, initial-scale=1'>"
                "<style>body{font-family:Inter,Segoe UI,Arial,sans-serif;background:#020617;color:#e2e8f0;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;padding:24px;}"
                ".card{max-width:520px;background:#0f172a;border:1px solid rgba(148,163,184,.2);border-radius:20px;padding:28px;box-shadow:0 20px 60px rgba(15,23,42,.45);}"
                "h1{margin:0 0 10px;font-size:28px;color:#f8fafc;}p{margin:0;color:#cbd5e1;line-height:1.6;}strong{color:#facc15;}</style></head>"
                "<body><div class='card'><h1>You're unsubscribed</h1>"
                "<p><strong>Done.</strong> This address has been added to the do-not-contact list and future alerts will be blocked.</p>"
                "</div></body></html>"
            ),
            status_code=200,
        )

    @app.get("/api/blacklist")
    def get_blacklist_entries(request: Request) -> dict:
        require_current_user_id(request)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            items = (
                client.table("lead_blacklist")
                .select("id,kind,value,reason,created_at")
                .order("created_at", desc=True)
                .limit(200)
                .execute()
                .data
                or []
            )
            return {"items": items, "count": len(items)}

        db_path = DEFAULT_DB_PATH
        ensure_blacklist_table(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, kind, value, reason, created_at FROM lead_blacklist ORDER BY datetime(created_at) DESC, id DESC LIMIT 200"
            ).fetchall()
        return {
            "items": [dict(row) for row in rows],
            "count": len(rows),
        }

    @app.post("/api/blacklist")
    def create_blacklist_entry(payload: BlacklistEntryRequest, request: Request) -> dict:
        require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            return add_blacklist_entry_supabase(
                DEFAULT_CONFIG_PATH,
                kind=payload.kind,
                value=payload.value,
                reason=payload.reason or "Manual blacklist",
            )

        return add_blacklist_entry(
            db_path,
            kind=payload.kind,
            value=payload.value,
            reason=payload.reason or "Manual blacklist",
        )

    @app.delete("/api/blacklist")
    def delete_blacklist_entry(request: Request, kind: str = Query(...), value: str = Query(...)) -> dict:
        require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            return remove_blacklist_entry_supabase(DEFAULT_CONFIG_PATH, kind=kind, value=value)

        return remove_blacklist_entry(db_path, kind=kind, value=value)

    @app.post("/api/leads/{lead_id}/blacklist")
    def blacklist_lead(lead_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            rows = client.table("leads").select("id,user_id").eq("id", lead_id).limit(1).execute().data or []
            if not rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            return blacklist_lead_and_matches_supabase(lead_id, "Manual blacklist", DEFAULT_CONFIG_PATH)

        with sqlite3.connect(db_path) as conn:
            owner_row = conn.execute("SELECT user_id FROM leads WHERE id = ?", (lead_id,)).fetchone()
            if owner_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(owner_row[0] or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

        return blacklist_lead_and_matches(db_path, lead_id, "Manual blacklist")

    @app.delete("/api/leads/{lead_id}/blacklist")
    def unblacklist_lead(lead_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            rows = client.table("leads").select("id,user_id").eq("id", lead_id).limit(1).execute().data or []
            if not rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            return remove_lead_blacklist_and_matches_supabase(lead_id, DEFAULT_CONFIG_PATH)

        with sqlite3.connect(db_path) as conn:
            owner_row = conn.execute("SELECT user_id FROM leads WHERE id = ?", (lead_id,)).fetchone()
            if owner_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(owner_row[0] or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

        return remove_lead_blacklist_and_matches(db_path, lead_id)

    @app.patch("/api/leads/{lead_id}/status")
    def update_lead_status(lead_id: int, payload: LeadStatusRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        requested_status = payload.status.strip()
        requested_status_normalized = requested_status.lower()
        requested_pipeline = requested_status if requested_status_normalized in {"scraped", "contacted", "replied", "won (paid)", "won paid", "won"} else None
        next_status = _status_from_pipeline_stage(requested_status, fallback_status=requested_status) if requested_pipeline else requested_status
        next_status_normalized = next_status.lower()
        pipeline_stage_value = _derive_pipeline_stage(status=next_status, pipeline_stage=requested_pipeline)
        now_iso = utc_now_iso()

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            if next_status_normalized == "blacklisted":
                owner_rows = client.table("leads").select("id,user_id").eq("id", lead_id).limit(1).execute().data or []
                if not owner_rows:
                    raise HTTPException(status_code=404, detail="Lead not found")
                if str(owner_rows[0].get("user_id") or "") != user_id:
                    raise HTTPException(status_code=403, detail="Forbidden")
                return blacklist_lead_and_matches_supabase(lead_id, "Manual status blacklist", DEFAULT_CONFIG_PATH)

            lead_rows = client.table("leads").select("id,user_id,paid_at,sent_at,last_contacted_at,reply_detected_at").eq("id", lead_id).limit(1).execute().data or []
            if not lead_rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

            existing_paid_at = lead_rows[0].get("paid_at")
            existing_sent_at = lead_rows[0].get("sent_at")
            existing_last_contacted_at = lead_rows[0].get("last_contacted_at")
            existing_reply_detected_at = lead_rows[0].get("reply_detected_at")
            paid_at_value = now_iso if next_status_normalized == "paid" and not existing_paid_at else existing_paid_at
            sent_at_value = now_iso if pipeline_stage_value in {"Contacted", "Replied", "Won (Paid)"} and not existing_sent_at else existing_sent_at
            last_contacted_at_value = now_iso if pipeline_stage_value in {"Contacted", "Replied", "Won (Paid)"} else existing_last_contacted_at
            reply_detected_at_value = now_iso if pipeline_stage_value == "Replied" and not existing_reply_detected_at else existing_reply_detected_at

            client.table("leads").update(
                {
                    "status": next_status,
                    "pipeline_stage": pipeline_stage_value,
                    "status_updated_at": now_iso,
                    "next_mail_at": None,
                    "paid_at": paid_at_value,
                    "sent_at": sent_at_value,
                    "last_contacted_at": last_contacted_at_value,
                    "reply_detected_at": reply_detected_at_value,
                }
            ).eq("id", lead_id).execute()

            auto_assign_result = None
            delivery_result = None
            if next_status_normalized == "paid":
                auto_assign_result = auto_assign_worker_to_paid_lead_supabase(lead_id, DEFAULT_CONFIG_PATH)
                delivery_result = ensure_delivery_task_for_paid_lead_supabase(lead_id, DEFAULT_CONFIG_PATH)

            _invalidate_leads_cache()
            return {
                "status": "updated",
                "lead_id": lead_id,
                "new_status": next_status,
                "pipeline_stage": pipeline_stage_value,
                "auto_assign": auto_assign_result,
                "delivery_task": delivery_result,
            }

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if next_status_normalized == "blacklisted":
            with sqlite3.connect(db_path) as conn:
                owner_row = conn.execute("SELECT user_id FROM leads WHERE id = ?", (lead_id,)).fetchone()
                if owner_row is None:
                    raise HTTPException(status_code=404, detail="Lead not found")
                if str(owner_row[0] or "") != user_id:
                    raise HTTPException(status_code=403, detail="Forbidden")
            return blacklist_lead_and_matches(db_path, lead_id, "Manual status blacklist")

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            owner_row = conn.execute(
                "SELECT user_id, paid_at, sent_at, last_contacted_at, reply_detected_at FROM leads WHERE id = ?",
                (lead_id,),
            ).fetchone()
            if owner_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(owner_row["user_id"] or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            paid_at_value = now_iso if next_status_normalized == "paid" and not owner_row["paid_at"] else owner_row["paid_at"]
            sent_at_value = now_iso if pipeline_stage_value in {"Contacted", "Replied", "Won (Paid)"} and not owner_row["sent_at"] else owner_row["sent_at"]
            last_contacted_at_value = now_iso if pipeline_stage_value in {"Contacted", "Replied", "Won (Paid)"} else owner_row["last_contacted_at"]
            reply_detected_at_value = now_iso if pipeline_stage_value == "Replied" and not owner_row["reply_detected_at"] else owner_row["reply_detected_at"]
            cursor = conn.execute(
                """
                UPDATE leads
                SET
                    status = ?,
                    pipeline_stage = ?,
                    status_updated_at = CURRENT_TIMESTAMP,
                    next_mail_at = NULL,
                    paid_at = CASE WHEN ? IS NOT NULL THEN COALESCE(paid_at, ?) ELSE paid_at END,
                    sent_at = CASE WHEN ? IS NOT NULL THEN COALESCE(sent_at, ?) ELSE sent_at END,
                    last_contacted_at = CASE WHEN ? IS NOT NULL THEN ? ELSE last_contacted_at END,
                    reply_detected_at = CASE WHEN ? IS NOT NULL THEN COALESCE(reply_detected_at, ?) ELSE reply_detected_at END
                WHERE id = ?
                """,
                (
                    next_status,
                    pipeline_stage_value,
                    paid_at_value,
                    paid_at_value,
                    sent_at_value,
                    sent_at_value,
                    last_contacted_at_value,
                    last_contacted_at_value,
                    reply_detected_at_value,
                    reply_detected_at_value,
                    lead_id,
                ),
            )
            conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Lead not found")

        auto_assign_result = None
        delivery_result = None
        if next_status_normalized == "paid":
            auto_assign_result = auto_assign_worker_to_paid_lead(db_path, lead_id)
            delivery_result = ensure_delivery_task_for_paid_lead(db_path, lead_id)

        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        _invalidate_leads_cache()

        return {
            "status": "updated",
            "lead_id": lead_id,
            "new_status": next_status,
            "pipeline_stage": pipeline_stage_value,
            "auto_assign": auto_assign_result,
            "delivery_task": delivery_result,
        }

    @app.get("/api/saved-segments")
    def get_saved_segments(request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            rows = supabase_select_rows(
                client,
                "SavedSegments",
                columns="id,user_id,name,filters_json,created_at,updated_at",
                filters={"user_id": user_id},
                order_by="updated_at",
                desc=True,
                limit=100,
            )
            return {"items": [_normalize_saved_segment_row(row) for row in rows]}

        return {"items": list_saved_segments(DEFAULT_DB_PATH, user_id)}

    @app.post("/api/saved-segments")
    def save_segment_route(payload: SavedSegmentRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            now_iso = utc_now_iso()
            name = payload.name.strip()
            filters_json = json.dumps(payload.filters or {}, ensure_ascii=False)
            existing_rows = client.table("SavedSegments").select("id").eq("user_id", user_id).eq("name", name).limit(1).execute().data or []
            if existing_rows:
                segment_id = int(existing_rows[0].get("id"))
                client.table("SavedSegments").update({"filters_json": filters_json, "updated_at": now_iso}).eq("id", segment_id).eq("user_id", user_id).execute()
                saved_rows = client.table("SavedSegments").select("id,user_id,name,filters_json,created_at,updated_at").eq("id", segment_id).limit(1).execute().data or []
            else:
                saved_rows = client.table("SavedSegments").insert({
                    "user_id": user_id,
                    "name": name,
                    "filters_json": filters_json,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }).execute().data or []
            if not saved_rows:
                raise HTTPException(status_code=500, detail="Could not save segment")
            return _normalize_saved_segment_row(saved_rows[0])

        return create_saved_segment(DEFAULT_DB_PATH, user_id, payload.model_dump())

    @app.delete("/api/saved-segments/{segment_id}")
    def delete_saved_segment_route(segment_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            existing_rows = client.table("SavedSegments").select("id").eq("id", int(segment_id)).eq("user_id", user_id).limit(1).execute().data or []
            if not existing_rows:
                raise HTTPException(status_code=404, detail="Saved segment not found")
            client.table("SavedSegments").delete().eq("id", int(segment_id)).eq("user_id", user_id).execute()
            return {"status": "deleted", "id": int(segment_id)}

        return delete_saved_segment(DEFAULT_DB_PATH, user_id, segment_id)

    @app.get("/api/client-folders")
    def get_client_folders(request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="client_success_dashboard")
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            folder_rows = supabase_select_rows(
                client,
                "ClientFolders",
                columns="id,user_id,name,color,notes,created_at,updated_at",
                filters={"user_id": user_id},
                order_by="id",
                desc=True,
                limit=200,
            )
            lead_rows = supabase_select_rows(
                client,
                "leads",
                columns="id,business_name,status,sent_at,last_contacted_at,reply_detected_at,paid_at,pipeline_stage,client_folder_id,ai_score",
                filters={"user_id": user_id},
                order_by="id",
                desc=True,
                limit=5000,
            )
            return {"items": _summarize_client_folders(folder_rows, lead_rows)}

        return {"items": list_client_folders(DEFAULT_DB_PATH, user_id)}

    @app.post("/api/client-folders")
    def create_client_folder_route(payload: ClientFolderRequest, request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="client_success_dashboard")
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            now_iso = utc_now_iso()
            inserted = client.table("ClientFolders").insert(
                {
                    "user_id": user_id,
                    "name": payload.name.strip(),
                    "color": (payload.color or "cyan").strip() or "cyan",
                    "notes": payload.notes.strip() if payload.notes else None,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            ).execute().data or []
            return inserted[0] if inserted else {"status": "created"}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        folder = create_client_folder(
            db_path,
            user_id,
            {"name": payload.name, "color": payload.color, "notes": payload.notes},
        )
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        return folder

    @app.patch("/api/leads/{lead_id}/client-folder")
    def update_lead_client_folder(lead_id: int, payload: LeadClientFolderRequest, request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="client_success_dashboard")
        user_id = require_current_user_id(request)
        normalized_folder_id = int(payload.client_folder_id) if payload.client_folder_id not in (None, 0) else None
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            lead_rows = client.table("leads").select("id,user_id,business_name").eq("id", lead_id).limit(1).execute().data or []
            if not lead_rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            folder_name = None
            if normalized_folder_id is not None:
                folder_rows = client.table("ClientFolders").select("id,name,user_id").eq("id", normalized_folder_id).limit(1).execute().data or []
                if not folder_rows or str(folder_rows[0].get("user_id") or "") != user_id:
                    raise HTTPException(status_code=404, detail="Client folder not found")
                folder_name = str(folder_rows[0].get("name") or "").strip() or None
            client.table("leads").update({"client_folder_id": normalized_folder_id}).eq("id", lead_id).execute()
            _invalidate_leads_cache()
            return {
                "status": "updated",
                "lead_id": lead_id,
                "client_folder_id": normalized_folder_id,
                "client_folder_name": folder_name,
                "business_name": str(lead_rows[0].get("business_name") or "").strip(),
            }

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        result = assign_lead_to_client_folder(db_path, user_id, lead_id, normalized_folder_id)
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        return result

    @app.get("/api/client-dashboard")
    def get_client_dashboard(request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="client_success_dashboard")
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            stats_snapshot = get_dashboard_stats_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            folder_rows = supabase_select_rows(
                client,
                "ClientFolders",
                columns="id,user_id,name,color,notes,created_at,updated_at",
                filters={"user_id": user_id},
                order_by="id",
                desc=True,
                limit=200,
            )
            lead_rows = supabase_select_rows(
                client,
                "leads",
                columns="id,business_name,status,sent_at,last_contacted_at,reply_detected_at,paid_at,pipeline_stage,client_folder_id,ai_score",
                filters={"user_id": user_id},
                order_by="id",
                desc=True,
                limit=5000,
            )
            folders = _summarize_client_folders(folder_rows, lead_rows)
            unassigned_count = sum(1 for lead in lead_rows if lead.get("client_folder_id") in (None, "", 0))
            return {
                "folder_count": len(folders),
                "unassigned_count": unassigned_count,
                "pipeline": dict(stats_snapshot.get("pipeline") or {}),
                "found_this_month": int(stats_snapshot.get("found_this_month") or 0),
                "contacted_this_month": int(stats_snapshot.get("contacted_this_month") or 0),
                "won_this_month": int(stats_snapshot.get("won_this_month") or 0),
                "folders": folders,
            }

        return build_client_dashboard_snapshot(DEFAULT_DB_PATH, user_id)

    @app.get("/api/workers")
    def get_workers(request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            return get_workers_snapshot_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        return get_workers_snapshot(db_path, user_id=user_id)

    @app.post("/api/workers")
    def create_worker(payload: WorkerCreateRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            status_value = payload.status.strip().title()
            if status_value not in {"Active", "Idle"}:
                raise HTTPException(status_code=422, detail="Status must be Active or Idle")

            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            inserted = client.table("workers").insert(
                {
                    "user_id": user_id,
                    "worker_name": payload.worker_name.strip(),
                    "role": payload.role.strip().upper(),
                    "monthly_cost": float(payload.monthly_cost),
                    "status": status_value,
                    "comms_link": payload.comms_link.strip() if payload.comms_link else None,
                    "created_at": utc_now_iso(),
                    "updated_at": utc_now_iso(),
                }
            ).execute().data or []
            worker_id = int(inserted[0].get("id")) if inserted else None

            add_worker_audit_supabase(
                DEFAULT_CONFIG_PATH,
                action="worker_created",
                worker_id=worker_id,
                message=f"Created worker '{payload.worker_name.strip()}' ({payload.role.strip().upper()}).",
                actor="api",
            )
            return {"status": "created", "worker_id": worker_id}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        status_value = payload.status.strip().title()
        if status_value not in {"Active", "Idle"}:
            raise HTTPException(status_code=422, detail="Status must be Active or Idle")

        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO workers (user_id, worker_name, role, monthly_cost, status, comms_link, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    payload.worker_name.strip(),
                    payload.role.strip().upper(),
                    float(payload.monthly_cost),
                    status_value,
                    payload.comms_link.strip() if payload.comms_link else None,
                    utc_now_iso(),
                    utc_now_iso(),
                ),
            )
            conn.commit()

        worker_id = int(cursor.lastrowid)
        add_worker_audit(
            db_path,
            action="worker_created",
            worker_id=worker_id,
            message=f"Created worker '{payload.worker_name.strip()}' ({payload.role.strip().upper()}).",
            actor="api",
        )
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        return {"status": "created", "worker_id": worker_id}

    @app.patch("/api/workers/{worker_id}")
    def update_worker(worker_id: int, payload: WorkerUpdateRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            rows = client.table("workers").select("id,user_id,worker_name,role,monthly_cost,status,comms_link").eq("id", worker_id).limit(1).execute().data or []
            if not rows:
                raise HTTPException(status_code=404, detail="Worker not found")

            row = rows[0]
            if str(row.get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            worker_name = payload.worker_name.strip() if payload.worker_name is not None else str(row.get("worker_name"))
            role = payload.role.strip().upper() if payload.role is not None else str(row.get("role"))
            monthly_cost = float(payload.monthly_cost) if payload.monthly_cost is not None else float(row.get("monthly_cost") or 0)
            status = payload.status.strip().title() if payload.status is not None else str(row.get("status"))
            comms_link = row.get("comms_link") if payload.comms_link is None else (payload.comms_link.strip() or None)

            if status not in {"Active", "Idle"}:
                raise HTTPException(status_code=422, detail="Status must be Active or Idle")

            client.table("workers").update(
                {
                    "worker_name": worker_name,
                    "role": role,
                    "monthly_cost": monthly_cost,
                    "status": status,
                    "comms_link": comms_link,
                    "updated_at": utc_now_iso(),
                }
            ).eq("id", worker_id).execute()

            add_worker_audit_supabase(
                DEFAULT_CONFIG_PATH,
                action="worker_updated",
                worker_id=worker_id,
                message=f"Updated worker '{worker_name}' profile.",
                actor="api",
            )
            return {"status": "updated", "worker_id": worker_id}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, user_id, worker_name, role, monthly_cost, status, comms_link FROM workers WHERE id = ?",
                (worker_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Worker not found")
            if str(row["user_id"] or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

            worker_name = payload.worker_name.strip() if payload.worker_name is not None else str(row["worker_name"])
            role = payload.role.strip().upper() if payload.role is not None else str(row["role"])
            monthly_cost = float(payload.monthly_cost) if payload.monthly_cost is not None else float(row["monthly_cost"] or 0)
            status = payload.status.strip().title() if payload.status is not None else str(row["status"])
            comms_link = row["comms_link"] if payload.comms_link is None else (payload.comms_link.strip() or None)

            if status not in {"Active", "Idle"}:
                raise HTTPException(status_code=422, detail="Status must be Active or Idle")

            conn.execute(
                """
                UPDATE workers
                SET worker_name = ?, role = ?, monthly_cost = ?, status = ?, comms_link = ?, updated_at = ?
                WHERE id = ?
                """,
                (worker_name, role, monthly_cost, status, comms_link, utc_now_iso(), worker_id),
            )
            conn.commit()

        add_worker_audit(
            db_path,
            action="worker_updated",
            worker_id=worker_id,
            message=f"Updated worker '{worker_name}' profile.",
            actor="api",
        )
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        return {"status": "updated", "worker_id": worker_id}

    @app.delete("/api/workers/{worker_id}")
    def delete_worker(worker_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            rows = client.table("workers").select("user_id,worker_name").eq("id", worker_id).limit(1).execute().data or []
            if not rows:
                raise HTTPException(status_code=404, detail="Worker not found")

            if str(rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            worker_name = str(rows[0].get("worker_name") or f"Worker #{worker_id}")
            updated_leads = client.table("leads").update({"worker_id": None, "assigned_worker_at": None}).eq("worker_id", worker_id).eq("user_id", user_id).execute().data or []
            client.table("delivery_tasks").update({"worker_id": None, "updated_at": utc_now_iso()}).eq("worker_id", worker_id).eq("user_id", user_id).neq("status", "done").execute()
            client.table("workers").delete().eq("id", worker_id).execute()

            unassigned_leads = len(updated_leads)
            add_worker_audit_supabase(
                DEFAULT_CONFIG_PATH,
                action="worker_deleted",
                worker_id=worker_id,
                message=f"Deleted worker '{worker_name}'. Unassigned leads: {unassigned_leads}.",
                actor="api",
            )
            return {"status": "deleted", "worker_id": worker_id, "unassigned_leads": unassigned_leads}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT user_id, worker_name FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Worker not found")
            if str(row["user_id"] or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

            worker_name = str(row["worker_name"] or f"Worker #{worker_id}")
            lead_cursor = conn.execute(
                "UPDATE leads SET worker_id = NULL, assigned_worker_at = NULL WHERE worker_id = ? AND user_id = ?",
                (worker_id, user_id),
            )
            conn.execute(
                "UPDATE delivery_tasks SET worker_id = NULL, updated_at = ? WHERE worker_id = ? AND user_id = ? AND LOWER(COALESCE(status,'')) != 'done'",
                (utc_now_iso(), worker_id, user_id),
            )
            conn.execute("DELETE FROM workers WHERE id = ?", (worker_id,))
            conn.commit()
            unassigned_leads = int(lead_cursor.rowcount or 0)

        add_worker_audit(
            db_path,
            action="worker_deleted",
            worker_id=worker_id,
            message=f"Deleted worker '{worker_name}'. Unassigned leads: {unassigned_leads}.",
            actor="api",
        )

        _ = delete_supabase_row("workers", worker_id, DEFAULT_CONFIG_PATH)
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        return {"status": "deleted", "worker_id": worker_id, "unassigned_leads": unassigned_leads}

    @app.patch("/api/leads/{lead_id}/assign-worker")
    def assign_worker_to_lead(lead_id: int, payload: AssignWorkerRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            worker_id = payload.worker_id
            worker_name = None
            if worker_id is not None:
                wrows = client.table("workers").select("id,user_id,worker_name").eq("id", worker_id).limit(1).execute().data or []
                if not wrows:
                    raise HTTPException(status_code=404, detail="Worker not found")
                if str(wrows[0].get("user_id") or "") != user_id:
                    raise HTTPException(status_code=403, detail="Forbidden")
                worker_name = wrows[0].get("worker_name")

            lrows = client.table("leads").select("id,user_id").eq("id", lead_id).limit(1).execute().data or []
            if not lrows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lrows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

            assigned_at = utc_now_iso() if worker_id is not None else None
            client.table("leads").update({"worker_id": worker_id, "assigned_worker_at": assigned_at}).eq("id", lead_id).execute()

            if worker_id is not None:
                client.table("delivery_tasks").update({"worker_id": worker_id, "updated_at": utc_now_iso()}).eq("lead_id", lead_id).eq("user_id", user_id).in_("status", ["todo", "in_progress", "blocked"]).execute()

            message = (
                f"Assigned lead #{lead_id} to {worker_name}."
                if worker_id is not None
                else f"Removed worker assignment from lead #{lead_id}."
            )
            add_worker_audit_supabase(
                DEFAULT_CONFIG_PATH,
                action="manual_assign",
                worker_id=worker_id,
                lead_id=lead_id,
                message=message,
                actor="api",
            )

            delivery_result = ensure_delivery_task_for_paid_lead_supabase(lead_id, DEFAULT_CONFIG_PATH) if worker_id is not None else None
            return {"status": "updated", "lead_id": lead_id, "worker_id": worker_id, "delivery_task": delivery_result}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        worker_id = payload.worker_id
        worker_name = None
        with sqlite3.connect(db_path) as conn:
            if worker_id is not None:
                row = conn.execute("SELECT id, user_id, worker_name FROM workers WHERE id = ?", (worker_id,)).fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail="Worker not found")
                if str(row[1] or "") != user_id:
                    raise HTTPException(status_code=403, detail="Forbidden")
                worker_name = row[2]

            lead_owner = conn.execute("SELECT user_id FROM leads WHERE id = ?", (lead_id,)).fetchone()
            if lead_owner is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_owner[0] or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

            assigned_at = utc_now_iso() if worker_id is not None else None
            cursor = conn.execute(
                """
                UPDATE leads
                SET worker_id = ?, assigned_worker_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (worker_id, assigned_at, lead_id, user_id),
            )

            if worker_id is not None:
                conn.execute(
                    """
                    UPDATE delivery_tasks
                    SET worker_id = ?, updated_at = ?
                    WHERE lead_id = ?
                      AND user_id = ?
                      AND LOWER(COALESCE(status, '')) IN ('todo', 'in_progress', 'blocked')
                    """,
                    (worker_id, utc_now_iso(), lead_id, user_id),
                )
            conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Lead not found")

        message = (
            f"Assigned lead #{lead_id} to {worker_name}."
            if worker_id is not None
            else f"Removed worker assignment from lead #{lead_id}."
        )
        add_worker_audit(
            db_path,
            action="manual_assign",
            worker_id=worker_id,
            lead_id=lead_id,
            message=message,
            actor="api",
        )

        delivery_result = ensure_delivery_task_for_paid_lead(db_path, lead_id) if worker_id is not None else None
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        return {"status": "updated", "lead_id": lead_id, "worker_id": worker_id, "delivery_task": delivery_result}

    @app.get("/api/delivery-tasks")
    def get_delivery_tasks(request: Request, status: Optional[str] = Query(default=None)) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            items = supabase_select_rows(
                client,
                "delivery_tasks",
                columns="id,lead_id,worker_id,business_name,task_type,status,notes,due_at,done_at,position,created_at,updated_at",
                filters={"user_id": user_id},
                order_by="position",
                desc=False,
            )
            if status:
                filter_status = status.strip().lower()
                items = [row for row in items if str(row.get("status") or "").strip().lower() == filter_status]

            leads = {
                int(row.get("id")): row
                for row in supabase_select_rows(client, "leads", columns="id,client_tier,paid_at", filters={"user_id": user_id})
                if row.get("id") is not None
            }
            workers = {
                int(row.get("id")): row
                for row in supabase_select_rows(client, "workers", columns="id,worker_name", filters={"user_id": user_id})
                if row.get("id") is not None
            }

            for item in items:
                lead = leads.get(int(item.get("lead_id")) if item.get("lead_id") is not None else -1)
                worker = workers.get(int(item.get("worker_id")) if item.get("worker_id") is not None else -1)
                item["client_tier"] = lead.get("client_tier") if lead else None
                item["paid_at"] = lead.get("paid_at") if lead else None
                item["worker_name"] = worker.get("worker_name") if worker else None

            items.sort(
                key=lambda row: (
                    int(row.get("position") or 0),
                    {"todo": 1, "in_progress": 2, "blocked": 3, "done": 4}.get(str(row.get("status") or "").lower(), 5),
                    str(row.get("due_at") or row.get("created_at") or ""),
                    int(row.get("id") or 0),
                )
            )

            summary = {
                "todo": sum(1 for row in items if str(row.get("status", "")).lower() == "todo"),
                "in_progress": sum(1 for row in items if str(row.get("status", "")).lower() == "in_progress"),
                "blocked": sum(1 for row in items if str(row.get("status", "")).lower() == "blocked"),
                "done": sum(1 for row in items if str(row.get("status", "")).lower() == "done"),
                "total": len(items),
            }
            return {"items": items, "summary": summary}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        where_clause = "WHERE dt.user_id = ?"
        params: list = [user_id]
        if status:
            where_clause += " AND LOWER(COALESCE(dt.status, '')) = ?"
            params.append(status.strip().lower())

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT
                    dt.id,
                    dt.lead_id,
                    dt.worker_id,
                    dt.business_name,
                    dt.task_type,
                    dt.status,
                    dt.notes,
                    dt.due_at,
                    dt.done_at,
                    dt.position,
                    dt.created_at,
                    dt.updated_at,
                    l.client_tier,
                    l.paid_at,
                    w.worker_name
                FROM delivery_tasks dt
                LEFT JOIN leads l ON l.id = dt.lead_id
                LEFT JOIN workers w ON w.id = dt.worker_id
                {where_clause}
                ORDER BY
                    COALESCE(NULLIF(dt.position, 0), dt.id) ASC,
                    CASE LOWER(COALESCE(dt.status, ''))
                        WHEN 'todo' THEN 1
                        WHEN 'in_progress' THEN 2
                        WHEN 'blocked' THEN 3
                        WHEN 'done' THEN 4
                        ELSE 5
                    END,
                    datetime(COALESCE(dt.due_at, dt.created_at)) ASC,
                    dt.id ASC
                """,
                params,
            ).fetchall()

        items = [dict(row) for row in rows]
        summary = {
            "todo": sum(1 for row in items if str(row.get("status", "")).lower() == "todo"),
            "in_progress": sum(1 for row in items if str(row.get("status", "")).lower() == "in_progress"),
            "blocked": sum(1 for row in items if str(row.get("status", "")).lower() == "blocked"),
            "done": sum(1 for row in items if str(row.get("status", "")).lower() == "done"),
            "total": len(items),
        }
        return {"items": items, "summary": summary}

    @app.patch("/api/delivery-tasks/{task_id}")
    def update_delivery_task(task_id: int, payload: DeliveryTaskUpdateRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            allowed_statuses = {"todo", "in_progress", "blocked", "done"}
            next_status = payload.status.strip().lower() if payload.status is not None else None
            if next_status is not None and next_status not in allowed_statuses:
                raise HTTPException(status_code=422, detail="Invalid delivery task status")

            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            rows = client.table("delivery_tasks").select("id,user_id,worker_id,lead_id,business_name,status,notes,done_at").eq("id", task_id).limit(1).execute().data or []
            if not rows:
                raise HTTPException(status_code=404, detail="Delivery task not found")

            row = rows[0]
            if str(row.get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            worker_id = row.get("worker_id") if payload.worker_id is None else payload.worker_id
            if worker_id is not None:
                wrow = client.table("workers").select("id").eq("id", worker_id).eq("user_id", user_id).limit(1).execute().data or []
                if not wrow:
                    raise HTTPException(status_code=404, detail="Worker not found")

            status_value = str(row.get("status") or "todo").lower() if next_status is None else next_status
            notes_value = row.get("notes") if payload.notes is None else (payload.notes.strip() or None)
            done_at_value = row.get("done_at")
            if status_value == "done" and not done_at_value:
                done_at_value = utc_now_iso()
            if status_value != "done":
                done_at_value = None

            client.table("delivery_tasks").update(
                {
                    "worker_id": worker_id,
                    "status": status_value,
                    "notes": notes_value,
                    "done_at": done_at_value,
                    "updated_at": utc_now_iso(),
                }
            ).eq("id", task_id).execute()

            add_worker_audit_supabase(
                DEFAULT_CONFIG_PATH,
                action="delivery_task_updated",
                worker_id=int(worker_id) if worker_id is not None else None,
                lead_id=int(row.get("lead_id")) if row.get("lead_id") is not None else None,
                message=f"Updated delivery task '{row.get('business_name')}' to status '{status_value}'.",
                actor="api",
            )

            return {
                "status": "updated",
                "task_id": task_id,
                "task_status": status_value,
                "worker_id": worker_id,
            }

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        allowed_statuses = {"todo", "in_progress", "blocked", "done"}
        next_status = payload.status.strip().lower() if payload.status is not None else None
        if next_status is not None and next_status not in allowed_statuses:
            raise HTTPException(status_code=422, detail="Invalid delivery task status")

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, user_id, worker_id, lead_id, business_name, status, notes, done_at FROM delivery_tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Delivery task not found")
            if str(row["user_id"] or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

            worker_id = row["worker_id"] if payload.worker_id is None else payload.worker_id
            if worker_id is not None:
                wrow = conn.execute("SELECT id FROM workers WHERE id = ? AND user_id = ?", (worker_id, user_id)).fetchone()
                if wrow is None:
                    raise HTTPException(status_code=404, detail="Worker not found")

            status_value = str(row["status"] or "todo").lower() if next_status is None else next_status
            notes_value = row["notes"] if payload.notes is None else (payload.notes.strip() or None)
            done_at_value = row["done_at"]
            if status_value == "done" and not done_at_value:
                done_at_value = utc_now_iso()
            if status_value != "done":
                done_at_value = None

            conn.execute(
                """
                UPDATE delivery_tasks
                SET worker_id = ?, status = ?, notes = ?, done_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (worker_id, status_value, notes_value, done_at_value, utc_now_iso(), task_id),
            )
            conn.commit()

        add_worker_audit(
            db_path,
            action="delivery_task_updated",
            worker_id=int(worker_id) if worker_id is not None else None,
            lead_id=int(row["lead_id"]) if row["lead_id"] is not None else None,
            message=f"Updated delivery task '{row['business_name']}' to status '{status_value}'.",
            actor="api",
        )
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        return {
            "status": "updated",
            "task_id": task_id,
            "task_status": status_value,
            "worker_id": worker_id,
        }

    @app.patch("/api/leads/{lead_id}/tier")
    def update_lead_tier(lead_id: int, payload: LeadTierRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            allowed_tiers = {"standard", "premium_ads", "saas"}
            tier_value = payload.tier.strip().lower()
            if tier_value not in allowed_tiers:
                raise HTTPException(status_code=422, detail=f"Invalid tier. Allowed: {', '.join(sorted(allowed_tiers))}")

            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")

            lead_rows = client.table("leads").select("id,user_id").eq("id", lead_id).limit(1).execute().data or []
            if not lead_rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

            client.table("leads").update({"client_tier": tier_value}).eq("id", lead_id).execute()
            _invalidate_leads_cache()
            return {"status": "updated", "lead_id": lead_id, "new_tier": tier_value}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        allowed_tiers = {"standard", "premium_ads", "saas"}
        tier_value = payload.tier.strip().lower()
        if tier_value not in allowed_tiers:
            raise HTTPException(status_code=422, detail=f"Invalid tier. Allowed: {', '.join(sorted(allowed_tiers))}")

        with sqlite3.connect(db_path) as conn:
            row = conn.execute("SELECT user_id FROM leads WHERE id = ?", (lead_id,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(row[0] or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            cursor = conn.execute(
                "UPDATE leads SET client_tier = ? WHERE id = ?",
                (tier_value, lead_id),
            )
            conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Lead not found")

        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        return {"status": "updated", "lead_id": lead_id, "new_tier": tier_value}

    # ------------------------------------------------------------------
    # Job Queue endpoints  (Supabase-first, SQLite fallback)
    # ------------------------------------------------------------------
    import uuid as _uuid

    @app.post("/api/jobs")
    @auth_required
    def create_job(payload: dict, request: Request) -> dict:
        """
        Enqueue a new job.  Payload must contain at least {"type": "scrape|enrich|mailer", ...}.
        Returns {"job_id": <id>, "status": "pending"} immediately.
        The actual work is done by the background worker process (worker.py).
        """
        user_id = require_current_user_id(request)
        job_type = str(payload.get("type") or "").strip().lower()
        if job_type not in {"scrape", "enrich", "mailer"}:
            raise HTTPException(status_code=422, detail="Invalid job type. Must be scrape | enrich | mailer")

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            inserted = client.table("jobs").insert({
                "user_id": user_id,
                "type": job_type,
                "status": "pending",
                "payload": payload,
            }).execute().data or []
            if not inserted:
                raise HTTPException(status_code=500, detail="Failed to create job")
            job_id = inserted[0].get("id")
            return {"job_id": job_id, "status": "pending"}

        # SQLite fallback — jobs table in leads.db
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        _ensure_jobs_table_sqlite(db_path)
        with sqlite3.connect(db_path) as conn:
            import json as _json
            cursor = conn.execute(
                "INSERT INTO jobs (user_id, type, status, payload) VALUES (?, ?, 'pending', ?)",
                (user_id, job_type, _json.dumps(payload)),
            )
            conn.commit()
        return {"job_id": cursor.lastrowid, "status": "pending"}

    @app.get("/api/jobs/{job_id}")
    @auth_required
    def get_job(job_id: str, request: Request) -> dict:
        """Poll a job's status.  Returns status + result once completed."""
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            rows = client.table("jobs").select(
                "id,user_id,type,status,result,error,created_at,started_at,completed_at,updated_at"
            ).eq("id", job_id).eq("user_id", user_id).limit(1).execute().data or []
            if not rows:
                raise HTTPException(status_code=404, detail="Job not found")
            return rows[0]

        # SQLite fallback
        db_path = DEFAULT_DB_PATH
        _ensure_jobs_table_sqlite(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id,user_id,type,status,result,error,created_at,started_at,completed_at FROM jobs WHERE id=? AND user_id=?",
                (job_id, user_id),
            ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        return dict(row)

    @app.get("/api/jobs")
    @auth_required
    def list_jobs(request: Request, limit: int = Query(20, ge=1, le=200)) -> dict:
        """List recent jobs for a user."""
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            rows = (
                client.table("jobs")
                .select("id,user_id,type,status,error,created_at,started_at,completed_at,updated_at")
                .eq("user_id", user_id)
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
                .data or []
            )
            return {"items": rows}

        db_path = DEFAULT_DB_PATH
        _ensure_jobs_table_sqlite(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id,user_id,type,status,error,created_at,started_at,completed_at FROM jobs WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
        return {"items": [dict(r) for r in rows]}

    @app.post("/api/scrape")
    def run_scrape(payload: ScrapeRequest, background_tasks: BackgroundTasks, request: Request) -> dict:
        print(f"[scrape] POST /api/scrape — keyword={payload.keyword!r} results={payload.results}")
        _, billing, access = resolve_plan_access_context(request, feature_key="basic_search")
        user_id = require_current_user_id(request)
        print(f"[scrape] user_id={user_id}")
        db_path = resolve_path(payload.db_path, DEFAULT_DB_PATH)
        try:
            ensure_system_tables(db_path)
        except Exception as _db_exc:
            print(f"[scrape] DB init error: {_db_exc}")
            raise HTTPException(status_code=500, detail="Database offline")

        available_credits = max(0, int(billing.get("credits_balance") or 0))
        if available_credits <= 0:
            raise HTTPException(status_code=403, detail="Out of credits. Please upgrade.")

        if bool(payload.export_targets):
            require_feature_access(access.get("plan_key"), "bulk_export")

        payload_data = payload.model_dump()
        requested_results = max(1, int(payload.results or 25))
        payload_data["country"] = normalize_country_value(payload.country, payload.country_code)
        payload_data["results"] = min(requested_results, available_credits)
        payload_data["_queue_priority"] = bool(access.get("queue_priority"))
        return enqueue_task(
            app,
            background_tasks,
            db_path,
            user_id,
            "scrape",
            {
                **payload_data,
                "keyword": payload.keyword,
                "results": payload_data["results"],
                "country": payload_data["country"],
            },
        )

    @app.post("/api/export-targets")
    def run_export_targets(payload: ExportTargetsRequest, request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="bulk_export")
        user_id = require_current_user_id(request)
        db_path = resolve_path(payload.db_path, DEFAULT_DB_PATH)
        ensure_system_tables(db_path)

        output_path = resolve_path(payload.output_csv, DEFAULT_TARGET_EXPORT)
        exported = export_target_leads(
            db_path=str(db_path),
            output_csv=str(output_path),
            min_score=payload.min_score,
            user_id=user_id,
        )
        return {"exported": exported, "output_csv": str(output_path)}

    @app.get("/api/export-leads")
    def download_export_leads(
        request: Request,
        kind: str = Query("target"),
        min_score: float = Query(HIGH_AI_SCORE_THRESHOLD, ge=0.0, le=10.0),
        db_path: Optional[str] = Query(None),
    ) -> Response:
        resolve_plan_access_context(request, feature_key="bulk_export")
        user_id = require_current_user_id(request)
        resolved_db_path = resolve_path(db_path, DEFAULT_DB_PATH)
        ensure_system_tables(resolved_db_path)
        filename, fieldnames, rows = _collect_lead_export_rows(
            resolved_db_path,
            kind=kind,
            user_id=user_id,
            min_score=min_score,
        )
        return _render_csv_download(filename, fieldnames, rows)

    @app.post("/api/export/webhook")
    def export_leads_to_webhook(payload: WebhookExportRequest, request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="webhooks")
        user_id = require_current_user_id(request)
        resolved_db_path = DEFAULT_DB_PATH
        ensure_system_tables(resolved_db_path)
        filename, fieldnames, rows = _collect_lead_export_rows(
            resolved_db_path,
            kind=payload.kind,
            user_id=user_id,
            min_score=payload.min_score,
        )
        target_key = str(payload.target or "").strip().lower().replace(" ", "_")
        webhook_url = _get_export_webhook_url(DEFAULT_CONFIG_PATH, target_key)
        outbound_payload = {
            "target": target_key,
            "kind": str(payload.kind or "target").strip().lower(),
            "filename": filename,
            "fieldnames": fieldnames,
            "exported": len(rows),
            "items": rows,
            "generated_at": utc_now_iso(),
        }
        try:
            delivery = deliver_export_webhook(webhook_url, outbound_payload)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Webhook delivery failed: {exc}")
        return {
            "status": "sent",
            "target": target_key,
            "exported": len(rows),
            "delivery": delivery,
        }

    def _extract_bearer_token(request: Optional[Request]) -> str:
        if request is None:
            return ""
        auth_header = str(request.headers.get("Authorization", "") or "").strip()
        if not auth_header.lower().startswith("bearer "):
            return ""
        return auth_header[7:].strip()

    def _resolve_session_token(request: Optional[Request] = None, fallback_token: Optional[str] = None) -> str:
        bearer = _extract_bearer_token(request)
        if bearer:
            return bearer
        return str(fallback_token or "").strip()

    def _session_token_exists(session_token: str, db_path: Optional[Path] = None) -> bool:
        token = str(session_token or "").strip()
        if not token:
            return False

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                return False
            try:
                response = sb_client.table("users").select("id").eq("token", token).limit(1).execute()
                rows = list(getattr(response, "data", None) or [])
                return bool(rows)
            except Exception:
                return False

        sqlite_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(sqlite_db_path)
        with sqlite3.connect(sqlite_db_path) as conn:
            row = conn.execute("SELECT 1 FROM users WHERE token = ? LIMIT 1", (token,)).fetchone()
        return bool(row)

    def require_authenticated_session(request: Optional[Request] = None, fallback_token: Optional[str] = None) -> str:
        token = _resolve_session_token(request=request, fallback_token=fallback_token)
        if not _session_token_exists(token):
            raise HTTPException(status_code=401, detail="Authentication required.")
        return token

    def resolve_user_id_from_session_token(session_token: str, db_path: Optional[Path] = None) -> str:
        token = str(session_token or "").strip()
        if not token:
            raise HTTPException(status_code=401, detail="Authentication required.")

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                response = sb_client.table("users").select("id,email").eq("token", token).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase session lookup failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            row = rows[0]
            candidate = str(row.get("id") or "").strip() or str(row.get("email") or "").strip()
            if not candidate:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            return candidate

        sqlite_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(sqlite_db_path)
        with sqlite3.connect(sqlite_db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT id FROM users WHERE token = ?", (token,)).fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")
        return str(row["id"])

    def require_current_user_id(request: Request, fallback_token: Optional[str] = None, db_path: Optional[Path] = None) -> str:
        cached_user_id = getattr(request.state, "current_user_id", None)
        if cached_user_id:
            return str(cached_user_id)
        token = require_authenticated_session(request, fallback_token=fallback_token)
        user_id = resolve_user_id_from_session_token(token, db_path=db_path)
        request.state.current_user_id = user_id
        return user_id

    def resolve_plan_access_context(
        request: Optional[Request] = None,
        fallback_token: Optional[str] = None,
        *,
        feature_key: Optional[str] = None,
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        session_token = require_authenticated_session(request, fallback_token=fallback_token)
        billing = load_user_billing_context(session_token)
        plan_key = _normalize_plan_key(billing.get("plan_key"), fallback=DEFAULT_PLAN_KEY)
        access = require_feature_access(plan_key, feature_key) if feature_key else get_plan_feature_access(plan_key)
        return session_token, billing, access

    def _coerce_subscription_active(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(int(value))
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "active", "paid"}

    def _safe_int(value: Any, fallback: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return int(fallback)

    def _next_monthly_reset_iso(subscription_start_date_raw: Optional[str]) -> Optional[str]:
        start_dt = parse_iso_datetime(subscription_start_date_raw)
        if start_dt is None:
            return None

        now = datetime.now(timezone.utc)
        target_day = max(1, min(31, int(start_dt.day)))
        hour, minute, second, microsecond = start_dt.hour, start_dt.minute, start_dt.second, start_dt.microsecond

        year = now.year
        month = now.month
        day_this_month = min(target_day, calendar.monthrange(year, month)[1])
        candidate = datetime(year, month, day_this_month, hour, minute, second, microsecond, tzinfo=timezone.utc)

        if candidate <= now:
            month += 1
            if month > 12:
                month = 1
                year += 1
            day_next_month = min(target_day, calendar.monthrange(year, month)[1])
            candidate = datetime(year, month, day_next_month, hour, minute, second, microsecond, tzinfo=timezone.utc)

        return candidate.isoformat()

    def _monthly_reset_days_left(subscription_start_date_raw: Optional[str]) -> Optional[int]:
        next_iso = _next_monthly_reset_iso(subscription_start_date_raw)
        if not next_iso:
            return None
        next_dt = parse_iso_datetime(next_iso)
        if next_dt is None:
            return None
        now = datetime.now(timezone.utc)
        return max(0, (next_dt.date() - now.date()).days)

    def load_user_billing_context(session_token: str) -> dict:
        token = str(session_token or "").strip()
        if not token:
            raise HTTPException(status_code=401, detail="Authentication required.")

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")

            base = sb_client.table("users").select("id,email").eq("token", token).limit(1).execute()
            base_rows = list(getattr(base, "data", None) or [])
            if not base_rows:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            base_row = base_rows[0]

            extras: dict[str, Any] = {}
            try:
                extra_resp = sb_client.table("users").select(
                    "credits_balance,credits_limit,monthly_limit,monthly_quota,topup_credits_balance,subscription_start_date,subscription_active,subscription_status,subscription_cancel_at,subscription_cancel_at_period_end,plan_key,stripe_customer_id,updated_at"
                ).eq("token", token).limit(1).execute()
                extra_rows = list(getattr(extra_resp, "data", None) or [])
                if extra_rows:
                    extras = extra_rows[0]
            except Exception:
                extras = {}

            runtime_billing = load_runtime_billing_snapshot(base_row.get("id"), base_row.get("email"))
            if runtime_billing:
                runtime_is_paid = _coerce_subscription_active(runtime_billing.get("subscription_active")) or _normalize_plan_key(runtime_billing.get("plan_key"), fallback="free") != "free"
                extras_is_paid = _coerce_subscription_active(extras.get("subscription_active")) or _normalize_plan_key(extras.get("plan_key"), fallback="free") != "free"
                if runtime_is_paid and not extras_is_paid:
                    extras = {**extras, **runtime_billing}
                else:
                    for key, value in runtime_billing.items():
                        if key not in extras or extras.get(key) in (None, "", 0, False):
                            extras[key] = value

            extras_plan_key = _normalize_plan_key(extras.get("plan_key"), fallback="free")
            extras_is_paid = _coerce_subscription_active(extras.get("subscription_active")) or extras_plan_key != "free"
            if not extras_is_paid:
                stripe_recovered = recover_billing_snapshot_from_stripe(
                    user_email=base_row.get("email"),
                    stripe_customer_id=extras.get("stripe_customer_id"),
                    fallback_plan_key="pro",
                    config_path=DEFAULT_CONFIG_PATH,
                )
                if stripe_recovered and bool(stripe_recovered.get("subscription_active")):
                    recovered_limit = max(
                        1,
                        _safe_int(
                            stripe_recovered.get("monthly_quota")
                            or stripe_recovered.get("monthly_limit")
                            or stripe_recovered.get("credits_limit")
                            or DEFAULT_MONTHLY_CREDIT_LIMIT,
                            DEFAULT_MONTHLY_CREDIT_LIMIT,
                        ),
                    )
                    existing_topup = max(0, _safe_int(extras.get("topup_credits_balance"), 0))
                    stripe_recovered["credits_balance"] = max(
                        _safe_int(stripe_recovered.get("credits_balance"), 0),
                        recovered_limit + existing_topup,
                    )
                    stripe_recovered["topup_credits_balance"] = existing_topup
                    stripe_recovered["updated_at"] = utc_now_iso()
                    extras = {**extras, **stripe_recovered}
                    store_runtime_billing_snapshot(base_row.get("id"), base_row.get("email"), extras)
                    try:
                        execute_supabase_update_with_retry(
                            sb_client,
                            "users",
                            stripe_recovered,
                            eq_filters={"id": base_row.get("id")},
                            operation_name="stripe_profile_recovery",
                        )
                    except Exception:
                        pass

            pending_topup = recover_pending_topup_credits_from_stripe(
                user_id=base_row.get("id"),
                user_email=base_row.get("email"),
                stripe_customer_id=extras.get("stripe_customer_id"),
                updated_at_raw=extras.get("updated_at"),
                config_path=DEFAULT_CONFIG_PATH,
            )
            recovered_topup_delta = max(0, _safe_int(pending_topup.get("credits_delta"), 0))
            if recovered_topup_delta > 0:
                current_topup_balance = max(0, _safe_int(extras.get("topup_credits_balance"), 0))
                current_balance = max(0, _safe_int(extras.get("credits_balance"), 0))
                extras["stripe_customer_id"] = str(pending_topup.get("stripe_customer_id") or extras.get("stripe_customer_id") or "").strip()
                extras["topup_credits_balance"] = current_topup_balance + recovered_topup_delta
                extras["credits_balance"] = current_balance + recovered_topup_delta
                extras["updated_at"] = utc_now_iso()
                store_runtime_billing_snapshot(base_row.get("id"), base_row.get("email"), extras)
                mark_stripe_topup_payments_applied(
                    list(pending_topup.get("payment_ids") or []),
                    user_id=str(base_row.get("id") or "").strip(),
                    credits_delta=recovered_topup_delta,
                )
                try:
                    execute_supabase_update_with_retry(
                        sb_client,
                        "users",
                        {
                            "stripe_customer_id": extras["stripe_customer_id"],
                            "topup_credits_balance": extras["topup_credits_balance"],
                            "credits_balance": extras["credits_balance"],
                            "updated_at": extras["updated_at"],
                        },
                        eq_filters={"id": base_row.get("id")},
                        operation_name="stripe_topup_profile_recovery",
                    )
                except Exception:
                    pass

            free_quota = int(PLAN_MONTHLY_QUOTAS.get("free", DEFAULT_MONTHLY_CREDIT_LIMIT))
            topup_balance = max(0, _safe_int(extras.get("topup_credits_balance"), 0))
            cancel_at_raw = str(extras.get("subscription_cancel_at") or "").strip() or None
            cancel_at_period_end = bool(extras.get("subscription_cancel_at_period_end"))
            is_active = _coerce_subscription_active(extras.get("subscription_active"))
            cancel_expired = cancel_at_period_end and _is_subscription_cancel_expired(cancel_at_raw)
            if is_active and cancel_expired:
                downgraded_balance = free_quota + topup_balance
                try:
                    sb_client.table("users").update(
                        {
                            "plan_key": "free",
                            "subscription_active": False,
                            "subscription_status": "expired",
                            "subscription_cancel_at_period_end": False,
                            "monthly_quota": free_quota,
                            "monthly_limit": free_quota,
                            "credits_limit": free_quota,
                            "credits_balance": downgraded_balance,
                            "subscription_start_date": utc_now_iso(),
                            "updated_at": utc_now_iso(),
                        }
                    ).eq("id", base_row.get("id")).execute()
                    extras["subscription_active"] = False
                    extras["subscription_status"] = "expired"
                    extras["subscription_cancel_at_period_end"] = False
                    extras["monthly_quota"] = free_quota
                    extras["monthly_limit"] = free_quota
                    extras["credits_limit"] = free_quota
                    extras["credits_balance"] = downgraded_balance
                except Exception:
                    pass

            monthly_quota = max(
                1,
                _safe_int(
                    extras.get("monthly_quota")
                    or extras.get("monthly_limit")
                    or extras.get("credits_limit")
                    or DEFAULT_MONTHLY_CREDIT_LIMIT,
                    DEFAULT_MONTHLY_CREDIT_LIMIT,
                ),
            )
            monthly_limit = monthly_quota
            subscription_start_date = str(extras.get("subscription_start_date") or "").strip() or None
            next_reset_at = _next_monthly_reset_iso(subscription_start_date)
            next_reset_in_days = _monthly_reset_days_left(subscription_start_date)

            return {
                "id": str(base_row.get("id") or "").strip(),
                "email": str(base_row.get("email") or "").strip().lower(),
                "credits_balance": _safe_int(extras.get("credits_balance"), 0),
                "credits_limit": monthly_limit,
                "monthly_limit": monthly_limit,
                "monthly_quota": monthly_quota,
                "topup_credits_balance": max(0, _safe_int(extras.get("topup_credits_balance"), 0)),
                "subscription_start_date": subscription_start_date,
                "next_reset_at": next_reset_at,
                "next_reset_in_days": next_reset_in_days,
                "subscription_active": _coerce_subscription_active(extras.get("subscription_active")),
                "subscription_status": str(extras.get("subscription_status") or "").strip().lower(),
                "subscription_cancel_at": cancel_at_raw,
                "subscription_cancel_at_period_end": bool(extras.get("subscription_cancel_at_period_end")),
                "plan_key": _normalize_plan_key(extras.get("plan_key"), fallback=("pro" if _coerce_subscription_active(extras.get("subscription_active")) else "free")),
                "stripe_customer_id": str(extras.get("stripe_customer_id") or "").strip(),
                "updated_at": str(extras.get("updated_at") or "").strip(),
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                f"""
                SELECT
                    id,
                    email,
                    COALESCE(credits_balance, 0) AS credits_balance,
                    COALESCE(NULLIF(monthly_quota, 0), NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT}) AS monthly_limit,
                    COALESCE(topup_credits_balance, 0) AS topup_credits_balance,
                    COALESCE(subscription_start_date, '') AS subscription_start_date,
                    COALESCE(subscription_active, 0) AS subscription_active,
                    COALESCE(subscription_status, '') AS subscription_status,
                    COALESCE(subscription_cancel_at, '') AS subscription_cancel_at,
                    COALESCE(subscription_cancel_at_period_end, 0) AS subscription_cancel_at_period_end,
                    COALESCE(NULLIF(plan_key, ''), 'free') AS plan_key,
                    COALESCE(stripe_customer_id, '') AS stripe_customer_id,
                    COALESCE(updated_at, '') AS updated_at
                FROM users
                WHERE token = ?
                """,
                (token,),
            ).fetchone()

        if row is None:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")

        row_data = dict(row)
        stored_plan_key = _normalize_plan_key(row_data.get("plan_key"), fallback="free")
        stored_is_paid = _coerce_subscription_active(row_data.get("subscription_active")) or stored_plan_key != "free"
        if not stored_is_paid:
            stripe_recovered = recover_billing_snapshot_from_stripe(
                user_email=row_data.get("email"),
                stripe_customer_id=row_data.get("stripe_customer_id"),
                fallback_plan_key="pro",
                config_path=DEFAULT_CONFIG_PATH,
            )
            if stripe_recovered and bool(stripe_recovered.get("subscription_active")):
                recovered_limit = max(
                    1,
                    _safe_int(
                        stripe_recovered.get("monthly_quota")
                        or stripe_recovered.get("monthly_limit")
                        or stripe_recovered.get("credits_limit")
                        or DEFAULT_MONTHLY_CREDIT_LIMIT,
                        DEFAULT_MONTHLY_CREDIT_LIMIT,
                    ),
                )
                existing_topup = max(0, _safe_int(row_data.get("topup_credits_balance"), 0))
                recovered_balance = max(
                    _safe_int(stripe_recovered.get("credits_balance"), 0),
                    recovered_limit + existing_topup,
                )
                with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                    conn.execute(
                        """
                        UPDATE users
                        SET stripe_customer_id = ?,
                            plan_key = ?,
                            subscription_active = 1,
                            subscription_status = ?,
                            subscription_cancel_at = ?,
                            subscription_cancel_at_period_end = ?,
                            monthly_quota = ?,
                            monthly_limit = ?,
                            credits_limit = ?,
                            credits_balance = ?,
                            subscription_start_date = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            str(stripe_recovered.get("stripe_customer_id") or row_data.get("stripe_customer_id") or "").strip(),
                            _normalize_plan_key(stripe_recovered.get("plan_key"), fallback="pro"),
                            str(stripe_recovered.get("subscription_status") or "active").strip().lower(),
                            stripe_recovered.get("subscription_cancel_at"),
                            int(bool(stripe_recovered.get("subscription_cancel_at_period_end"))),
                            recovered_limit,
                            recovered_limit,
                            recovered_limit,
                            recovered_balance,
                            str(stripe_recovered.get("subscription_start_date") or utc_now_iso()),
                            utc_now_iso(),
                            row_data.get("id"),
                        ),
                    )
                    conn.commit()
                row_data.update(
                    {
                        "stripe_customer_id": str(stripe_recovered.get("stripe_customer_id") or row_data.get("stripe_customer_id") or "").strip(),
                        "plan_key": _normalize_plan_key(stripe_recovered.get("plan_key"), fallback="pro"),
                        "subscription_active": 1,
                        "subscription_status": str(stripe_recovered.get("subscription_status") or "active").strip().lower(),
                        "subscription_cancel_at": stripe_recovered.get("subscription_cancel_at") or "",
                        "subscription_cancel_at_period_end": int(bool(stripe_recovered.get("subscription_cancel_at_period_end"))),
                        "monthly_limit": recovered_limit,
                        "topup_credits_balance": existing_topup,
                        "credits_balance": recovered_balance,
                        "subscription_start_date": str(stripe_recovered.get("subscription_start_date") or utc_now_iso()),
                        "updated_at": utc_now_iso(),
                    }
                )
                store_runtime_billing_snapshot(row_data.get("id"), row_data.get("email"), row_data)

        pending_topup = recover_pending_topup_credits_from_stripe(
            user_id=row_data.get("id"),
            user_email=row_data.get("email"),
            stripe_customer_id=row_data.get("stripe_customer_id"),
            updated_at_raw=row_data.get("updated_at"),
            config_path=DEFAULT_CONFIG_PATH,
        )
        recovered_topup_delta = max(0, _safe_int(pending_topup.get("credits_delta"), 0))
        if recovered_topup_delta > 0:
            next_topup_balance = max(0, _safe_int(row_data.get("topup_credits_balance"), 0)) + recovered_topup_delta
            next_balance = max(0, _safe_int(row_data.get("credits_balance"), 0)) + recovered_topup_delta
            now_iso = utc_now_iso()
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                conn.execute(
                    """
                    UPDATE users
                    SET stripe_customer_id = ?,
                        topup_credits_balance = ?,
                        credits_balance = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        str(pending_topup.get("stripe_customer_id") or row_data.get("stripe_customer_id") or "").strip(),
                        next_topup_balance,
                        next_balance,
                        now_iso,
                        row_data.get("id"),
                    ),
                )
                conn.commit()
            row_data["stripe_customer_id"] = str(pending_topup.get("stripe_customer_id") or row_data.get("stripe_customer_id") or "").strip()
            row_data["topup_credits_balance"] = next_topup_balance
            row_data["credits_balance"] = next_balance
            row_data["updated_at"] = now_iso
            store_runtime_billing_snapshot(row_data.get("id"), row_data.get("email"), row_data)
            mark_stripe_topup_payments_applied(
                list(pending_topup.get("payment_ids") or []),
                user_id=str(row_data.get("id") or "").strip(),
                credits_delta=recovered_topup_delta,
            )

        free_quota = int(PLAN_MONTHLY_QUOTAS.get("free", DEFAULT_MONTHLY_CREDIT_LIMIT))
        topup_balance = max(0, _safe_int(row_data["topup_credits_balance"], 0))
        cancel_at_raw = str(row_data["subscription_cancel_at"] or "").strip() or None
        cancel_at_period_end = bool(int(row_data["subscription_cancel_at_period_end"] or 0))
        is_active = _coerce_subscription_active(row_data["subscription_active"])

        if is_active and cancel_at_period_end and _is_subscription_cancel_expired(cancel_at_raw):
            downgraded_balance = free_quota + topup_balance
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                conn.execute(
                    """
                    UPDATE users
                    SET plan_key = 'free',
                        subscription_active = 0,
                        subscription_status = 'expired',
                        subscription_cancel_at_period_end = 0,
                        monthly_quota = ?,
                        monthly_limit = ?,
                        credits_limit = ?,
                        credits_balance = ?,
                        subscription_start_date = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (free_quota, free_quota, free_quota, downgraded_balance, utc_now_iso(), utc_now_iso(), row_data["id"]),
                )
                conn.commit()
            row_data["plan_key"] = "free"
            row_data["subscription_active"] = 0
            row_data["subscription_status"] = "expired"
            row_data["subscription_cancel_at_period_end"] = 0
            row_data["monthly_limit"] = free_quota
            row_data["credits_balance"] = downgraded_balance
            row_data["subscription_start_date"] = utc_now_iso()
            row_data["updated_at"] = utc_now_iso()
            monthly_quota = free_quota
            is_active = False
        else:
            monthly_quota = max(1, _safe_int(row_data["monthly_limit"], DEFAULT_MONTHLY_CREDIT_LIMIT))
        monthly_limit = monthly_quota
        subscription_start_date = str(row_data["subscription_start_date"] or "").strip() or None
        next_reset_at = _next_monthly_reset_iso(subscription_start_date)
        next_reset_in_days = _monthly_reset_days_left(subscription_start_date)

        return {
            "id": str(row_data["id"]),
            "email": str(row_data["email"] or "").strip().lower(),
            "credits_balance": int(row_data["credits_balance"] or 0),
            "credits_limit": monthly_limit,
            "monthly_limit": monthly_limit,
            "monthly_quota": monthly_quota,
            "topup_credits_balance": max(0, _safe_int(row_data["topup_credits_balance"], 0)),
            "subscription_start_date": subscription_start_date,
            "next_reset_at": next_reset_at,
            "next_reset_in_days": next_reset_in_days,
            "subscription_active": bool(is_active),
            "subscription_status": str(row_data["subscription_status"] or "").strip().lower(),
            "subscription_cancel_at": cancel_at_raw,
            "subscription_cancel_at_period_end": cancel_at_period_end,
            "plan_key": _normalize_plan_key(row_data["plan_key"], fallback=("pro" if is_active else "free")),
            "stripe_customer_id": str(row_data["stripe_customer_id"] or "").strip(),
            "updated_at": str(row_data["updated_at"] or "").strip(),
        }

    def create_stripe_billing_portal_session(customer_id: str, return_url: str) -> str:
        secret_key = get_stripe_secret_key(DEFAULT_CONFIG_PATH)
        if not secret_key:
            raise HTTPException(status_code=503, detail="Stripe is not configured. Add `stripe.secret_key` in `config.json` or set `STRIPE_SECRET_KEY`.")

        payload = urlencode({"customer": customer_id, "return_url": return_url}).encode("utf-8")
        req = urllib.request.Request(
            "https://api.stripe.com/v1/billing_portal/sessions",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {secret_key}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logging.warning("Stripe portal session creation failed: %s", exc)
            raise HTTPException(status_code=502, detail="Could not create Stripe portal session.")

        portal_url = str(data.get("url") or "").strip()
        if not portal_url:
            raise HTTPException(status_code=502, detail="Stripe portal did not return a URL.")
        return portal_url

    def create_stripe_subscription_checkout_session(
        user_id: str,
        user_email: str,
        plan_id: str,
        success_url: str,
        cancel_url: str,
        stripe_customer_id: Optional[str] = None,
    ) -> str:
        secret_key = get_stripe_secret_key(DEFAULT_CONFIG_PATH)
        if not secret_key:
            raise HTTPException(status_code=503, detail="Stripe is not configured. Add `stripe.secret_key` in `config.json` or set `STRIPE_SECRET_KEY`.")

        plan_key = str(plan_id or "").strip().lower()
        plan = STRIPE_SUBSCRIPTION_PLANS.get(plan_key)
        if not plan:
            raise HTTPException(status_code=400, detail="Invalid subscription plan.")

        price_id = str(plan.get("price_id") or "").strip()
        monthly_credits = int(plan.get("credits") or 0)
        if not price_id or monthly_credits <= 0:
            raise HTTPException(status_code=500, detail="Subscription plan is misconfigured.")

        form_items: list[tuple[str, str]] = [
            ("mode", "subscription"),
            ("success_url", success_url),
            ("cancel_url", cancel_url),
            ("line_items[0][price]", price_id),
            ("line_items[0][quantity]", "1"),
            ("client_reference_id", str(user_id)),
            ("metadata[user_id]", str(user_id)),
            ("metadata[email]", str(user_email or "")),
            ("metadata[plan_key]", plan_key),
            ("metadata[monthly_limit]", str(monthly_credits)),
            ("metadata[credits_limit]", str(monthly_credits)),
            ("subscription_data[metadata][user_id]", str(user_id)),
            ("subscription_data[metadata][email]", str(user_email or "")),
            ("subscription_data[metadata][plan_key]", plan_key),
            ("subscription_data[metadata][monthly_limit]", str(monthly_credits)),
            ("subscription_data[metadata][credits_limit]", str(monthly_credits)),
            ("allow_promotion_codes", "true"),
        ]

        normalized_customer_id = str(stripe_customer_id or "").strip()
        if normalized_customer_id:
            form_items.append(("customer", normalized_customer_id))
        elif str(user_email or "").strip():
            form_items.append(("customer_email", str(user_email).strip()))

        payload = urlencode(form_items).encode("utf-8")
        req = urllib.request.Request(
            "https://api.stripe.com/v1/checkout/sessions",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {secret_key}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logging.warning("Stripe subscription checkout session creation failed: %s", exc)
            raise HTTPException(status_code=502, detail="Could not create Stripe subscription checkout session.")

        checkout_url = str(data.get("url") or "").strip()
        if not checkout_url:
            raise HTTPException(status_code=502, detail="Stripe subscription checkout did not return a URL.")
        return checkout_url

    def create_stripe_topup_checkout_session(
        user_id: str,
        user_email: str,
        package_id: str,
        success_url: str,
        cancel_url: str,
        stripe_customer_id: Optional[str] = None,
    ) -> str:
        secret_key = get_stripe_secret_key(DEFAULT_CONFIG_PATH)
        if not secret_key:
            raise HTTPException(status_code=503, detail="Stripe is not configured. Add `stripe.secret_key` in `config.json` or set `STRIPE_SECRET_KEY`.")

        package_key = str(package_id or "").strip().lower()
        package = STRIPE_TOP_UP_PACKAGES.get(package_key)
        if not package:
            raise HTTPException(status_code=400, detail="Invalid top-up package.")

        credits = int(package.get("credits") or 0)
        amount_cents = int(package.get("amount_cents") or 0)
        price_id = str(package.get("price_id") or "").strip()
        price_usd = float(package.get("price_usd") or 0)
        if credits <= 0 or amount_cents <= 0 or not price_id:
            raise HTTPException(status_code=500, detail="Top-up package is misconfigured.")

        form_items: list[tuple[str, str]] = [
            ("mode", "payment"),
            ("success_url", success_url),
            ("cancel_url", cancel_url),
            ("payment_method_types[]", "card"),
            ("line_items[0][quantity]", "1"),
            ("line_items[0][price]", price_id),
            ("client_reference_id", str(user_id)),
            ("metadata[user_id]", str(user_id)),
            ("metadata[email]", str(user_email or "")),
            ("metadata[package_id]", package_key),
            ("metadata[credits_added]", str(credits)),
            ("metadata[payment_kind]", "topup"),
            ("metadata[stripe_price_id]", price_id),
            ("metadata[price_usd]", f"{price_usd:.2f}"),
            ("payment_intent_data[metadata][user_id]", str(user_id)),
            ("payment_intent_data[metadata][email]", str(user_email or "")),
            ("payment_intent_data[metadata][package_id]", package_key),
            ("payment_intent_data[metadata][credits_added]", str(credits)),
            ("payment_intent_data[metadata][payment_kind]", "topup"),
            ("payment_intent_data[metadata][stripe_price_id]", price_id),
        ]

        normalized_customer_id = str(stripe_customer_id or "").strip()
        if normalized_customer_id:
            form_items.append(("customer", normalized_customer_id))
        elif str(user_email or "").strip():
            form_items.append(("customer_email", str(user_email).strip()))

        payload = urlencode(form_items).encode("utf-8")
        req = urllib.request.Request(
            "https://api.stripe.com/v1/checkout/sessions",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {secret_key}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logging.warning("Stripe checkout session creation failed: %s", exc)
            raise HTTPException(status_code=502, detail="Could not create Stripe checkout session.")

        checkout_url = str(data.get("url") or "").strip()
        if not checkout_url:
            raise HTTPException(status_code=502, detail="Stripe checkout did not return a URL.")
        return checkout_url

    def touch_user_updated_at(user_id: str) -> None:
        target_user_id = str(user_id or "").strip()
        if not target_user_id:
            return

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                return
            try:
                execute_supabase_update_with_retry(
                    sb_client,
                    "users",
                    {"updated_at": utc_now_iso()},
                    eq_filters={"id": target_user_id},
                    operation_name=f"user updated_at touch for {target_user_id}",
                )
            except Exception:
                logging.debug("Could not touch Supabase user updated_at for user_id=%s", target_user_id)
            return

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.execute(
                "UPDATE users SET updated_at = ? WHERE id = ?",
                (utc_now_iso(), target_user_id),
            )
            conn.commit()

    def consume_ai_usage_or_raise(session_token: str, units: int = 1, db_path: Optional[Path] = None) -> dict:
        token = str(session_token or "").strip()
        if not token:
            raise HTTPException(status_code=401, detail="Authentication required.")

        budget = max(1, int(AI_DAILY_USAGE_LIMIT))
        units_to_add = max(1, int(units))
        day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]
        runtime_key = f"ai_usage:{day_key}:{token_hash}"
        target_db = db_path or DEFAULT_DB_PATH

        with _AI_USAGE_LOCK:
            raw = get_runtime_value(target_db, runtime_key)
            used = int(raw or "0") if str(raw or "").strip().isdigit() else 0
            if (used + units_to_add) > budget:
                raise HTTPException(
                    status_code=429,
                    detail=f"Daily AI usage limit exceeded ({budget} units/day).",
                )
            updated = used + units_to_add
            set_runtime_value(target_db, runtime_key, str(updated))

        return {"used": updated, "remaining": max(0, budget - updated), "limit": budget}

    def resolve_user_niche_from_session_token(token: str, db_path: Optional[Path] = None) -> str:
        session_token = str(token or "").strip()
        if not session_token:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                response = sb_client.table("users").select("niche").eq("token", session_token).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase session lookup failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            return str(rows[0].get("niche") or "").strip()

        sqlite_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(sqlite_db_path)
        with sqlite3.connect(sqlite_db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT niche FROM users WHERE token = ?", (session_token,)
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")
        return str(row["niche"] or "").strip()

    def resolve_user_niche_from_user_id(user_id: str, db_path: Optional[Path] = None) -> str:
        target_user_id = str(user_id or "").strip()
        if not target_user_id:
            raise HTTPException(status_code=401, detail="Missing authenticated user.")

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                response = (
                    sb_client.table("users")
                    .select("niche")
                    .eq("id", target_user_id)
                    .limit(1)
                    .execute()
                )
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase user niche lookup failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Authenticated user does not exist.")
            return str(rows[0].get("niche") or "").strip()

        sqlite_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(sqlite_db_path)
        with sqlite3.connect(sqlite_db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT niche FROM users WHERE id = ?",
                (target_user_id,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="Authenticated user does not exist.")
        return str(row["niche"] or "").strip()

    @app.post("/api/enrich")
    def run_enrichment(payload: EnrichRequest, background_tasks: BackgroundTasks, request: Request) -> JSONResponse:
        print(f"[enrich] POST /api/enrich — limit={payload.limit}")
        db_path = resolve_path(payload.db_path, DEFAULT_DB_PATH)
        try:
            ensure_system_tables(db_path)
        except Exception as _db_exc:
            print(f"[enrich] DB init error: {_db_exc}")
            raise HTTPException(status_code=500, detail="Database offline")

        payload_data = payload.model_dump()
        session_token, billing, access = resolve_plan_access_context(
            request,
            fallback_token=payload_data.get("token"),
            feature_key="deep_analysis",
        )
        user_id = require_current_user_id(request, fallback_token=payload_data.get("token"), db_path=db_path)
        session_token = require_authenticated_session(request, fallback_token=payload_data.pop("token", ""))
        payload_data["user_niche"] = resolve_user_niche_from_user_id(user_id, db_path=db_path)
        payload_data["_ai_model"] = str(access.get("ai_model") or DEFAULT_AI_MODEL)
        payload_data["_queue_priority"] = bool(access.get("queue_priority"))
        payload_data["_plan_type"] = str(access.get("plan_type") or billing.get("plan_key") or DEFAULT_PLAN_KEY)
        requested_limit = int(payload_data.get("limit") or 50)
        reserve_ai_credits_or_raise(user_id, feature_key="enrich", db_path=db_path)
        consume_ai_usage_or_raise(session_token, units=max(1, min(requested_limit, 500)), db_path=db_path)
        payload_data["_credits_per_success"] = get_ai_credit_cost("enrich")

        # High-scale mode guard: production throughput should run on Supabase/PostgreSQL primary mode.
        if os.environ.get("SNIPED_SCALE_MODE", os.environ.get("LEADFLOW_SCALE_MODE", "0")) == "1" and not is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            raise HTTPException(
                status_code=503,
                detail="High-scale mode requires Supabase primary mode. Enable supabase.primary_mode before enrichment.",
            )

        enrich_semaphore = getattr(app.state, "enrich_semaphore", None)
        if enrich_semaphore is None:
            enrich_semaphore = BoundedSemaphore(value=ENRICH_CONCURRENCY_LIMIT)
            app.state.enrich_semaphore = enrich_semaphore

        try:
            reserve_timeout = ENRICH_SEMAPHORE_TIMEOUT_SECONDS * (2 if bool(payload_data.get("_queue_priority")) else 1)
            acquired = bool(enrich_semaphore.acquire(timeout=reserve_timeout))
            if not acquired:
                raise TimeoutError("Timed out waiting for enrichment capacity slot.")
        except TimeoutError:
            return JSONResponse(
                status_code=429,
                content={"error": ENRICH_CAPACITY_ERROR_MESSAGE},
            )

        payload_data["_enrich_slot_reserved"] = True
        try:
            task = enqueue_task(app, background_tasks, db_path, user_id, "enrich", payload_data)
            return JSONResponse(status_code=202, content=task)
        except Exception:
            enrich_semaphore.release()
            raise

    @app.post("/api/export-ai")
    def run_export_ai(payload: ExportAIRequest, request: Request) -> dict:
        resolve_plan_access_context(request, feature_key="bulk_export")
        user_id = require_current_user_id(request)
        db_path = resolve_path(payload.db_path, DEFAULT_DB_PATH)
        ensure_system_tables(db_path)

        output_path = resolve_path(payload.output_csv, DEFAULT_AI_EXPORT)
        enricher = LeadEnricher(
            db_path=str(db_path),
            headless=True,
            config_path=str(DEFAULT_CONFIG_PATH),
            user_id=user_id,
        )
        exported = enricher.export_ai_mailer_ready(output_csv=str(output_path))
        return {"exported": exported, "output_csv": str(output_path)}

    @app.post("/api/mailer/send")
    def run_mailer(payload: MailerRequest, background_tasks: BackgroundTasks, request: Request) -> dict:
        print(f"[mailer] POST /api/mailer/send — limit={payload.limit}")
        _session_token, billing, access = resolve_plan_access_context(request)
        user_id = require_current_user_id(request)
        print(f"[mailer] user_id={user_id}")
        db_path = resolve_path(payload.db_path, DEFAULT_DB_PATH)
        try:
            ensure_system_tables(db_path)
        except Exception as _db_exc:
            print(f"[mailer] DB init error: {_db_exc}")
            raise HTTPException(status_code=500, detail="Database offline")

        if payload.delay_min > payload.delay_max:
            raise HTTPException(status_code=400, detail="delay_min must be <= delay_max")

        available_credits = max(0, int(billing.get("credits_balance") or 0))
        if available_credits <= 0:
            raise HTTPException(status_code=403, detail="Out of credits. Please upgrade.")

        payload_data = payload.model_dump()
        requested_limit = max(1, int(payload_data.get("limit") or 10))
        payload_data["limit"] = min(requested_limit, available_credits)
        payload_data["_queue_priority"] = bool(access.get("queue_priority"))
        payload_data["_ai_model"] = str(access.get("ai_model") or DEFAULT_AI_MODEL)
        payload_data["_credit_capped"] = bool(payload_data["limit"] < requested_limit)
        # Auto-detect server base URL for open tracking pixel (used if not manually configured)
        payload_data["_auto_base_url"] = str(request.base_url).rstrip("/")
        return enqueue_task(app, background_tasks, db_path, user_id, "mailer", payload_data)

    @app.post("/api/mailer/preview")
    def generate_mail_preview(payload: MailPreviewRequest, request: Request) -> dict:
        session_token, _billing, access = resolve_plan_access_context(request)
        user_id = resolve_user_id_from_session_token(session_token)
        db_path = resolve_path(payload.db_path, DEFAULT_DB_PATH)
        reserve_ai_credits_or_raise(user_id, feature_key="mail_preview", db_path=db_path)
        consume_ai_usage_or_raise(session_token, units=1)
        ensure_system_tables(db_path)
        config_path = resolve_path(payload.config_path, DEFAULT_CONFIG_PATH)

        try:
            mailer = AIMailer(
                db_path=str(db_path),
                config_path=str(config_path),
                model_name_override=str(access.get("ai_model") or DEFAULT_AI_MODEL),
                user_id=user_id,
                smtp_accounts_override=load_user_smtp_accounts(session_token=session_token, db_path=db_path),
            )
            if payload.mail_signature is not None:
                mailer.mail_signature = payload.mail_signature.strip()
            if payload.ghost_subject_template is not None:
                mailer.ghost_subject_template = payload.ghost_subject_template.strip()
            if payload.ghost_body_template is not None:
                mailer.ghost_body_template = payload.ghost_body_template.strip()
            if payload.golden_subject_template is not None:
                mailer.golden_subject_template = payload.golden_subject_template.strip()
            if payload.golden_body_template is not None:
                mailer.golden_body_template = payload.golden_body_template.strip()
            if payload.competitor_subject_template is not None:
                mailer.competitor_subject_template = payload.competitor_subject_template.strip()
            if payload.competitor_body_template is not None:
                mailer.competitor_body_template = payload.competitor_body_template.strip()
            if payload.speed_subject_template is not None:
                mailer.speed_subject_template = payload.speed_subject_template.strip()
            if payload.speed_body_template is not None:
                mailer.speed_body_template = payload.speed_body_template.strip()

            (subject, body), billing = run_ai_with_credit_policy(
                user_id=user_id,
                feature_key="mail_preview",
                generate_fn=lambda: mailer.generate_preview_email(regenerate=bool(payload.regenerate)),
                db_path=db_path,
            )
            return {
                "subject": subject,
                "body": body,
                "generated_at": utc_now_iso(),
                "regenerated": bool(payload.regenerate),
                "credits_charged": int(billing.get("credits_charged") or 0),
                "credits_balance": int(billing.get("credits_balance") or 0),
                "credits_limit": int(billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            logging.exception("Failed to generate mail preview")
            raise HTTPException(status_code=500, detail=f"Preview generation failed: {exc}")

    @app.get("/api/mailer/sequences")
    def get_mailer_sequences(request: Request) -> dict:
        user_id = require_current_user_id(request)
        return {"items": list_mailer_campaign_sequences(DEFAULT_DB_PATH, user_id=user_id)}

    @app.post("/api/mailer/sequences")
    def create_mailer_sequence(payload: CampaignSequenceRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        item = create_mailer_campaign_sequence(DEFAULT_DB_PATH, user_id=user_id, payload=payload.model_dump())
        maybe_sync_supabase(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)
        return {"status": "created", "item": item}

    @app.get("/api/mailer/templates")
    def get_mailer_templates(request: Request) -> dict:
        user_id = require_current_user_id(request)
        return {"items": list_saved_mail_templates(DEFAULT_DB_PATH, user_id=user_id)}

    @app.post("/api/mailer/templates")
    def create_mailer_template(payload: SavedTemplateRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        item = create_saved_mail_template(DEFAULT_DB_PATH, user_id=user_id, payload=payload.model_dump())
        maybe_sync_supabase(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)
        return {"status": "created", "item": item}

    @app.post("/api/mailer/events")
    def create_mailer_event(payload: CampaignEventRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        item = record_mailer_campaign_event(DEFAULT_DB_PATH, user_id=user_id, payload=payload.model_dump())
        maybe_sync_supabase(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)
        return {"status": "recorded", "item": item}

    @app.get("/api/mailer/campaign-stats")
    def get_mailer_stats(request: Request) -> dict:
        user_id = require_current_user_id(request)
        return get_mailer_campaign_stats(DEFAULT_DB_PATH, user_id=user_id)

    @app.post("/api/mailer/cold-outreach")
    def generate_cold_outreach(payload: ColdOutreachRequest, request: Request) -> dict:
        """Generate a World-Class Cold Outreach email for a specific business."""
        session_token, _billing, access = resolve_plan_access_context(request)
        user_id = resolve_user_id_from_session_token(session_token)
        reserve_ai_credits_or_raise(user_id, feature_key="cold_outreach", db_path=DEFAULT_DB_PATH)
        consume_ai_usage_or_raise(session_token, units=1)
        config_path = resolve_path(payload.config_path, DEFAULT_CONFIG_PATH)
        try:
            mailer = AIMailer(
                db_path=str(DEFAULT_DB_PATH),
                config_path=str(config_path),
                model_name_override=str(access.get("ai_model") or DEFAULT_AI_MODEL),
                user_id=user_id,
                smtp_accounts_override=load_user_smtp_accounts(session_token=session_token, db_path=DEFAULT_DB_PATH),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        (subject, body), billing = run_ai_with_credit_policy(
            user_id=user_id,
            feature_key="cold_outreach",
            generate_fn=lambda: mailer.generate_cold_outreach_email(
                business_name=payload.business_name.strip(),
                city=payload.city.strip(),
                niche=str(payload.niche or "").strip(),
                pain_point=str(payload.pain_point or "").strip(),
                competitors=[c.strip() for c in (payload.competitors or []) if c.strip()],
                monthly_loss=str(payload.monthly_loss or "").strip(),
                website_content=str(payload.website_content or "").strip(),
                linkedin_data=str(payload.linkedin_data or "").strip(),
                user_defined_icp=str(payload.user_defined_icp or "").strip(),
            ),
            db_path=DEFAULT_DB_PATH,
        )
        return {
            "subject": subject,
            "body": body,
            "generated_at": utc_now_iso(),
            "credits_charged": int(billing.get("credits_charged") or 0),
            "credits_balance": int(billing.get("credits_balance") or 0),
            "credits_limit": int(billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
        }

    @app.post("/api/intelligence/outreach-plan")
    def generate_deep_outreach_plan(payload: DeepOutreachIntelRequest, request: Request) -> dict:
        """Deep analysis + outreach plan from raw HTML/text content."""
        payload_data = payload.model_dump()
        session_token, _billing, access = resolve_plan_access_context(
            request,
            fallback_token=payload_data.get("token"),
            feature_key="deep_analysis",
        )
        user_id = resolve_user_id_from_session_token(session_token)
        reserve_ai_credits_or_raise(user_id, feature_key="mail_preview", db_path=DEFAULT_DB_PATH)
        consume_ai_usage_or_raise(session_token, units=1)

        client, model_name = load_openai_client(DEFAULT_CONFIG_PATH)
        if client is None:
            raise HTTPException(status_code=503, detail="OpenAI API key not configured.")

        raw_content = str(payload.raw_content or "").strip()
        detected_phones = sorted(
            {
                phone.strip()
                for phone in re.findall(r"(?:\+386\s?\d[\d\s\-/]{6,}\d)", raw_content)
                if phone and str(phone).strip()
            }
        )
        detected_emails = sorted(
            {
                email.strip().lower()
                for email in re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", raw_content)
                if email and str(email).strip()
            }
        )

        generic_prefixes = {"info", "office", "hello", "support", "kontakt", "contact", "admin", "sales"}
        personal_emails = [
            email
            for email in detected_emails
            if str(email).split("@", 1)[0].lower() not in generic_prefixes
        ]
        prioritized_emails = personal_emails if personal_emails else detected_emails

        inferred_niche = str(payload.user_niche or "").strip() or resolve_user_niche_from_session_token(session_token)
        factory = PromptFactory()
        system_prompt = factory.get_deep_outreach_system_prompt(inferred_niche)
        user_prompt = factory.get_deep_outreach_user_prompt(
            raw_content=raw_content,
            user_niche=inferred_niche,
            company_name=payload.company_name,
            location=payload.location,
        )

        try:
            response = client.chat.completions.create(
                model=str(access.get("ai_model") or model_name or DEFAULT_AI_MODEL).strip() or DEFAULT_AI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=700,
                temperature=0.35,
            )
            raw_text = str(response.choices[0].message.content or "").strip()
            json_match = re.search(r"\{.*\}", raw_text, re.DOTALL)
            parsed = json.loads(json_match.group() if json_match else raw_text)
        except Exception as exc:
            logging.exception("Deep outreach analysis failed")
            raise HTTPException(status_code=502, detail=f"Deep outreach analysis failed: {exc}")

        contact_info = parsed.get("contact_info") if isinstance(parsed, dict) else {}
        if not isinstance(contact_info, dict):
            contact_info = {}
        ai_phones = contact_info.get("phones") if isinstance(contact_info.get("phones"), list) else []
        ai_emails = contact_info.get("emails") if isinstance(contact_info.get("emails"), list) else []

        merged_phones = sorted(
            {
                str(v).strip()
                for v in [*detected_phones, *ai_phones]
                if v and str(v).strip()
            }
        )
        merged_emails = sorted(
            {
                str(v).strip().lower()
                for v in [*prioritized_emails, *ai_emails]
                if v and str(v).strip()
            }
        )

        final_output = {
            "contact_info": {
                "phones": merged_phones,
                "emails": merged_emails,
                "decision_maker": str(contact_info.get("decision_maker") or "").strip(),
            },
            "identified_gap": str(parsed.get("identified_gap") or "").strip(),
            "email_draft": str(parsed.get("email_draft") or "").strip(),
            "cold_call_script": str(parsed.get("cold_call_script") or "").strip(),
        }
        return final_output

    @app.post("/api/phone/extract")
    def extract_phone(payload: PhoneExtractRequest) -> dict:
        """Normalize and classify a raw phone number string."""
        # Sanitize country_hint to alpha-2 uppercase only
        raw_hint = (payload.country_hint or "").strip().upper()
        country_hint = raw_hint if re.match(r'^[A-Z]{2}$', raw_hint) else None
        result = PhoneExtractor().extract(payload.text.strip(), country_hint=country_hint)
        return result

    @app.post("/api/mailer/stop")
    def stop_mailer(request: Request) -> dict:
        """Signal the running mailer to stop, or clear an orphaned task immediately."""
        print("[mailer] POST /api/mailer/stop")
        user_id = require_current_user_id(request)
        reconcile_orphaned_active_tasks(app, DEFAULT_DB_PATH)
        latest_task = fetch_latest_task(DEFAULT_DB_PATH, "mailer", user_id=user_id)
        if latest_task.get("running") and not _is_task_thread_alive(app, latest_task.get("id")):
            finish_task_record(
                DEFAULT_DB_PATH,
                int(latest_task["id"]),
                status="stopped",
                result_payload=latest_task.get("result") if isinstance(latest_task.get("result"), dict) else None,
                error="Stopped by user.",
            )
            return {"status": "stopped", "task_id": latest_task.get("id")}
        app.state.mailer_stop_event.set()
        return {"status": "stop_requested"}

    @app.post("/api/tasks/{task_id}/retry")
    def retry_task(task_id: int, background_tasks: BackgroundTasks, request: Request) -> dict:
        _, _billing, access = resolve_plan_access_context(request)
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        task = fetch_task_by_id(db_path, task_id, user_id=user_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        if task["task_type"] not in TASK_TYPES:
            raise HTTPException(status_code=400, detail="Unsupported task type")

        request_payload = dict(task.get("last_request") or {})
        if not request_payload:
            raise HTTPException(status_code=400, detail="Task payload missing, cannot retry")

        if task["task_type"] == "scrape":
            request_payload.setdefault("country", "US")
            if bool(request_payload.get("export_targets")):
                require_feature_access(access.get("plan_key"), "bulk_export")
            available_credits = max(0, int(_billing.get("credits_balance") or 0))
            if available_credits <= 0:
                raise HTTPException(status_code=403, detail="Out of credits. Please upgrade.")
            requested_results = max(1, int(request_payload.get("results") or 25))
            request_payload["results"] = min(requested_results, available_credits)
            request_payload["_queue_priority"] = bool(access.get("queue_priority"))
        elif task["task_type"] == "enrich":
            require_feature_access(access.get("plan_key"), "deep_analysis")
            reserve_ai_credits_or_raise(user_id, feature_key="enrich", db_path=db_path)
            request_payload["_ai_model"] = str(access.get("ai_model") or DEFAULT_AI_MODEL)
            request_payload["_queue_priority"] = bool(access.get("queue_priority"))
        elif task["task_type"] == "mailer":
            available_credits = max(0, int(_billing.get("credits_balance") or 0))
            if available_credits <= 0:
                raise HTTPException(status_code=403, detail="Out of credits. Please upgrade.")
            requested_limit = max(1, int(request_payload.get("limit") or 10))
            request_payload["limit"] = min(requested_limit, available_credits)
            request_payload["_ai_model"] = str(access.get("ai_model") or DEFAULT_AI_MODEL)
            request_payload["_queue_priority"] = bool(access.get("queue_priority"))

        response = enqueue_task(app, background_tasks, db_path, user_id, task["task_type"], request_payload)
        response["retried_from"] = task_id
        return response

    # ── Lead Qualifier ─────────────────────────────────────────────────────────
    @app.get("/api/leads/qualify")
    def qualify_leads(request: Request) -> dict:
        """
                Dynamic Lead Qualifier (Niche-Agnostic).

                Uses user selected niche + context benchmark instead of fixed city-only logic.
                Buckets:
                    1. ghost            – digital presence exists, but critical niche signal is missing
                    2. invisible_giant  – operationally large, digitally quiet
                    3. tech_debt        – technical stack drags conversion and visibility

                Benchmark signals:
                    - niche_avg_score
                    - city_max_reviews

                Diagnostic rules (always computed):
                    - under_optimized: lead.ai_score < niche_avg_score * 0.5
                    - tech_laggard: tech_stack_score < 4
                    - authority_gap: authority < competitor_avg
        """
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        resolve_plan_access_context(request, feature_key="ai_lead_scoring")
        session_token = require_authenticated_session(request)
        user_id = require_current_user_id(request)
        selected_niche = resolve_user_niche_from_session_token(session_token)

        excluded_statuses = {
            "blacklisted", "closed", "low_priority", "paid",
            "qualified_not_interested",
        }

        qualifier_scope = "user"

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    id, business_name, website_url, phone_number,
                    rating, review_count, address, search_keyword,
                    insecure_site, main_shortcoming, ai_description,
                    ai_score, client_tier, status, enrichment_data
                FROM leads
                WHERE user_id = ?
                ORDER BY ai_score DESC, review_count ASC
                """,
                (user_id,),
            ).fetchall()

            # Backward-compatible fallback for pre-migration local datasets:
            # if no user-scoped leads exist, allow legacy leads so qualifier is usable.
            if not rows:
                legacy_rows = conn.execute(
                    """
                    SELECT
                        id, business_name, website_url, phone_number,
                        rating, review_count, address, search_keyword,
                        insecure_site, main_shortcoming, ai_description,
                        ai_score, client_tier, status, enrichment_data
                    FROM leads
                    WHERE user_id = 'legacy'
                    ORDER BY ai_score DESC, review_count ASC
                    """
                ).fetchall()
                if legacy_rows:
                    rows = legacy_rows
                    qualifier_scope = "legacy_fallback"

        leads_raw = [dict(r) for r in rows]

        # Filter out excluded statuses
        leads_raw = [
            l for l in leads_raw
            if str(l.get("status") or "").strip().lower() not in excluded_statuses
        ]

        # Build city → review counts (kept as part of context benchmark)
        city_reviews: dict[str, list[int]] = {}
        ai_scores: list[float] = []
        for lead in leads_raw:
            city = _qualifier_extract_city(lead.get("address", ""))
            if city:
                rc = lead.get("review_count")
                if isinstance(rc, int) and rc > 0:
                    city_reviews.setdefault(city, []).append(rc)
            score_val = _qualifier_to_float(lead.get("ai_score"), default=0.0)
            if score_val > 0:
                ai_scores.append(score_val)

        niche_avg_score = (sum(ai_scores) / len(ai_scores)) if ai_scores else 0.0

        # Persist latest qualifier benchmark snapshot per user.
        try:
            set_runtime_value(
                db_path,
                f"qualifier_context_benchmark:{user_id}",
                json.dumps(
                    {
                        "selected_niche": selected_niche,
                        "niche_avg_score": round(niche_avg_score, 2),
                        "city_reviews": city_reviews,
                        "updated_at": utc_now_iso(),
                    },
                    ensure_ascii=False,
                ),
            )
        except Exception:
            logging.debug("Could not persist qualifier benchmark snapshot for user %s", user_id)

        ghost: list[dict] = []
        invisible_giant: list[dict] = []
        tech_debt: list[dict] = []

        seen_ids: set[int] = set()

        for lead in leads_raw:
            lead_id = int(lead["id"])
            website = str(lead.get("website_url") or "").strip().lower()
            has_website = bool(website and website not in ("none", ""))
            rating = lead.get("rating")
            review_count = int(lead.get("review_count") or 0)
            city = _qualifier_extract_city(lead.get("address", ""))
            city_max = max(city_reviews.get(city, [0]))
            insecure = bool(lead.get("insecure_site"))
            ai_score = _qualifier_to_float(lead.get("ai_score"), default=0.0)
            metrics = _qualifier_extract_metrics(lead)

            under_optimized = bool(niche_avg_score > 0 and ai_score < (niche_avg_score * 0.5))
            tech_laggard = bool(metrics["tech_stack_score"] < 4)
            authority_gap = bool(metrics["competitor_avg"] > 0 and metrics["authority"] < metrics["competitor_avg"])

            # Niche-specific "Ghost" conditions
            ghost_by_paid_ads = (
                selected_niche == "Paid Ads Agency"
                and has_website
                and (not metrics["has_meta_pixel"] or not metrics["has_google_pixel"])
            )
            ghost_by_seo = (
                selected_niche == "SEO & Content"
                and has_website
                and (metrics["backlink_count"] < 5 or metrics["organic_traffic"] <= 0)
            )
            ghost_fallback = bool(under_optimized and not has_website)
            is_ghost = ghost_by_paid_ads or ghost_by_seo or ghost_fallback

            # Niche-specific "Invisible Giant"
            invisible_giant_b2b = (
                selected_niche == "B2B Service Provider"
                and metrics["employee_count"] > 20
                and metrics["social_activity_score"] < 3
            )
            invisible_giant_generic = bool(metrics["employee_count"] >= 50 and metrics["social_activity_score"] < 3)
            is_invisible_giant = invisible_giant_b2b or invisible_giant_generic

            # "Tech Debt"
            is_http = insecure or website.startswith("http://")
            is_tech_debt = bool(is_http or metrics["pagespeed_score"] < 40 or ai_score >= 6.0 or tech_laggard)

            # Determine enrichment competitive_hook if stored
            competitive_hook = ""
            raw_enrichment = lead.get("enrichment_data")
            if raw_enrichment:
                try:
                    enrichment_obj = json.loads(raw_enrichment)
                    competitive_hook = str(enrichment_obj.get("competitive_hook", "") or "")
                except Exception:
                    pass

            pain_point = _qualifier_pain_point(
                business_name=str(lead.get("business_name") or ""),
                has_website=has_website,
                rating=rating,
                review_count=review_count,
                city=city,
                city_max_reviews=city_max,
                keyword=str(lead.get("search_keyword") or ""),
                insecure=insecure,
                main_shortcoming=str(lead.get("main_shortcoming") or ""),
                ai_description=str(lead.get("ai_description") or ""),
                competitive_hook=competitive_hook,
            )

            out = {
                "id": lead_id,
                "business_name": lead.get("business_name"),
                "website_url": lead.get("website_url"),
                "rating": rating,
                "review_count": review_count,
                "address": lead.get("address"),
                "city": city,
                "search_keyword": lead.get("search_keyword"),
                "phone_number": lead.get("phone_number"),
                "ai_score": lead.get("ai_score"),
                "client_tier": lead.get("client_tier"),
                "status": lead.get("status"),
                "pain_point": pain_point,
                "suggested_hook": _qualifier_suggested_hook(
                    selected_niche=selected_niche,
                    business_name=str(lead.get("business_name") or ""),
                    keyword=str(lead.get("search_keyword") or ""),
                    city=city,
                    metrics=metrics,
                ),
                "benchmark": {
                    "selected_niche": selected_niche,
                    "niche_avg_score": round(niche_avg_score, 2),
                    "city_max_reviews": city_max,
                    "under_optimized": under_optimized,
                    "tech_laggard": tech_laggard,
                    "authority_gap": authority_gap,
                },
                "signals": {
                    "has_meta_pixel": metrics["has_meta_pixel"],
                    "has_google_pixel": metrics["has_google_pixel"],
                    "backlink_count": metrics["backlink_count"],
                    "organic_traffic": metrics["organic_traffic"],
                    "employee_count": metrics["employee_count"],
                    "social_activity_score": metrics["social_activity_score"],
                    "pagespeed_score": metrics["pagespeed_score"],
                    "tech_stack_score": metrics["tech_stack_score"],
                    "authority": metrics["authority"],
                    "competitor_avg": metrics["competitor_avg"],
                },
            }

            # Bucket #1 — The Ghost
            if is_ghost and lead_id not in seen_ids:
                out["pain_point"] = _qualifier_dynamic_pain_point(
                    bucket_name="ghost",
                    business_name=str(lead.get("business_name") or ""),
                    selected_niche=selected_niche,
                    city=city,
                    keyword=str(lead.get("search_keyword") or ""),
                    metrics=metrics,
                    ai_score=ai_score,
                    niche_avg_score=niche_avg_score,
                )
                ghost.append(out)
                seen_ids.add(lead_id)
                continue

            # Bucket #2 — The Invisible Giant
            if is_invisible_giant and lead_id not in seen_ids:
                out["pain_point"] = _qualifier_dynamic_pain_point(
                    bucket_name="invisible_giant",
                    business_name=str(lead.get("business_name") or ""),
                    selected_niche=selected_niche,
                    city=city,
                    keyword=str(lead.get("search_keyword") or ""),
                    metrics=metrics,
                    ai_score=ai_score,
                    niche_avg_score=niche_avg_score,
                )
                invisible_giant.append({**out, "city_max_reviews": city_max})
                seen_ids.add(lead_id)
                continue

            # Bucket #3 — Tech Debt
            if is_tech_debt and lead_id not in seen_ids:
                out["pain_point"] = _qualifier_dynamic_pain_point(
                    bucket_name="tech_debt",
                    business_name=str(lead.get("business_name") or ""),
                    selected_niche=selected_niche,
                    city=city,
                    keyword=str(lead.get("search_keyword") or ""),
                    metrics=metrics,
                    ai_score=ai_score,
                    niche_avg_score=niche_avg_score,
                )
                tech_debt.append(out)
                seen_ids.add(lead_id)

        # Backward compatibility for existing frontend keys.
        no_website = ghost
        invisible_local = invisible_giant
        low_authority = tech_debt

        return {
            "selected_niche": selected_niche,
            "scope": qualifier_scope,
            "context_benchmark": {
                "niche_avg_score": round(niche_avg_score, 2),
                "city_reviews": city_reviews,
            },
            "ghost": ghost,
            "invisible_giant": invisible_giant,
            "tech_debt": tech_debt,
            "no_website": no_website,
            "invisible_local": invisible_local,
            "low_authority": low_authority,
            "total": len(ghost) + len(invisible_giant) + len(tech_debt),
            "counts": {
                "ghost": len(ghost),
                "invisible_giant": len(invisible_giant),
                "tech_debt": len(tech_debt),
                "no_website": len(no_website),
                "invisible_local": len(invisible_local),
                "low_authority": len(low_authority),
            },
        }

    # ── Auth ───────────────────────────────────────────────────────────────────
    @app.post("/api/auth/register")
    def auth_register(req: RegisterRequest) -> dict:
        if req.niche not in NICHES:
            raise HTTPException(status_code=400, detail=f"Invalid niche. Must be one of: {', '.join(NICHES)}")
        email = req.email.strip().lower()
        if not email or "@" not in email:
            raise HTTPException(status_code=400, detail="Invalid email address.")
        if len(req.password) < 8:
            raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
        account_type = (req.account_type or "entrepreneur").strip().lower()
        display_name = (req.display_name or "").strip()
        contact_name = (req.contact_name or "").strip()
        free_monthly_quota = int(PLAN_MONTHLY_QUOTAS.get("free", DEFAULT_MONTHLY_CREDIT_LIMIT))
        salt = secrets.token_hex(32)
        password_hash = _hash_password(req.password, salt)
        token = str(uuid.uuid4())

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                existing = sb_client.table("users").select("id").eq("email", email).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase register query failed: {exc}")
            if list(getattr(existing, "data", None) or []):
                raise HTTPException(status_code=409, detail="An account with this email already exists.")
            try:
                sb_client.table("users").insert(
                    {
                        "email": email,
                        "password_hash": password_hash,
                        "salt": salt,
                        "niche": req.niche,
                        "account_type": account_type,
                        "display_name": display_name,
                        "contact_name": contact_name,
                        "token": token,
                        "credits_balance": free_monthly_quota,
                        "monthly_quota": free_monthly_quota,
                        "monthly_limit": free_monthly_quota,
                        "credits_limit": free_monthly_quota,
                        "subscription_start_date": utc_now_iso(),
                        "created_at": utc_now_iso(),
                    }
                ).execute()
            except Exception as exc:
                fallback_ok = False
                try:
                    # Backward-compatible insert for old users schemas without new profile columns
                    sb_client.table("users").insert(
                        {
                            "email": email,
                            "password_hash": password_hash,
                            "salt": salt,
                            "niche": req.niche,
                            "token": token,
                            "created_at": utc_now_iso(),
                        }
                    ).execute()
                    fallback_ok = True
                except Exception:
                    fallback_ok = False
                if fallback_ok:
                    return {"token": token, "niche": req.niche, "email": email, "display_name": display_name}
                if "duplicate" in str(exc).lower() or "unique" in str(exc).lower():
                    raise HTTPException(status_code=409, detail="An account with this email already exists.")
                raise HTTPException(status_code=502, detail=f"Supabase register failed: {exc}")
            return {"token": token, "niche": req.niche, "email": email, "display_name": display_name}

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            existing = conn.execute(
                "SELECT 1 FROM users WHERE email = ? LIMIT 1",
                (email,),
            ).fetchone()
            if existing is not None:
                raise HTTPException(status_code=409, detail="An account with this email already exists.")
        try:
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                conn.execute(
                    "INSERT INTO users (email, password_hash, salt, niche, account_type, display_name, contact_name, token, credits_balance, monthly_quota, monthly_limit, credits_limit, subscription_start_date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        email,
                        password_hash,
                        salt,
                        req.niche,
                        account_type,
                        display_name,
                        contact_name,
                        token,
                        free_monthly_quota,
                        free_monthly_quota,
                        free_monthly_quota,
                        free_monthly_quota,
                        utc_now_iso(),
                        utc_now_iso(),
                    ),
                )
                conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="An account with this email already exists.")
        return {"token": token, "niche": req.niche, "email": email, "display_name": display_name}

    @app.post("/api/auth/login")
    def auth_login(req: LoginRequest) -> dict:
        email = req.email.strip().lower()

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                response = sb_client.table("users").select(
                    "id,password_hash,salt,niche,token,display_name,contact_name,account_type"
                ).eq("email", email).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase login query failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Invalid email or password.")
            row = rows[0]
            expected = _hash_password(req.password, str(row.get("salt") or ""))
            if not secrets.compare_digest(expected, str(row.get("password_hash") or "")):
                raise HTTPException(status_code=401, detail="Invalid email or password.")
            token = str(row.get("token") or "") or str(uuid.uuid4())
            try:
                sb_client.table("users").update({"token": token}).eq("id", row.get("id")).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase token update failed: {exc}")
            return {
                "token": token,
                "niche": row.get("niche"),
                "email": email,
                "display_name": row.get("display_name") or "",
                "contact_name": row.get("contact_name") or "",
                "account_type": row.get("account_type") or "entrepreneur",
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, password_hash, salt, niche, token, display_name, contact_name, account_type FROM users WHERE email = ?",
                (email,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="Invalid email or password.")
        expected = _hash_password(req.password, row["salt"])
        if not secrets.compare_digest(expected, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid email or password.")
        token = row["token"] or str(uuid.uuid4())
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.execute("UPDATE users SET token = ? WHERE id = ?", (token, row["id"]))
            conn.commit()
        return {
            "token": token,
            "niche": row["niche"],
            "email": email,
            "display_name": row["display_name"] or "",
            "contact_name": row["contact_name"] or "",
            "account_type": row["account_type"] or "entrepreneur",
        }

    @app.post("/api/auth/request-password-reset")
    def auth_request_password_reset(req: ForgotPasswordRequest) -> dict:
        email = str(req.email or "").strip().lower()
        if not email or "@" not in email:
            raise HTTPException(status_code=400, detail="Invalid email address.")

        base_url = str(req.reset_base_url or "").strip().rstrip("/") or "http://localhost:5173/reset-password"
        reset_token = secrets.token_urlsafe(32)
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                response = sb_client.table("users").select("id,email").eq("email", email).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase password reset lookup failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                return {"ok": True, "message": "If the account exists, a reset link has been sent."}
            row = rows[0]
            smtp_user_id = str(row.get("id") or "").strip()
            try:
                sb_client.table("users").update(
                    {"reset_token": reset_token, "reset_token_expires_at": expires_at}
                ).eq("id", row.get("id")).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase password reset update failed: {exc}")
        else:
            ensure_users_table(DEFAULT_DB_PATH)
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT id, email FROM users WHERE email = ?", (email,)).fetchone()
                if row is None:
                    return {"ok": True, "message": "If the account exists, a reset link has been sent."}
                smtp_user_id = str(row["id"])
                conn.execute(
                    "UPDATE users SET reset_token = ?, reset_token_expires_at = ? WHERE id = ?",
                    (reset_token, expires_at, row["id"]),
                )
                conn.commit()

        smtp_account = get_primary_user_smtp_account(user_id=smtp_user_id, db_path=DEFAULT_DB_PATH)
        reset_link = f"{base_url}?token={quote_plus(reset_token)}"
        text_body = (
            "Sniped password reset\n\n"
            "We received a request to reset your password.\n"
            f"Reset link: {reset_link}\n\n"
            "This link expires in 1 hour. If you did not request this, you can ignore this email."
        )
        html_body = (
            "<p>We received a request to reset your Sniped password.</p>"
            f"<p><a href=\"{reset_link}\">Reset your password</a></p>"
            "<p>This link expires in 1 hour. If you did not request this, you can ignore this email.</p>"
        )
        try:
            send_auth_email(smtp_account, email, "Sniped password reset", text_body, html_body)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Password reset email failed: {classify_smtp_error(exc)}")

        return {"ok": True, "message": "If the account exists, a reset link has been sent."}

    @app.post("/api/auth/reset-password")
    def auth_reset_password(req: ResetPasswordRequest) -> dict:
        reset_token = str(req.token or "").strip()
        new_password = str(req.new_password or "")
        if not reset_token:
            raise HTTPException(status_code=400, detail="Reset token is required.")
        if len(new_password) < 8:
            raise HTTPException(status_code=400, detail="New password must be at least 8 characters.")

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                response = sb_client.table("users").select(
                    "id,reset_token_expires_at"
                ).eq("reset_token", reset_token).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase reset token lookup failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=400, detail="Reset link is invalid or expired.")
            row = rows[0]
            expires_at = parse_iso_datetime(str(row.get("reset_token_expires_at") or ""))
            if expires_at is None or expires_at <= datetime.now(timezone.utc):
                raise HTTPException(status_code=400, detail="Reset link is invalid or expired.")
            new_salt = secrets.token_hex(32)
            try:
                sb_client.table("users").update(
                    {
                        "salt": new_salt,
                        "password_hash": _hash_password(new_password, new_salt),
                        "reset_token": None,
                        "reset_token_expires_at": None,
                    }
                ).eq("id", row.get("id")).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase password reset failed: {exc}")
            return {"ok": True, "message": "Password updated successfully."}

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, reset_token_expires_at FROM users WHERE reset_token = ?",
                (reset_token,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=400, detail="Reset link is invalid or expired.")
            expires_at = parse_iso_datetime(row["reset_token_expires_at"])
            if expires_at is None or expires_at <= datetime.now(timezone.utc):
                raise HTTPException(status_code=400, detail="Reset link is invalid or expired.")
            new_salt = secrets.token_hex(32)
            conn.execute(
                "UPDATE users SET salt = ?, password_hash = ?, reset_token = NULL, reset_token_expires_at = NULL WHERE id = ?",
                (new_salt, _hash_password(new_password, new_salt), row["id"]),
            )
            conn.commit()
        return {"ok": True, "message": "Password updated successfully."}

    @app.post("/api/auth/profile")
    def auth_profile(req: SessionTokenRequest) -> dict:
        token = str(req.token or "").strip()
        if not token:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")
        billing = load_user_billing_context(token)

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            row_dict = None
            # Try full select first, fall back to minimal columns if schema is incomplete
            for attempt_cols in [
                "email,niche,display_name,contact_name,account_type,quickstart_completed,average_deal_value",
                "email,niche,display_name,contact_name,account_type",
            ]:
                try:
                    response = sb_client.table("users").select(attempt_cols).eq("token", token).limit(1).execute()
                    rows = list(getattr(response, "data", None) or [])
                    if not rows:
                        raise HTTPException(status_code=401, detail="Invalid or expired session token.")
                    row_dict = rows[0]
                    break
                except HTTPException:
                    raise
                except Exception as exc:
                    if "does not exist" in str(exc):
                        logging.warning(f"Supabase column mismatch on '{attempt_cols}': {exc}, retrying with fewer columns")
                        continue
                    raise HTTPException(status_code=502, detail=f"Supabase profile lookup failed: {exc}")
            if row_dict is None:
                raise HTTPException(status_code=502, detail="Supabase profile lookup failed: schema mismatch")

            plan_key = str(billing.get("plan_key") or "free").strip().lower()
            is_subscribed = bool(billing.get("subscription_active"))
            access = get_plan_feature_access(plan_key)
            return {
                "email": row_dict.get("email") or "",
                "niche": row_dict.get("niche") or "",
                "display_name": row_dict.get("display_name") or "",
                "contact_name": row_dict.get("contact_name") or "",
                "account_type": row_dict.get("account_type") or "entrepreneur",
                "credits_balance": int(billing.get("credits_balance") or 0),
                "credits_limit": int(billing.get("monthly_quota") or billing.get("monthly_limit") or billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
                "monthly_limit": int(billing.get("monthly_quota") or billing.get("monthly_limit") or billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
                "monthly_quota": int(billing.get("monthly_quota") or billing.get("monthly_limit") or billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
                "topup_credits_balance": int(billing.get("topup_credits_balance") or 0),
                "subscription_start_date": billing.get("subscription_start_date"),
                "next_reset_at": billing.get("next_reset_at"),
                "next_reset_in_days": billing.get("next_reset_in_days"),
                "subscription_active": is_subscribed,
                "isSubscribed": is_subscribed,
                "currentPlanName": str(PLAN_DISPLAY_NAMES.get(plan_key, 'Pro Plan' if is_subscribed else 'Free Plan')),
                "subscription_status": str(billing.get("subscription_status") or "").strip().lower(),
                "subscription_cancel_at": billing.get("subscription_cancel_at"),
                "subscription_cancel_at_period_end": bool(billing.get("subscription_cancel_at_period_end")),
                "plan_key": plan_key,
                "plan_type": str(access.get("plan_type") or "Starter"),
                "feature_access": access,
                "quickstart_completed": bool(row_dict.get("quickstart_completed") or False),
                "average_deal_value": float(row_dict.get("average_deal_value") or DEFAULT_AVERAGE_DEAL_VALUE),
            }

        # Fallback to SQLite if Supabase not available
        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT email, niche, display_name, contact_name, account_type, quickstart_completed, average_deal_value FROM users WHERE token = ?",
                (token,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")
        plan_key = str(billing.get("plan_key") or "free").strip().lower()
        is_subscribed = bool(billing.get("subscription_active"))
        access = get_plan_feature_access(plan_key)
        return {
            "email": row["email"] or "",
            "niche": row["niche"] or "",
            "display_name": row["display_name"] or "",
            "contact_name": row["contact_name"] or "",
            "account_type": row["account_type"] or "entrepreneur",
            "credits_balance": int(billing.get("credits_balance") or 0),
            "credits_limit": int(billing.get("monthly_quota") or billing.get("monthly_limit") or billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
            "monthly_limit": int(billing.get("monthly_quota") or billing.get("monthly_limit") or billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
            "monthly_quota": int(billing.get("monthly_quota") or billing.get("monthly_limit") or billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
            "topup_credits_balance": int(billing.get("topup_credits_balance") or 0),
            "subscription_start_date": billing.get("subscription_start_date"),
            "next_reset_at": billing.get("next_reset_at"),
            "next_reset_in_days": billing.get("next_reset_in_days"),
            "subscription_active": is_subscribed,
            "isSubscribed": is_subscribed,
            "currentPlanName": str(PLAN_DISPLAY_NAMES.get(plan_key, 'Pro Plan' if is_subscribed else 'Free Plan')),
            "subscription_status": str(billing.get("subscription_status") or "").strip().lower(),
            "subscription_cancel_at": billing.get("subscription_cancel_at"),
            "subscription_cancel_at_period_end": bool(billing.get("subscription_cancel_at_period_end")),
            "plan_key": plan_key,
            "plan_type": str(access.get("plan_type") or "Starter"),
            "feature_access": access,
            "quickstart_completed": bool(row["quickstart_completed"] or False),
            "average_deal_value": float(row["average_deal_value"] or DEFAULT_AVERAGE_DEAL_VALUE),
        }

    @app.post("/api/stripe/create-portal-session")
    def stripe_create_portal_session(request: Request) -> dict:
        session_token = require_authenticated_session(request)
        billing = load_user_billing_context(session_token)

        if not bool(billing.get("subscription_active")):
            return {
                "ok": True,
                "redirect_url": "/pricing",
                "reason": "no_active_subscription",
            }

        stripe_customer_id = str(billing.get("stripe_customer_id") or "").strip()
        if not stripe_customer_id:
            return {
                "ok": True,
                "redirect_url": "/pricing",
                "reason": "missing_customer_id",
            }

        return_url = str(os.environ.get("STRIPE_PORTAL_RETURN_URL") or "").strip()
        if not return_url:
            return_url = f"{get_dashboard_base_url(DEFAULT_CONFIG_PATH, request=request)}/app"

        portal_url = create_stripe_billing_portal_session(stripe_customer_id, return_url)
        return {
            "ok": True,
            "url": portal_url,
        }

    @app.post("/api/stripe/create-subscription-session")
    def stripe_create_subscription_session(payload: StripeSubscriptionSessionRequest, request: Request) -> dict:
        session_token = require_authenticated_session(request)
        billing = load_user_billing_context(session_token)

        user_id = str(billing.get("id") or "").strip()
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")

        user_email = str(billing.get("email") or "").strip().lower()
        plan_key = str(payload.plan_id or "").strip().lower()
        plan = STRIPE_SUBSCRIPTION_PLANS.get(plan_key)
        if not plan:
            raise HTTPException(status_code=400, detail="Invalid subscription plan.")

        dashboard_base_url = get_dashboard_base_url(DEFAULT_CONFIG_PATH, request=request)
        success_url = str(os.environ.get("STRIPE_SUBSCRIPTION_SUCCESS_URL") or "").strip()
        cancel_url = str(os.environ.get("STRIPE_SUBSCRIPTION_CANCEL_URL") or "").strip()
        if not success_url:
            success_url = f"{dashboard_base_url}/app?checkout=success&plan={plan_key}"
        if not cancel_url:
            cancel_url = f"{dashboard_base_url}/pricing?checkout=cancel&plan={plan_key}"

        checkout_url = create_stripe_subscription_checkout_session(
            user_id=user_id,
            user_email=user_email,
            plan_id=plan_key,
            success_url=success_url,
            cancel_url=cancel_url,
            stripe_customer_id=str(billing.get("stripe_customer_id") or "").strip() or None,
        )

        return {
            "ok": True,
            "url": checkout_url,
            "plan_id": plan_key,
            "credits": int(plan.get("credits") or 0),
            "price_id": str(plan.get("price_id") or "").strip(),
            "display_name": str(plan.get("display_name") or plan_key.title()),
        }

    @app.post("/api/stripe/create-topup-session")
    def stripe_create_topup_session(payload: StripeTopUpSessionRequest, request: Request) -> dict:
        session_token = require_authenticated_session(request)
        billing = load_user_billing_context(session_token)

        user_id = str(billing.get("id") or "").strip()
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")

        user_email = str(billing.get("email") or "").strip().lower()
        package_key = str(payload.package_id or "").strip().lower()
        if package_key not in STRIPE_TOP_UP_PACKAGES:
            raise HTTPException(status_code=400, detail="Invalid top-up package.")
        package = STRIPE_TOP_UP_PACKAGES.get(package_key, {})
        package_credits = max(0, int(package.get("credits") or 0))

        dashboard_base_url = get_dashboard_base_url(DEFAULT_CONFIG_PATH, request=request)
        success_url = str(os.environ.get("STRIPE_TOPUP_SUCCESS_URL") or "").strip()
        cancel_url = str(os.environ.get("STRIPE_TOPUP_CANCEL_URL") or "").strip()
        if not success_url:
            success_url = f"{dashboard_base_url}/app?topup=success&topup_package={quote_plus(package_key)}&topup_credits={package_credits}"
        if not cancel_url:
            cancel_url = f"{dashboard_base_url}/app?topup=cancel"

        checkout_url = create_stripe_topup_checkout_session(
            user_id=user_id,
            user_email=user_email,
            package_id=package_key,
            success_url=success_url,
            cancel_url=cancel_url,
            stripe_customer_id=str(billing.get("stripe_customer_id") or "").strip() or None,
        )

        return {
            "ok": True,
            "url": checkout_url,
            "package_id": package_key,
            "credits": int(package.get("credits") or 0),
            "price_usd": float(package.get("price_usd") or 0),
            "price_id": str(package.get("price_id") or "").strip(),
        }

    @app.put("/api/auth/profile")
    def auth_profile_update(req: ProfileUpdateRequest) -> dict:
        token = str(req.token or "").strip()
        if not token:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")

        new_password = (req.new_password or "").strip()
        current_password = req.current_password or ""
        if new_password:
            if len(new_password) < 8:
                raise HTTPException(status_code=400, detail="New password must be at least 8 characters.")
            if not current_password:
                raise HTTPException(status_code=400, detail="Current password is required to set a new password.")

        updates: dict[str, Any] = {}
        if req.display_name is not None:
            updates["display_name"] = str(req.display_name).strip()
        if req.contact_name is not None:
            updates["contact_name"] = str(req.contact_name).strip()
        if req.niche is not None:
            niche = str(req.niche).strip()
            if niche not in NICHES:
                raise HTTPException(status_code=400, detail=f"Invalid niche. Must be one of: {', '.join(NICHES)}")
            updates["niche"] = niche
        if req.account_type is not None:
            account_type = str(req.account_type).strip().lower()
            if account_type not in ACCOUNT_TYPES:
                raise HTTPException(status_code=400, detail="Invalid account type.")
            updates["account_type"] = account_type
        if req.quickstart_completed is not None:
            updates["quickstart_completed"] = bool(req.quickstart_completed)
        if req.average_deal_value is not None:
            try:
                average_deal_value = round(float(req.average_deal_value), 2)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Average deal value must be a valid number.")
            updates["average_deal_value"] = average_deal_value if average_deal_value > 0 else float(DEFAULT_AVERAGE_DEAL_VALUE)
        updates["updated_at"] = utc_now_iso()

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")

            try:
                response = sb_client.table("users").select(
                    "id,email,niche,display_name,contact_name,account_type,password_hash,salt,quickstart_completed,average_deal_value"
                ).eq("token", token).limit(1).execute()
            except Exception as exc:
                if "does not exist" in str(exc):
                    # Retry with minimal columns if schema is incomplete
                    logging.warning(f"Supabase column mismatch on profile update lookup: {exc}, retrying with fewer columns")
                    try:
                        response = sb_client.table("users").select(
                            "id,email,niche,display_name,contact_name,account_type,password_hash,salt"
                        ).eq("token", token).limit(1).execute()
                        # Remove columns that don't exist from the updates dict too
                        updates.pop("quickstart_completed", None)
                        updates.pop("average_deal_value", None)
                    except Exception as exc2:
                        raise HTTPException(status_code=502, detail=f"Supabase profile lookup failed: {exc2}")
                else:
                    raise HTTPException(status_code=502, detail=f"Supabase profile lookup failed: {exc}")

            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            row = rows[0]

            if new_password:
                expected = _hash_password(current_password, str(row.get("salt") or ""))
                if not secrets.compare_digest(expected, str(row.get("password_hash") or "")):
                    raise HTTPException(status_code=401, detail="Current password is incorrect.")
                new_salt = secrets.token_hex(32)
                updates["salt"] = new_salt
                updates["password_hash"] = _hash_password(new_password, new_salt)

            if updates:
                try:
                    _, applied_updates = execute_supabase_update_with_retry(
                        sb_client,
                        "users",
                        updates,
                        eq_filters={"id": row.get("id")},
                        operation_name="profile update",
                    )
                    if applied_updates:
                        updates = applied_updates
                except Exception as exc:
                    # If update fails due to missing columns, strip those columns and retry
                    if "does not exist" in str(exc):
                        logging.warning(f"Supabase column mismatch on profile update: {exc}, retrying without missing columns")
                        updates.pop("quickstart_completed", None)
                        updates.pop("average_deal_value", None)
                        try:
                            _, applied_updates = execute_supabase_update_with_retry(
                                sb_client,
                                "users",
                                updates,
                                eq_filters={"id": row.get("id")},
                                operation_name="profile update",
                            )
                            if applied_updates:
                                updates = applied_updates
                        except Exception as exc2:
                            raise HTTPException(status_code=502, detail=f"Supabase profile update failed: {exc2}")
                    else:
                        raise HTTPException(status_code=502, detail=f"Supabase profile update failed: {exc}")

            return {
                "email": row.get("email") or "",
                "niche": updates.get("niche", row.get("niche") or ""),
                "display_name": updates.get("display_name", row.get("display_name") or ""),
                "contact_name": updates.get("contact_name", row.get("contact_name") or ""),
                "account_type": updates.get("account_type", row.get("account_type") or "entrepreneur"),
                "quickstart_completed": bool(updates.get("quickstart_completed", row.get("quickstart_completed") or False)),
                "average_deal_value": float(updates.get("average_deal_value", row.get("average_deal_value") or DEFAULT_AVERAGE_DEAL_VALUE)),
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, email, niche, display_name, contact_name, account_type, password_hash, salt, quickstart_completed, average_deal_value FROM users WHERE token = ?",
                (token,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")

            if new_password:
                expected = _hash_password(current_password, row["salt"])
                if not secrets.compare_digest(expected, row["password_hash"]):
                    raise HTTPException(status_code=401, detail="Current password is incorrect.")
                new_salt = secrets.token_hex(32)
                updates["salt"] = new_salt
                updates["password_hash"] = _hash_password(new_password, new_salt)

            if updates:
                conn.execute(
                    """
                    UPDATE users
                    SET display_name = COALESCE(?, display_name),
                        contact_name = COALESCE(?, contact_name),
                        niche = COALESCE(?, niche),
                        account_type = COALESCE(?, account_type),
                        quickstart_completed = COALESCE(?, quickstart_completed),
                        average_deal_value = COALESCE(?, average_deal_value),
                        salt = COALESCE(?, salt),
                        password_hash = COALESCE(?, password_hash),
                        updated_at = COALESCE(?, updated_at)
                    WHERE id = ?
                    """,
                    (
                        updates.get("display_name"),
                        updates.get("contact_name"),
                        updates.get("niche"),
                        updates.get("account_type"),
                        updates.get("quickstart_completed"),
                        updates.get("average_deal_value"),
                        updates.get("salt"),
                        updates.get("password_hash"),
                        updates.get("updated_at"),
                        row["id"],
                    ),
                )
                conn.commit()

        return {
            "email": row["email"] or "",
            "niche": updates.get("niche", row["niche"] or ""),
            "display_name": updates.get("display_name", row["display_name"] or ""),
            "contact_name": updates.get("contact_name", row["contact_name"] or ""),
            "account_type": updates.get("account_type", row["account_type"] or "entrepreneur"),
            "quickstart_completed": bool(updates.get("quickstart_completed", row["quickstart_completed"] or False)),
            "average_deal_value": float(updates.get("average_deal_value", row["average_deal_value"] or DEFAULT_AVERAGE_DEAL_VALUE)),
        }

    @app.get("/api/auth/personal-goal")
    def auth_personal_goal(request: Request) -> dict:
        session_token = require_authenticated_session(request)
        user_id = resolve_user_id_from_session_token(session_token, db_path=DEFAULT_DB_PATH)
        runtime_key = f"personal_goal:{user_id}"
        raw = get_runtime_value(DEFAULT_DB_PATH, runtime_key)

        if raw:
            try:
                parsed = json.loads(raw)
                name = str(parsed.get("name") or "").strip() or "My Goal"
                amount = float(parsed.get("amount") or 0)
                if amount <= 0:
                    amount = float(MRR_GOAL)
                currency = str(parsed.get("currency") or DEFAULT_GOAL_CURRENCY).upper().strip()
                if currency not in ALLOWED_GOAL_CURRENCIES:
                    currency = DEFAULT_GOAL_CURRENCY
                return {
                    "name": name,
                    "amount": amount,
                    "currency": currency,
                    "source": "runtime",
                }
            except Exception:
                pass

        return {
            "name": "My Goal",
            "amount": float(MRR_GOAL),
            "currency": DEFAULT_GOAL_CURRENCY,
            "source": "default",
        }

    @app.put("/api/auth/personal-goal")
    def auth_personal_goal_update(req: PersonalGoalUpdateRequest, request: Request) -> dict:
        session_token = require_authenticated_session(request, fallback_token=req.token)
        user_id = resolve_user_id_from_session_token(session_token, db_path=DEFAULT_DB_PATH)

        name = str(req.name or "").strip() or "My Goal"
        amount = float(req.amount or 0)
        if amount <= 0:
            raise HTTPException(status_code=400, detail="Goal amount must be greater than 0.")

        currency = str(req.currency or DEFAULT_GOAL_CURRENCY).upper().strip()
        if currency not in ALLOWED_GOAL_CURRENCIES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid currency. Must be one of: {', '.join(sorted(ALLOWED_GOAL_CURRENCIES))}",
            )

        payload = {
            "name": name,
            "amount": round(amount, 2),
            "currency": currency,
            "updated_at": utc_now_iso(),
        }
        runtime_key = f"personal_goal:{user_id}"
        set_runtime_value(DEFAULT_DB_PATH, runtime_key, json.dumps(payload, ensure_ascii=True))

        return {
            "name": payload["name"],
            "amount": payload["amount"],
            "currency": payload["currency"],
            "ok": True,
        }

    @app.post("/api/auth/delete-account")
    def auth_delete_account(req: DeleteAccountRequest) -> dict:
        token = str(req.token or "").strip()
        current_password = str(req.current_password or "")
        if not token:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")
        if not current_password:
            raise HTTPException(status_code=400, detail="Current password is required.")

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                response = sb_client.table("users").select(
                    "id,email,password_hash,salt"
                ).eq("token", token).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase account lookup failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            row = rows[0]
            expected = _hash_password(current_password, str(row.get("salt") or ""))
            if not secrets.compare_digest(expected, str(row.get("password_hash") or "")):
                raise HTTPException(status_code=401, detail="Current password is incorrect.")
            try:
                if supabase_table_available(DEFAULT_CONFIG_PATH, "jobs"):
                    sb_client.table("jobs").delete().eq("user_id", str(row.get("email") or "")).execute()
                    sb_client.table("jobs").delete().eq("user_id", token).execute()
                sb_client.table("users").delete().eq("id", row.get("id")).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase account deletion failed: {exc}")
            return {"ok": True, "message": "Account deleted successfully."}

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, email, password_hash, salt FROM users WHERE token = ?",
                (token,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            expected = _hash_password(current_password, row["salt"])
            if not secrets.compare_digest(expected, row["password_hash"]):
                raise HTTPException(status_code=401, detail="Current password is incorrect.")
            _ensure_jobs_table_sqlite(DEFAULT_DB_PATH)
            conn.execute("DELETE FROM jobs WHERE user_id IN (?, ?)", (row["email"], token))
            conn.execute("DELETE FROM users WHERE id = ?", (row["id"],))
            conn.commit()
        return {"ok": True, "message": "Account deleted successfully."}

    # ── Cold Email Opener ──────────────────────────────────────────────────────
    @app.post("/api/cold-email-opener")
    def cold_email_opener(req: ColdEmailOpenerRequest, request: Request) -> dict:
        session_token, _billing, access = resolve_plan_access_context(request, fallback_token=req.token)
        user_id = resolve_user_id_from_session_token(session_token)
        reserve_ai_credits_or_raise(user_id, feature_key="cold_email_opener", db_path=DEFAULT_DB_PATH)
        consume_ai_usage_or_raise(session_token, units=1)
        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                response = sb_client.table("users").select("niche").eq("token", session_token).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase session lookup failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            niche = str(rows[0].get("niche") or "").strip()
        else:
            ensure_users_table(DEFAULT_DB_PATH)
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT niche FROM users WHERE token = ?", (session_token,)
                ).fetchone()
            if row is None:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            niche = row["niche"]
        opener, billing = run_ai_with_credit_policy(
            user_id=user_id,
            feature_key="cold_email_opener",
            generate_fn=lambda: generate_cold_email_opener_for_niche(
                niche,
                req.prospect_data,
                pack_mode=req.pack_mode,
                model_name_override=str(access.get("ai_model") or DEFAULT_AI_MODEL),
            ),
            db_path=DEFAULT_DB_PATH,
        )
        return {
            "opener": opener,
            "niche": niche,
            "credits_charged": int(billing.get("credits_charged") or 0),
            "credits_balance": int(billing.get("credits_balance") or 0),
            "credits_limit": int(billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
        }

    @app.post("/api/onboarding/complete")
    def onboarding_complete(req: OnboardingCompleteRequest) -> dict:
        if req.niche not in NICHES:
            raise HTTPException(status_code=400, detail=f"Invalid niche. Must be one of: {', '.join(NICHES)}")

        email = req.email.strip().lower()
        if not email or "@" not in email:
            raise HTTPException(status_code=400, detail="Invalid email address.")
        if len(req.password) < 8:
            raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
        if not str(req.prospect_data or "").strip():
            raise HTTPException(status_code=400, detail="Prospect description is required.")

        # Requirement: persist the user only after opener generation succeeds.
        opener = generate_cold_email_opener_for_niche(req.niche, req.prospect_data)

        account_type = (req.account_type or "entrepreneur").strip().lower()
        display_name = (req.display_name or "").strip()
        contact_name = (req.contact_name or "").strip()
        free_monthly_quota = int(PLAN_MONTHLY_QUOTAS.get("free", DEFAULT_MONTHLY_CREDIT_LIMIT))
        salt = secrets.token_hex(32)
        password_hash = _hash_password(req.password, salt)
        token = str(uuid.uuid4())

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            try:
                existing = sb_client.table("users").select("id").eq("email", email).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase onboarding query failed: {exc}")
            if list(getattr(existing, "data", None) or []):
                raise HTTPException(status_code=409, detail="An account with this email already exists.")

            try:
                sb_client.table("users").insert(
                    {
                        "email": email,
                        "password_hash": password_hash,
                        "salt": salt,
                        "niche": req.niche,
                        "account_type": account_type,
                        "display_name": display_name,
                        "contact_name": contact_name,
                        "token": token,
                        "credits_balance": free_monthly_quota,
                        "monthly_quota": free_monthly_quota,
                        "monthly_limit": free_monthly_quota,
                        "credits_limit": free_monthly_quota,
                        "subscription_start_date": utc_now_iso(),
                        "created_at": utc_now_iso(),
                    }
                ).execute()
            except Exception as exc:
                if "duplicate" in str(exc).lower() or "unique" in str(exc).lower():
                    raise HTTPException(status_code=409, detail="An account with this email already exists.")
                raise HTTPException(status_code=502, detail=f"Supabase onboarding save failed: {exc}")

            return {
                "token": token,
                "niche": req.niche,
                "email": email,
                "display_name": display_name,
                "opener": opener,
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            existing = conn.execute(
                "SELECT 1 FROM users WHERE email = ? LIMIT 1",
                (email,),
            ).fetchone()
            if existing is not None:
                raise HTTPException(status_code=409, detail="An account with this email already exists.")
        try:
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                conn.execute(
                    "INSERT INTO users (email, password_hash, salt, niche, account_type, display_name, contact_name, token, credits_balance, monthly_quota, monthly_limit, credits_limit, subscription_start_date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        email,
                        password_hash,
                        salt,
                        req.niche,
                        account_type,
                        display_name,
                        contact_name,
                        token,
                        free_monthly_quota,
                        free_monthly_quota,
                        free_monthly_quota,
                        free_monthly_quota,
                        utc_now_iso(),
                        utc_now_iso(),
                    ),
                )
                conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="An account with this email already exists.")

        return {
            "token": token,
            "niche": req.niche,
            "email": email,
            "display_name": display_name,
            "opener": opener,
        }

    @app.post("/api/redeem/appsumo")
    def redeem_appsumo(req: AppSumoRedeemRequest, request: Request) -> dict:
        session_token = require_authenticated_session(request)
        user_id = resolve_user_id_from_session_token(session_token)

        coupon = str(req.coupon_code or "").strip().upper()
        if not coupon:
            raise HTTPException(status_code=400, detail="Coupon code is required.")
        if not re.match(r"^[A-Z0-9]{4,}(?:-[A-Z0-9]{4,}){1,4}$", coupon):
            raise HTTPException(status_code=400, detail="Invalid coupon format.")

        storage_key = f"appsumo:redeemed:{user_id}"
        existing_raw = get_runtime_value(DEFAULT_DB_PATH, storage_key)
        if existing_raw:
            try:
                existing = json.loads(existing_raw)
            except Exception:
                existing = {"coupon_code": str(existing_raw), "redeemed_at": None}
            return {
                "ok": True,
                "already_redeemed": True,
                "coupon_code": str(existing.get("coupon_code") or coupon),
                "redeemed_at": existing.get("redeemed_at"),
                "message": "Coupon already redeemed for this account.",
            }

        redeemed_at = utc_now_iso()
        set_runtime_value(
            DEFAULT_DB_PATH,
            storage_key,
            json.dumps({"coupon_code": coupon, "redeemed_at": redeemed_at}, ensure_ascii=False),
        )
        return {
            "ok": True,
            "already_redeemed": False,
            "coupon_code": coupon,
            "redeemed_at": redeemed_at,
            "message": "Coupon redeemed successfully.",
        }

    @app.post("/api/stripe/webhook")
    async def stripe_webhook(request: Request) -> dict:
        raw_body = await request.body()
        signature_header = str(request.headers.get("Stripe-Signature") or "").strip()
        webhook_secret = get_stripe_webhook_secret(DEFAULT_CONFIG_PATH)

        if webhook_secret:
            try:
                parts = {}
                for chunk in signature_header.split(","):
                    if "=" not in chunk:
                        continue
                    key, value = chunk.split("=", 1)
                    parts[key.strip()] = value.strip()
                timestamp = str(parts.get("t") or "").strip()
                received_v1 = str(parts.get("v1") or "").strip()
                signed_payload = f"{timestamp}.{raw_body.decode('utf-8')}"
                expected_v1 = hmac.new(
                    webhook_secret.encode("utf-8"),
                    signed_payload.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()
                if not timestamp or not received_v1 or not hmac.compare_digest(expected_v1, received_v1):
                    raise HTTPException(status_code=400, detail="Invalid Stripe webhook signature.")
            except HTTPException:
                raise
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid Stripe webhook signature.")

        try:
            event = json.loads(raw_body.decode("utf-8"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid webhook payload.")

        parsed = extract_payment_refresh_payload(event)
        if not bool(parsed.get("should_process")):
            return {"ok": True, "ignored": True}

        user_id = str(parsed.get("user_id") or "").strip()
        user_email = str(parsed.get("user_email") or "").strip().lower()
        stripe_customer_id = str(parsed.get("stripe_customer_id") or "").strip()
        credits_delta = int(parsed.get("credits_delta") or 0)
        event_type = str(parsed.get("event_type") or "").strip().lower()
        checkout_mode = str(parsed.get("checkout_mode") or "").strip().lower()
        billing_reason = str(parsed.get("billing_reason") or "").strip().lower()
        parsed_monthly_limit = int(parsed.get("monthly_limit") or 0)
        subscription_status_event = str(parsed.get("subscription_status") or "").strip().lower()
        cancel_at_period_end = bool(parsed.get("cancel_at_period_end"))
        current_period_end = int(parsed.get("current_period_end") or 0)
        cancel_at = int(parsed.get("cancel_at") or 0)
        canceled_at = int(parsed.get("canceled_at") or 0)
        ended_at = int(parsed.get("ended_at") or 0)
        event_obj = parsed.get("object") if isinstance(parsed.get("object"), dict) else {}
        event_metadata = parsed.get("metadata") if isinstance(parsed.get("metadata"), dict) else {}

        # Some Stripe webhooks may omit checkout metadata; hydrate from Stripe session to recover top-up details.
        if event_type == "checkout.session.completed" and checkout_mode == "payment" and credits_delta <= 0:
            checkout_session_id = str(event_obj.get("id") or "").strip()
            if checkout_session_id:
                try:
                    session_payload = _stripe_api_get_json(f"checkout/sessions/{quote_plus(checkout_session_id)}")
                    if isinstance(session_payload, dict):
                        session_metadata = session_payload.get("metadata") if isinstance(session_payload.get("metadata"), dict) else {}
                        if not user_id:
                            user_id = str(session_metadata.get("user_id") or session_payload.get("client_reference_id") or "").strip()
                        if not user_email:
                            customer_details = session_payload.get("customer_details") if isinstance(session_payload.get("customer_details"), dict) else {}
                            user_email = str(session_metadata.get("email") or session_payload.get("customer_email") or customer_details.get("email") or "").strip().lower()
                        if not stripe_customer_id:
                            stripe_customer_id = str(session_payload.get("customer") or "").strip()

                        raw_credits = session_metadata.get("credits_added") or session_metadata.get("credits")
                        try:
                            credits_delta = max(credits_delta, int(raw_credits or 0))
                        except Exception:
                            pass

                    if credits_delta <= 0:
                        line_items_payload = _stripe_api_get_json(
                            f"checkout/sessions/{quote_plus(checkout_session_id)}/line_items",
                            params={"limit": 10},
                        )
                        line_items = line_items_payload.get("data") if isinstance(line_items_payload.get("data"), list) else []
                        if line_items:
                            first_line = line_items[0] if isinstance(line_items[0], dict) else {}
                            price = first_line.get("price") if isinstance(first_line.get("price"), dict) else {}
                            price_id = str(price.get("id") or "").strip()
                            mapped_package = STRIPE_TOP_UP_PRICE_ID_TO_PACKAGE.get(price_id)
                            if isinstance(mapped_package, dict):
                                credits_delta = max(credits_delta, int(mapped_package.get("credits") or 0))
                except Exception as exc:
                    logging.warning("Top-up checkout hydration failed for session %s: %s", checkout_session_id, exc)

        now_iso = utc_now_iso()
        now_dt = datetime.now(timezone.utc)
        free_quota = int(PLAN_MONTHLY_QUOTAS.get("free", DEFAULT_MONTHLY_CREDIT_LIMIT))
        pro_quota_default = int(PLAN_MONTHLY_QUOTAS.get("pro", DEFAULT_MONTHLY_CREDIT_LIMIT))

        is_topup_event = credits_delta > 0
        is_subscription_cycle_event = (
            (event_type in {"invoice.payment_succeeded", "invoice.paid"} and billing_reason in {"subscription_create", "subscription_update"})
            or (event_type == "checkout.session.completed" and checkout_mode == "subscription")
        )
        is_subscription_state_event = event_type in {"customer.subscription.updated", "customer.subscription.deleted", "invoice.payment_failed"}

        def _unix_to_iso(value: int) -> Optional[str]:
            if int(value or 0) <= 0:
                return None
            try:
                return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
            except Exception:
                return None

        def _resolve_subscription_cancel_at() -> Optional[str]:
            if cancel_at > 0:
                return _unix_to_iso(cancel_at)
            if current_period_end > 0:
                return _unix_to_iso(current_period_end)
            if ended_at > 0:
                return _unix_to_iso(ended_at)
            if canceled_at > 0:
                return _unix_to_iso(canceled_at)
            return None

        def _resolve_plan_key_from_limit(monthly_limit_value: int, fallback: str = "pro") -> str:
            limit = max(1, int(monthly_limit_value or pro_quota_default))
            for key, quota in PLAN_MONTHLY_QUOTAS.items():
                if int(quota or 0) == limit:
                    return key
            if fallback in PLAN_MONTHLY_QUOTAS:
                return fallback
            return DEFAULT_PLAN_KEY

        def _resolve_monthly_limit(current_limit: int) -> int:
            if parsed_monthly_limit > 0:
                return parsed_monthly_limit

            raw_meta_limit = event_metadata.get("monthly_limit") or event_metadata.get("credits_limit")
            try:
                meta_limit = int(raw_meta_limit)
            except Exception:
                meta_limit = 0
            if meta_limit > 0:
                return meta_limit

            price_id = ""
            lines = event_obj.get("lines") if isinstance(event_obj.get("lines"), dict) else {}
            data_lines = lines.get("data") if isinstance(lines.get("data"), list) else []
            if data_lines:
                first_line = data_lines[0] if isinstance(data_lines[0], dict) else {}
                price = first_line.get("price") if isinstance(first_line.get("price"), dict) else {}
                price_id = str(price.get("id") or "").strip()

            if price_id:
                mapped_plan = STRIPE_PRICE_ID_TO_PLAN.get(price_id)
                if isinstance(mapped_plan, dict):
                    mapped_credits = int(mapped_plan.get("credits") or 0)
                    if mapped_credits > 0:
                        return mapped_credits

            limits_map_raw = str(os.environ.get("STRIPE_MONTHLY_LIMITS_BY_PRICE_ID") or "").strip()
            if price_id and limits_map_raw:
                try:
                    limits_map = json.loads(limits_map_raw)
                    if isinstance(limits_map, dict):
                        mapped = int(limits_map.get(price_id) or 0)
                        if mapped > 0:
                            return mapped
                except Exception:
                    pass

            return max(1, int(current_limit or DEFAULT_MONTHLY_CREDIT_LIMIT))

        cancel_effective_at = _resolve_subscription_cancel_at()
        cancel_effective_dt = parse_iso_datetime(cancel_effective_at) if cancel_effective_at else None
        has_paid_access_until_end = bool(cancel_effective_dt and now_dt < cancel_effective_dt)
        deleted_or_terminal = event_type == "customer.subscription.deleted" or subscription_status_event in {
            "canceled",
            "incomplete_expired",
            "unpaid",
            "past_due",
        }

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                return {"ok": True, "ignored": True, "reason": "supabase_unavailable"}

            query = sb_client.table("users").select(
                "id,credits_balance,topup_credits_balance,monthly_quota,monthly_limit,credits_limit,subscription_start_date,subscription_active,plan_key"
            )
            if user_id:
                query = query.eq("id", user_id)
            elif stripe_customer_id:
                query = query.eq("stripe_customer_id", stripe_customer_id)
            elif user_email:
                query = query.eq("email", user_email)
            else:
                return {"ok": True, "ignored": True, "reason": "no_user_reference"}

            try:
                rows = list(getattr(query.limit(1).execute(), "data", None) or [])
            except Exception as exc:
                logging.warning("Stripe webhook Supabase billing lookup fallback: %s", exc)
                fallback_query = sb_client.table("users").select("id,email")
                if user_id:
                    fallback_query = fallback_query.eq("id", user_id)
                elif stripe_customer_id:
                    fallback_query = fallback_query.eq("stripe_customer_id", stripe_customer_id)
                elif user_email:
                    fallback_query = fallback_query.eq("email", user_email)
                rows = list(getattr(fallback_query.limit(1).execute(), "data", None) or [])
            if not rows:
                return {"ok": True, "ignored": True, "reason": "user_not_found"}

            row = rows[0]
            resolved_user_id = str(row.get("id") or "").strip()
            resolved_user_email = str(row.get("email") or user_email or "").strip().lower()
            runtime_billing = load_runtime_billing_snapshot(resolved_user_id, resolved_user_email)
            current_balance = int(row.get("credits_balance") or runtime_billing.get("credits_balance") or 0)
            topup_balance = max(0, int(row.get("topup_credits_balance") or runtime_billing.get("topup_credits_balance") or 0))
            current_monthly_limit = max(
                1,
                int(row.get("monthly_quota") or row.get("monthly_limit") or row.get("credits_limit") or runtime_billing.get("monthly_quota") or runtime_billing.get("monthly_limit") or runtime_billing.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
            )
            next_monthly_limit = _resolve_monthly_limit(current_monthly_limit)
            current_plan_key = _normalize_plan_key(row.get("plan_key") or runtime_billing.get("plan_key"), fallback=_resolve_plan_key_from_limit(current_monthly_limit))

            payload: dict[str, Any] = {"updated_at": now_iso}
            if stripe_customer_id:
                payload["stripe_customer_id"] = stripe_customer_id

            if is_topup_event:
                payload["credits_balance"] = current_balance + credits_delta
                payload["topup_credits_balance"] = topup_balance + credits_delta

            if is_subscription_cycle_event and not is_topup_event:
                payload["monthly_quota"] = next_monthly_limit
                payload["monthly_limit"] = next_monthly_limit
                payload["credits_limit"] = next_monthly_limit
                payload["subscription_start_date"] = now_iso
                payload["credits_balance"] = next_monthly_limit + topup_balance
                payload["subscription_active"] = True
                payload["subscription_status"] = "active"
                payload["subscription_cancel_at"] = None
                payload["subscription_cancel_at_period_end"] = False
                payload["plan_key"] = _resolve_plan_key_from_limit(next_monthly_limit, fallback="pro")

            if is_subscription_state_event:
                if deleted_or_terminal and not has_paid_access_until_end:
                    payload["plan_key"] = "free"
                    payload["subscription_active"] = False
                    payload["subscription_status"] = "expired"
                    payload["subscription_cancel_at"] = cancel_effective_at or now_iso
                    payload["subscription_cancel_at_period_end"] = False
                    payload["monthly_quota"] = free_quota
                    payload["monthly_limit"] = free_quota
                    payload["credits_limit"] = free_quota
                    payload["credits_balance"] = free_quota + topup_balance
                    payload["subscription_start_date"] = now_iso
                elif cancel_at_period_end or (deleted_or_terminal and has_paid_access_until_end):
                    payload["plan_key"] = current_plan_key if current_plan_key != "free" else "pro"
                    payload["subscription_active"] = True
                    payload["subscription_status"] = "cancelled_pending"
                    payload["subscription_cancel_at"] = cancel_effective_at
                    payload["subscription_cancel_at_period_end"] = True
                else:
                    payload["plan_key"] = current_plan_key if current_plan_key != "free" else "pro"
                    payload["subscription_active"] = True
                    payload["subscription_status"] = subscription_status_event or "active"
                    payload["subscription_cancel_at"] = None
                    payload["subscription_cancel_at_period_end"] = False

            execute_supabase_update_with_retry(
                sb_client,
                "users",
                payload,
                eq_filters={"id": resolved_user_id},
                operation_name="stripe_webhook_update",
            )
            final_monthly_limit = int(payload.get("monthly_quota") or payload.get("monthly_limit") or payload.get("credits_limit") or next_monthly_limit or current_monthly_limit)
            final_credits_balance = int(payload.get("credits_balance") or current_balance)
            final_topup_balance = max(0, int(payload.get("topup_credits_balance") or topup_balance))
            final_subscription_active = bool(payload.get("subscription_active", row.get("subscription_active") or runtime_billing.get("subscription_active") or False))
            final_plan_key = _normalize_plan_key(payload.get("plan_key") or current_plan_key, fallback=_resolve_plan_key_from_limit(final_monthly_limit))
            store_runtime_billing_snapshot(
                user_id=resolved_user_id,
                user_email=resolved_user_email,
                snapshot={
                    "credits_balance": final_credits_balance,
                    "credits_limit": final_monthly_limit,
                    "monthly_limit": final_monthly_limit,
                    "monthly_quota": final_monthly_limit,
                    "topup_credits_balance": final_topup_balance,
                    "subscription_start_date": payload.get("subscription_start_date") or row.get("subscription_start_date") or runtime_billing.get("subscription_start_date") or now_iso,
                    "subscription_active": final_subscription_active,
                    "subscription_status": str(payload.get("subscription_status") or runtime_billing.get("subscription_status") or ("active" if final_subscription_active else "")).strip().lower(),
                    "subscription_cancel_at": payload.get("subscription_cancel_at"),
                    "subscription_cancel_at_period_end": bool(payload.get("subscription_cancel_at_period_end") or False),
                    "plan_key": final_plan_key,
                    "stripe_customer_id": stripe_customer_id or str(row.get("stripe_customer_id") or runtime_billing.get("stripe_customer_id") or "").strip(),
                    "updated_at": now_iso,
                },
            )
            if is_topup_event:
                mark_stripe_topup_payments_applied(
                    [str(event_obj.get("payment_intent") or event_obj.get("id") or "").strip()],
                    user_id=resolved_user_id,
                    credits_delta=max(0, credits_delta),
                )
            return {
                "ok": True,
                "user_id": resolved_user_id,
                "credits_delta": max(0, credits_delta),
                "monthly_refill_applied": bool(is_subscription_cycle_event and not is_topup_event),
                "subscription_state_applied": bool(is_subscription_state_event),
                "updated_at": now_iso,
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            if user_id:
                row = conn.execute(
                    "SELECT id, credits_balance, topup_credits_balance, monthly_quota, monthly_limit, credits_limit, subscription_active, plan_key FROM users WHERE id = ? LIMIT 1",
                    (user_id,),
                ).fetchone()
            elif stripe_customer_id:
                row = conn.execute(
                    "SELECT id, credits_balance, topup_credits_balance, monthly_quota, monthly_limit, credits_limit, subscription_active, plan_key FROM users WHERE stripe_customer_id = ? LIMIT 1",
                    (stripe_customer_id,),
                ).fetchone()
            elif user_email:
                row = conn.execute(
                    "SELECT id, credits_balance, topup_credits_balance, monthly_quota, monthly_limit, credits_limit, subscription_active, plan_key FROM users WHERE LOWER(email) = ? LIMIT 1",
                    (user_email,),
                ).fetchone()
            else:
                row = None

            if row is None:
                return {"ok": True, "ignored": True, "reason": "user_not_found"}

            resolved_user_id = str(row["id"])
            current_balance = int(row["credits_balance"] or 0)
            topup_balance = max(0, int(row["topup_credits_balance"] or 0))
            current_monthly_limit = max(1, int(row["monthly_quota"] or row["monthly_limit"] or row["credits_limit"] or DEFAULT_MONTHLY_CREDIT_LIMIT))
            next_monthly_limit = _resolve_monthly_limit(current_monthly_limit)
            current_plan_key = _normalize_plan_key(row["plan_key"], fallback=_resolve_plan_key_from_limit(current_monthly_limit))

            credits_balance_to_store = current_balance
            topup_balance_to_store = topup_balance
            monthly_limit_to_store = current_monthly_limit
            credits_limit_to_store = current_monthly_limit
            subscription_start_date_to_store: Optional[str] = None
            subscription_active_to_store = int(1 if _coerce_subscription_flag(row["subscription_active"]) else 0)
            subscription_status_to_store: Optional[str] = None
            subscription_cancel_at_to_store: Optional[str] = None
            subscription_cancel_at_period_end_to_store = 0
            plan_key_to_store = current_plan_key

            if is_topup_event:
                credits_balance_to_store = current_balance + credits_delta
                topup_balance_to_store = topup_balance + credits_delta

            if is_subscription_cycle_event and not is_topup_event:
                monthly_limit_to_store = next_monthly_limit
                credits_limit_to_store = next_monthly_limit
                subscription_start_date_to_store = now_iso
                credits_balance_to_store = next_monthly_limit + topup_balance
                subscription_active_to_store = 1
                subscription_status_to_store = "active"
                subscription_cancel_at_to_store = None
                subscription_cancel_at_period_end_to_store = 0
                plan_key_to_store = _resolve_plan_key_from_limit(next_monthly_limit, fallback="pro")

            if is_subscription_state_event:
                if deleted_or_terminal and not has_paid_access_until_end:
                    subscription_active_to_store = 0
                    subscription_status_to_store = "expired"
                    subscription_cancel_at_to_store = cancel_effective_at or now_iso
                    subscription_cancel_at_period_end_to_store = 0
                    plan_key_to_store = "free"
                    monthly_limit_to_store = free_quota
                    credits_limit_to_store = free_quota
                    credits_balance_to_store = free_quota + topup_balance_to_store
                    subscription_start_date_to_store = now_iso
                elif cancel_at_period_end or (deleted_or_terminal and has_paid_access_until_end):
                    subscription_active_to_store = 1
                    subscription_status_to_store = "cancelled_pending"
                    subscription_cancel_at_to_store = cancel_effective_at
                    subscription_cancel_at_period_end_to_store = 1
                    plan_key_to_store = current_plan_key if current_plan_key != "free" else "pro"
                else:
                    subscription_active_to_store = 1
                    subscription_status_to_store = subscription_status_event or "active"
                    subscription_cancel_at_to_store = None
                    subscription_cancel_at_period_end_to_store = 0
                    plan_key_to_store = current_plan_key if current_plan_key != "free" else "pro"

            conn.execute(
                """
                UPDATE users
                SET subscription_active = ?,
                    subscription_status = ?,
                    subscription_cancel_at = ?,
                    subscription_cancel_at_period_end = ?,
                    plan_key = ?,
                    stripe_customer_id = COALESCE(NULLIF(?, ''), stripe_customer_id),
                    credits_balance = ?,
                    topup_credits_balance = ?,
                    monthly_quota = ?,
                    monthly_limit = ?,
                    credits_limit = ?,
                    subscription_start_date = COALESCE(NULLIF(?, ''), subscription_start_date),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    subscription_active_to_store,
                    subscription_status_to_store,
                    subscription_cancel_at_to_store,
                    subscription_cancel_at_period_end_to_store,
                    plan_key_to_store,
                    stripe_customer_id,
                    credits_balance_to_store,
                    topup_balance_to_store,
                    monthly_limit_to_store,
                    monthly_limit_to_store,
                    credits_limit_to_store,
                    subscription_start_date_to_store or "",
                    now_iso,
                    resolved_user_id,
                ),
            )
            conn.commit()

        if is_topup_event:
            mark_stripe_topup_payments_applied(
                [str(event_obj.get("payment_intent") or event_obj.get("id") or "").strip()],
                user_id=resolved_user_id,
                credits_delta=max(0, credits_delta),
            )
        return {
            "ok": True,
            "user_id": resolved_user_id,
            "credits_delta": max(0, credits_delta),
            "monthly_refill_applied": bool(is_subscription_cycle_event and not is_topup_event),
            "subscription_state_applied": bool(is_subscription_state_event),
            "updated_at": now_iso,
        }

    return app


app = create_app()
