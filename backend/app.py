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
import pgdb
import urllib.error
import urllib.request
import uuid
import base64
import asyncio
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from functools import wraps
from pathlib import Path
from threading import BoundedSemaphore, Event, Lock, Thread
from typing import Any, Callable, List, Optional
from urllib.parse import parse_qsl, quote_plus, unquote, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor as APSchedulerThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError

try:
    stripe = importlib.import_module("stripe")
except Exception:
    stripe = None  # type: ignore

try:
    _supabase_module = importlib.import_module("supabase")
    create_supabase_client = getattr(_supabase_module, "create_client")
    _HAS_SUPABASE = True
except Exception:
    create_supabase_client = None  # type: ignore
    _HAS_SUPABASE = False

# Load root .env early so imported modules read the same runtime settings.
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

from backend.check_access import get_plan_feature_access, normalize_plan_key, require_feature_access
from backend.scraper.db import batch_upsert_leads, get_database_url as pg_get_database_url, get_engine as pg_get_engine, init_db, record_pool_saturation_event, upsert_lead
from backend.scraper.exporter import export_target_leads
from backend.scraper.full_enrichment import enrich_leads_full_data
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
from backend.services.ai_provider import create_sync_ai_client, has_any_ai_credentials, resolve_ai_provider_settings
from backend.services.prompt_service import PromptFactory
from backend.stripe_webhook import extract_payment_refresh_payload


def _get_google_maps_scraper_class():
    from backend.scraper.google_maps import GoogleMapsScraper

    return GoogleMapsScraper


def _create_lead_enricher(*, db_path: str, headless: bool, config_path: str, **kwargs):
    from backend.services.enrichment_service import LeadEnricher

    return LeadEnricher(db_path=db_path, headless=headless, config_path=config_path, **kwargs)

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT_DIR / "runtime-db"
DEFAULT_CONFIG_PATH = ROOT_DIR / "environment settings"
DEFAULT_PROFILE_DIR = ROOT_DIR / "profiles" / "maps_profile"
DEFAULT_TARGET_EXPORT = ROOT_DIR / "target_leads.csv"
DEFAULT_AI_EXPORT = ROOT_DIR / "ai_mailer_ready.csv"
TASK_TYPES = ("scrape", "enrich", "mailer")
ACTIVE_TASK_STATUSES = {"queued", "running"}
TASK_HISTORY_LIMIT = 25
STALE_QUEUED_TASK_SECONDS = 180
STALE_RUNNING_TASK_SECONDS = 7200
ORPHAN_TASK_GRACE_SECONDS = 300
SMTP_TEST_RECIPIENT = "opnjc06@gmail.com"
TRACKING_PIXEL_GIF = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==")
SYSTEM_SMTP_DEFAULT_SEND_LIMIT = max(1, int(os.environ.get("SNIPED_SYSTEM_SMTP_SEND_LIMIT", "50") or 50))
LEAD_TREND_POINTS_LIMIT = 8


def get_allowed_cors_origins() -> list[str]:
    configured = str(os.environ.get("CORS_ALLOWED_ORIGINS", "") or "").strip()
    frontend_origin = str(
        os.environ.get("FRONTEND_URL")
        or os.environ.get("SNIPED_DASHBOARD_URL")
        or os.environ.get("LEADFLOW_DASHBOARD_URL")
        or ""
    ).strip().rstrip("/")
    if configured:
        origins = [origin.strip().rstrip("/") for origin in configured.split(",") if origin.strip()]
        if "https://sniped-one.vercel.app" not in origins:
            origins.append("https://sniped-one.vercel.app")
        if "https://sniped-production.up.railway.app" not in origins:
            origins.append("https://sniped-production.up.railway.app")
        if "https://www.sniped.io" not in origins:
            origins.append("https://www.sniped.io")
        if "https://sniped.io" not in origins:
            origins.append("https://sniped.io")
        if frontend_origin and frontend_origin not in origins:
            origins.append(frontend_origin)
        return origins
    defaults = [
        "https://sniped-one.vercel.app",
        "https://sniped-production.up.railway.app",
        "https://www.sniped.io",
        "https://sniped.io",
        "http://localhost:5173",
        "http://localhost:3000",
        "http://localhost:8000",
    ]
    if frontend_origin and frontend_origin not in defaults:
        defaults.append(frontend_origin)
    return defaults


def _extract_db_host_port(raw_url: str) -> tuple[str, Optional[int]]:
    value = str(raw_url or "").strip()
    if not value:
        return "", None

    cleaned = value.replace("\n", "").replace("\r", "").strip()
    if "://" in cleaned:
        cleaned = cleaned.split("://", 1)[1]
    authority = cleaned.split("/", 1)[0].strip()
    if "@" in authority:
        authority = authority.rsplit("@", 1)[1].strip()

    host = authority
    port: Optional[int] = None

    if authority.startswith("[") and "]" in authority:
        inside = authority[1:authority.index("]")]
        tail = authority[authority.index("]") + 1 :]
        host = inside
        if tail.startswith(":"):
            try:
                port = int(tail[1:])
            except ValueError:
                port = None
    else:
        if authority.count(":") == 1:
            candidate_host, candidate_port = authority.rsplit(":", 1)
            host = candidate_host
            try:
                port = int(candidate_port)
            except ValueError:
                port = None

    host = host.strip().strip("[]")
    return host, port


def _read_env_int(name: str, default: int, minimum: int = 1) -> int:
    raw_value = str(os.environ.get(name, default) or default).strip()
    try:
        return max(minimum, int(raw_value))
    except (TypeError, ValueError):
        return max(minimum, default)


def _env_flag(name: str, default: bool = False) -> bool:
    raw_value = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    return raw_value in {"1", "true", "yes", "on"}

AUTOPILOT_ENRICH_LIMIT = 150
HIGH_AI_SCORE_THRESHOLD = 7.0
DRIP_MINUTES_MIN = 10
DRIP_MINUTES_MAX = 15
AUTO_DRIP_DISPATCH_ENABLED = False
DEFAULT_LOG_LEVEL = str(os.environ.get("LOG_LEVEL", "WARNING") or "WARNING").strip().upper() or "WARNING"
APP_THREADPOOL_WORKERS = _read_env_int("APP_THREADPOOL_WORKERS", 2)
SCHEDULER_MAX_WORKERS = _read_env_int("SCHEDULER_MAX_WORKERS", 1)
RUN_STARTUP_JOBS = _env_flag("RUN_STARTUP_JOBS", default=False)
STATELESS_SUPABASE_ONLY = _env_flag("STATELESS_SUPABASE_ONLY", default=False)
FORCE_IN_PROCESS_SCRAPE = _env_flag("FORCE_IN_PROCESS_SCRAPE", default=True)
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
# If environment settings does not provide proxy_urls/proxy_url, scraper will use this list.
# Add one full URL per item, e.g. "http://user:pass@host:port".
HARDCODED_PROXY_URLS: List[str] = []
DEFAULT_PROXY_LIST_PATH = ROOT_DIR / "backend" / "proxies.txt"

# Per-user AI usage guardrail (units/day). Enrichment consumes units ~= lead limit.
AI_DAILY_USAGE_LIMIT = int(os.environ.get("SNIPED_AI_DAILY_USAGE_LIMIT", os.environ.get("LEADFLOW_AI_DAILY_USAGE_LIMIT", "1000")))
_AI_USAGE_LOCK = Lock()
_DASHBOARD_SCHEMA_READY = False
_SCHEMA_INIT_LOCK = Lock()
_SYSTEM_SCHEMA_READY = False
_USERS_SCHEMA_READY = False
ENRICH_CONCURRENCY_LIMIT = _read_env_int("ENRICH_CONCURRENCY_LIMIT", 2)
ENRICH_SEMAPHORE_TIMEOUT_SECONDS = 30
ENRICH_CAPACITY_ERROR_MESSAGE = "Server is currently at capacity. Please try again in a few minutes."
SCRAPE_CREDIT_COST_PER_LEAD = 1
ENRICH_CREDIT_COST_PER_LEAD = 2

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
_API_CACHE: dict[str, dict[str, Any]] = {}
_CONFIG_CACHE_TTL = 45
_WORKERS_CACHE_TTL = 12


def _invalidate_leads_cache() -> None:
    _LEADS_CACHE.clear()


def _get_cached_leads(key: str):
    entry = _LEADS_CACHE.get(key)
    if entry and (_time.monotonic() - entry["ts"]) < _LEADS_CACHE_TTL:
        return entry["data"]
    return None


def _set_cached_leads(key: str, data: dict) -> None:
    _LEADS_CACHE[key] = {"data": data, "ts": _time.monotonic()}


def _is_db_capacity_error(exc: Exception) -> bool:
    if isinstance(exc, SQLAlchemyOperationalError):
        return True
    message = str(exc or "").lower()
    markers = (
        "emaxconn",
        "max client connections reached",
        "too many clients",
        "remaining connection slots are reserved",
        "queuepool limit",
        "timed out waiting for connection",
    )
    return any(marker in message for marker in markers)


def _get_cached_api(scope: str, key: str, ttl_seconds: int):
    scope_cache = _API_CACHE.get(scope) or {}
    entry = scope_cache.get(key)
    if entry and (_time.monotonic() - float(entry.get("ts") or 0.0)) < max(1, int(ttl_seconds)):
        return entry.get("data")
    return None


def _set_cached_api(scope: str, key: str, data: Any) -> None:
    scope_cache = _API_CACHE.setdefault(scope, {})
    scope_cache[key] = {"data": data, "ts": _time.monotonic()}


def _invalidate_api_cache(scope: str, key_prefix: Optional[str] = None) -> None:
    if key_prefix is None:
        _API_CACHE.pop(scope, None)
        return
    scope_cache = _API_CACHE.get(scope)
    if not scope_cache:
        return
    for key in list(scope_cache.keys()):
        if str(key).startswith(str(key_prefix)):
            scope_cache.pop(key, None)
    if not scope_cache:
        _API_CACHE.pop(scope, None)


def _normalize_proxy_url(raw_proxy: Optional[str]) -> str:
    value = str(raw_proxy or "").strip()
    if not value:
        return ""
    if "://" in value:
        return value

    parts = value.split(":")
    if len(parts) == 4:
        host, port, username, password = [part.strip() for part in parts]
        if host and port and username and password:
            return f"http://{username}:{password}@{host}:{port}"
    return value


def _load_file_proxy_urls(proxy_file_path: Path = DEFAULT_PROXY_LIST_PATH) -> list[str]:
    try:
        lines = proxy_file_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []

    proxy_urls: list[str] = []
    for line in lines:
        formatted = _normalize_proxy_url(line)
        if formatted:
            proxy_urls.append(formatted)
    return proxy_urls


# User-scoped in-memory cache for niche recommendations.
_NICHE_REC_CACHE: dict[str, dict[str, Any]] = {}


class ScrapeRequest(BaseModel):
    keyword: str = Field(..., min_length=2)
    results: int = Field(25, ge=1, le=500)
    headless: bool = True
    country: str = "US"
    country_code: Optional[str] = None
    user_data_dir: Optional[str] = None
    export_targets: bool = False
    output_csv: Optional[str] = None
    min_rating: float = Field(3.5, ge=0.0, le=5.0)
    db_path: Optional[str] = None
    speed_mode: bool = False


class EnrichRequest(BaseModel):
    limit: Optional[int] = Field(default=None, ge=1)
    lead_ids: Optional[list[int]] = None
    headless: bool = True
    output_csv: Optional[str] = None
    skip_export: bool = False
    db_path: Optional[str] = None
    token: Optional[str] = None
    user_niche: Optional[str] = Field(default=None, max_length=120)


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
    developer_webhook_url: Optional[str] = None
    developer_score_drop_threshold: Optional[float] = Field(default=None, ge=0.0, le=10.0)
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


class WonDealRevenueRequest(BaseModel):
    lead_id: int = Field(..., ge=1)
    amount: float = Field(..., gt=0)
    currency: str = Field(default="EUR", min_length=3, max_length=8)
    note: Optional[str] = Field(default=None, max_length=500)


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


class IncomingEmailWebhookRequest(BaseModel):
    event_type: str = Field(default="reply", min_length=3, max_length=30)
    lead_id: Optional[int] = None
    thread_token: Optional[str] = Field(default=None, max_length=300)
    email: Optional[str] = Field(default=None, max_length=320)
    from_email: Optional[str] = Field(default=None, max_length=320)
    subject_line: Optional[str] = Field(default=None, max_length=300)
    body_html: Optional[str] = None
    body_text: Optional[str] = None
    reason: Optional[str] = Field(default=None, max_length=300)
    metadata: Optional[dict[str, Any]] = None


class AILeadFilterRequest(BaseModel):
    prompt: str = Field(..., min_length=2, max_length=600)
    limit: int = Field(default=5000, ge=1, le=10000)
    include_blacklisted: bool = False


class AdminCreditUpdateRequest(BaseModel):
    user_id: Optional[str] = Field(default=None, max_length=128)
    email: Optional[str] = Field(default=None, max_length=320)
    action: str = Field(default="add", max_length=16)
    amount: Optional[int] = Field(default=None, ge=-5_000_000, le=5_000_000)
    note: Optional[str] = Field(default=None, max_length=300)


class AdminBlockUserRequest(BaseModel):
    blocked: bool = True
    reason: Optional[str] = Field(default=None, max_length=300)


class AdminPlanUpdateRequest(BaseModel):
    plan_key: str = Field(..., min_length=2, max_length=32)


class AdminResetPasswordRequest(BaseModel):
    reset_base_url: Optional[str] = Field(default=None, max_length=600)


class AdminGlobalNotificationRequest(BaseModel):
    message: Optional[str] = Field(default=None, max_length=300)
    active: bool = True


class AdminAiSignalsToggleRequest(BaseModel):
    enabled: bool = True


NICHES = [
    "Paid Ads Agency",
    "Web Design & Dev",
    "SEO & Content",
    "Lead Gen Agency",
    "B2B Service Provider",
]
ACCOUNT_TYPES = {"entrepreneur", "freelancer", "agency", "company"}

STRIPE_TOP_UP_PACKAGES: dict[str, dict[str, Any]] = {
    "credits_1000": {"credits": 1000, "price_usd": 29.99, "amount_cents": 2999, "price_id": "price_1TV8i8IHcumhGMC4mW4LYWvN"},
    "credits_3000": {"credits": 3000, "price_usd": 59.00, "amount_cents": 5900, "price_id": "price_1TV8iZIHcumhGMC4l76oD4e2"},
    "credits_5000": {"credits": 5000, "price_usd": 99.00, "amount_cents": 9900, "price_id": "price_1TV8j8IHcumhGMC49IsLxyC3"},
    "credits_10000": {"credits": 10000, "price_usd": 169.00, "amount_cents": 16900, "price_id": "price_1TV8jlIHcumhGMC4v4bUu4n9"},
    "credits_25000": {"credits": 25000, "price_usd": 349.00, "amount_cents": 34900, "price_id": "price_1TV8kBIHcumhGMC4ZbFRduw4"},
    "credits_50000": {"credits": 50000, "price_usd": 699.00, "amount_cents": 69900, "price_id": "price_1TV8lGIHcumhGMC45IWe3NSE"},
    "credits_100000": {"credits": 100000, "price_usd": 1119.00, "amount_cents": 111900, "price_id": "price_1TV8lrIHcumhGMC4JLgLpp4y"},
    "credits_250000": {"credits": 250000, "price_usd": 2119.00, "amount_cents": 211900, "price_id": "price_1TV8maIHcumhGMC4DEeyQSUl"},
    "credits_500000": {"credits": 500000, "price_usd": 3499.00, "amount_cents": 349900, "price_id": "price_1TV8n0IHcumhGMC4icci2M6X"},
}
STRIPE_TOP_UP_PRICE_ID_TO_PACKAGE: dict[str, dict[str, Any]] = {
    str(config.get("price_id") or "").strip(): {"package_id": key, **config}
    for key, config in STRIPE_TOP_UP_PACKAGES.items()
    if str(config.get("price_id") or "").strip()
}
STRIPE_SUBSCRIPTION_PLANS: dict[str, dict[str, Any]] = {
    "hustler": {
        "price_id": "price_1TV8fWIHcumhGMC4U4pDKM7S",
        "credits": 2000,
        "display_name": "The Hustler",
    },
    "growth": {
        "price_id": "price_1TV8fzIHcumhGMC4MDVaUcBx",
        "credits": 7000,
        "display_name": "The Growth",
    },
    "scale": {
        "price_id": "price_1TV8gOIHcumhGMC4WZZqHo78",
        "credits": 20000,
        "display_name": "The Scale",
    },
    "empire": {
        "price_id": "price_1TV8gsIHcumhGMC4IOFDxDte",
        "credits": 100000,
        "display_name": "The Empire",
    },
}

STRIPE_SETTINGS_SUCCESS_URL = "https://www.sniped.io/app?tab=settings&payment=success"
STRIPE_SETTINGS_CANCEL_URL = "https://www.sniped.io/app?tab=settings&payment=cancelled"
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
DEFAULT_ADMIN_EMAILS = {
    email.strip().lower()
    for email in str(os.environ.get("SNIPED_ADMIN_EMAILS", "info@sniped.io") or "info@sniped.io").split(",")
    if email.strip()
}
ADMIN_OVERRIDE_PLAN_KEY = "empire"
ADMIN_UNLIMITED_CREDITS = 10**9
FREE_PLAN_NICHE_RECOMMENDATIONS_PER_MONTH = 1
FREE_PLAN_NICHE_REFRESH_DAYS = 7
PAID_PLAN_NICHE_REFRESH_HOURS = 1
PAID_PLAN_NICHE_REFRESH_DAYS = PAID_PLAN_NICHE_REFRESH_HOURS / 24


def _is_admin_email(email: Any) -> bool:
    return str(email or "").strip().lower() in DEFAULT_ADMIN_EMAILS


def _apply_admin_billing_override(payload: dict[str, Any], *, email: Any = None) -> dict[str, Any]:
    if not _is_admin_email(email or payload.get("email")):
        return payload
    monthly_limit = max(1, int(PLAN_MONTHLY_QUOTAS.get(ADMIN_OVERRIDE_PLAN_KEY, PLAN_MONTHLY_QUOTAS.get("empire", 100000))))
    payload.update(
        {
            "plan_key": ADMIN_OVERRIDE_PLAN_KEY,
            "subscription_active": True,
            "subscription_status": "active",
            "credits_limit": monthly_limit,
            "monthly_limit": monthly_limit,
            "monthly_quota": monthly_limit,
            "credits_balance": ADMIN_UNLIMITED_CREDITS,
            "topup_credits_balance": max(0, int(payload.get("topup_credits_balance") or 0)),
            "is_admin": True,
        }
    )
    return payload


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


class BulkDeleteLeadsRequest(BaseModel):
    lead_ids: List[int] = Field(default_factory=list)


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
        pack_mode: Optional tone modifier â€” "local_first" or "aggressive"
    """
    client, model_name = load_openai_client(DEFAULT_CONFIG_PATH)
    if client is None:
        raise HTTPException(status_code=503, detail="Azure OpenAI deployment is not configured.")

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
        raise HTTPException(status_code=502, detail="AI generation failed. Check your Azure OpenAI settings.")


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

    # Compatible with both DB row mappings and Supabase dicts
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


def parse_task_row(row: pgdb.Row) -> dict:
    return row_to_task_dict(row, row["task_type"])


def ensure_dashboard_columns(db_path: Path) -> None:
    global _DASHBOARD_SCHEMA_READY
    if _DASHBOARD_SCHEMA_READY:
        return
    init_db(db_path=str(db_path))

    optional_columns = {
        "contact_name": "ALTER TABLE leads ADD COLUMN contact_name TEXT",
        "email": "ALTER TABLE leads ADD COLUMN email TEXT",
        "google_claimed": "ALTER TABLE leads ADD COLUMN google_claimed INTEGER",
        "maps_url": "ALTER TABLE leads ADD COLUMN maps_url TEXT",
        "linkedin_url": "ALTER TABLE leads ADD COLUMN linkedin_url TEXT",
        "instagram_url": "ALTER TABLE leads ADD COLUMN instagram_url TEXT",
        "facebook_url": "ALTER TABLE leads ADD COLUMN facebook_url TEXT",
        "linkedin": "ALTER TABLE leads ADD COLUMN linkedin TEXT",
        "instagram": "ALTER TABLE leads ADD COLUMN instagram TEXT",
        "facebook": "ALTER TABLE leads ADD COLUMN facebook TEXT",
        "tiktok_url": "ALTER TABLE leads ADD COLUMN tiktok_url TEXT",
        "twitter_url": "ALTER TABLE leads ADD COLUMN twitter_url TEXT",
        "youtube_url": "ALTER TABLE leads ADD COLUMN youtube_url TEXT",
        "ig_link": "ALTER TABLE leads ADD COLUMN ig_link TEXT",
        "fb_link": "ALTER TABLE leads ADD COLUMN fb_link TEXT",
        "has_pixel": "ALTER TABLE leads ADD COLUMN has_pixel INTEGER DEFAULT 0",
        "tech_stack": "ALTER TABLE leads ADD COLUMN tech_stack TEXT",
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
        "qualification_score": "ALTER TABLE leads ADD COLUMN qualification_score REAL",
        "seo_score": "ALTER TABLE leads ADD COLUMN seo_score REAL",
        "performance_score": "ALTER TABLE leads ADD COLUMN performance_score REAL",
        "pipeline_stage": "ALTER TABLE leads ADD COLUMN pipeline_stage TEXT DEFAULT 'Scraped'",
        "client_folder_id": "ALTER TABLE leads ADD COLUMN client_folder_id INTEGER",
        "user_id": "ALTER TABLE leads ADD COLUMN user_id TEXT",
        "created_at": "ALTER TABLE leads ADD COLUMN created_at TEXT",
    }

    with pgdb.connect(db_path) as conn:
        try:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
            for column_name, statement in optional_columns.items():
                if column_name not in columns:
                    conn.execute(statement)
            conn.execute("UPDATE leads SET user_id = 'legacy' WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''")
            conn.execute(
                """
                UPDATE leads
                SET created_at = COALESCE(NULLIF(CAST(scraped_at AS TEXT), ''), CAST(CURRENT_TIMESTAMP AS TEXT))
                -- SYSTEM-WIDE: intentionally unscoped.
                WHERE created_at IS NULL
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
                -- SYSTEM-WIDE: intentionally unscoped.
                WHERE pipeline_stage IS NULL OR TRIM(COALESCE(pipeline_stage, '')) = ''
                """
            )
            conn.commit()
            _DASHBOARD_SCHEMA_READY = True
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            logging.exception("ensure_dashboard_columns failed; transaction rolled back")
            raise


def ensure_blacklist_table(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lead_blacklist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL DEFAULT 'legacy',
                    kind TEXT NOT NULL,
                    value TEXT NOT NULL,
                    reason TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(lead_blacklist)").fetchall()}
            if "user_id" not in columns:
                conn.execute("ALTER TABLE lead_blacklist ADD COLUMN user_id TEXT NOT NULL DEFAULT 'legacy'")
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_blacklist_user_kind_value ON lead_blacklist(user_id, kind, value)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_blacklist_user_id ON lead_blacklist(user_id)")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            logging.exception("ensure_blacklist_table failed; transaction rolled back")
            raise


def fetch_blacklist_sets(db_path: Path, user_id: Optional[str] = None) -> tuple[set[str], set[str]]:
    ensure_blacklist_table(db_path)
    with pgdb.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT kind, value FROM lead_blacklist WHERE (? IS NULL OR user_id = ?)",
            (user_id, user_id),
        ).fetchall()

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


def sync_blacklisted_leads(db_path: Path, user_id: Optional[str] = None) -> int:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return sync_blacklisted_leads_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)

    emails, domains = fetch_blacklist_sets(db_path, user_id=user_id)
    if not emails and not domains:
        return 0

    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT id, email, website_url, status
            FROM leads
            WHERE LOWER(COALESCE(status, '')) != 'paid'
                            AND (? IS NULL OR user_id = ?)
            """
            ,
            (user_id, user_id),
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


def blacklist_lead_and_matches(db_path: Path, lead_id: int, reason: str = "Manual blacklist", user_id: Optional[str] = None) -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return blacklist_lead_and_matches_supabase(lead_id, reason, DEFAULT_CONFIG_PATH)

    ensure_system_tables(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        row = conn.execute(
            "SELECT id, business_name, email, website_url, user_id FROM leads WHERE id = ? AND (? IS NULL OR user_id = ?)",
            (lead_id, user_id, user_id),
        ).fetchone()

        if row is None:
            if user_id is not None:
                exists_row = conn.execute("SELECT 1 FROM leads WHERE id = ? LIMIT 1", (lead_id,)).fetchone()
                if exists_row is not None:
                    raise HTTPException(status_code=403, detail="Forbidden")
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
                INSERT OR IGNORE INTO lead_blacklist (user_id, kind, value, reason, created_at)
                VALUES (?, 'email', ?, ?, ?)
                """,
                (str(row["user_id"] or "legacy"), email_value, reason, utc_now_iso()),
            )
        for domain_value in sorted(domain_values):
            conn.execute(
                """
                INSERT OR IGNORE INTO lead_blacklist (user_id, kind, value, reason, created_at)
                VALUES (?, 'domain', ?, ?, ?)
                """,
                (str(row["user_id"] or "legacy"), domain_value, reason, utc_now_iso()),
            )
        conn.commit()

    affected = sync_blacklisted_leads(db_path, user_id=str(row["user_id"] or "legacy"))
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


def add_blacklist_entry(db_path: Path, *, user_id: str, kind: str, value: str, reason: str = "Manual blacklist") -> dict:
    normalized_kind, normalized_value = normalize_blacklist_entry(kind, value)
    clean_reason = str(reason or "Manual blacklist").strip() or "Manual blacklist"
    normalized_user_id = str(user_id or "legacy").strip() or "legacy"
    ensure_blacklist_table(db_path)

    with pgdb.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO lead_blacklist (user_id, kind, value, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (normalized_user_id, normalized_kind, normalized_value, clean_reason, utc_now_iso()),
        )
        row = conn.execute(
            "SELECT kind, value, reason, created_at FROM lead_blacklist WHERE user_id = ? AND kind = ? AND value = ? LIMIT 1",
            (normalized_user_id, normalized_kind, normalized_value),
        ).fetchone()
        conn.commit()

    affected = sync_blacklisted_leads(db_path, user_id=normalized_user_id)
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


def restore_released_blacklisted_leads(db_path: Path, removed_entries: list[tuple[str, str]], user_id: Optional[str] = None) -> int:
    normalized_entries = [normalize_blacklist_entry(kind, value) for kind, value in removed_entries if str(value or "").strip()]
    if not normalized_entries:
        return 0

    emails, domains = fetch_blacklist_sets(db_path, user_id=user_id)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT id, email, website_url, status
            FROM leads
            WHERE LOWER(COALESCE(status, '')) IN ('blacklisted', 'skipped (unsubscribed)')
              AND (? IS NULL OR user_id = ?)
            """
            ,
            (user_id, user_id),
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


def remove_blacklist_entry(db_path: Path, *, user_id: str, kind: str, value: str) -> dict:
    normalized_kind, normalized_value = normalize_blacklist_entry(kind, value)
    normalized_user_id = str(user_id or "legacy").strip() or "legacy"
    ensure_blacklist_table(db_path)

    with pgdb.connect(db_path) as conn:
        cursor = conn.execute(
            "DELETE FROM lead_blacklist WHERE user_id = ? AND kind = ? AND value = ?",
            (normalized_user_id, normalized_kind, normalized_value),
        )
        conn.commit()
        deleted_count = int(cursor.rowcount or 0)

    restored = restore_released_blacklisted_leads(db_path, [(normalized_kind, normalized_value)], user_id=normalized_user_id)
    maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
    _invalidate_leads_cache()
    return {
        "status": "removed" if deleted_count else "not_found",
        "kind": normalized_kind,
        "value": normalized_value,
        "deleted_entries": deleted_count,
        "restored_leads": restored,
    }


def remove_lead_blacklist_and_matches(db_path: Path, lead_id: int, user_id: Optional[str] = None) -> dict:
    ensure_system_tables(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        row = conn.execute(
            "SELECT id, business_name, email, website_url, user_id FROM leads WHERE id = ? AND (? IS NULL OR user_id = ?)",
            (lead_id, user_id, user_id),
        ).fetchone()
        if row is None:
            if user_id is not None:
                exists_row = conn.execute("SELECT 1 FROM leads WHERE id = ? LIMIT 1", (lead_id,)).fetchone()
                if exists_row is not None:
                    raise HTTPException(status_code=403, detail="Forbidden")
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
        normalized_user_id = str(row["user_id"] or "legacy")
        for entry_kind, entry_value in removed_entries:
            cursor = conn.execute(
                "DELETE FROM lead_blacklist WHERE user_id = ? AND kind = ? AND value = ?",
                (normalized_user_id, entry_kind, entry_value),
            )
            deleted_count += int(cursor.rowcount or 0)
        conn.commit()

    restored = restore_released_blacklisted_leads(db_path, removed_entries, user_id=str(row["user_id"] or "legacy"))
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
    with pgdb.connect(db_path) as conn:
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
        except pgdb.OperationalError:
            first_user_row = None
        if first_user_row is not None:
            conn.execute(
                "UPDATE revenue_log SET user_id = ? WHERE user_id = 'legacy'",
                (str(first_user_row[0]),),
            )
        conn.commit()


def ensure_revenue_logs_table(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS revenue_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                lead_id INTEGER,
                amount REAL NOT NULL,
                currency TEXT NOT NULL DEFAULT 'EUR',
                event_type TEXT NOT NULL DEFAULT 'won_stage_manual',
                note TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_revenue_logs_user_created ON revenue_logs(user_id, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_revenue_logs_lead_created ON revenue_logs(lead_id, created_at DESC)")
        conn.commit()


def ensure_jobs_queue_table(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
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
        columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        if "user_id" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN user_id TEXT")
            conn.execute("UPDATE jobs SET user_id = 'legacy' WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, created_at ASC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_user_status ON jobs (user_id, status, created_at ASC)")
        conn.commit()


def ensure_workers_table(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
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
    with pgdb.connect(db_path) as conn:
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
    with pgdb.connect(db_path) as conn:
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
    with pgdb.connect(db_path) as conn:
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

    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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

    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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

    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
    with pgdb.connect(db_path) as conn:
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
    with pgdb.connect(db_path) as conn:
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


def ensure_credit_logs_table(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS credit_logs (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_credit_logs_user_created ON credit_logs(user_id, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_credit_logs_action_created ON credit_logs(action_type, created_at DESC)")
        conn.commit()


def ensure_users_table(db_path: Path) -> None:
    global _USERS_SCHEMA_READY
    if _USERS_SCHEMA_READY:
        return
    with _SCHEMA_INIT_LOCK:
        if _USERS_SCHEMA_READY:
            return
        try:
            ensure_blacklist_table(db_path)
        except Exception:
            logging.exception("ensure_users_table prerequisites failed")
            raise
        with pgdb.connect(db_path) as conn:
            try:
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
                        credits INTEGER NOT NULL DEFAULT 0,
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
                        last_login_at TEXT,
                        is_admin INTEGER NOT NULL DEFAULT 0,
                        is_blocked INTEGER NOT NULL DEFAULT 0,
                        blocked_at TEXT,
                        blocked_reason TEXT,
                        created_at    TEXT    NOT NULL,
                        updated_at    TEXT
                    )
                    """
                )
                for col, typedef in [
                    ("account_type", "TEXT NOT NULL DEFAULT 'entrepreneur'"),
                    ("display_name", "TEXT NOT NULL DEFAULT ''"),
                    ("contact_name", "TEXT NOT NULL DEFAULT ''"),
                    ("credits", "INTEGER NOT NULL DEFAULT 0"),
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
                    ("last_login_at", "TEXT"),
                    ("is_admin", "INTEGER NOT NULL DEFAULT 0"),
                    ("is_blocked", "INTEGER NOT NULL DEFAULT 0"),
                    ("blocked_at", "TEXT"),
                    ("blocked_reason", "TEXT"),
                    ("updated_at", "TEXT"),
                ]:
                    try:
                        conn.execute(f"ALTER TABLE users ADD COLUMN {col} {typedef}")
                    except Exception:
                        pass
                if DEFAULT_ADMIN_EMAILS:
                    conn.execute(
                        "UPDATE users SET is_admin = TRUE WHERE LOWER(COALESCE(email, '')) IN ({})".format(
                            ",".join(["?"] * len(DEFAULT_ADMIN_EMAILS))
                        ),
                        tuple(sorted(DEFAULT_ADMIN_EMAILS)),
                    )
                conn.commit()

                users_table_exists_row = conn.execute(
                    """
                    SELECT COUNT(*) AS table_count
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'users'
                    """
                ).fetchone()
                users_table_exists = bool(int((users_table_exists_row or {}).get("table_count", 0))) if isinstance(users_table_exists_row, dict) else bool(int((users_table_exists_row[0] if users_table_exists_row else 0) or 0))
                if not users_table_exists:
                    logging.warning("ensure_users_table: users table not available after CREATE, skipping update phase.")
                    return

                try:
                    conn.execute("UPDATE users SET credits = COALESCE(credits, COALESCE(credits_balance, 0))")
                except Exception as exc:
                    logging.warning("ensure_users_table: non-fatal credits sync failed: %s", exc)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                try:
                    conn.execute("UPDATE users SET credits_balance = COALESCE(credits_balance, 0)")
                except Exception as exc:
                    logging.warning("ensure_users_table: non-fatal credits_balance update failed: %s", exc)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                conn.execute(f"UPDATE users SET credits_limit = COALESCE(NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})")
                conn.execute(f"UPDATE users SET monthly_quota = COALESCE(NULLIF(monthly_quota, 0), NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})")
                conn.execute(f"UPDATE users SET monthly_limit = COALESCE(NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})")
                conn.execute("UPDATE users SET monthly_limit = monthly_quota WHERE COALESCE(NULLIF(monthly_quota, 0), 0) > 0")
                conn.execute("UPDATE users SET credits_limit = monthly_quota WHERE COALESCE(NULLIF(monthly_quota, 0), 0) > 0")
                conn.execute("UPDATE users SET credits = COALESCE(credits_balance, credits, 0)")
                conn.execute("UPDATE users SET topup_credits_balance = COALESCE(topup_credits_balance, 0)")
                conn.execute("UPDATE users SET subscription_active = COALESCE(subscription_active, FALSE)")
                conn.execute("UPDATE users SET subscription_cancel_at_period_end = COALESCE(subscription_cancel_at_period_end, FALSE)")
                conn.execute("UPDATE users SET quickstart_completed = COALESCE(quickstart_completed, FALSE)")
                conn.execute(
                    f"UPDATE users SET average_deal_value = CASE WHEN COALESCE(average_deal_value, 0) <= 0 THEN {DEFAULT_AVERAGE_DEAL_VALUE} ELSE average_deal_value END"
                )
                conn.execute(
                    """
                    UPDATE users
                    SET plan_key = CASE
                        WHEN LOWER(TRIM(COALESCE(plan_key, ''))) IN ('free', 'hustler', 'growth', 'scale', 'empire', 'pro')
                            THEN LOWER(TRIM(COALESCE(plan_key, '')))
                        WHEN COALESCE(subscription_active, FALSE) = TRUE
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
                        credits_balance = GREATEST(COALESCE(credits_balance, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT} + COALESCE(topup_credits_balance, 0))
                    WHERE LOWER(COALESCE(NULLIF(plan_key, ''), 'free')) = 'free'
                    AND COALESCE(subscription_active, FALSE) = FALSE
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
                    SET updated_at = COALESCE(NULLIF(updated_at, ''), created_at, CAST(CURRENT_TIMESTAMP AS TEXT))
                    WHERE updated_at IS NULL OR TRIM(COALESCE(updated_at, '')) = ''
                    """
                )
                conn.commit()
                _USERS_SCHEMA_READY = True
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logging.exception("ensure_users_table failed; transaction rolled back")
                raise


def ensure_lead_history_table(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                seo_score REAL,
                performance_score REAL,
                captured_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_history_user_lead_at ON lead_history(user_id, lead_id, captured_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_history_lead_at ON lead_history(lead_id, captured_at DESC)")
        conn.commit()


def ensure_lead_report_table(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                token TEXT NOT NULL,
                report_html TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                active INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_reports_token ON lead_reports(token)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_reports_user_lead ON lead_reports(user_id, lead_id, created_at DESC)")
        conn.commit()


def _to_float_or_none(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
        return numeric if numeric == numeric else None
    except Exception:
        return None


def _extract_scores_from_lead_payload(lead_row: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    seo_direct = _to_float_or_none(lead_row.get("seo_score"))
    perf_direct = _to_float_or_none(lead_row.get("performance_score"))

    payload: dict[str, Any] = {}
    raw_enrichment = lead_row.get("enrichment_data")
    if isinstance(raw_enrichment, dict):
        payload = dict(raw_enrichment)
    elif isinstance(raw_enrichment, str) and str(raw_enrichment).strip():
        try:
            parsed = json.loads(raw_enrichment)
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = {}

    website_signals = payload.get("website_signals") if isinstance(payload.get("website_signals"), dict) else {}
    shortcoming_blob = " ".join(
        [
            str(lead_row.get("main_shortcoming") or ""),
            str(payload.get("reason") or ""),
            str(payload.get("enrichment_summary") or ""),
        ]
    ).lower()
    insecure_site = bool(lead_row.get("insecure_site"))
    has_https = not insecure_site and bool(website_signals.get("https"))
    tech_stack = payload.get("tech_stack") if isinstance(payload.get("tech_stack"), list) else []
    recent_update = bool(payload.get("recent_site_update"))
    lead_score_100 = _to_float_or_none(payload.get("lead_score_100"))
    ai_sentiment_score = _to_float_or_none(payload.get("ai_sentiment_score"))
    best_lead_score = _to_float_or_none(payload.get("best_lead_score"))

    computed_seo = 52.0
    computed_seo += 10.0 if has_https else -18.0
    computed_seo += 8.0 if recent_update else -6.0
    computed_seo += min(8.0, len(tech_stack) * 2.0)
    if "slow" in shortcoming_blob or "speed" in shortcoming_blob:
        computed_seo -= 10.0
    if "seo" in shortcoming_blob and "missing" in shortcoming_blob:
        computed_seo -= 8.0
    if lead_score_100 is not None:
        computed_seo += (lead_score_100 - 50.0) * 0.12
    computed_seo = max(0.0, min(100.0, round(computed_seo, 1)))

    computed_performance = 58.0
    social_activity = _to_float_or_none(payload.get("social_activity_score"))
    employee_count = _to_float_or_none(payload.get("employee_count"))
    if social_activity is not None:
        computed_performance += social_activity * 1.2
    if employee_count is not None:
        computed_performance += min(8.0, employee_count / 20.0)
    if "slow" in shortcoming_blob or "speed" in shortcoming_blob:
        computed_performance -= 16.0
    if insecure_site:
        computed_performance -= 10.0
    if ai_sentiment_score is not None:
        computed_performance += (ai_sentiment_score - 50.0) * 0.08
    if best_lead_score is not None:
        computed_performance += (best_lead_score - 50.0) * 0.05
    computed_performance = max(0.0, min(100.0, round(computed_performance, 1)))

    return (
        round(float(seo_direct), 1) if seo_direct is not None else computed_seo,
        round(float(perf_direct), 1) if perf_direct is not None else computed_performance,
    )


def _capture_lead_history_snapshots(
    db_path: Path,
    *,
    user_id: str,
    lead_ids: list[int],
    snapshot_map: dict[int, dict[str, Optional[float]]],
) -> int:
    if not lead_ids:
        return 0
    now_iso = utc_now_iso()
    inserted = 0
    with pgdb.connect(db_path) as conn:
        for lead_id in lead_ids:
            previous = snapshot_map.get(int(lead_id)) or {}
            prev_seo = _to_float_or_none(previous.get("seo_score"))
            prev_perf = _to_float_or_none(previous.get("performance_score"))
            if prev_seo is None and prev_perf is None:
                continue
            conn.execute(
                """
                INSERT INTO lead_history (lead_id, user_id, seo_score, performance_score, captured_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (int(lead_id), str(user_id or "legacy"), prev_seo, prev_perf, now_iso),
            )
            inserted += 1
        conn.commit()
    return inserted


def _refresh_enriched_lead_scores(
    db_path: Path,
    *,
    user_id: str,
    lead_ids: list[int],
) -> dict[int, tuple[Optional[float], Optional[float]]]:
    if not lead_ids:
        return {}
    placeholders = ",".join(["?"] * len(lead_ids))
    updated_map: dict[int, tuple[Optional[float], Optional[float]]] = {}
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            f"""
            SELECT id, user_id, insecure_site, main_shortcoming, enrichment_data, seo_score, performance_score
            FROM leads
            WHERE user_id = ? AND id IN ({placeholders})
            """,
            [str(user_id or "legacy"), *[int(x) for x in lead_ids]],
        ).fetchall()
        for row in rows:
            lead_row = dict(row)
            seo_score, performance_score = _extract_scores_from_lead_payload(lead_row)
            conn.execute(
                "UPDATE leads SET seo_score = ?, performance_score = ? WHERE id = ? AND user_id = ?",
                (seo_score, performance_score, int(lead_row.get("id") or 0), str(user_id or "legacy")),
            )
            updated_map[int(lead_row.get("id") or 0)] = (seo_score, performance_score)
        conn.commit()
    return updated_map


def _average_score_pair(seo_score: Optional[float], performance_score: Optional[float]) -> Optional[float]:
    values = [float(v) for v in [seo_score, performance_score] if v is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _notify_score_drop_events(
    *,
    db_path: Path,
    user_id: str,
    previous_scores: dict[int, dict[str, Optional[float]]],
    refreshed_scores: dict[int, tuple[Optional[float], Optional[float]]],
) -> int:
    if not refreshed_scores or not previous_scores:
        return 0
    _, threshold = _get_developer_webhook_settings(DEFAULT_CONFIG_PATH)
    candidate_ids: list[int] = []
    for lead_id, pair in refreshed_scores.items():
        prev = previous_scores.get(int(lead_id)) or {}
        prev_avg = _average_score_pair(prev.get("seo_score"), prev.get("performance_score"))
        new_avg = _average_score_pair(pair[0], pair[1])
        if prev_avg is None or new_avg is None:
            continue
        if prev_avg >= threshold and new_avg < threshold:
            candidate_ids.append(int(lead_id))

    if not candidate_ids:
        return 0

    placeholders = ",".join(["?"] * len(candidate_ids))
    rows_map: dict[int, dict[str, Any]] = {}
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            f"SELECT id, business_name, status, pipeline_stage FROM leads WHERE user_id = ? AND id IN ({placeholders})",
            [str(user_id or "legacy"), *candidate_ids],
        ).fetchall()
    for row in rows:
        rows_map[int(row["id"])] = dict(row)

    delivered = 0
    for lead_id in candidate_ids:
        prev = previous_scores.get(int(lead_id)) or {}
        new_pair = refreshed_scores.get(int(lead_id))
        lead_meta = rows_map.get(int(lead_id)) or {}
        if not new_pair:
            continue
        _dispatch_developer_webhook_event(
            event_type="lead.score_dropped_below_threshold",
            user_id=str(user_id or "legacy"),
            lead_id=int(lead_id),
            payload={
                "lead_id": int(lead_id),
                "business_name": str(lead_meta.get("business_name") or "").strip() or None,
                "status": str(lead_meta.get("status") or "").strip() or None,
                "pipeline_stage": str(lead_meta.get("pipeline_stage") or "").strip() or None,
                "threshold": threshold,
                "previous_score": _average_score_pair(prev.get("seo_score"), prev.get("performance_score")),
                "new_score": _average_score_pair(new_pair[0], new_pair[1]),
                "previous_seo_score": prev.get("seo_score"),
                "previous_performance_score": prev.get("performance_score"),
                "seo_score": new_pair[0],
                "performance_score": new_pair[1],
            },
        )
        delivered += 1
    return delivered


def _build_score_trend_points(values: list[float], limit: int = LEAD_TREND_POINTS_LIMIT) -> list[float]:
    normalized = [round(float(v), 1) for v in values if _to_float_or_none(v) is not None]
    if not normalized:
        return []
    if len(normalized) <= limit:
        return normalized
    return normalized[-limit:]


def _resolve_trend_direction(points: list[float]) -> tuple[str, float]:
    if len(points) < 2:
        return "flat", 0.0
    delta = round(float(points[-1]) - float(points[0]), 1)
    if delta > 0.35:
        return "up", delta
    if delta < -0.35:
        return "down", delta
    return "flat", delta


def _build_gap_report_html(
    *,
    lead: dict[str, Any],
    seo_score: Optional[float],
    performance_score: Optional[float],
    report_title: str,
) -> str:
    business_name = html_escape(str(lead.get("business_name") or "Prospect").strip() or "Prospect")
    contact_name = html_escape(str(lead.get("contact_name") or "").strip())
    website_url = str(lead.get("website_url") or "").strip()
    safe_website = html_escape(website_url)
    insecure_site = bool(lead.get("insecure_site"))
    shortcoming = html_escape(str(lead.get("main_shortcoming") or "No major shortcoming detected yet.").strip())

    payload: dict[str, Any] = {}
    raw_enrichment = lead.get("enrichment_data")
    if isinstance(raw_enrichment, dict):
        payload = dict(raw_enrichment)
    elif isinstance(raw_enrichment, str) and raw_enrichment.strip():
        try:
            parsed = json.loads(raw_enrichment)
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = {}

    summary_blob = " ".join(
        [
            str(lead.get("main_shortcoming") or ""),
            str(payload.get("reason") or ""),
            str(payload.get("enrichment_summary") or ""),
        ]
    ).lower()
    has_speed_gap = "slow" in summary_blob or "speed" in summary_blob
    has_seo_gap = "seo" in summary_blob or (seo_score is not None and float(seo_score) < 62.0)

    seo_display = f"{float(seo_score):.1f}" if seo_score is not None else "n/a"
    perf_display = f"{float(performance_score):.1f}" if performance_score is not None else "n/a"
    now_label = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    page_title = html_escape(str(report_title or "Sniped Gap Report"))

    cards = [
        ("Missing SEO Signals", has_seo_gap, "Core on-page and discoverability signals are under-optimized for high-intent local traffic."),
        ("Slow Site Performance", has_speed_gap, "Page speed likely leaks buyers before contact form submission."),
        ("SSL / HTTPS Trust Gap", insecure_site, "Site is not fully HTTPS, reducing trust and conversion confidence."),
    ]

    cards_html = ""
    for title, active, description in cards:
        state_label = "Detected" if active else "Stable"
        state_class = "#f97316" if active else "#10b981"
        cards_html += (
            "<article style='border:1px solid rgba(148,163,184,.22);border-radius:16px;padding:14px;background:rgba(15,23,42,.55)'>"
            f"<div style='display:flex;justify-content:space-between;gap:8px;align-items:center'><h3 style='margin:0;color:#f8fafc;font-size:15px'>{html_escape(title)}</h3>"
            f"<span style='font-size:11px;font-weight:700;color:{state_class};text-transform:uppercase;letter-spacing:.08em'>{state_label}</span></div>"
            f"<p style='margin:8px 0 0;color:#cbd5e1;font-size:13px;line-height:1.5'>{html_escape(description)}</p>"
            "</article>"
        )

    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<meta name='robots' content='noindex,nofollow,noarchive'>"
        f"<title>{page_title}</title>"
        "<style>"
        "body{margin:0;background:radial-gradient(circle at 20% 0%,#1e293b 0%,#020617 55%);font-family:Inter,Segoe UI,Arial,sans-serif;color:#e2e8f0;padding:24px;}"
        ".wrap{max-width:900px;margin:0 auto;background:rgba(2,6,23,.72);border:1px solid rgba(148,163,184,.18);border-radius:22px;padding:26px;box-shadow:0 24px 60px rgba(2,6,23,.45);}"
        ".badge{display:inline-block;padding:6px 12px;border-radius:999px;background:rgba(14,165,233,.15);border:1px solid rgba(14,165,233,.35);font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#67e8f9;font-weight:700;}"
        ".metric{background:rgba(15,23,42,.65);border:1px solid rgba(148,163,184,.2);padding:14px;border-radius:14px;}"
        "</style></head><body><div class='wrap'>"
        "<div style='display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center'>"
        "<span class='badge'>Sniped Opportunity Snapshot</span>"
        f"<span style='font-size:12px;color:#94a3b8'>Generated {html_escape(now_label)}</span></div>"
        f"<h1 style='margin:14px 0 6px;font-size:30px;color:#f8fafc'>{business_name}</h1>"
        f"<p style='margin:0 0 18px;color:#cbd5e1'>{contact_name if contact_name else 'Prospect'} {('- ' + safe_website) if safe_website else ''}</p>"
        "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:18px'>"
        f"<div class='metric'><div style='font-size:12px;color:#94a3b8'>SEO Score</div><div style='font-size:26px;font-weight:800;color:#22d3ee'>{html_escape(seo_display)}</div></div>"
        f"<div class='metric'><div style='font-size:12px;color:#94a3b8'>Performance Score</div><div style='font-size:26px;font-weight:800;color:#38bdf8'>{html_escape(perf_display)}</div></div>"
        f"<div class='metric'><div style='font-size:12px;color:#94a3b8'>Primary Gap</div><div style='font-size:14px;font-weight:700;color:#f8fafc;line-height:1.35'>{shortcoming}</div></div>"
        "</div>"
        f"<section style='display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:10px'>{cards_html}</section>"
        "<p style='margin:20px 0 0;color:#94a3b8;font-size:12px'>This page is generated for outreach context and is intentionally not indexed by search engines.</p>"
        "</div></body></html>"
    )


def _hash_password(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), 260_000
    ).hex()


def ensure_system_tables(db_path: Path) -> None:
    global _SYSTEM_SCHEMA_READY
    if _SYSTEM_SCHEMA_READY:
        return
    with _SCHEMA_INIT_LOCK:
        if _SYSTEM_SCHEMA_READY:
            return
        ensure_dashboard_columns(db_path)
        ensure_system_task_table(db_path)
        ensure_runtime_table(db_path)
        ensure_blacklist_table(db_path)
        ensure_revenue_log_table(db_path)
        ensure_revenue_logs_table(db_path)
        ensure_workers_table(db_path)
        ensure_worker_audit_table(db_path)
        ensure_delivery_tasks_table(db_path)
        ensure_users_table(db_path)
        ensure_credit_logs_table(db_path)
        ensure_mailer_campaign_tables(db_path)
        ensure_client_success_tables(db_path)
        ensure_lead_history_table(db_path)
        ensure_lead_report_table(db_path)
        _SYSTEM_SCHEMA_READY = True


def ensure_scrape_tables(db_path: Path) -> None:
    # Scrape flow needs core lead/task/runtime tables only.
    # Keep this narrower than ensure_system_tables so unrelated schema drifts
    # (for dashboards/mailer/client success) do not block launching scrape jobs.
    ensure_dashboard_columns(db_path)
    ensure_system_task_table(db_path)
    ensure_runtime_table(db_path)
    ensure_blacklist_table(db_path)


def ensure_dashboard_indexes_startup(db_path: Path) -> None:
    lead_index_sql = [
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_leads_user_id ON leads(user_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_leads_user_created_at ON leads(user_id, created_at DESC, id DESC)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_leads_user_scraped_at ON leads(user_id, scraped_at DESC, id DESC)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_leads_pipeline_stage ON leads(user_id, pipeline_stage)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_leads_client_folder_id ON leads(user_id, client_folder_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_reset_token ON users(reset_token)",
    ]

    def _is_non_fatal_migration_lock_error(exc: Exception) -> bool:
        message = str(exc or "").lower()
        lock_tokens = (
            "deadlock detected",
            "lock timeout",
            "canceling statement due to lock timeout",
            "could not obtain lock",
            "statement timeout",
            "timeout",
        )
        return any(token in message for token in lock_tokens)

    if _is_postgres_task_store_enabled():
        engine = pg_get_engine()
        # CREATE INDEX CONCURRENTLY must run outside transaction blocks.
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            for ddl in lead_index_sql:
                try:
                    conn.execute(text(ddl))
                except SQLAlchemyOperationalError as exc:
                    if _is_non_fatal_migration_lock_error(exc):
                        logging.warning("Startup concurrent index skipped due to lock/timeout: %s", ddl)
                        continue
                    raise
                except Exception as exc:
                    if _is_non_fatal_migration_lock_error(exc):
                        logging.warning("Startup concurrent index skipped due to lock/timeout: %s", ddl)
                        continue
                    raise
        return

    # Local sqlite fallback for dev runs.
    with pgdb.connect(db_path) as conn:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_user_id ON leads(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_user_created_at ON leads(user_id, created_at DESC, id DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_user_scraped_at ON leads(user_id, scraped_at DESC, id DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_pipeline_stage ON leads(user_id, pipeline_stage)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_client_folder_id ON leads(user_id, client_folder_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_reset_token ON users(reset_token)")
        conn.commit()


def ensure_client_success_tables(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
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
        folder_columns = {row[1] for row in conn.execute("PRAGMA table_info(client_folders)").fetchall()}
        if "user_id" not in folder_columns:
            conn.execute("ALTER TABLE client_folders ADD COLUMN user_id TEXT")
            conn.execute("UPDATE client_folders SET user_id = 'legacy' WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''")
        segment_columns = {row[1] for row in conn.execute("PRAGMA table_info(saved_segments)").fetchall()}
        if "user_id" not in segment_columns:
            conn.execute("ALTER TABLE saved_segments ADD COLUMN user_id TEXT")
            conn.execute("UPDATE saved_segments SET user_id = 'legacy' WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''")
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
            -- SYSTEM-WIDE: intentionally unscoped.
            WHERE pipeline_stage IS NULL OR TRIM(COALESCE(pipeline_stage, '')) = ''
            """
        )
        conn.commit()


def ensure_mailer_campaign_tables(db_path: Path) -> None:
    with pgdb.connect(db_path) as conn:
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_communications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                direction TEXT NOT NULL,
                subject TEXT,
                body_html TEXT,
                body_text TEXT,
                status TEXT NOT NULL DEFAULT 'sent',
                tracking_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_sequences_user_active ON CampaignSequences(user_id, active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_saved_templates_user_category ON SavedTemplates(user_id, category)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_events_user_type ON CampaignEvents(user_id, event_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_email_comms_lead_created ON email_communications(lead_id, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_email_comms_user_lead ON email_communications(user_id, lead_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_email_comms_tracking ON email_communications(tracking_id)")
        conn.execute("UPDATE leads SET campaign_step = 1 WHERE campaign_step IS NULL -- SYSTEM-WIDE: intentionally unscoped.")
        conn.commit()


def _normalize_campaign_sequence_row(row: pgdb.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    item = dict(row)
    item["active"] = bool(int(item.get("active") or 0))
    return item


def _normalize_saved_template_row(row: pgdb.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return dict(row)


def _normalize_campaign_event_row(row: pgdb.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    item = dict(row)
    item["metadata"] = deserialize_json(item.get("metadata_json")) or {}
    return item


def _normalize_email_communication_row(row: pgdb.Row | dict | None) -> dict[str, Any]:
    if row is None:
        return {}
    item = dict(row)
    item["lead_id"] = int(item.get("lead_id") or 0)
    item["id"] = int(item.get("id") or 0)
    item["direction"] = str(item.get("direction") or "").strip().lower()
    item["status"] = str(item.get("status") or "").strip().lower()
    item["subject"] = str(item.get("subject") or "").strip() or None
    item["body_html"] = str(item.get("body_html") or "").strip() or None
    item["body_text"] = str(item.get("body_text") or "").strip() or None
    item["tracking_id"] = str(item.get("tracking_id") or "").strip() or None
    item["timestamp"] = str(item.get("created_at") or item.get("timestamp") or "").strip() or utc_now_iso()
    return item


def _insert_email_communication_local(
    db_path: Path,
    *,
    lead_id: int,
    user_id: str,
    direction: str,
    status: str,
    subject: Optional[str] = None,
    body_html: Optional[str] = None,
    body_text: Optional[str] = None,
    tracking_id: Optional[str] = None,
) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    now_iso = utc_now_iso()
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        cursor = conn.execute(
            """
            INSERT INTO email_communications (
                lead_id, user_id, direction, subject, body_html, body_text, status, tracking_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(lead_id),
                str(user_id or "legacy"),
                str(direction or "").strip().lower(),
                str(subject or "").strip() or None,
                str(body_html or "").strip() or None,
                str(body_text or "").strip() or None,
                str(status or "sent").strip().lower(),
                str(tracking_id or "").strip() or None,
                now_iso,
                now_iso,
            ),
        )
        row = conn.execute("SELECT * FROM email_communications WHERE id = ? LIMIT 1", (cursor.lastrowid,)).fetchone()
        conn.commit()
    return _normalize_email_communication_row(row)


def _update_email_communication_status_local(
    db_path: Path,
    *,
    status: str,
    communication_id: Optional[int] = None,
    tracking_id: Optional[str] = None,
    lead_id: Optional[int] = None,
) -> None:
    ensure_mailer_campaign_tables(db_path)
    now_iso = utc_now_iso()
    safe_status = str(status or "").strip().lower() or "sent"
    with pgdb.connect(db_path) as conn:
        if communication_id is not None:
            conn.execute(
                "UPDATE email_communications SET status = ?, updated_at = ? WHERE id = ?",
                (safe_status, now_iso, int(communication_id)),
            )
        elif str(tracking_id or "").strip():
            conn.execute(
                """
                UPDATE email_communications
                SET status = ?, updated_at = ?
                WHERE tracking_id = ?
                  AND direction = 'sent'
                  AND (status IS NULL OR LOWER(COALESCE(status, '')) <> 'replied')
                """,
                (safe_status, now_iso, str(tracking_id or "").strip()),
            )
        elif lead_id is not None:
            conn.execute(
                """
                UPDATE email_communications
                SET status = ?, updated_at = ?
                WHERE id = (
                    SELECT id
                    FROM email_communications
                    WHERE lead_id = ? AND direction = 'sent'
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                )
                """,
                (safe_status, now_iso, int(lead_id)),
            )
        conn.commit()


def _insert_email_communication_supabase(payload: dict[str, Any]) -> None:
    if not is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return
    if not supabase_table_available(DEFAULT_CONFIG_PATH, "communications"):
        return
    client = get_supabase_client(DEFAULT_CONFIG_PATH)
    if client is None:
        return
    try:
        table_name = resolve_supabase_table_name(client, "communications")
        client.table(table_name).insert(payload).execute()
    except Exception:
        logging.debug("Failed to insert communication into Supabase communications table.")


def _update_email_communication_status_supabase(
    *,
    status: str,
    communication_id: Optional[int] = None,
    tracking_id: Optional[str] = None,
    lead_id: Optional[int] = None,
) -> None:
    if not is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return
    if not supabase_table_available(DEFAULT_CONFIG_PATH, "communications"):
        return
    client = get_supabase_client(DEFAULT_CONFIG_PATH)
    if client is None:
        return
    now_iso = utc_now_iso()
    payload = {"status": str(status or "").strip().lower() or "sent", "updated_at": now_iso}
    try:
        table_name = resolve_supabase_table_name(client, "communications")
        if communication_id is not None:
            client.table(table_name).update(payload).eq("id", int(communication_id)).execute()
            return
        if str(tracking_id or "").strip():
            client.table(table_name).update(payload).eq("tracking_id", str(tracking_id or "").strip()).eq("direction", "sent").neq("status", "replied").execute()
            return
        if lead_id is not None:
            rows = client.table(table_name).select("id").eq("lead_id", int(lead_id)).eq("direction", "sent").order("created_at", desc=True).limit(1).execute().data or []
            if rows:
                client.table(table_name).update(payload).eq("id", int(rows[0].get("id") or 0)).execute()
    except Exception:
        logging.debug("Failed to update communication status in Supabase communications table.")


def _record_email_communication(
    *,
    lead_id: int,
    user_id: str,
    direction: str,
    status: str,
    subject: Optional[str] = None,
    body_html: Optional[str] = None,
    body_text: Optional[str] = None,
    tracking_id: Optional[str] = None,
) -> dict[str, Any]:
    item = _insert_email_communication_local(
        DEFAULT_DB_PATH,
        lead_id=int(lead_id),
        user_id=str(user_id or "legacy"),
        direction=str(direction or "").strip().lower(),
        status=str(status or "sent").strip().lower(),
        subject=subject,
        body_html=body_html,
        body_text=body_text,
        tracking_id=tracking_id,
    )
    _insert_email_communication_supabase(
        {
            "lead_id": int(item.get("lead_id") or lead_id),
            "user_id": str(user_id or "legacy"),
            "direction": str(item.get("direction") or direction or "sent"),
            "subject": item.get("subject"),
            "body_html": item.get("body_html"),
            "body_text": item.get("body_text"),
            "status": str(item.get("status") or status or "sent"),
            "tracking_id": item.get("tracking_id"),
            "created_at": item.get("timestamp") or utc_now_iso(),
            "updated_at": item.get("updated_at") or utc_now_iso(),
        }
    )
    return item


def _mark_email_communication_opened(*, communication_id: Optional[int] = None, tracking_id: Optional[str] = None, lead_id: Optional[int] = None) -> None:
    _update_email_communication_status_local(
        DEFAULT_DB_PATH,
        status="opened",
        communication_id=communication_id,
        tracking_id=tracking_id,
        lead_id=lead_id,
    )
    _update_email_communication_status_supabase(
        status="opened",
        communication_id=communication_id,
        tracking_id=tracking_id,
        lead_id=lead_id,
    )


def _mark_email_communication_replied(*, tracking_id: Optional[str] = None, lead_id: Optional[int] = None) -> None:
    _update_email_communication_status_local(
        DEFAULT_DB_PATH,
        status="replied",
        tracking_id=tracking_id,
        lead_id=lead_id,
    )
    _update_email_communication_status_supabase(
        status="replied",
        tracking_id=tracking_id,
        lead_id=lead_id,
    )


def list_email_communications_for_lead(*, lead_id: int, user_id: str, limit: int = 100) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 100), 500))

    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "communications"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                table_name = resolve_supabase_table_name(client, "communications")
                rows = client.table(table_name).select("id,lead_id,user_id,direction,subject,body_html,body_text,status,tracking_id,created_at,updated_at").eq("lead_id", int(lead_id)).eq("user_id", str(user_id or "legacy")).order("created_at", desc=False).limit(safe_limit).execute().data or []
                return [_normalize_email_communication_row(row) for row in rows]
            except Exception:
                logging.debug("Supabase communications read failed for lead_id=%s", int(lead_id))

    ensure_mailer_campaign_tables(DEFAULT_DB_PATH)
    with pgdb.connect(DEFAULT_DB_PATH) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT id, lead_id, user_id, direction, subject, body_html, body_text, status, tracking_id, created_at, updated_at
            FROM email_communications
            WHERE lead_id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ?
            ORDER BY created_at ASC, id ASC
            LIMIT ?
            """,
            (int(lead_id), str(user_id or "legacy"), safe_limit),
        ).fetchall()
    return [_normalize_email_communication_row(row) for row in rows]


def _normalize_campaign_event_type(raw_event_type: Any) -> str:
    value = str(raw_event_type or "").strip().lower().replace("_", " ")
    aliases = {
        "opened": "open",
        "clicked": "click",
        "reply detected": "reply",
        "replied": "reply",
        "bounced": "bounce",
    }
    return aliases.get(value, value.replace(" ", "_"))


def create_mailer_campaign_sequence(db_path: Path, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    now_iso = utc_now_iso()
    is_active = bool(payload.get("active", True))
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT *
            FROM CampaignSequences
            WHERE user_id = ?
            ORDER BY COALESCE(active, 0) DESC, updated_at DESC, id DESC
            LIMIT ?
            """,
            (user_id, int(limit)),
        ).fetchall()
    return [_normalize_campaign_sequence_row(row) for row in rows]


def auth_email_exists(email: str, *, config_path: Path = DEFAULT_CONFIG_PATH, db_path: Path = DEFAULT_DB_PATH) -> bool:
    normalized_email = str(email or "").strip().lower()
    if not normalized_email:
        return False

    if is_supabase_auth_enabled(config_path):
        if not ensure_supabase_users_table(config_path):
            raise HTTPException(
                status_code=503,
                detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
            )
        sb_client = get_supabase_client(config_path)
        if sb_client is None:
            raise HTTPException(status_code=503, detail="Supabase is not reachable.")
        try:
            existing = sb_client.table("users").select("id").eq("email", normalized_email).limit(1).execute()
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Supabase auth email lookup failed: {exc}")
        return bool(list(getattr(existing, "data", None) or []))

    ensure_users_table(db_path)
    with pgdb.connect(db_path) as conn:
        existing = conn.execute(
            "SELECT 1 FROM users WHERE email = ? LIMIT 1",
            (normalized_email,),
        ).fetchone()
    return existing is not None


def create_saved_mail_template(db_path: Path, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    now_iso = utc_now_iso()
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT *
            FROM SavedTemplates
            WHERE user_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (user_id, int(limit)),
        ).fetchall()
    return [_normalize_saved_template_row(row) for row in rows]


def record_mailer_campaign_event(db_path: Path, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    event_type = _normalize_campaign_event_type(payload.get("event_type"))
    allowed_event_types = {"sent", "open", "click", "reply", "bounce"}
    if event_type not in allowed_event_types:
        raise HTTPException(status_code=422, detail=f"Unsupported event_type '{event_type}'")

    raw_lead_id = payload.get("lead_id")
    lead_id = int(raw_lead_id) if raw_lead_id is not None else None
    now_iso = utc_now_iso()
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    reason = str(payload.get("reason") or "").strip()
    if reason and not metadata.get("reason"):
        metadata = {**metadata, "reason": reason}

    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        lead_row = None
        email = str(payload.get("email") or "").strip()
        if lead_id is not None:
            lead_row = conn.execute(
                "SELECT id, email, open_tracking_token, COALESCE(NULLIF(user_id, ''), 'legacy') AS user_id, status FROM leads WHERE id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ? LIMIT 1",
                (lead_id, str(user_id or "legacy")),
            ).fetchone()
            if lead_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
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
                WHERE id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ?
                """,
                (
                    str(payload.get("subject_line") or "").strip() or None,
                    str(payload.get("subject_variant") or "").strip() or None,
                    lead_id,
                    str(user_id or "legacy"),
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
                    WHERE id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ?
                    """,
                    (now_iso, now_iso, lead_id, str(user_id or "legacy")),
                )
                try:
                    _record_email_communication(
                        lead_id=int(lead_id),
                        user_id=str(user_id or "legacy"),
                        direction="sent",
                        status="sent",
                        subject=str(payload.get("subject_line") or "").strip() or None,
                        body_html=str(metadata.get("body_html") or "").strip() or None,
                        body_text=str(
                            payload.get("body_text")
                            or metadata.get("body_text")
                            or metadata.get("generated_email_body")
                            or ""
                        ).strip() or None,
                        tracking_id=str(metadata.get("thread_token") or metadata.get("token") or lead_row.get("open_tracking_token") or "").strip() or None,
                    )
                except Exception:
                    logging.debug("Failed to record sent email communication for lead_id=%s", lead_id)
            elif event_type == "open":
                conn.execute(
                    """
                    UPDATE leads
                    SET
                        open_count = COALESCE(open_count, 0) + 1,
                        first_opened_at = COALESCE(first_opened_at, ?),
                        last_opened_at = ?
                    WHERE id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ?
                    """,
                    (now_iso, now_iso, lead_id, str(user_id or "legacy")),
                )
                _mark_email_communication_opened(
                    tracking_id=str(metadata.get("thread_token") or metadata.get("token") or lead_row.get("open_tracking_token") or "").strip() or None,
                    lead_id=int(lead_id),
                )
            elif event_type == "reply":
                conn.execute(
                    """
                    UPDATE leads
                    SET
                        reply_detected_at = COALESCE(reply_detected_at, ?),
                        next_mail_at = NULL,
                        status_updated_at = ?,
                        pipeline_stage = CASE
                            WHEN LOWER(COALESCE(status, '')) IN ('paid', 'closed') THEN 'Won (Paid)'
                            ELSE 'Replied'
                        END,
                        status = CASE
                            WHEN LOWER(COALESCE(status, '')) IN ('paid', 'closed') THEN status
                            ELSE 'replied'
                        END
                    WHERE id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ?
                    """,
                    (now_iso, now_iso, lead_id, str(user_id or "legacy")),
                )
                _mark_email_communication_replied(
                    tracking_id=str(metadata.get("thread_token") or metadata.get("token") or lead_row.get("open_tracking_token") or "").strip() or None,
                    lead_id=int(lead_id),
                )
                try:
                    _record_email_communication(
                        lead_id=int(lead_id),
                        user_id=str(user_id or "legacy"),
                        direction="received",
                        status="replied",
                        subject=str(payload.get("subject_line") or "").strip() or None,
                        body_html=str(metadata.get("body_html") or "").strip() or None,
                        body_text=str(payload.get("body_text") or metadata.get("body_text") or "").strip() or None,
                        tracking_id=str(metadata.get("thread_token") or metadata.get("token") or lead_row.get("open_tracking_token") or "").strip() or None,
                    )
                except Exception:
                    logging.debug("Failed to record received reply communication for lead_id=%s", lead_id)
            elif event_type == "click":
                conn.execute(
                    """
                    UPDATE leads
                    SET status_updated_at = ?
                    WHERE id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ?
                    """,
                    (now_iso, lead_id, str(user_id or "legacy")),
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
                    WHERE id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ?
                    """,
                    (now_iso, reason or None, lead_id, str(user_id or "legacy")),
                )

        row = conn.execute(
            "SELECT * FROM CampaignEvents WHERE id = ? LIMIT 1",
            (cursor.lastrowid,),
        ).fetchone()
        conn.commit()

    return _normalize_campaign_event_row(row)


def get_mailer_campaign_stats(db_path: Path, user_id: str) -> dict[str, Any]:
    ensure_mailer_campaign_tables(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
            ORDER BY e.occurred_at DESC, e.id DESC
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
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT
                id,
                COALESCE(subscription_active, FALSE) AS subscription_active,
                COALESCE(subscription_status, '') AS subscription_status,
                COALESCE(subscription_start_date, '') AS subscription_start_date,
                COALESCE(subscription_cancel_at, '') AS subscription_cancel_at,
                COALESCE(subscription_cancel_at_period_end, FALSE) AS subscription_cancel_at_period_end,
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
                        subscription_active = FALSE,
                        subscription_status = 'expired',
                        subscription_cancel_at_period_end = FALSE,
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
                    subscription_active = FALSE,
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
        if _is_db_capacity_error(exc):
            logging.warning("Monthly credit reset cycle skipped because the database is saturated.")
        else:
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
                logging.warning("Supabase create_task_record fallback to legacy store: %s", exc)

    ensure_system_task_table(db_path)
    with pgdb.connect(db_path) as conn:
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
                logging.warning("Supabase mark_task_running fallback to legacy store: %s", exc)

    with pgdb.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE system_tasks
            SET status = ?, started_at = COALESCE(started_at, ?), error = NULL
            -- SYSTEM-WIDE: intentionally unscoped.
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
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                ).eq("id", task_id).execute()
                return
            except Exception as exc:
                logging.warning("Supabase update_task_progress fallback to legacy store: %s", exc)

    with pgdb.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE system_tasks
            SET result_payload = ?
            -- SYSTEM-WIDE: intentionally unscoped.
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
                logging.warning("Supabase finish_task_record fallback to legacy store: %s", exc)

    with pgdb.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE system_tasks
            SET status = ?, result_payload = ?, error = ?, finished_at = ?
            -- SYSTEM-WIDE: intentionally unscoped.
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
                logging.warning("Supabase fetch_latest_task fallback to legacy store: %s", exc)

    ensure_system_task_table(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
                -- SYSTEM-WIDE: intentionally unscoped.
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
                logging.warning("Supabase fetch_all_latest_tasks fallback to legacy store: %s", exc)

    ensure_system_task_table(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
                    -- SYSTEM-WIDE: intentionally unscoped.
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
                logging.warning("Supabase fetch_task_history fallback to legacy store: %s", exc)

    ensure_system_task_table(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
                -- SYSTEM-WIDE: intentionally unscoped.
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
                logging.warning("Supabase fetch_task_by_id fallback to legacy store: %s", exc)

    ensure_system_task_table(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
                -- SYSTEM-WIDE: intentionally unscoped.
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


def _is_postgres_task_store_enabled() -> bool:
    if not is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return False
    if not supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
        return False
    try:
        pg_get_engine()
    except Exception:
        return False
    return True


def _is_postgres_runtime_store_enabled() -> bool:
    if not is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return False
    if not supabase_table_available(DEFAULT_CONFIG_PATH, "system_runtime"):
        return False
    try:
        pg_get_engine()
    except Exception:
        return False
    return True


def _has_live_external_worker(max_age_seconds: int = 120) -> bool:
    if not _is_postgres_runtime_store_enabled():
        return False
    try:
        with pg_get_engine().begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT value
                    FROM system_runtime
                    WHERE key LIKE 'worker:%:last_heartbeat_at'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                )
            ).fetchone()
    except Exception:
        return False

    if not row:
        return False

    heartbeat = parse_iso_datetime(row[0])
    if heartbeat is None:
        return False

    age_seconds = (datetime.now(timezone.utc) - heartbeat).total_seconds()
    return age_seconds <= max_age_seconds


def _should_use_external_worker(task_type: str) -> bool:
    if task_type == "scrape" and FORCE_IN_PROCESS_SCRAPE:
        return False
    return _is_postgres_task_store_enabled() and _has_live_external_worker()


def get_runtime_value(db_path: Path, key: str) -> Optional[str]:
    if _is_postgres_runtime_store_enabled():
        try:
            with pg_get_engine().begin() as conn:
                row = conn.execute(
                    text("SELECT value FROM system_runtime WHERE key = :key LIMIT 1"),
                    {"key": key},
                ).fetchone()
            if not row:
                return None
            return row[0]
        except Exception as exc:
            logging.warning("Postgres get_runtime_value fallback to Supabase/legacy store: %s", exc)

    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "system_runtime"):
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is not None:
            try:
                rows = client.table("system_runtime").select("value").eq("key", key).limit(1).execute().data or []
                if not rows:
                    return None
                return rows[0].get("value")
            except Exception as exc:
                logging.warning("Supabase get_runtime_value fallback to legacy store: %s", exc)

    ensure_runtime_table(db_path)
    with pgdb.connect(db_path) as conn:
        row = conn.execute("SELECT value FROM system_runtime WHERE key = ?", (key,)).fetchone()
    if not row:
        return None
    return row[0]


def set_runtime_value(db_path: Path, key: str, value: str) -> None:
    if _is_postgres_runtime_store_enabled():
        try:
            with pg_get_engine().begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO system_runtime (key, value, updated_at)
                        VALUES (:key, :value, :updated_at)
                        ON CONFLICT(key)
                        DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                        """
                    ),
                    {
                        "key": key,
                        "value": value,
                        "updated_at": utc_now_iso(),
                    },
                )
            return
        except Exception as exc:
            logging.warning("Postgres set_runtime_value fallback to Supabase/legacy store: %s", exc)

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
                logging.warning("Supabase set_runtime_value fallback to legacy store: %s", exc)

    ensure_runtime_table(db_path)
    with pgdb.connect(db_path) as conn:
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


AI_SIGNALS_RUNTIME_KEY = "global_ai_signals_state"


def load_ai_signals_runtime_state(db_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    default_state: dict[str, Any] = {
        "enabled": True,
        "updated_at": None,
        "updated_by": "",
    }
    raw_state = get_runtime_value(db_path, AI_SIGNALS_RUNTIME_KEY)
    if not raw_state:
        return default_state
    try:
        parsed = json.loads(str(raw_state))
    except Exception:
        return default_state
    if not isinstance(parsed, dict):
        return default_state
    return {
        "enabled": bool(parsed.get("enabled", True)),
        "updated_at": parsed.get("updated_at"),
        "updated_by": str(parsed.get("updated_by") or "").strip().lower(),
    }


def save_ai_signals_runtime_state(enabled: bool, updated_by: str = "", db_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    state = {
        "enabled": bool(enabled),
        "updated_at": utc_now_iso(),
        "updated_by": str(updated_by or "").strip().lower(),
    }
    set_runtime_value(db_path, AI_SIGNALS_RUNTIME_KEY, json.dumps(state, ensure_ascii=False))
    return state


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
        os.getenv("STRIPE_SECRET_KEY")
        or os.getenv("SNIPED_STRIPE_SECRET_KEY")
        or stripe_cfg.get("secret_key", "")
        or stripe_cfg.get("secretKey", "")
        or stripe_cfg.get("api_key", "")
        or ""
    ).strip()


def get_stripe_webhook_secret(config_path: Path = DEFAULT_CONFIG_PATH) -> str:
    stripe_cfg = get_stripe_config(config_path)
    return str(
        os.getenv("STRIPE_WEBHOOK_SECRET")
        or os.getenv("SNIPED_STRIPE_WEBHOOK_SECRET")
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


def get_stripe_checkout_app_base_url(config_path: Path = DEFAULT_CONFIG_PATH, request: Optional[Request] = None) -> str:
    configured = str(
        os.environ.get("STRIPE_CHECKOUT_APP_URL")
        or os.environ.get("SNIPED_DASHBOARD_URL")
        or os.environ.get("LEADFLOW_DASHBOARD_URL")
        or os.environ.get("FRONTEND_URL")
        or _read_json_config(config_path).get("dashboard_url", "")
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

    return "https://www.sniped.io"


def build_checkout_app_redirect_url(
    app_base_url: str,
    *,
    checkout_status: Optional[str] = None,
    topup_status: Optional[str] = None,
    plan_key: Optional[str] = None,
    package_key: Optional[str] = None,
    package_credits: Optional[int] = None,
    include_session_id: bool = False,
) -> str:
    normalized_base_url = str(app_base_url or "").strip().rstrip("/") or "https://www.sniped.io"
    params: list[tuple[str, str]] = []
    if checkout_status:
        params.append(("checkout", str(checkout_status).strip().lower()))
    if topup_status:
        params.append(("topup", str(topup_status).strip().lower()))
    if plan_key:
        params.append(("plan", str(plan_key).strip().lower()))
    if package_key:
        params.append(("topup_package", str(package_key).strip().lower()))
    if package_credits is not None:
        params.append(("topup_credits", str(int(package_credits))))
    if include_session_id:
        params.append(("session_id", "{CHECKOUT_SESSION_ID}"))
    return f"{normalized_base_url}/app?{urlencode(params)}"


def extract_stripe_http_error_details(exc: urllib.error.HTTPError) -> tuple[str, str]:
    raw_body = ""
    try:
        raw_body = exc.read().decode("utf-8", errors="replace")
    except Exception:
        raw_body = ""

    parsed_body: dict[str, Any] = {}
    if raw_body:
        try:
            payload = json.loads(raw_body)
            if isinstance(payload, dict):
                parsed_body = payload
        except json.JSONDecodeError:
            parsed_body = {}

    stripe_error = parsed_body.get("error") if isinstance(parsed_body.get("error"), dict) else {}
    stripe_message = str(
        stripe_error.get("message")
        or raw_body
        or exc.reason
        or exc
    ).strip()
    if not stripe_message:
        stripe_message = "Unknown Stripe error."
    return stripe_message, raw_body


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
    allow_email_lookup: bool = False,
) -> dict[str, Any]:
    normalized_email = str(user_email or "").strip().lower()
    customer_id = str(stripe_customer_id or "").strip()

    if not get_stripe_secret_key(config_path):
        return {}

    if not customer_id and allow_email_lookup and normalized_email:
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
        "subscription_status": status or "active",
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
    allow_email_lookup: bool = False,
) -> dict[str, Any]:
    normalized_user_id = str(user_id or "").strip()
    normalized_email = str(user_email or "").strip().lower()
    customer_id = str(stripe_customer_id or "").strip()

    if not get_stripe_secret_key(config_path):
        return {}

    if not customer_id and allow_email_lookup and normalized_email:
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
    config: dict[str, Any] = {}
    file_error: Optional[str] = None
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
    except FileNotFoundError:
        # Config file is optional in hosted environments that use env vars.
        config = {}
        file_error = None
    except Exception as exc:
        # In production we rely primarily on env vars; missing local settings file
        # should not hard-fail health checks.
        config = {}
        file_error = f"Could not read environment settings: {exc}"

    openai_ok = has_any_ai_credentials(config_path)

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

    # In hosted environments we often rely on env vars only.
    # Report file read issues only when no config source appears usable.
    effective_error = file_error if not (openai_ok or smtp_ok or supabase_ok) else None

    return {
        "ok": openai_ok and smtp_ok,
        "openai_ok": openai_ok,
        "azure_openai_ok": openai_ok,
        "smtp_ok": smtp_ok,
        "supabase_ok": supabase_ok,
        "error": effective_error,
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
        raise HTTPException(status_code=503, detail=f"Could not read environment settings: {exc}")

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
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        if session_token:
            row = conn.execute("SELECT smtp_accounts_json FROM users WHERE token = ?", (session_token,)).fetchone()
        else:
            row = conn.execute("SELECT smtp_accounts_json FROM users WHERE id = ?", (user_id,)).fetchone()
    if row is None:
        return []
    return _parse_smtp_accounts_json(row["smtp_accounts_json"])


def _is_smtp_account_ready(account: Optional[dict[str, Any]]) -> bool:
    if not isinstance(account, dict):
        return False
    return bool(
        str(account.get("host", "") or "").strip()
        and str(account.get("email", "") or "").strip()
        and str(account.get("password", "") or "").strip()
    )


def get_system_smtp_account(config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
            cfg = loaded if isinstance(loaded, dict) else {}
    except Exception:
        cfg = {}

    system_cfg = cfg.get("system_smtp", {}) if isinstance(cfg.get("system_smtp"), dict) else {}
    account = {
        "host": str(os.environ.get("SNIPED_SYSTEM_SMTP_HOST") or system_cfg.get("host", "") or "").strip(),
        "port": int(os.environ.get("SNIPED_SYSTEM_SMTP_PORT") or system_cfg.get("port", 587) or 587),
        "email": str(os.environ.get("SNIPED_SYSTEM_SMTP_EMAIL") or system_cfg.get("email", "") or "").strip(),
        "password": str(os.environ.get("SNIPED_SYSTEM_SMTP_PASSWORD") or system_cfg.get("password", "") or "").strip(),
        "use_tls": str(os.environ.get("SNIPED_SYSTEM_SMTP_USE_TLS") or system_cfg.get("use_tls", "true") or "true").strip().lower() in {"1", "true", "yes", "on"},
        "use_ssl": str(os.environ.get("SNIPED_SYSTEM_SMTP_USE_SSL") or system_cfg.get("use_ssl", "false") or "false").strip().lower() in {"1", "true", "yes", "on"},
        "from_name": str(os.environ.get("SNIPED_SYSTEM_SMTP_FROM_NAME") or system_cfg.get("from_name", "Sniped.io") or "Sniped.io").strip(),
        "signature": str(os.environ.get("SNIPED_SYSTEM_SMTP_SIGNATURE") or system_cfg.get("signature", "") or "").strip(),
    }
    return account if _is_smtp_account_ready(account) else {}


def count_system_smtp_emails_sent(*, user_id: str, sender_email: str, db_path: Path = DEFAULT_DB_PATH) -> int:
    normalized_user = str(user_id or "").strip() or "legacy"
    normalized_sender = str(sender_email or "").strip().lower()
    if not normalized_sender:
        return 0

    with pgdb.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM leads
            WHERE COALESCE(NULLIF(user_id, ''), 'legacy') = ?
              AND sent_at IS NOT NULL
              AND LOWER(COALESCE(last_sender_email, '')) = ?
            """,
            (normalized_user, normalized_sender),
        ).fetchone()
    return int(row[0] if row else 0)


def resolve_mailer_smtp_accounts_for_send(
    *,
    session_token: str,
    user_id: str,
    billing: dict[str, Any],
    requested_limit: int,
    db_path: Path = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    normalized_plan = _normalize_plan_key((billing or {}).get("plan_key"), fallback=DEFAULT_PLAN_KEY)
    custom_smtp_allowed = normalized_plan != "free"

    user_accounts = load_user_smtp_accounts(session_token=session_token, user_id=user_id, db_path=db_path)
    primary_user_account = dict(user_accounts[0] or {}) if user_accounts else {}
    if custom_smtp_allowed and _is_smtp_account_ready(primary_user_account):
        return {
            "source": "custom",
            "accounts": user_accounts,
            "effective_limit": int(requested_limit),
            "system_quota_limit": SYSTEM_SMTP_DEFAULT_SEND_LIMIT,
            "system_quota_remaining": SYSTEM_SMTP_DEFAULT_SEND_LIMIT,
            "custom_smtp_allowed": custom_smtp_allowed,
        }

    system_account = get_system_smtp_account(DEFAULT_CONFIG_PATH)
    if not system_account:
        if custom_smtp_allowed:
            raise HTTPException(status_code=400, detail="Add your SMTP account in Settings, or ask admin to configure SNIPED_SYSTEM_SMTP_* fallback credentials.")
        raise HTTPException(status_code=503, detail="System SMTP fallback is not configured yet. Add SNIPED_SYSTEM_SMTP_* env values first.")

    system_sender = str(system_account.get("email", "") or "").strip()
    already_sent = count_system_smtp_emails_sent(user_id=user_id, sender_email=system_sender, db_path=db_path)
    remaining = max(0, int(SYSTEM_SMTP_DEFAULT_SEND_LIMIT) - int(already_sent))
    if remaining <= 0:
        raise HTTPException(
            status_code=403,
            detail=(
                f"System SMTP quota reached ({SYSTEM_SMTP_DEFAULT_SEND_LIMIT} emails). "
                "Connect your own SMTP / Google OAuth to continue."
            ),
        )

    return {
        "source": "system",
        "accounts": [system_account],
        "effective_limit": min(int(requested_limit), int(remaining)),
        "system_quota_limit": SYSTEM_SMTP_DEFAULT_SEND_LIMIT,
        "system_quota_remaining": int(remaining),
        "custom_smtp_allowed": custom_smtp_allowed,
    }


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
    with pgdb.connect(db_path) as conn:
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


def ensure_user_mailer_smtp_ready(*, session_token: Optional[str] = None, user_id: Optional[str] = None, db_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    try:
        return get_primary_user_smtp_account(session_token=session_token, user_id=user_id, db_path=db_path)
    except HTTPException as exc:
        detail = str(exc.detail or "")
        if exc.status_code == 503 and (
            "SMTP is not configured" in detail or "SMTP is not fully configured" in detail
        ):
            raise HTTPException(status_code=400, detail="Please add your SMTP account in Settings.")
        raise


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
    env_only = STATELESS_SUPABASE_ONLY
    url = str(os.environ.get("SUPABASE_URL") or ("" if env_only else supabase_cfg.get("url", "")) or "").strip()
    database_url = str(
        os.environ.get("DATABASE_URL")
        or os.environ.get("SUPABASE_DATABASE_URL")
        or os.environ.get("SUPABASE_DB_POOLER_URL")
        or os.environ.get("SUPABASE_POOLER_URL")
        or ("" if env_only else supabase_cfg.get("database_url", ""))
        or ""
    ).strip()
    try:
        resolved_database_url = pg_get_database_url()
    except Exception:
        resolved_database_url = database_url
    shared_key = str(os.environ.get("SUPABASE_KEY") or ("" if env_only else supabase_cfg.get("key", "")) or "").strip()
    # IMPORTANT: never infer service-role from shared/anon key. It must be explicit.
    service_role_key = str(
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or ("" if env_only else supabase_cfg.get("service_role_key", ""))
        or ("" if env_only else supabase_cfg.get("serviceRoleKey", ""))
        or ""
    ).strip()
    publishable_key = str(
        os.environ.get("SUPABASE_PUBLISHABLE_KEY")
        or shared_key
        or ("" if env_only else supabase_cfg.get("publishable_key", ""))
        or ("" if env_only else supabase_cfg.get("publishableKey", ""))
        or ""
    ).strip()
    primary_mode_raw = os.environ.get("SUPABASE_PRIMARY_DB")

    if env_only:
        primary_mode = _env_flag("SUPABASE_PRIMARY_DB", default=True)
    elif primary_mode_raw is None:
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
        "service_role_key": service_role_key,
        "publishable_key": publishable_key,
        "database_url": database_url,
        "resolved_database_url": resolved_database_url,
        "has_database_url": bool(resolved_database_url or database_url),
        "has_service_role": bool(service_role_key),
        "has_publishable": bool(publishable_key),
        "primary_mode": primary_mode,
    }


def _looks_local_hostname(hostname: str) -> bool:
    host = str(hostname or "").strip().lower()
    if not host:
        return False
    return host in {"localhost", "127.0.0.1", "0.0.0.0", "::1"} or host.endswith(".local")


def _is_railway_runtime() -> bool:
    return bool(
        str(os.environ.get("RAILWAY_PROJECT_ID") or "").strip()
        or str(os.environ.get("RAILWAY_ENVIRONMENT") or "").strip()
        or str(os.environ.get("RAILWAY_STATIC_URL") or "").strip()
    )


def _ensure_sslmode_require(db_url: str) -> str:
    raw = str(db_url or "").strip()
    if not raw:
        return raw
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw

    scheme = str(parsed.scheme or "").lower()
    if not scheme.startswith("postgres"):
        return raw

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    sslmode = str(query.get("sslmode") or "").strip().lower()
    if sslmode == "require":
        return raw
    query["sslmode"] = "require"
    return urlunparse(parsed._replace(query=urlencode(query)))


def ensure_supabase_runtime(context: str = "backend") -> dict[str, Any]:
    settings = load_supabase_settings(DEFAULT_CONFIG_PATH)
    if not settings.get("enabled"):
        raise HTTPException(
            status_code=503,
            detail=(
                f"Supabase is required for {context}. Configure SUPABASE_URL plus SUPABASE_KEY "
                f"(or SUPABASE_SERVICE_ROLE_KEY / SUPABASE_PUBLISHABLE_KEY)."
            ),
        )
    return settings


def is_supabase_primary_enabled(config_path: Path) -> bool:
    settings = load_supabase_settings(config_path)
    return bool(settings["enabled"] and settings.get("primary_mode"))


def get_supabase_client(config_path: Path) -> Optional[Any]:
    if not _HAS_SUPABASE or create_supabase_client is None:
        return None
    settings = load_supabase_settings(config_path)
    if not settings["enabled"]:
        return None
    if STATELESS_SUPABASE_ONLY and not settings.get("has_service_role"):
        logging.warning(
            "Supabase client initialized without service-role key while STATELESS_SUPABASE_ONLY=1; "
            "writes can fail under RLS."
        )
    try:
        return create_supabase_client(settings["url"], settings["key"])
    except Exception as exc:
        logging.warning("Supabase init failed: %s", exc)
        return None


def _looks_like_jwt(token: str) -> bool:
    value = str(token or "").strip()
    return value.count(".") == 2


def _apply_supabase_jwt(client: Any, jwt_token: str) -> None:
    token = str(jwt_token or "").strip()
    if not token or client is None:
        return
    try:
        postgrest = getattr(client, "postgrest", None)
        if postgrest is not None and hasattr(postgrest, "auth"):
            postgrest.auth(token)
            return
        if postgrest is not None and hasattr(postgrest, "headers") and isinstance(postgrest.headers, dict):
            postgrest.headers.update({"Authorization": f"Bearer {token}"})
    except Exception as exc:
        logging.warning("Failed to bind JWT token to Supabase client: %s", exc)


def get_supabase_client_for_token(config_path: Path, session_token: str) -> Optional[Any]:
    if not _HAS_SUPABASE or create_supabase_client is None:
        return None
    settings = load_supabase_settings(config_path)
    if not settings.get("enabled"):
        return None

    token = str(session_token or "").strip()
    if _looks_like_jwt(token):
        key = str(settings.get("publishable_key") or settings.get("key") or "").strip()
        if not key:
            return None
        try:
            client = create_supabase_client(str(settings.get("url") or ""), key)
            _apply_supabase_jwt(client, token)
            return client
        except Exception as exc:
            logging.warning("Supabase JWT client init failed: %s", exc)
            return None

    return get_supabase_client(config_path)


def resolve_supabase_auth_user_id(session_token: str, config_path: Path = DEFAULT_CONFIG_PATH) -> Optional[str]:
    token = str(session_token or "").strip()
    if not _looks_like_jwt(token):
        return None

    client = get_supabase_client_for_token(config_path, token)
    if client is None:
        return None

    try:
        auth_api = getattr(client, "auth", None)
        if auth_api is None or not hasattr(auth_api, "get_user"):
            return None
        response = auth_api.get_user(token)
        user_obj = getattr(response, "user", None)
        if user_obj is None and isinstance(response, dict):
            user_obj = response.get("user")
        if user_obj is None:
            return None
        candidate = getattr(user_obj, "id", None)
        if candidate is None and isinstance(user_obj, dict):
            candidate = user_obj.get("id")
        value = str(candidate or "").strip()
        return value or None
    except Exception:
        return None


def get_supabase_request_client(request: Optional[Request], config_path: Path = DEFAULT_CONFIG_PATH) -> Optional[Any]:
    auth_header = ""
    if request is not None:
        auth_header = str(request.headers.get("Authorization", "") or "").strip()
    token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else ""
    if token:
        client = get_supabase_client_for_token(config_path, token)
        if client is not None:
            return client
    return get_supabase_client(config_path)


def get_supabase_admin_client(config_path: Path) -> Optional[Any]:
    if not _HAS_SUPABASE or create_supabase_client is None:
        return None
    settings = load_supabase_settings(config_path)
    service_role_key = str(settings.get("service_role_key") or "").strip()
    if not settings.get("url") or not service_role_key:
        return None
    try:
        return create_supabase_client(settings["url"], service_role_key)
    except Exception as exc:
        logging.warning("Supabase admin init failed: %s", exc)
        return None


def set_supabase_primary_mode(config_path: Path, enabled: bool) -> None:
    if STATELESS_SUPABASE_ONLY:
        return

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


_SUPABASE_TABLE_NAME_CACHE: dict[str, str] = {}


def _is_supabase_missing_table_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return (
        "pgrst205" in message
        or "could not find the table" in message
        or "does not exist" in message
    )


def _supabase_table_candidates(table_name: str) -> list[str]:
    raw = str(table_name or "").strip()
    if not raw:
        return []
    candidates = [raw]
    lowered = raw.lower()
    if lowered not in candidates:
        candidates.append(lowered)
    return candidates


def resolve_supabase_table_name(client: Any, table_name: str) -> str:
    raw = str(table_name or "").strip()
    if not raw:
        return raw

    cache_key = raw.lower()
    cached = _SUPABASE_TABLE_NAME_CACHE.get(cache_key)
    if cached:
        return cached

    for candidate in _supabase_table_candidates(raw):
        try:
            client.table(candidate).select("*").limit(1).execute()
            _SUPABASE_TABLE_NAME_CACHE[cache_key] = candidate
            return candidate
        except Exception as exc:
            if _is_supabase_missing_table_error(exc):
                continue
            return raw

    return raw


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
    resolved_table_name = resolve_supabase_table_name(client, table_name)
    query = client.table(resolved_table_name).select(columns)
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
    cache_key = str(table_name or "").strip().lower()
    for candidate in _supabase_table_candidates(table_name):
        try:
            client.table(candidate).select("*").limit(1).execute()
            if cache_key:
                _SUPABASE_TABLE_NAME_CACHE[cache_key] = candidate
            return True
        except Exception as exc:
            if _is_supabase_missing_table_error(exc):
                continue
            return False
    return False


def is_supabase_auth_enabled(config_path: Path) -> bool:
    settings = load_supabase_settings(config_path)
    return bool(settings.get("enabled"))


def ensure_supabase_users_table(config_path: Path) -> bool:
    client = get_supabase_admin_client(config_path) or get_supabase_client(config_path)
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
        credits BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT},
        credits_balance BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT},
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
        last_login_at TEXT,
        is_admin BOOLEAN NOT NULL DEFAULT FALSE,
        is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
        blocked_at TEXT,
        blocked_reason TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT
    );

    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'entrepreneur';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS contact_name TEXT NOT NULL DEFAULT '';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS niche TEXT NOT NULL DEFAULT 'B2B Service Provider';
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS token TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS credits BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS credits_balance BIGINT NOT NULL DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
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
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS last_login_at TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS blocked_at TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS blocked_reason TEXT;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS created_at TEXT NOT NULL DEFAULT NOW()::text;
    ALTER TABLE public.users ADD COLUMN IF NOT EXISTS updated_at TEXT;
    ALTER TABLE public.users ALTER COLUMN credits_balance SET DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ALTER COLUMN credits SET DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ALTER COLUMN monthly_quota SET DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ALTER COLUMN credits_limit SET DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ALTER COLUMN monthly_limit SET DEFAULT {DEFAULT_MONTHLY_CREDIT_LIMIT};
    ALTER TABLE public.users ALTER COLUMN plan_key SET DEFAULT 'free';

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
    SET credits = COALESCE(credits_balance, credits, {DEFAULT_MONTHLY_CREDIT_LIMIT})
    WHERE credits IS NULL OR credits != COALESCE(credits_balance, credits, {DEFAULT_MONTHLY_CREDIT_LIMIT});

        UPDATE public.users
        SET credits_balance = COALESCE(NULLIF(credits_balance, 0), NULLIF(monthly_quota, 0), NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT})
        WHERE COALESCE(NULLIF(plan_key, ''), 'free') = 'free'
            AND COALESCE(subscription_active, FALSE) = FALSE
            AND COALESCE(credits_balance, 0) <= 0;

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
    SET is_admin = TRUE
    WHERE LOWER(COALESCE(email, '')) IN ({','.join([f"'{email}'" for email in sorted(DEFAULT_ADMIN_EMAILS)])});

    UPDATE public.users
    SET plan_key = CASE
        WHEN LOWER(TRIM(COALESCE(plan_key, ''))) IN ('free', 'hustler', 'growth', 'scale', 'empire', 'pro')
            THEN LOWER(TRIM(COALESCE(plan_key, '')))
        WHEN COALESCE(subscription_active, FALSE) = TRUE THEN 'pro'
        ELSE 'free'
    END;

    CREATE INDEX IF NOT EXISTS idx_users_token
        ON public.users(token);
    CREATE INDEX IF NOT EXISTS idx_users_id_lookup
        ON public.users(id);
    CREATE INDEX IF NOT EXISTS idx_users_reset_token
        ON public.users(reset_token);

    DO $$
    BEGIN
        IF to_regclass('public.subscriptions') IS NOT NULL THEN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id ON public.subscriptions(user_id)';
        END IF;
    END
    $$;

    CREATE TABLE IF NOT EXISTS public.credit_logs (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        amount INTEGER NOT NULL,
        action_type TEXT NOT NULL,
        metadata JSONB,
        created_at TEXT NOT NULL DEFAULT NOW()::text
    );
    CREATE INDEX IF NOT EXISTS idx_credit_logs_user_created ON public.credit_logs(user_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_credit_logs_action_created ON public.credit_logs(action_type, created_at DESC);
    """

    try:
        client.rpc("exec_sql", {"sql": users_sql}).execute()
    except Exception as exc:
        exc_text = str(exc)
        if "PGRST202" in exc_text or "Could not find the function public.exec_sql" in exc_text:
            logging.info("Supabase users table auto-create skipped; exec_sql RPC unavailable. Proceeding without auto-create.")
        else:
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


def fetch_blacklist_sets_supabase(config_path: Path, user_id: Optional[str] = None) -> tuple[set[str], set[str]]:
    client = get_supabase_client(config_path)
    if client is None:
        return set(), set()

    emails: set[str] = set()
    domains: set[str] = set()
    try:
        rows = supabase_select_rows(
            client,
            "lead_blacklist",
            columns="kind,value",
            filters={"user_id": str(user_id)} if user_id else None,
        )
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


def sync_blacklisted_leads_supabase(config_path: Path, user_id: Optional[str] = None) -> int:
    client = get_supabase_client(config_path)
    if client is None:
        return 0

    emails, domains = fetch_blacklist_sets_supabase(config_path, user_id=user_id)
    if not emails and not domains:
        return 0

    rows = supabase_select_rows(
        client,
        "leads",
        columns="id,email,website_url,status,user_id",
        filters={"user_id": str(user_id)} if user_id else None,
    )
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

    lead_rows = client.table("leads").select("id,business_name,email,website_url,user_id").eq("id", lead_id).limit(1).execute().data or []
    if not lead_rows:
        raise HTTPException(status_code=404, detail="Lead not found")

    row = lead_rows[0]
    email_value = str(row.get("email") or "").strip().lower()
    domain_values = {
        normalize_blacklist_domain(row.get("email")),
        normalize_blacklist_domain(row.get("website_url")),
    }
    domain_values = {value for value in domain_values if value}

    lead_user_id = str(row.get("user_id") or "legacy").strip() or "legacy"
    existing_rows = client.table("lead_blacklist").select("kind,value").eq("user_id", lead_user_id).execute().data or []
    existing = {(str(item.get("kind") or "").lower(), str(item.get("value") or "").lower()) for item in existing_rows}

    to_insert: list[dict] = []
    if email_value and ("email", email_value) not in existing:
        to_insert.append({"user_id": lead_user_id, "kind": "email", "value": email_value, "reason": reason, "created_at": utc_now_iso()})
    for domain_value in sorted(domain_values):
        if ("domain", domain_value) in existing:
            continue
        to_insert.append({"user_id": lead_user_id, "kind": "domain", "value": domain_value, "reason": reason, "created_at": utc_now_iso()})

    if to_insert:
        client.table("lead_blacklist").insert(to_insert).execute()

    affected = sync_blacklisted_leads_supabase(config_path, user_id=lead_user_id)
    _invalidate_leads_cache()
    return {
        "status": "blacklisted",
        "lead_id": lead_id,
        "business_name": row.get("business_name"),
        "blacklisted_email": bool(email_value),
        "blacklisted_domains": sorted(domain_values),
        "affected_leads": affected,
    }


def add_blacklist_entry_supabase(config_path: Path, *, user_id: str, kind: str, value: str, reason: str = "Manual blacklist") -> dict:
    normalized_kind, normalized_value = normalize_blacklist_entry(kind, value)
    clean_reason = str(reason or "Manual blacklist").strip() or "Manual blacklist"
    normalized_user_id = str(user_id or "legacy").strip() or "legacy"
    client = get_supabase_client(config_path)
    if client is None:
        raise HTTPException(status_code=500, detail="Supabase not configured")

    existing_rows = (
        client.table("lead_blacklist")
        .select("kind,value,reason,created_at")
        .eq("user_id", normalized_user_id)
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
                "user_id": normalized_user_id,
                "kind": normalized_kind,
                "value": normalized_value,
                "reason": clean_reason,
                "created_at": utc_now_iso(),
            }
        ).execute()
        existing_rows = (
            client.table("lead_blacklist")
            .select("kind,value,reason,created_at")
            .eq("user_id", normalized_user_id)
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
    affected = sync_blacklisted_leads_supabase(config_path, user_id=normalized_user_id)
    _invalidate_leads_cache()
    return {
        "status": "blacklisted",
        "kind": normalized_kind,
        "value": normalized_value,
        "reason": row.get("reason") or clean_reason,
        "created_at": row.get("created_at") or utc_now_iso(),
        "affected_leads": affected,
    }


def restore_released_blacklisted_leads_supabase(config_path: Path, removed_entries: list[tuple[str, str]], user_id: Optional[str] = None) -> int:
    normalized_entries = [normalize_blacklist_entry(kind, value) for kind, value in removed_entries if str(value or "").strip()]
    if not normalized_entries:
        return 0

    client = get_supabase_client(config_path)
    if client is None:
        return 0

    emails, domains = fetch_blacklist_sets_supabase(config_path, user_id=user_id)
    rows = supabase_select_rows(
        client,
        "leads",
        columns="id,email,website_url,status,user_id",
        filters={"user_id": str(user_id)} if user_id else None,
    )
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


def remove_blacklist_entry_supabase(config_path: Path, *, user_id: str, kind: str, value: str) -> dict:
    normalized_kind, normalized_value = normalize_blacklist_entry(kind, value)
    normalized_user_id = str(user_id or "legacy").strip() or "legacy"
    client = get_supabase_client(config_path)
    if client is None:
        raise HTTPException(status_code=500, detail="Supabase not configured")

    existing_rows = (
        client.table("lead_blacklist")
        .select("id")
        .eq("user_id", normalized_user_id)
        .eq("kind", normalized_kind)
        .eq("value", normalized_value)
        .execute()
        .data
        or []
    )
    if existing_rows:
        client.table("lead_blacklist").delete().eq("user_id", normalized_user_id).eq("kind", normalized_kind).eq("value", normalized_value).execute()
    deleted_count = len(existing_rows)

    restored = restore_released_blacklisted_leads_supabase(config_path, [(normalized_kind, normalized_value)], user_id=normalized_user_id)
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

    lead_rows = client.table("leads").select("id,business_name,email,website_url,user_id").eq("id", lead_id).limit(1).execute().data or []
    if not lead_rows:
        raise HTTPException(status_code=404, detail="Lead not found")

    row = lead_rows[0]
    lead_user_id = str(row.get("user_id") or "legacy").strip() or "legacy"
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
            .eq("user_id", lead_user_id)
            .eq("kind", entry_kind)
            .eq("value", entry_value)
            .execute()
            .data
            or []
        )
        if existing_rows:
            client.table("lead_blacklist").delete().eq("user_id", lead_user_id).eq("kind", entry_kind).eq("value", entry_value).execute()
            deleted_count += len(existing_rows)

    restored = restore_released_blacklisted_leads_supabase(config_path, removed_entries, user_id=lead_user_id)
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
    try:
        rows = supabase_select_rows(
            client,
            "leads",
            columns="status,email,next_mail_at",
            filters={"user_id": user_id} if user_id else None,
        )
    except Exception as exc:
        logging.warning("Supabase queued mail fallback without next_mail_at: %s", exc)
        try:
            rows = supabase_select_rows(
                client,
                "leads",
                columns="status,email",
                filters={"user_id": user_id} if user_id else None,
            )
        except Exception as nested_exc:
            logging.warning("Supabase queued mail count unavailable: %s", nested_exc)
            return 0
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
    try:
        revenue_log = supabase_select_rows(
            client,
            "revenue_log",
            columns="amount,is_recurring",
            filters=uid_filter,
        )
    except Exception as exc:
        logging.warning("Supabase stats revenue_log fallback to empty set: %s", exc)
        revenue_log = []

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


def get_table_rows_snapshot(db_path: Path, table_name: str) -> list[dict]:
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    return [dict(row) for row in rows]


def get_table_columns_snapshot(db_path: Path, table_name: str) -> list[str]:
    with pgdb.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def _normalize_leads_bigint_flags(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for key in (
        "google_claimed",
        "has_pixel",
        "insecure_site",
        "is_ads_client",
        "is_website_client",
        "follow_up_count",
        "open_count",
        "campaign_step",
    ):
        value = normalized.get(key)
        normalized[key] = int(value) if isinstance(value, bool) else value
    return normalized


def replace_table_rows_snapshot(db_path: Path, table_name: str, rows: list[dict], columns: list[str]) -> None:
    with pgdb.connect(db_path) as conn:
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
        local_columns = get_table_columns_snapshot(db_path, table_name)
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
        replace_table_rows_snapshot(db_path, table_name, normalized_rows, write_columns)
        return len(remote_rows), None
    except Exception as exc:
        return 0, str(exc)


def sync_table_to_supabase(db_path: Path, table_name: str, config_path: Path) -> tuple[int, Optional[str]]:
    import re as _re
    client = get_supabase_client(config_path)
    if client is None:
        return 0, "Supabase not configured"

    try:
        rows = get_table_rows_snapshot(db_path, table_name)
        if not rows:
            return 0, None

        excluded_cols: set[str] = set()
        last_error: Optional[str] = None
        for _attempt in range(15):
            try:
                filtered = [{k: v for k, v in row.items() if k not in excluded_cols} for row in rows]
                if table_name == "leads":
                    filtered = [_normalize_leads_bigint_flags(row) for row in filtered]
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


def load_openai_client(config_path: Path) -> tuple[Optional[object], str]:
    client, model_name, provider = create_sync_ai_client(config_path=config_path, model_name_override=DEFAULT_AI_MODEL)
    if client is None:
        return None, model_name
    if provider == "azure":
        resolved = resolve_ai_provider_settings(config_path=config_path, model_name_override=DEFAULT_AI_MODEL)
        resolved_api_version = resolved.api_version if resolved is not None else "unknown"
        logging.info("Azure OpenAI client configured for deployment %s with API version %s.", model_name, resolved_api_version)
    return client, model_name


def has_any_ai_api_key(config_path: Path) -> bool:
    anthropic_key = str(os.environ.get("ANTHROPIC_API_KEY", "") or "").strip()

    if has_any_ai_credentials(config_path):
        return True
    if anthropic_key and anthropic_key != "YOUR_ANTHROPIC_API_KEY":
        return True

    try:
        with config_path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
        if isinstance(cfg, dict):
            anthropic_cfg = cfg.get("anthropic", {}) if isinstance(cfg.get("anthropic"), dict) else {}
            cfg_anthropic = str(anthropic_cfg.get("api_key", "") or "").strip()
            if cfg_anthropic and cfg_anthropic != "YOUR_ANTHROPIC_API_KEY":
                return True
    except Exception:
        pass

    return False


def extract_keyword_performance(db_path: Path, limit: int = 8, user_id: Optional[str] = None) -> list[dict]:
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
                logging.warning("Supabase keyword performance fallback to legacy store: %s", exc)

    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT
                TRIM(search_keyword) AS keyword,
                COUNT(*) AS total_leads,
                SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END) AS sent_count,
                SUM(CASE WHEN LOWER(COALESCE(status, '')) IN ('interested', 'meeting set') THEN 1 ELSE 0 END) AS replies
            FROM leads
            WHERE search_keyword IS NOT NULL AND TRIM(search_keyword) != ''
              AND (? IS NULL OR user_id = ?)
            GROUP BY TRIM(search_keyword)
            HAVING SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END) > 0
            ORDER BY
                (CAST(SUM(CASE WHEN LOWER(COALESCE(status, '')) IN ('interested', 'meeting set') THEN 1 ELSE 0 END) AS REAL)
                 / SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END)) DESC,
                SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END) DESC,
                COUNT(*) DESC
            LIMIT ?
            """,
            (user_id, user_id, limit),
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
                "keyword": "Heat pumps in Ljubljana, SI",
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


def build_market_intelligence_mock_response(
    country_code: str = "US",
    *,
    maintenance: bool = False,
    maintenance_message: str = "",
) -> dict[str, Any]:
    selected_country = normalize_country_value(country_code)
    country_labels = {
        "US": "United States",
        "DE": "Germany",
        "AT": "Austria",
        "SI": "Slovenia",
    }
    selected_country_label = country_labels.get(selected_country, selected_country)
    recommendations = [
        {
            "keyword": "Roofing in Miami, FL",
            "location": "Miami, FL",
            "country_code": "US",
            "reason": "High growth detected in Miami roofing sector.",
            "expected_reply_rate": 6.9,
        },
        {
            "keyword": "HVAC Services in Las Vegas, NV",
            "location": "Las Vegas, NV",
            "country_code": "US",
            "reason": "Low competition in HVAC Nevada.",
            "expected_reply_rate": 6.3,
        },
        {
            "keyword": "Solar Installation in Austin, TX",
            "location": "Austin, TX",
            "country_code": "US",
            "reason": "Rising local demand and high-ticket project sizes sustain strong margin potential.",
            "expected_reply_rate": 5.8,
        },
    ]
    maintenance_text = str(maintenance_message or "").strip()
    return {
        "source": "mock",
        "generated_at": utc_now_iso(),
        "recommendations": recommendations,
        "top_pick": recommendations[0],
        "top_pick_index": 0,
        "performance_snapshot": [],
        "selected_country_code": selected_country,
        "selected_country_label": selected_country_label,
        "maintenance": bool(maintenance),
        "maintenance_message": maintenance_text,
    }


def get_niche_recommendation(
    db_path: Path,
    config_path: Path,
    country_code: str = "US",
    user_id: Optional[str] = None,
    search_context: Optional[str] = None,
) -> dict:
    ensure_system_tables(db_path)
    selected_country = normalize_country_value(country_code)
    country_labels = {
        "US": "United States",
        "DE": "Germany",
        "AT": "Austria",
        "SI": "Slovenia",
    }
    selected_country_label = country_labels.get(selected_country, selected_country)
    normalized_search_context = str(search_context or "").strip()
    performance = extract_keyword_performance(db_path, user_id=user_id)
    heuristic = heuristic_recommendations_from_performance(performance, selected_country)

    try:
        client, model_name = load_openai_client(config_path)
    except Exception as exc:
        logging.warning("Niche recommendation AI init failed, using heuristic fallback: %s", exc)
        client, model_name = None, DEFAULT_AI_MODEL
    if client is None:
        return {
            "source": "heuristic",
            "generated_at": utc_now_iso(),
            "recommendations": heuristic,
            "top_pick": heuristic[0],
            "performance_snapshot": performance,
            "selected_country_code": selected_country,
            "selected_country_label": selected_country_label,
            "search_context": normalized_search_context,
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
Based on today's date (April 2026), current economic trends, and the fact that the user sells Google Ads / SEO services, suggest the 3 most profitable niches
ONLY for the selected country: {selected_country_label} ({selected_country}).

You must take our keyword-level reply-rate history into account:
{perf_lines}

Examples that match the selected country:
{country_examples}

Return a JSON object with this exact structure:
{{
    "recommendations": [
        {{
            "keyword": "Roofers in Miami, FL",
            "location": "Miami, FL",
            "country_code": "US",
            "reason": "Short reason (seasonality / margin / demand)",
            "expected_reply_rate": 6.2
        }}
    ],
    "top_pick_index": 0
}}

Rules:
- recommendations must contain exactly 3 items.
- All 3 locations must be in country {selected_country}.
- country_code must always be '{selected_country}'.
- expected_reply_rate must be a realistic number between 1.0 and 15.0.
- If search_context is provided, prioritize niches close to this context and avoid generic defaults.

search_context: {normalized_search_context or 'none'}
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
            "search_context": normalized_search_context,
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
            "search_context": normalized_search_context,
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
                with pgdb.connect(db_path) as conn:
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
                            WHERE id = ? AND (? IS NULL OR user_id = ?)
                            """,
                            (now_iso, now_iso, lead_id, user_id, user_id),
                        )
                        queued_count += 1

                    conn.commit()
                return queued_count
            except Exception as exc:
                logging.warning("Supabase queue_high_score_enriched_leads fallback to legacy store: %s", exc)

    with pgdb.connect(db_path) as conn:
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
                logging.warning("Supabase get_scraped_lead_count fallback to legacy store: %s", exc)

    with pgdb.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status, '')) = 'scraped'"
        ).fetchone()
    return int(row[0] if row else 0)


def get_queued_mail_count(db_path: Path, user_id: Optional[str] = None) -> int:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return get_queued_mail_count_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)

    uid_clause = "AND user_id = ?" if user_id else ""
    uid_params = [user_id] if user_id else []
    now_iso = utc_now_iso()

    with pgdb.connect(db_path) as conn:
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
                    OR next_mail_at <= ?
                )
            """
            ,
            [*uid_params, now_iso],
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
                logging.warning("Supabase lead export fallback to legacy store: %s", exc)

    if source_rows is None:
        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
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
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
        row = conn.execute(
            "SELECT * FROM client_folders WHERE id = ? AND user_id = ? LIMIT 1",
            (cursor.lastrowid, str(user_id or "legacy")),
        ).fetchone()
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
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        folders = [dict(row) for row in conn.execute(
            """
            SELECT id, user_id, name, color, notes, created_at, updated_at
            FROM client_folders
            WHERE user_id = ?
            ORDER BY updated_at DESC, id DESC
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


def _normalize_saved_segment_row(row: pgdb.Row | dict | None) -> dict[str, Any]:
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
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        existing = conn.execute(
            "SELECT id FROM saved_segments WHERE user_id = ? AND LOWER(name) = LOWER(?) LIMIT 1",
            (str(user_id or "legacy"), name),
        ).fetchone()
        if existing is not None:
            conn.execute(
                "UPDATE saved_segments SET filters_json = ?, updated_at = ? WHERE id = ? AND user_id = ?",
                (filters_json, now_iso, int(existing["id"]), str(user_id or "legacy")),
            )
            row = conn.execute(
                "SELECT * FROM saved_segments WHERE id = ? AND user_id = ? LIMIT 1",
                (int(existing["id"]), str(user_id or "legacy")),
            ).fetchone()
        else:
            cursor = conn.execute(
                """
                INSERT INTO saved_segments (user_id, name, filters_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(user_id or "legacy"), name, filters_json, now_iso, now_iso),
            )
            row = conn.execute(
                "SELECT * FROM saved_segments WHERE id = ? AND user_id = ? LIMIT 1",
                (cursor.lastrowid, str(user_id or "legacy")),
            ).fetchone()
        conn.commit()
    return _normalize_saved_segment_row(row)


def list_saved_segments(db_path: Path, user_id: str) -> list[dict[str, Any]]:
    ensure_client_success_tables(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT id, user_id, name, filters_json, created_at, updated_at
            FROM saved_segments
            WHERE user_id = ?
            ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
            """,
            (user_id,),
        ).fetchall()
    return [_normalize_saved_segment_row(row) for row in rows]


def delete_saved_segment(db_path: Path, user_id: str, segment_id: int) -> dict[str, Any]:
    ensure_client_success_tables(db_path)
    with pgdb.connect(db_path) as conn:
        cursor = conn.execute("DELETE FROM saved_segments WHERE id = ? AND user_id = ?", (int(segment_id), user_id))
        conn.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Saved segment not found")
    return {"status": "deleted", "id": int(segment_id)}


def assign_lead_to_client_folder(db_path: Path, user_id: str, lead_id: int, client_folder_id: Optional[int]) -> dict[str, Any]:
    ensure_client_success_tables(db_path)
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        lead_row = conn.execute(
            "SELECT id, user_id, business_name FROM leads WHERE id = ? AND user_id = ? LIMIT 1",
            (lead_id, user_id),
        ).fetchone()
        if lead_row is None:
            raise HTTPException(status_code=404, detail="Lead not found")

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

        update_cursor = conn.execute(
            "UPDATE leads SET client_folder_id = ?, status_updated_at = COALESCE(status_updated_at, ?) WHERE id = ? AND user_id = ?",
            (normalized_folder_id, utc_now_iso(), lead_id, user_id),
        )
        conn.commit()
        if int(update_cursor.rowcount or 0) == 0:
            raise HTTPException(status_code=404, detail="Lead not found")

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


def _get_developer_webhook_settings(config_path: Path) -> tuple[str, float]:
    cfg = _read_json_config(config_path)
    url = str(cfg.get("developer_webhook_url", "") or "").strip()
    raw_threshold = cfg.get("developer_score_drop_threshold", 6.0)
    try:
        threshold = float(raw_threshold)
    except (TypeError, ValueError):
        threshold = 6.0
    threshold = max(0.0, min(10.0, threshold))
    return url, threshold


def _dispatch_developer_webhook_event(
    *,
    event_type: str,
    user_id: str,
    lead_id: Optional[int],
    payload: dict[str, Any],
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> None:
    url, _ = _get_developer_webhook_settings(config_path)
    if not url:
        return
    body = {
        "event": str(event_type or "unknown").strip() or "unknown",
        "timestamp": utc_now_iso(),
        "user_id": str(user_id or "legacy"),
        "lead_id": int(lead_id) if lead_id is not None else None,
        "payload": payload,
    }
    try:
        deliver_export_webhook(url, body)
    except Exception as exc:
        logging.warning("Developer webhook delivery failed for event=%s lead_id=%s: %s", event_type, lead_id, exc)


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
    uid = str(user_id or "").strip()
    if not uid:
        return ""

    # Try Supabase first (both primary-mode and auth-enabled deployments).
    if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
        sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if sb_client is not None:
            try:
                query = sb_client.table("users").select("email")
                try:
                    query = query.eq("id", int(uid))
                except (ValueError, TypeError):
                    query = query.eq("id", uid)
                rows = list(getattr(query.limit(1).execute(), "data", None) or [])
                if rows:
                    email = str(rows[0].get("email") or "").strip()
                    if email:
                        return email
            except Exception as exc:
                logging.warning("_load_user_email_for_reports Supabase lookup failed: %s", exc)

    # Fall back to local SQLite store.
    try:
        ensure_users_table(db_path)
        with pgdb.connect(db_path) as conn:
            row = conn.execute("SELECT email FROM users WHERE id = ? LIMIT 1", (uid,)).fetchone()
        return str(row[0] or "").strip() if row else ""
    except Exception as exc:
        logging.warning("_load_user_email_for_reports SQLite fallback failed: %s", exc)
        return ""


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
    subject = f"Sniped {'Weekly' if is_weekly else 'Monthly'} Summary â€” {label}"
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
        f"<li style=\"margin:0 0 8px;\"><strong>{html_escape(str(folder.get('name') or 'Client'))}</strong> Â· {int(folder.get('lead_count') or 0)} leads Â· {int(folder.get('won_paid_count') or 0)} won</li>"
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
            <p style=\"margin:0;font-size:14px;color:#e5e7eb;\"><strong>Pipeline:</strong> Scraped {int(pipeline.get('scraped') or 0)} Â· Contacted {int(pipeline.get('contacted') or 0)} Â· Replied {int(pipeline.get('replied') or 0)} Â· Won {int(pipeline.get('won_paid') or 0)}</p>
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
    with pgdb.connect(db_path) as conn:
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


def get_task_executor(task_type: str) -> Callable[[FastAPI, dict], Any]:
    if task_type == "scrape":
        return execute_scrape_task
    if task_type == "enrich":
        return execute_enrich_task
    if task_type == "mailer":
        return execute_mailer_task
    raise ValueError(f"Unsupported task type: {task_type}")


def launch_detached_task(executor: Callable[[FastAPI, dict], Any], app: FastAPI, payload_data: dict) -> None:
    task_id = payload_data.get("task_id")
    task_name = getattr(executor, "__name__", executor.__class__.__name__)

    def _run() -> None:
        registry: dict[int, Thread] = getattr(app.state, "active_task_threads", {})
        try:
            if isinstance(task_id, int):
                registry[task_id] = thread
            try:
                outcome = executor(app, payload_data)
                if inspect.isawaitable(outcome):
                    loop = asyncio.new_event_loop()
                    try:
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(outcome)
                    finally:
                        try:
                            loop.close()
                        except Exception:
                            pass
                        asyncio.set_event_loop(None)
            except Exception as exc:
                if _is_db_capacity_error(exc):
                    logging.warning("Detached task %s skipped because the database is saturated.", task_name)
                else:
                    logging.exception("Detached task %s failed", task_name)
        finally:
            if isinstance(task_id, int):
                registry.pop(task_id, None)

    thread = Thread(target=_run, daemon=True)
    thread.start()


def _task_reference_time(task: dict) -> Optional[datetime]:
    """Return the most recent known timestamp for a task.

    Checks (in order of freshness): heartbeat_ts from result payload,
    then started_at, then created_at.  Using the most recent value
    prevents orphan-resets for tasks that are still producing heartbeats
    but whose thread registry entry is missing (e.g. worker process).
    """
    candidates: list[Optional[datetime]] = []
    result = task.get("result")
    if isinstance(result, dict):
        candidates.append(parse_iso_datetime(result.get("heartbeat_ts")))
    candidates.append(parse_iso_datetime(task.get("started_at")))
    candidates.append(parse_iso_datetime(task.get("created_at")))
    valid = [t for t in candidates if t is not None]
    return max(valid) if valid else None


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
            logging.info(
                "Task enqueue skipped because task is already active | type=%s user_id=%s task_id=%s",
                task_type,
                user_id,
                running_task.get("id"),
            )
            return {"status": "running", "task_id": running_task.get("id")}

        normalized_user_id = str(user_id or "").strip()
        normalized_request_user_id = str(request_payload.get("user_id") or "").strip() if isinstance(request_payload, dict) else ""
        if normalized_request_user_id and normalized_request_user_id != normalized_user_id:
            logging.warning(
                "Task enqueue payload user_id mismatch ignored | type=%s auth_user_id=%s payload_user_id=%s",
                task_type,
                normalized_user_id,
                normalized_request_user_id,
            )

        safe_payload = dict(request_payload)
        safe_payload["user_id"] = normalized_user_id
        safe_payload["owner_id"] = normalized_user_id
        safe_payload["request_user_id"] = normalized_user_id

        task_id = create_task_record(db_path, normalized_user_id, task_type, "queued", safe_payload, source=source)
        logging.info(
            "Task queued | type=%s user_id=%s task_id=%s source=%s",
            task_type,
            normalized_user_id,
            task_id,
            source,
        )

    payload_data = dict(safe_payload)
    payload_data["task_id"] = task_id
    payload_data["task_type"] = task_type
    payload_data["user_id"] = normalized_user_id

    if _should_use_external_worker(task_type):
        logging.info(
            "Delegating task to external worker | type=%s user_id=%s task_id=%s",
            task_type,
            normalized_user_id,
            task_id,
        )
        return {
            "status": "started",
            "task_id": task_id,
            "job_status": "queued",
            "execution_mode": "worker",
        }
    if _is_postgres_task_store_enabled():
        logging.info("No live worker heartbeat detected; using in-process task execution.")
    if task_type == "scrape" and FORCE_IN_PROCESS_SCRAPE:
        logging.info("Scrape task configured for in-process execution | task_id=%s", task_id)

    executor = get_task_executor(task_type)
    if background_tasks is not None:
        # FastAPI-managed background kickoff (returns response immediately).
        background_tasks.add_task(launch_detached_task, executor, app, payload_data)
    else:
        launch_detached_task(executor, app, payload_data)

    return {"status": "started", "task_id": task_id, "job_status": "processing", "execution_mode": "in-process"}


def get_dashboard_stats(db_path: Path, user_id: Optional[str] = None) -> dict:
    if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
        return get_dashboard_stats_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)

    ensure_system_tables(db_path)
    uid_clause = "AND user_id = ?" if user_id else ""
    uid_params = [user_id] if user_id else []
    now_utc = datetime.now(timezone.utc)
    month_prefix = now_utc.strftime("%Y-%m")
    week_cutoff = (now_utc - timedelta(days=7)).isoformat()

    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
    with pgdb.connect(db_path) as conn:
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
                .select("id,email,is_admin,credits_balance,monthly_quota,monthly_limit,credits_limit,topup_credits_balance")
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
        snapshot = {
            "user_id": target_user_id,
            "credits_balance": int(row.get("credits_balance") or 0),
            "credits_limit": max(1, int(row.get("monthly_quota") or row.get("monthly_limit") or row.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT)),
            "topup_credits_balance": max(0, int(row.get("topup_credits_balance") or 0)),
            "is_admin": bool(row.get("is_admin") or False) or _is_admin_email(row.get("email")),
            "email": str(row.get("email") or "").strip().lower(),
        }
        return _apply_admin_billing_override(snapshot, email=row.get("email"))

    auth_db_path = db_path or DEFAULT_DB_PATH
    ensure_users_table(auth_db_path)
    with pgdb.connect(auth_db_path) as conn:
        conn.row_factory = pgdb.Row
        row = conn.execute(
            """
            SELECT
                id,
                COALESCE(email, '') AS email,
                COALESCE(is_admin, FALSE) AS is_admin,
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

    snapshot = {
        "user_id": target_user_id,
        "credits_balance": int(row["credits_balance"] or 0),
        "credits_limit": max(1, int(row["credits_limit"] or DEFAULT_MONTHLY_CREDIT_LIMIT)),
        "topup_credits_balance": max(0, int(row["topup_credits_balance"] or 0)),
        "is_admin": bool(row["is_admin"] or False) or _is_admin_email(row["email"]),
        "email": str(row["email"] or "").strip().lower(),
    }
    return _apply_admin_billing_override(snapshot, email=row["email"])


def _append_credit_log(
    user_id: str,
    amount: int,
    action_type: str,
    metadata: Optional[dict[str, Any]] = None,
    db_path: Optional[Path] = None,
) -> None:
    target_user_id = str(user_id or "").strip()
    if not target_user_id:
        return
    log_amount = int(amount or 0)
    action = str(action_type or "usage").strip().lower() or "usage"
    metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
    now_iso = utc_now_iso()

    if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
        sb_client = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
        if sb_client is None:
            return
        try:
            sb_client.table("credit_logs").insert(
                {
                    "user_id": target_user_id,
                    "amount": log_amount,
                    "action_type": action,
                    "metadata": metadata or {},
                    "created_at": now_iso,
                }
            ).execute()
        except Exception as exc:
            logging.warning("Failed writing Supabase credit log: %s", exc)
        return

    target_db_path = db_path or DEFAULT_DB_PATH
    ensure_credit_logs_table(target_db_path)
    with pgdb.connect(target_db_path) as conn:
        conn.execute(
            """
            INSERT INTO credit_logs (user_id, amount, action_type, metadata, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (target_user_id, log_amount, action, metadata_json, now_iso),
        )
        conn.commit()


def deduct_credits_on_success(
    user_id: str,
    credits_to_deduct: int = 1,
    db_path: Optional[Path] = None,
    *,
    action_type: str = "usage",
    metadata: Optional[dict[str, Any]] = None,
) -> dict:
    target_user_id = str(user_id or "").strip()
    if not target_user_id:
        raise HTTPException(status_code=401, detail="Missing authenticated user.")

    amount = max(0, int(credits_to_deduct or 0))
    admin_snapshot = _load_user_credit_snapshot(target_user_id, db_path=db_path)
    if bool(admin_snapshot.get("is_admin") or False):
        return {
            "user_id": target_user_id,
            "credits_charged": 0,
            "credits_balance": int(admin_snapshot.get("credits_balance") or ADMIN_UNLIMITED_CREDITS),
            "credits_limit": int(admin_snapshot.get("credits_limit") or PLAN_MONTHLY_QUOTAS.get(ADMIN_OVERRIDE_PLAN_KEY, DEFAULT_MONTHLY_CREDIT_LIMIT)),
        }
    if amount <= 0:
        return {
            "user_id": target_user_id,
            "credits_charged": 0,
            "credits_balance": int(admin_snapshot.get("credits_balance") or 0),
            "credits_limit": int(admin_snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
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
                    "credits": next_balance,
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

        result = {
            "user_id": target_user_id,
            "credits_charged": amount,
            "credits_balance": verified_balance,
            "credits_limit": verified_limit,
        }
        _append_credit_log(target_user_id, -amount, action_type, {
            "credits_after": verified_balance,
            **(metadata or {}),
        }, db_path=db_path)
        return result

    auth_db_path = db_path or DEFAULT_DB_PATH
    ensure_users_table(auth_db_path)
    with pgdb.connect(auth_db_path) as conn:
        conn.row_factory = pgdb.Row
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
            SET credits = ?,
                credits_balance = ?,
                topup_credits_balance = ?,
                updated_at = ?
            WHERE id = ? AND COALESCE(credits_balance, 0) >= ? AND COALESCE(topup_credits_balance, 0) = ?
            """,
            (next_balance, next_balance, next_topup_balance, now_iso, target_user_id, amount, topup_balance),
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
    result = {
        "user_id": target_user_id,
        "credits_charged": amount,
        "credits_balance": next_balance,
        "credits_limit": credits_limit,
    }
    _append_credit_log(target_user_id, -amount, action_type, {
        "credits_after": next_balance,
        **(metadata or {}),
    }, db_path=db_path)
    return result


AI_CREDIT_COSTS: dict[str, int] = {
    "recommend_niche": 0,
    "lead_search": 1,
    "enrich": ENRICH_CREDIT_COST_PER_LEAD,
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
    # Keep scrape tasks isolated: open and close a dedicated DB connection per task start.
    with pgdb.connect(db_path) as _task_conn:
        _task_conn.execute("SELECT 1")
    ensure_scrape_tables(db_path)
    task_id = int(payload_data["task_id"])
    task_user_id = str(payload_data.get("user_id") or "").strip()
    owner_user_id = str(payload_data.get("owner_id") or payload_data.get("request_user_id") or "").strip()
    if owner_user_id and task_user_id and owner_user_id != task_user_id:
        logging.warning(
            "[scrape-task:%s] user_id mismatch in payload: task user_id=%s owner_id=%s; forcing owner_id",
            task_id,
            task_user_id,
            owner_user_id,
        )
        task_user_id = owner_user_id
    elif owner_user_id and not task_user_id:
        task_user_id = owner_user_id
    if not task_user_id:
        raise RuntimeError("Missing user_id in scrape task payload. Refusing to write unscoped leads.")
    keyword = str(payload_data.get("keyword", "") or "").strip()
    logging.info(
        "[scrape-task:%s] Starting scrape task | keyword=%r | results=%s | country=%s",
        task_id,
        keyword,
        int(payload_data.get("results", 25)),
        country_value,
    )

    default_profile = f"{DEFAULT_PROFILE_DIR}_{country_value.lower()}"
    user_data_dir = (
        Path(payload_data["user_data_dir"]).expanduser().resolve()
        if payload_data.get("user_data_dir")
        else Path(default_profile)
    )
    requested_total = int(payload_data.get("results", 25))
    progress_state: dict[str, Any] = {
        "phase": "processing",
        "total_to_find": requested_total,
        "current_found": 0,
        "scanned_count": 0,
        "inserted": 0,
        "status_message": f"Scraped 0/{requested_total}",
    }
    heartbeat_stop = Event()

    def _safe_update_progress() -> None:
        try:
            update_task_progress(db_path, task_id, progress_state)
        except Exception:
            logging.debug("[scrape-task:%s] Progress update failed", task_id)

    def _scrape_heartbeat() -> None:
        while not heartbeat_stop.wait(10):
            logging.info(
                "STILL_ALIVE scrape-task:%s keyword=%r user_id=%s",
                task_id,
                keyword,
                task_user_id,
            )
            try:
                progress_state["heartbeat_ts"] = datetime.now(timezone.utc).isoformat()
                if str(progress_state.get("status_message") or "").strip() == "":
                    progress_state["status_message"] = f"Scraped {int(progress_state.get('current_found') or 0)}/{requested_total}"
                _safe_update_progress()
            except Exception:
                logging.debug("[scrape-task:%s] Heartbeat progress update failed", task_id)

    heartbeat_thread = Thread(target=_scrape_heartbeat, daemon=True)
    heartbeat_thread.start()

    try:
        mark_task_running(db_path, task_id)
        logging.info("[scrape-task:%s] Marked task as running", task_id)
        _safe_update_progress()
        logging.info("[scrape-task:%s] Initial progress state saved", task_id)

        force_headless = str(os.environ.get("SCRAPE_FORCE_HEADLESS", "1") or "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        requested_headless = bool(payload_data.get("headless", True))
        if force_headless:
            requested_headless = True
            if not bool(payload_data.get("headless", True)):
                logging.info("[scrape-task:%s] Headless requested by server policy SCRAPE_FORCE_HEADLESS=1", task_id)
        else:
            logging.info("[scrape-task:%s] Headless policy disabled, using request value=%s", task_id, requested_headless)

        # Read proxy config â€” supports single proxy_url or a list proxy_urls
        try:
            with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as _cfg_fh:
                _scrape_cfg = json.load(_cfg_fh)
        except Exception:
            _scrape_cfg = {}
        _proxy_url = str(_scrape_cfg.get("proxy_url", "") or "").strip() or None
        _proxy_urls_raw = _scrape_cfg.get("proxy_urls") or []
        _proxy_urls: List[str] = [
            _normalize_proxy_url(p)
            for p in (
                _proxy_urls_raw
                if isinstance(_proxy_urls_raw, list)
                else str(_proxy_urls_raw).splitlines()
            )
            if _normalize_proxy_url(p)
        ]
        if not _proxy_urls and HARDCODED_PROXY_URLS:
            _proxy_urls = [_normalize_proxy_url(p) for p in HARDCODED_PROXY_URLS if _normalize_proxy_url(p)]
        if not _proxy_urls and not _proxy_url:
            _proxy_urls = _load_file_proxy_urls()
        if _proxy_url:
            _proxy_url = _normalize_proxy_url(_proxy_url) or None
        if _proxy_urls:
            _proxy_urls = random.sample(_proxy_urls, len(_proxy_urls))
            _proxy_url = None

        def _on_progress(current_found: int, total_to_find: int, scanned_count: int, _lead: Any) -> None:
            try:
                progress_state["phase"] = "processing"
                progress_state["current_found"] = int(current_found)
                progress_state["total_to_find"] = int(total_to_find or requested_total)
                progress_state["scanned_count"] = int(scanned_count)
                progress_state["status_message"] = (
                    f"Scraped {int(current_found)}/{int(total_to_find or requested_total)}"
                    f" (scanned {int(scanned_count)})"
                )
                # Keep heartbeat_ts fresh on every progress update so orphan
                # detection never fires while the scrape is actively running.
                progress_state["heartbeat_ts"] = datetime.now(timezone.utc).isoformat()
                _safe_update_progress()
                logging.info(
                    "[scrape-task:%s] Progress | found=%s | target=%s | scanned=%s",
                    task_id,
                    int(current_found),
                    int(total_to_find or requested_total),
                    int(scanned_count),
                )
            except Exception:
                logging.exception("[scrape-task:%s] Progress callback failed unexpectedly", task_id)

            # Save each discovered lead immediately by default so Lead Management updates live.
            progress_save_mode = str(os.environ.get("SCRAPE_PROGRESS_SAVE_MODE", "local") or "local").strip().lower()
            if _lead is not None and task_user_id and progress_save_mode in {"local", "sync"}:
                business_name_hint = str(getattr(_lead, "business_name", "") or "").strip() or "<unknown>"
                print(f"DEBUG: Attempting to save {business_name_hint} to DB...", flush=True)
                logging.info("[scrape-task:%s] Immediate save: %s", task_id, business_name_hint)
                try:
                    # Force single-row upsert so each discovered lead is committed immediately.
                    _immediate_saved = upsert_lead(_lead, db_path=str(db_path), user_id=task_user_id)
                    if _immediate_saved:
                        progress_state["inserted"] = progress_state.get("inserted", 0) + 1
                        _safe_update_progress()
                        if progress_save_mode == "sync" and is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
                            try:
                                maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
                            except Exception as _sync_exc:
                                logging.warning("[scrape-task:%s] Immediate Supabase sync failed: %s", task_id, _sync_exc)
                except Exception as _imm_exc:
                    logging.exception(
                        "[scrape-task:%s] Immediate lead save failed | user_id=%s | business=%s | error=%s",
                        task_id,
                        task_user_id,
                        business_name_hint,
                        _imm_exc,
                    )

        scrape_runtime_limit = max(60, int(os.environ.get("SCRAPE_MAX_RUNTIME_SECONDS", "300") or "300"))
        scrape_stall_limit = max(10, int(os.environ.get("SCRAPE_STALL_TIMEOUT_SECONDS", "45") or "45"))

        def _scrape_once(headless_value: bool):
            progress_state["status_message"] = "Launching browser..."
            _safe_update_progress()
            logging.info("[scrape-task:%s] Starting browser... headless=%s", task_id, bool(headless_value))
            logging.info("[scrape-task:%s] Navigating to Google Maps search...", task_id)
            logging.info(
                "[scrape-task:%s] Scrape limits | max_runtime_seconds=%s stall_timeout_seconds=%s",
                task_id,
                scrape_runtime_limit,
                scrape_stall_limit,
            )
            GoogleMapsScraper = _get_google_maps_scraper_class()
            with GoogleMapsScraper(
                headless=headless_value,
                country=country_value,
                user_data_dir=str(user_data_dir),
                proxy_url=_proxy_url,
                proxy_urls=_proxy_urls or None,
                speed_mode=bool(payload_data.get("speed_mode", False)),
            ) as scraper:
                progress_state["status_message"] = f"Browser launched. Searching for {keyword}..."
                _safe_update_progress()
                return scraper.scrape(
                    keyword=str(payload_data.get("keyword", "")),
                    max_results=int(payload_data.get("results", 25)),
                    progress_callback=_on_progress,
                    max_runtime_seconds=scrape_runtime_limit,
                    stall_timeout_seconds=scrape_stall_limit,
                )

        def _scrape_with_boot_timeout(headless_value: bool):
            timeout_raw = os.environ.get("SCRAPE_TASK_TIMEOUT_SECONDS") or os.environ.get("SCRAPE_BOOT_TIMEOUT_SECONDS")
            default_timeout_seconds = int(scrape_runtime_limit + max(90, scrape_stall_limit * 2))
            if str(timeout_raw or "").strip():
                boot_timeout_seconds = max(120, int(timeout_raw or "0"))
            else:
                boot_timeout_seconds = max(120, default_timeout_seconds)
            min_safe_timeout = int(scrape_runtime_limit + 30)
            if boot_timeout_seconds < min_safe_timeout:
                boot_timeout_seconds = min_safe_timeout
            done = Event()
            result_box: dict[str, Any] = {}
            error_box: dict[str, Exception] = {}

            def _target() -> None:
                try:
                    result_box["leads"] = _scrape_once(headless_value)
                except Exception as exc:
                    error_box["exc"] = exc
                finally:
                    done.set()

            worker = Thread(target=_target, daemon=True)
            worker.start()

            wait_started_at = datetime.now(timezone.utc)
            while True:
                if done.wait(timeout=5):
                    break

                elapsed_wait = int((datetime.now(timezone.utc) - wait_started_at).total_seconds())
                logging.info(
                    "[scrape-task:%s] Scrape worker watchdog | elapsed=%ss timeout=%ss found=%s scanned=%s",
                    task_id,
                    elapsed_wait,
                    int(boot_timeout_seconds),
                    progress_state.get("current_found", 0),
                    progress_state.get("scanned_count", 0),
                )
                if int(progress_state.get("current_found", 0) or 0) == 0 and int(progress_state.get("scanned_count", 0) or 0) == 0:
                    progress_state["status_message"] = f"Scraped 0/{requested_total} (scanned 0)"
                elif not str(progress_state.get("status_message") or "").startswith("Scraped "):
                    progress_state["status_message"] = (
                        f"Scraped {progress_state.get('current_found', 0)}"
                        f"/{progress_state.get('total_to_find', requested_total)}"
                        f" (scanned {progress_state.get('scanned_count', 0)})"
                    )
                _safe_update_progress()

                if elapsed_wait >= int(boot_timeout_seconds):
                    break

            if not done.is_set():
                raise TimeoutError(
                    f"Scrape task timeout after {boot_timeout_seconds}s while waiting for browser/Maps workflow."
                )

            if "exc" in error_box:
                raise error_box["exc"]

            return result_box.get("leads") or []

        try:
            leads = _scrape_with_boot_timeout(requested_headless)
            logging.info("[scrape-task:%s] Scrape thread returned leads=%s", task_id, len(leads))
        except Exception as scrape_exc:
            # Non-headless sessions can be interrupted by consent/ad popups or manual window close.
            msg = str(scrape_exc).lower()
            if (not requested_headless) and ("has been closed" in msg or "target page" in msg):
                logging.warning("Scrape interrupted in visible browser; retrying once in headless mode.")
                leads = _scrape_with_boot_timeout(True)
                logging.info("[scrape-task:%s] Fallback scrape thread returned leads=%s", task_id, len(leads))
            else:
                raise

        # Do not import Slovenian leads unless Slovenia was explicitly selected.
        if country_value != "SI":
            leads = [lead for lead in leads if not is_slovenia_address(getattr(lead, "address", None))]

        total_scraped_from_maps = len(leads)
        progress_state["status_message"] = f"Scraped {total_scraped_from_maps}/{requested_total} from Google Maps."
        _safe_update_progress()
        logging.info("[scrape-task:%s] Google Maps search finished | leads_found=%s", task_id, total_scraped_from_maps)

        deep_scan_mode = str(os.environ.get("SCRAPE_DEEP_SCAN_MODE", "off") or "off").strip().lower()
        if leads and deep_scan_mode == "inline":
            try:
                progress_state["phase"] = "deep_crawl"
                progress_state["deep_crawled"] = 0
                progress_state["deep_total"] = len([lead for lead in leads if str(getattr(lead, "website_url", "") or "").strip() not in {"", "None", "none"}])
                progress_state["status_message"] = "Deep enrichment crawl started..."
                _safe_update_progress()

                deep_crawl_concurrency = min(10, max(1, int(os.environ.get("SCRAPE_DEEP_CRAWL_CONCURRENCY", "8") or "8")))
                deep_crawl_timeout = max(4, int(os.environ.get("SCRAPE_DEEP_CRAWL_TIMEOUT_SECONDS", "12") or "12"))

                def _on_deep_crawl_progress(done_count: int, total_count: int, lead_name: Optional[str]) -> None:
                    progress_state["phase"] = "deep_crawl"
                    progress_state["deep_crawled"] = int(done_count)
                    progress_state["deep_total"] = int(total_count)
                    progress_state["current_lead"] = str(lead_name or "")
                    progress_state["status_message"] = (
                        f"Deep crawl {int(done_count)}/{int(total_count)}"
                        + (f" — {str(lead_name or '').strip()}" if str(lead_name or '').strip() else "")
                    )
                    _safe_update_progress()

                logging.info(
                    "[scrape-task:%s] Starting deep crawl enrichment | leads=%s concurrency=%s timeout=%ss",
                    task_id,
                    len(leads),
                    deep_crawl_concurrency,
                    deep_crawl_timeout,
                )
                deep_crawl_result = asyncio.run(
                    enrich_leads_full_data(
                        leads,
                        concurrency=deep_crawl_concurrency,
                        timeout_seconds=deep_crawl_timeout,
                        progress_callback=_on_deep_crawl_progress,
                    )
                )
                progress_state["deep_crawled"] = int(deep_crawl_result.get("crawled") or 0)
                progress_state["deep_total"] = int(deep_crawl_result.get("eligible") or 0)
                _safe_update_progress()
                logging.info(
                    "[scrape-task:%s] Deep crawl enrichment completed | crawled=%s eligible=%s",
                    task_id,
                    int(deep_crawl_result.get("crawled") or 0),
                    int(deep_crawl_result.get("eligible") or 0),
                )
            except Exception as deep_crawl_exc:
                logging.warning("Deep crawl enrichment failed; continuing with Maps-only leads: %s", deep_crawl_exc)
        elif leads:
            logging.info(
                "[scrape-task:%s] Inline deep crawl skipped (SCRAPE_DEEP_SCAN_MODE=%s).",
                task_id,
                deep_scan_mode,
            )

        # When Supabase is the primary DB, deduplicate against Supabase too.
        # The local legacy store may be missing leads from previous sessions, so we
        # must check the remote before inserting.
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            try:
                sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
                if sb_client is not None:
                    existing_rows = supabase_select_rows(
                        sb_client, "leads",
                        columns="business_name,address",
                        filters={"user_id": task_user_id},
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

        logging.info("[scrape-task:%s] Persisting leads with user_id=%s", task_id, task_user_id)
        inserted = batch_upsert_leads(leads, db_path=str(db_path), user_id=task_user_id)
        logging.info("[scrape-task:%s] Saving to DB complete | inserted=%s", task_id, int(inserted))
        logging.info("SUCCESS: %s leads saved to database", int(inserted))

        # Safety repair: if any rows for this scrape keyword were saved under legacy/user_id=1,
        # re-assign them to the authenticated task owner so they are visible in Lead Management.
        if keyword and task_user_id and task_user_id not in {"1", "legacy"}:
            try:
                with pgdb.connect(db_path) as conn:
                    repaired_local = conn.execute(
                        """
                        UPDATE leads
                        SET user_id = ?
                        WHERE search_keyword = ?
                          AND TRIM(COALESCE(user_id, '')) IN ('1', 'legacy')
                        """,
                        (task_user_id, keyword),
                    ).rowcount or 0
                    conn.commit()
                if int(repaired_local) > 0:
                    logging.warning(
                        "[scrape-task:%s] Repaired local leads user_id for keyword=%r -> user_id=%s rows=%s",
                        task_id,
                        keyword,
                        task_user_id,
                        int(repaired_local),
                    )
            except Exception as repair_exc:
                logging.warning("[scrape-task:%s] Local user_id repair skipped: %s", task_id, repair_exc)

            try:
                sb_admin = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
                if sb_admin is not None:
                    repaired_remote = 0
                    for orphan_uid in ("1", "legacy"):
                        orphan_rows = supabase_select_rows(
                            sb_admin,
                            "leads",
                            columns="id",
                            filters={"search_keyword": keyword, "user_id": orphan_uid},
                        )
                        for row in orphan_rows:
                            row_id = row.get("id")
                            if row_id is None:
                                continue
                            sb_admin.table("leads").update({"user_id": task_user_id}).eq("id", row_id).execute()
                            repaired_remote += 1
                    if repaired_remote > 0:
                        logging.warning(
                            "[scrape-task:%s] Repaired Supabase leads user_id for keyword=%r -> user_id=%s rows=%s",
                            task_id,
                            keyword,
                            task_user_id,
                            int(repaired_remote),
                        )
            except Exception as repair_sb_exc:
                logging.warning("[scrape-task:%s] Supabase user_id repair skipped: %s", task_id, repair_sb_exc)

        progress_state["phase"] = "post_process"
        progress_state["inserted"] = inserted
        progress_state["current_found"] = total_scraped_from_maps
        progress_state["total_to_find"] = requested_total
        progress_state["status_message"] = f"Saving {inserted} leads to database..."
        _safe_update_progress()

        with pgdb.connect(db_path) as conn:
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
                    task_user_id,
                ),
            )
            conn.commit()

        blacklisted_synced = sync_blacklisted_leads(db_path)
        # Always sync to Supabase regardless of insertion count so leads saved
        # via the per-lead immediate path are not double-synced but any that
        # failed immediate sync are caught here as a safety net.
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        _invalidate_leads_cache()

        exported = 0
        output_csv = None
        if payload_data.get("export_targets"):
            output_path = resolve_path(payload_data.get("output_csv"), DEFAULT_TARGET_EXPORT)
            exported = export_target_leads(
                db_path=str(db_path),
                output_csv=str(output_path),
                min_score=HIGH_AI_SCORE_THRESHOLD,
                user_id=task_user_id,
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
                action_type="scrape",
                metadata={
                    "task_id": int(task_id),
                    "inserted": int(inserted or 0),
                },
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
                "refresh_leads": True,
                "status_message": f"Completed. Inserted {inserted} leads.",
            },
        )

        # Build explicit lead_id handoff for enrichment to avoid empty enrichment selection.
        saved_lead_ids: list[int] = []
        if task_user_id and leads:
            try:
                with pgdb.connect(db_path) as conn:
                    conn.row_factory = pgdb.Row
                    for lead in leads:
                        business_name = str(getattr(lead, "business_name", "") or "").strip()
                        address = str(getattr(lead, "address", "") or "").strip()
                        if not business_name:
                            continue
                        row = conn.execute(
                            """
                            SELECT id
                            FROM leads
                            WHERE user_id = ? AND business_name = ? AND address = ?
                            ORDER BY id DESC
                            LIMIT 1
                            """,
                            (task_user_id, business_name, address),
                        ).fetchone()
                        if row and row["id"] is not None:
                            saved_lead_ids.append(int(row["id"]))

                # Fallback: take latest rows for this keyword/user if identity matching returned nothing.
                if not saved_lead_ids and inserted > 0:
                    with pgdb.connect(db_path) as conn:
                        conn.row_factory = pgdb.Row
                        fallback_rows = conn.execute(
                            """
                            SELECT id
                            FROM leads
                            WHERE user_id = ? AND search_keyword = ?
                            ORDER BY id DESC
                            LIMIT ?
                            """,
                            (task_user_id, keyword, int(max(1, inserted))),
                        ).fetchall()
                        saved_lead_ids = [int(row["id"]) for row in fallback_rows if row and row["id"] is not None]
            except Exception as lead_id_exc:
                logging.warning("[scrape-task:%s] Could not resolve saved lead IDs for enrichment handoff: %s", task_id, lead_id_exc)

        auto_enrich_queued = False
        if inserted > 0 and deep_scan_mode == "async":
            try:
                enrich_payload = {
                    "db_path": str(db_path),
                    "config_path": str(DEFAULT_CONFIG_PATH),
                    "limit": min(max(1, int(inserted)), int(requested_total or inserted or 1)),
                    "lead_ids": saved_lead_ids[:500],
                    "user_id": task_user_id,
                    "headless": True,
                    "source": "scrape_auto_async_deep_scan",
                    "_queue_priority": False,
                }
                enqueue_task(_app, None, db_path, task_user_id, "enrich", enrich_payload, source="scrape_auto_async_deep_scan")
                auto_enrich_queued = True
                logging.info("[scrape-task:%s] Auto async enrichment queued after scrape.", task_id)
            except Exception as queue_exc:
                logging.warning("[scrape-task:%s] Auto async enrichment queue failed: %s", task_id, queue_exc)

        if auto_enrich_queued:
            try:
                latest = fetch_task_by_id(db_path, task_id)
                latest_result = latest.get("result") if latest else {}
                latest_payload = dict(latest_result) if isinstance(latest_result, dict) else {}
                latest_payload["auto_enrich_queued"] = True
                latest_payload["status_message"] = (
                    "Completed. Leads saved fast; deep scan queued in background."
                )
                finish_task_record(
                    db_path,
                    task_id,
                    status="completed",
                    result_payload=latest_payload,
                )
            except Exception:
                logging.debug("[scrape-task:%s] Could not append auto_enrich_queued result payload.", task_id)
        logging.info("[scrape-task:%s] Task completed successfully", task_id)
    except Exception as exc:
        logging.exception("Background scrape failed")
        logging.error("[scrape-task:%s] Task failed: %s", task_id, exc)
        requested_total = int(payload_data.get("results", 25))
        fail_payload = {
            "phase": "failed",
            "total_to_find": requested_total,
            "current_found": 0,
            "scanned_count": 0,
            "inserted": 0,
            "status_message": f"Scrape failed: {exc}",
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
    finally:
        heartbeat_stop.set()


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
            "status_message": "Queued for enrichment...",
        }
        update_task_progress(db_path, task_id, progress_state)

        # In Supabase-primary mode, push any locally enriched data to Supabase first,
        # so that the enricher doesn't re-process leads that were already enriched in
        # a previous crashed/cancelled task and whose enrichment data is only in legacy store.
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        enricher = _create_lead_enricher(
            db_path=str(db_path),
            headless=bool(payload_data.get("headless", True)),
            config_path=str(config_path),
            user_niche=payload_data.get("user_niche"),
            user_id=payload_data.get("user_id"),
            model_name_override=str(payload_data.get("_ai_model") or DEFAULT_AI_MODEL),
            speed_mode=bool(payload_data.get("speed_mode", False)),
        )
        target_lead_ids: list[int] = []
        pre_enrich_scores: dict[int, dict[str, Optional[float]]] = {}
        try:
            prospective_rows = enricher._fetch_leads_for_enrichment(  # type: ignore[attr-defined]
                limit=payload_data.get("limit"),
                lead_ids=payload_data.get("lead_ids") or None,
            )
            target_lead_ids = [int(row.get("id")) for row in prospective_rows if str(row.get("id") or "").isdigit()]
        except Exception:
            target_lead_ids = [int(x) for x in (payload_data.get("lead_ids") or []) if str(x).isdigit()]

        if target_lead_ids:
            placeholders = ",".join(["?"] * len(target_lead_ids))
            with pgdb.connect(db_path) as conn:
                conn.row_factory = pgdb.Row
                rows = conn.execute(
                    f"SELECT id, seo_score, performance_score, enrichment_data, insecure_site, main_shortcoming FROM leads WHERE user_id = ? AND id IN ({placeholders})",
                    [str(payload_data.get("user_id") or "legacy"), *target_lead_ids],
                ).fetchall()
            for row in rows:
                row_map = dict(row)
                seo_score, performance_score = _extract_scores_from_lead_payload(row_map)
                pre_enrich_scores[int(row_map.get("id") or 0)] = {
                    "seo_score": seo_score,
                    "performance_score": performance_score,
                }

        ai_key_configured = bool(getattr(enricher, "ai_client", None))
        progress_state["ai_key_configured"] = ai_key_configured
        if not ai_key_configured:
            progress_state["status_message"] = "AZURE_OPENAI_API_KEY missing on Railway. Using heuristic enrichment mode."
            update_task_progress(db_path, task_id, progress_state)

        phase_labels = {
            "starting": "Preparing enrichment engine...",
            "checking_website": "Checking website and loading page signals...",
            "discovering_email": "Finding contact details and social profiles...",
            "analyzing_ai": "Analyzing SEO, conversion gaps, and intent signals...",
            "finalizing": "Saving enrichment data to database...",
            "completed_lead": "Lead enrichment complete.",
            "enriching": "Enrichment in progress...",
        }

        def _on_enrich_progress(
            processed_count: int,
            total_count: int,
            with_email_count: int,
            current_lead: Optional[str],
            phase: Optional[str] = None,
        ) -> None:
            phase_key = str(phase or "enriching").strip().lower() or "enriching"
            progress_state["phase"] = "enriching"
            progress_state["processed"] = int(processed_count)
            progress_state["with_email"] = int(with_email_count)
            progress_state["total"] = int(total_count)
            progress_state["current_lead"] = current_lead
            progress_state["current_phase"] = phase_key
            progress_state["status_message"] = phase_labels.get(phase_key, phase_labels["enriching"])
            progress_state["ai_key_configured"] = ai_key_configured
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
                lead_ids=payload_data.get("lead_ids") or None,
                progress_callback=_on_enrich_progress,
            )
        finally:
            if acquired_slot:
                enrich_semaphore.release()

        # In Supabase-primary mode, queueing reads from Supabase. Sync first so
        # freshly enriched rows are visible to the queue step in this same task.
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and processed:
            maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        refreshed_score_map = _refresh_enriched_lead_scores(
            db_path,
            user_id=str(payload_data.get("user_id") or "legacy"),
            lead_ids=target_lead_ids,
        )
        score_drop_events_sent = _notify_score_drop_events(
            db_path=db_path,
            user_id=str(payload_data.get("user_id") or "legacy"),
            previous_scores=pre_enrich_scores,
            refreshed_scores=refreshed_score_map,
        )
        history_snapshots = _capture_lead_history_snapshots(
            db_path,
            user_id=str(payload_data.get("user_id") or "legacy"),
            lead_ids=target_lead_ids,
            snapshot_map=pre_enrich_scores,
        )

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
        if processed > 0 and int(payload_data.get("_credits_per_success") or 0) > 0:
            try:
                per_lead_cost = max(1, int(payload_data.get("_credits_per_success") or ENRICH_CREDIT_COST_PER_LEAD))
                billing = deduct_credits_on_success(
                    str(payload_data.get("user_id") or ""),
                    credits_to_deduct=max(0, int(processed)) * per_lead_cost,
                    db_path=db_path,
                    action_type="enrich",
                    metadata={
                        "task_id": int(task_id),
                        "processed": int(processed),
                        "credits_per_lead": per_lead_cost,
                    },
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
                "requested_limit": int(payload_data.get("requested_limit") or payload_data.get("limit") or 0),
                "effective_limit": int(payload_data.get("limit") or 0),
                "requested_lead_ids": int(len(payload_data.get("lead_ids") or [])),
                "processed": processed,
                "updated_rows": processed,
                "enriched_count": processed,
                "with_email": with_email,
                "queued_for_mail": queued_for_mail,
                "history_snapshots": int(history_snapshots),
                "trend_scores_updated": int(len(refreshed_score_map)),
                "developer_score_drop_events": int(score_drop_events_sent),
                "exported": exported,
                "output_csv": output_csv,
                "credits_charged": credits_charged,
                "credits_balance": credits_balance,
                "credits_limit": credits_limit,
                "billing_warning": billing_warning,
                "ai_key_configured": ai_key_configured,
                "status_message": (
                    (
                        f"No eligible leads found for enrichment (requested ids: {len(payload_data.get('lead_ids') or [])}). "
                        "Requested IDs may already be completed or missing in the database."
                    )
                    if int(processed or 0) == 0
                    else "Enrichment completed."
                ),
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

        # â”€â”€ Scheduled start: wait until the requested New York (ET) hour â”€â”€â”€â”€
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
                    "Mailer scheduled for %02d:00 ET â€” current ET time is %02d:%02d. Waitingâ€¦",
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
            smtp_accounts_override=(
                list(payload_data.get("_smtp_accounts_override") or [])
                or load_user_smtp_accounts(user_id=str(payload_data.get("user_id") or "legacy"), db_path=db_path)
            ),
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
                    action_type="mailer",
                    metadata={
                        "task_id": int(task_id),
                        "sent": int(sent),
                    },
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
            "smtp_source": str(payload_data.get("_smtp_source") or "custom"),
            "smtp_quota_limit": int(payload_data.get("_smtp_quota_limit") or SYSTEM_SMTP_DEFAULT_SEND_LIMIT),
            "smtp_quota_remaining_before": int(payload_data.get("_smtp_quota_remaining_before") or 0),
            "smtp_quota_capped": bool(payload_data.get("_smtp_quota_capped")),
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
        # SMTP / network failure â€” mark leads for retry instead of hard-failed
        logging.warning("Mailer SMTP/network error (retry_later): %s", exc)
        with pgdb.connect(db_path) as conn:
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
    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
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
    except FileNotFoundError:
        # Optional file can be absent in production; treat as default settings.
        cfg = {}
    except Exception as exc:
        logging.warning("Weekly report: could not read environment settings â€” %s", exc)
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
            logging.warning("Weekly report: SMTP is not ready for %s â€” %s", recipient_info["user_id"], exc.detail)
        except Exception as exc:
            logging.warning("Weekly report SMTP send failed for %s: %s", recipient, exc)


def run_monthly_report_digest(_app: FastAPI) -> None:
    db_path = DEFAULT_DB_PATH
    config_path = DEFAULT_CONFIG_PATH
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except FileNotFoundError:
        # Optional file can be absent in production; treat as default settings.
        cfg = {}
    except Exception as exc:
        logging.warning("Monthly report: could not read environment settings â€” %s", exc)
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
            pdf_bytes = _build_report_pdf(f"Sniped Monthly Report â€” {summary.get('month_label', 'Current Month')}", summary, period_key="monthly")
            send_monthly_report_email(account, recipient, summary, pdf_bytes)
            logging.info("Monthly report sent to %s for %s.", recipient, recipient_info["user_id"])
        except HTTPException as exc:
            logging.warning("Monthly report: SMTP is not ready for %s â€” %s", recipient_info["user_id"], exc.detail)
        except Exception as exc:
            logging.warning("Monthly report SMTP send failed for %s: %s", recipient, exc)


def run_daily_digest(_app: FastAPI) -> None:
    """Send the morning Profit Digest email at 08:00 UTC."""
    db_path = DEFAULT_DB_PATH
    config_path = DEFAULT_CONFIG_PATH

    try:
        with config_path.open("r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except FileNotFoundError:
        # Optional file can be absent in production; treat as default settings.
        cfg = {}
    except Exception as exc:
        logging.warning("Daily digest: could not read environment settings - %s", exc)
        return

    if not bool(cfg.get("auto_daily_digest_email", False)):
        logging.info("Daily digest: disabled in config.")
        return

    smtp_accounts = cfg.get("smtp_accounts", [])
    if not smtp_accounts:
        logging.warning("Daily digest: no SMTP accounts configured - skipping.")
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

    recipient = str(cfg.get("digest_email", "") or "").strip() or smtp_email
    if not recipient or not smtp_host or not smtp_password:
        logging.warning("Daily digest: incomplete SMTP config - skipping.")
        return

    try:
        stats = get_dashboard_stats(db_path)
    except Exception as exc:
        logging.warning("Daily digest: could not gather stats - %s", exc)
        return

    mrr = stats.get("monthly_recurring_revenue", 0)
    mrr_goal = stats.get("mrr_goal", MRR_GOAL)
    mrr_progress = stats.get("mrr_progress_pct", 0)
    paid_count = stats.get("paid_count", 0)
    total_leads = stats.get("total_leads", 0)
    emails_sent = stats.get("emails_sent", 0)

    recommendation = get_niche_recommendation(db_path, config_path)
    top_pick = recommendation.get("top_pick", {}) if isinstance(recommendation, dict) else {}
    niche_keyword = str(top_pick.get("keyword", "AC Repair in Phoenix, AZ") or "AC Repair in Phoenix, AZ")
    expected_reply = float(top_pick.get("expected_reply_rate", 5.0) or 5.0)
    campaign_base = str(cfg.get("dashboard_url", "http://localhost:5173") or "http://localhost:5173").rstrip("/")
    campaign_link = f"{campaign_base}/?niche={quote_plus(niche_keyword)}"

    golden_count = 0
    uptime_alerts: list[dict] = []
    day_cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    try:
        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                """
                SELECT COUNT(*) FROM leads
                WHERE ai_score >= 9
                  AND COALESCE(enriched_at, '') >= ?
                  -- SYSTEM-WIDE: intentionally unscoped.
                """,
                (day_cutoff_iso,),
            ).fetchone()
            golden_count = int(row[0] or 0) if row else 0

            alert_rows = conn.execute(
                """
                SELECT request_payload FROM system_tasks
                WHERE task_type = 'uptime_alert'
                  AND COALESCE(created_at, '') >= ?
                  -- SYSTEM-WIDE: intentionally unscoped.
                ORDER BY id DESC
                LIMIT 20
                """,
                (day_cutoff_iso,),
            ).fetchall()
            for ar in alert_rows:
                try:
                    payload = json.loads(ar["request_payload"] or "{}")
                    uptime_alerts.append(payload)
                except json.JSONDecodeError:
                    continue
    except Exception as exc:
        logging.warning("Daily digest: DB read failed - %s", exc)

    today_str = datetime.now(timezone.utc).strftime("%A, %d %b %Y")
    progress_bar = ("\u2588" * (mrr_progress // 10)).ljust(10, "\u2591")

    if uptime_alerts:
        lines = []
        for a in uptime_alerts:
            name = a.get("business_name", "?")
            url = a.get("website_url", "?")
            code = a.get("http_status", 0) or "unreachable"
            lines.append(f"  !  {name} ({url}) - HTTP {code}")
        alert_lines = "\n\nUptime Alerts (last 24h):\n" + "\n".join(lines)
    else:
        alert_lines = "\n\nUptime Alerts: none"

    body = (
        f"Good morning! Here is your Daily Profit Digest for {today_str}.\n"
        f"{'=' * 50}\n\n"
        f"MRR:             EUR {mrr:,.0f} / EUR {mrr_goal:,.0f}\n"
        f"Goal progress:   [{progress_bar}] {mrr_progress}%\n"
        f"Paid clients:    {paid_count}\n"
        f"Total leads:     {total_leads}\n"
        f"Emails sent:     {emails_sent}\n"
        f"Golden Leads:    {golden_count} found in last 24h"
        f"\nToday's opportunity: {niche_keyword}. Expected reply rate: {expected_reply:.1f}%. "
        f"Click here to launch the campaign: {campaign_link}"
        f"{alert_lines}\n\n"
        f"{'=' * 50}\n"
        f"Keep pushing. You're {100 - mrr_progress}% from the finish line.\n"
    )
    subject = f"[Digest] MRR EUR {mrr:,.0f} | {mrr_progress}% to goal | {today_str}"

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
            "Daily digest sent to %s (MRR=EUR %s, golden=%s, alerts=%s).",
            recipient,
            mrr,
            golden_count,
            len(uptime_alerts),
        )
    except Exception as exc:
        logging.warning("Daily digest SMTP send failed: %s", exc)


def run_uptime_check(_app: FastAPI) -> None:
    """Check HTTP reachability of every paid client's website every 2 hours."""
    db_path = DEFAULT_DB_PATH
    ensure_system_tables(db_path)

    with pgdb.connect(db_path) as conn:
        conn.row_factory = pgdb.Row
        rows = conn.execute(
            """
            SELECT id, business_name, website_url
            FROM leads
            WHERE LOWER(COALESCE(status, '')) = 'paid'
              AND website_url IS NOT NULL
              AND TRIM(website_url) != ''
                        -- SYSTEM-WIDE: intentionally unscoped.
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
    try:
        ensure_system_tables(db_path)
    except Exception as exc:
        if _is_db_capacity_error(exc):
            logging.warning("Autopilot cycle skipped because the database is saturated.")
            return
        raise

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
    try:
        ensure_system_tables(db_path)
    except Exception as exc:
        if _is_db_capacity_error(exc):
            logging.warning("Drip dispatch cycle skipped because the database is saturated.")
            return
        raise

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

    scheduler = BackgroundScheduler(
        timezone="UTC",
        daemon=True,
        executors={"default": APSchedulerThreadPoolExecutor(max_workers=SCHEDULER_MAX_WORKERS)},
    )
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

    if RUN_STARTUP_JOBS:
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


# â”€â”€ Lead Qualifier helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
        raw_items = [segment.strip() for segment in re.split(r"\n|\||;|â€˘", value)]
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


_QUALIFIED_LEAD_STATUSES = {
    "queued_mail", "emailed", "interested", "replied",
    "meeting set", "zoom scheduled", "closed", "paid",
    "qualified_not_interested", "qualified not interested",
}

_REPLIED_LEAD_STATUSES = {
    "replied", "interested", "meeting set", "zoom scheduled",
    "closed", "paid", "qualified_not_interested", "qualified not interested",
}


def _lead_has_reply(lead: dict) -> bool:
    status = str(lead.get("status") or "").strip().lower()
    return bool(lead.get("reply_detected_at") or status in _REPLIED_LEAD_STATUSES)


def _lead_is_qualified(lead: dict) -> bool:
    status = str(lead.get("status") or "").strip().lower()
    score = _qualifier_to_float(lead.get("ai_score"), default=0.0)
    return status in _QUALIFIED_LEAD_STATUSES or score >= 7


def _lead_matches_quick_filter(lead: dict, quick_filter: str) -> bool:
    normalized = str(quick_filter or "all").strip().lower()
    if normalized in {"", "all"}:
        return True
    if normalized == "qualified":
        return _lead_is_qualified(lead)
    if normalized == "not_qualified":
        return not _lead_is_qualified(lead)
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


def _ai_prompt_extract_city(prompt_text: str) -> str:
    match = re.search(r"\b(?:in|near|around)\s+([a-zA-Z][a-zA-Z\s\-]{1,40})", prompt_text, flags=re.IGNORECASE)
    if not match:
        return ""
    raw = str(match.group(1) or "").strip()
    city = re.split(r"\b(with|where|that|who|and|or)\b", raw, maxsplit=1, flags=re.IGNORECASE)[0].strip(" ,.-")
    return city


def _ai_prompt_heuristic_filters(prompt: str) -> dict[str, Any]:
    text = str(prompt or "").strip()
    lower = text.lower()

    min_score: Optional[float] = None
    min_rating: Optional[float] = None
    max_rating: Optional[float] = None

    rating_above = re.search(r"\brating\s*(?:above|over|>=?)\s*(\d(?:\.\d+)?)", lower)
    if rating_above:
        min_rating = _qualifier_to_float(rating_above.group(1), default=0.0)

    rating_below = re.search(r"\brating\s*(?:below|under|<=?)\s*(\d(?:\.\d+)?)", lower)
    if rating_below:
        max_rating = _qualifier_to_float(rating_below.group(1), default=5.0)

    score_above = re.search(r"\b(?:score|priority)\s*(?:above|over|>=?)\s*(\d(?:\.\d+)?)", lower)
    if score_above:
        min_score = _qualifier_to_float(score_above.group(1), default=0.0)

    if "high priority" in lower:
        min_score = max(min_score or 0.0, 8.0)
    if "high-ticket" in lower or "high ticket" in lower:
        min_score = max(min_score or 0.0, 8.0)

    website_fix = any(term in lower for term in ["website fix", "no https", "without https", "slow load", "slow site", "page speed"])
    seo_gap = any(term in lower for term in ["weak seo", "poor seo", "low seo", "bad seo", "seo issue", "ranking issue", "low organic", "no seo"])
    social_gaps = any(term in lower for term in ["social gap", "social gaps", "missing instagram", "missing linkedin", "no instagram", "no linkedin", "no socials", "missing socials"])
    no_socials = any(term in lower for term in ["no socials", "without socials", "missing all socials", "no social profiles"])
    missing_pixel = any(term in lower for term in ["no facebook pixel", "without facebook pixel", "missing pixel", "no pixel"])

    city = _ai_prompt_extract_city(text)

    stop_words = {
        "find", "show", "me", "with", "without", "and", "or", "in", "near", "around", "above", "below",
        "rating", "score", "priority", "leads", "lead", "high", "ticket", "high-ticket", "no", "missing", "facebook",
        "pixel", "website", "fix", "social", "gaps", "instagram", "linkedin", "the", "a", "an", "for", "to",
    }
    search_terms: list[str] = []
    for token in re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{2,}", lower):
        if token in stop_words:
            continue
        if token.isdigit():
            continue
        search_terms.append(token)
    # Keep terms unique and compact to avoid over-filtering.
    search_terms = list(dict.fromkeys(search_terms))[:5]

    return {
        "city": city,
        "search_terms": search_terms,
        "min_score": min_score,
        "min_rating": min_rating,
        "max_rating": max_rating,
        "require_missing_pixel": missing_pixel,
        "require_website_fix": website_fix or seo_gap,
        "require_seo_gap": seo_gap,
        "require_social_gap": social_gaps,
        "require_no_socials": no_socials,
    }


def _ai_prompt_llm_filters(prompt: str) -> Optional[dict[str, Any]]:
    client, model_name = load_openai_client(DEFAULT_CONFIG_PATH)
    if client is None:
        return None

    system_prompt = (
        "You convert a natural-language CRM lead filtering prompt into JSON filters. "
        "Return ONLY strict JSON with keys: city, search_terms, min_score, min_rating, max_rating, "
        "require_missing_pixel, require_website_fix, require_seo_gap, require_social_gap, require_no_socials. "
        "Use null for unknown numeric values, [] for unknown arrays, and false for unknown booleans."
    )

    try:
        response = client.chat.completions.create(
            model=str(model_name or DEFAULT_AI_MODEL).strip() or DEFAULT_AI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": str(prompt or "").strip()},
            ],
            max_tokens=220,
            temperature=0.1,
        )
        raw = str(response.choices[0].message.content or "").strip()
        json_text = raw
        if "{" in raw and "}" in raw:
            json_text = raw[raw.find("{") : raw.rfind("}") + 1]
        parsed = json.loads(json_text)
        if not isinstance(parsed, dict):
            return None
        return {
            "city": str(parsed.get("city") or "").strip(),
            "search_terms": [str(term).strip().lower() for term in (parsed.get("search_terms") or []) if str(term).strip()],
            "min_score": parsed.get("min_score"),
            "min_rating": parsed.get("min_rating"),
            "max_rating": parsed.get("max_rating"),
            "require_missing_pixel": bool(parsed.get("require_missing_pixel")),
            "require_website_fix": bool(parsed.get("require_website_fix")),
            "require_seo_gap": bool(parsed.get("require_seo_gap")),
            "require_social_gap": bool(parsed.get("require_social_gap")),
            "require_no_socials": bool(parsed.get("require_no_socials")),
        }
    except Exception:
        return None


AI_FILTER_EMPTY_MESSAGE = "I couldn't find leads matching that specific criteria. Try a broader search!"

# These are semantic aliases that the LLM can use; they are expanded to concrete SQL expressions.
AI_FILTER_SQL_ALIAS_EXPRESSIONS: dict[str, str] = {
    "city": "LOWER(COALESCE(address, ''))",
    "niche": "LOWER(COALESCE(search_keyword, business_name, ''))",
    "has_instagram": "(NULLIF(TRIM(COALESCE(instagram_url, '')), '') IS NOT NULL)",
    "has_linkedin": "(NULLIF(TRIM(COALESCE(linkedin_url, '')), '') IS NOT NULL)",
    "has_facebook": "(NULLIF(TRIM(COALESCE(facebook_url, '')), '') IS NOT NULL)",
    "has_socials": "(NULLIF(TRIM(COALESCE(instagram_url, '')), '') IS NOT NULL OR NULLIF(TRIM(COALESCE(linkedin_url, '')), '') IS NOT NULL OR NULLIF(TRIM(COALESCE(facebook_url, '')), '') IS NOT NULL)",
    "missing_socials": "(NULLIF(TRIM(COALESCE(instagram_url, '')), '') IS NULL AND NULLIF(TRIM(COALESCE(linkedin_url, '')), '') IS NULL AND NULLIF(TRIM(COALESCE(facebook_url, '')), '') IS NULL)",
    "tech_stack_text": "LOWER(COALESCE(CAST(tech_stack AS TEXT), ''))",
}

AI_FILTER_SQL_ALLOWED_IDENTIFIERS: set[str] = {
    "and", "or", "not", "is", "null", "true", "false", "in", "like", "ilike", "between",
    "lower", "coalesce", "cast", "as", "text", "trim", "nullif",
    "business_name", "contact_name", "email", "website_url", "address", "search_keyword", "status",
    "pipeline_stage", "ai_score", "qualification_score", "seo_score", "performance_score", "rating",
    "review_count", "has_pixel", "insecure_site", "instagram_url", "linkedin_url", "facebook_url", "tech_stack",
    "city", "niche", "has_instagram", "has_linkedin", "has_facebook", "has_socials", "missing_socials", "tech_stack_text",
}

AI_FILTER_SQL_BANNED_PATTERN = re.compile(
    r"(;|--|/\*|\*/|\\x00|\\u0000|\b(select|insert|update|delete|drop|alter|create|grant|revoke|truncate|union|intersect|except|copy|execute|do|declare|into|from|join|pg_sleep|pg_|information_schema)\b)",
    re.IGNORECASE,
)


def _extract_sql_condition_text(raw_content: str) -> str:
    text_value = str(raw_content or "").strip()
    if not text_value:
        return ""

    if text_value.startswith("```"):
        text_value = re.sub(r"^```(?:sql)?", "", text_value, flags=re.IGNORECASE).strip()
        if text_value.endswith("```"):
            text_value = text_value[:-3].strip()

    lowered = text_value.lower()
    if lowered.startswith("where "):
        text_value = text_value[6:].strip()
    if lowered.startswith("sql:"):
        text_value = text_value[4:].strip()
    return text_value


def _ai_prompt_sql_where_clause(prompt: str) -> Optional[str]:
    client, model_name = load_openai_client(DEFAULT_CONFIG_PATH)
    if client is None:
        return None

    schema_prompt = (
        "Table leads columns: business_name, contact_name, email, website_url, address, search_keyword, status, "
        "pipeline_stage, ai_score, qualification_score, seo_score, performance_score, rating, review_count, has_pixel, insecure_site, "
        "instagram_url, linkedin_url, facebook_url, tech_stack. "
        "Derived aliases available: city, niche, has_instagram, has_linkedin, has_facebook, has_socials, missing_socials, tech_stack_text."
    )
    system_prompt = (
        "Convert a user CRM filtering request into ONE SQL WHERE condition fragment only. "
        "Return ONLY the condition (no SELECT, no FROM, no comments, no markdown). "
        "Use only allowed columns/aliases from schema. Keep it concise and safe. "
        "Example output: (niche ILIKE '%dentist%' AND city ILIKE '%berlin%' AND has_instagram = false)."
    )

    try:
        response = client.chat.completions.create(
            model=str(model_name or DEFAULT_AI_MODEL).strip() or DEFAULT_AI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "system", "content": schema_prompt},
                {"role": "user", "content": str(prompt or "").strip()},
            ],
            max_tokens=220,
            temperature=0.1,
        )
        return _extract_sql_condition_text(str(response.choices[0].message.content or ""))
    except Exception:
        return None


def _sanitize_ai_where_clause(raw_clause: str) -> Optional[str]:
    clause = _extract_sql_condition_text(raw_clause)
    if not clause:
        return None
    if len(clause) > 700:
        return None
    if AI_FILTER_SQL_BANNED_PATTERN.search(clause):
        return None

    expanded = clause
    for alias_name, expression in sorted(AI_FILTER_SQL_ALIAS_EXPRESSIONS.items(), key=lambda item: -len(item[0])):
        expanded = re.sub(rf"\b{re.escape(alias_name)}\b", expression, expanded, flags=re.IGNORECASE)

    # Strip quoted literals before keyword/token validation.
    expanded_without_literals = re.sub(r"'(?:''|[^'])*'", "''", expanded)
    expanded_without_literals = re.sub(r'"(?:""|[^"])*"', '""', expanded_without_literals)

    if AI_FILTER_SQL_BANNED_PATTERN.search(expanded_without_literals):
        return None

    # Restrict characters to a conservative subset.
    if re.search(r"[^a-zA-Z0-9_\s'\"%().,=<>!+-]", expanded):
        return None

    identifiers = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expanded_without_literals)
    for token in identifiers:
        if token.lower() not in AI_FILTER_SQL_ALLOWED_IDENTIFIERS:
            return None

    return expanded.strip()


def _merge_ai_filter_specs(base_spec: dict[str, Any], llm_spec: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not llm_spec:
        return base_spec

    merged = dict(base_spec)
    for key in ("city", "search_terms", "min_score", "min_rating", "max_rating"):
        value = llm_spec.get(key)
        if value is None:
            continue
        if key == "search_terms":
            terms = [str(term).strip().lower() for term in value if str(term).strip()]
            if terms:
                merged[key] = list(dict.fromkeys(terms))[:5]
            continue
        if key == "city":
            candidate = str(value or "").strip()
            if candidate:
                merged[key] = candidate
            continue
        merged[key] = _qualifier_to_float(value, default=_qualifier_to_float(merged.get(key), default=0.0))

    for key in ("require_missing_pixel", "require_website_fix", "require_seo_gap", "require_social_gap", "require_no_socials"):
        merged[key] = bool(base_spec.get(key) or llm_spec.get(key))
    return merged


def _lead_matches_ai_filter_spec(lead: dict[str, Any], spec: dict[str, Any]) -> bool:
    city = str(spec.get("city") or "").strip().lower()
    if city:
        address_blob = " ".join(
            [
                str(lead.get("address") or ""),
                str(lead.get("city") or ""),
                str(lead.get("search_keyword") or ""),
            ]
        ).lower()
        if city not in address_blob:
            return False

    min_score_raw = spec.get("min_score")
    if min_score_raw is not None and str(min_score_raw).strip() != "":
        threshold = _qualifier_to_float(min_score_raw, default=0.0)
        lead_score = _qualifier_to_float(
            lead.get("best_lead_score", lead.get("ai_score")),
            default=_qualifier_to_float(lead.get("ai_score"), default=0.0),
        )
        if lead_score < threshold:
            return False

    rating = _qualifier_to_float(lead.get("rating"), default=0.0)
    min_rating_raw = spec.get("min_rating")
    if min_rating_raw is not None and str(min_rating_raw).strip() != "":
        if rating < _qualifier_to_float(min_rating_raw, default=0.0):
            return False

    max_rating_raw = spec.get("max_rating")
    if max_rating_raw is not None and str(max_rating_raw).strip() != "":
        if rating > _qualifier_to_float(max_rating_raw, default=5.0):
            return False

    if bool(spec.get("require_missing_pixel")):
        has_pixel = _qualifier_to_bool(lead.get("has_pixel"))
        if has_pixel:
            return False

    if bool(spec.get("require_website_fix")):
        website = str(lead.get("website_url") or "").strip().lower()
        insecure = _qualifier_to_bool(lead.get("insecure_site")) or (website.startswith("http://") and not website.startswith("https://"))
        enrichment = _qualifier_parse_enrichment(lead.get("enrichment_data"))
        pagespeed = _qualifier_to_float(
            enrichment.get("pagespeed_score", enrichment.get("page_speed", 100.0)),
            default=100.0,
        )
        if not (insecure or pagespeed < 55):
            return False

    if bool(spec.get("require_seo_gap")):
        enrichment = _qualifier_parse_enrichment(lead.get("enrichment_data"))
        organic_traffic = _qualifier_to_float(
            enrichment.get("organic_traffic", enrichment.get("organic_visits", 0.0)),
            default=0.0,
        )
        backlinks = _qualifier_to_int(
            enrichment.get("backlink_count", enrichment.get("backlinks", enrichment.get("ref_domains", 0))),
            default=0,
        )
        authority = _qualifier_to_float(
            enrichment.get("authority", enrichment.get("domain_authority", enrichment.get("authority_score", 0.0))),
            default=0.0,
        )
        seo_blob = " ".join(
            [
                str(lead.get("main_shortcoming") or ""),
                str(lead.get("ai_description") or ""),
                str(enrichment.get("weak_points") or ""),
                str(enrichment.get("weaknesses") or ""),
            ]
        ).lower()
        has_seo_problem_text = any(term in seo_blob for term in ["seo", "organic", "ranking", "search visibility"])
        has_seo_problem_metrics = organic_traffic < 150 or backlinks < 25 or authority < 20
        if not (has_seo_problem_text or has_seo_problem_metrics):
            return False

    if bool(spec.get("require_social_gap")):
        has_instagram = bool(str(lead.get("instagram_url") or "").strip())
        has_linkedin = bool(str(lead.get("linkedin_url") or "").strip())
        # Social gap means at least one major social profile missing.
        if has_instagram and has_linkedin:
            return False

    if bool(spec.get("require_no_socials")):
        has_instagram = bool(str(lead.get("instagram_url") or "").strip())
        has_linkedin = bool(str(lead.get("linkedin_url") or "").strip())
        has_facebook = bool(str(lead.get("facebook_url") or "").strip())
        if has_instagram or has_linkedin or has_facebook:
            return False

    terms = [str(term).strip().lower() for term in (spec.get("search_terms") or []) if str(term).strip()]
    if terms:
        haystack = " ".join(
            [
                str(lead.get("business_name") or ""),
                str(lead.get("search_keyword") or ""),
                str(lead.get("address") or ""),
                str(lead.get("main_offer") or ""),
                " ".join(_lead_normalize_string_list(lead.get("tech_stack"), limit=8)),
            ]
        ).lower()
        if not all(term in haystack for term in terms):
            return False

    return True


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
        rating_str = f"{rating:.1f}â…" if isinstance(rating, (int, float)) else "good"
        return (
            f"{name} has {rating_str} reviews but ZERO online presence. "
            f"Every day, customers {city_str} search Google for '{niche_str}' â€” "
            f"they can't find {name} and click the first competitor that shows up instead. "
            f"Estimated impact: dozens of lost high-value jobs every month, going straight to competitors."
        )

    if review_count < 5 and city_max_reviews >= 100:
        return (
            f"{name} has only {review_count} review{'s' if review_count != 1 else ''} "
            f"while leading businesses {city_str} have {city_max_reviews}+. "
            f"Google's local algorithm buries low-review businesses below the fold â€” "
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
            f"Google's local pack prioritises businesses with strong review velocity â€” "
            f"without at least 20â€“30 reviews, {name} is consistently outranked "
            f"by competitors in the '{niche_str}' category {city_str}."
        )

    shortcoming_lower = main_shortcoming.lower()
    if "missing" in shortcoming_lower or "no website" in shortcoming_lower:
        return (
            f"{name} lacks a properly optimised website. "
            f"Competitors {city_str} are capturing high-intent Google traffic "
            f"while {name} relies on referrals alone â€” that's a shrinking pipeline."
        )

    return (
        f"{name} has a digital presence but is missing key authority signals. "
        f"Competing businesses {city_str} rank higher in the '{niche_str}' category, "
        f"meaning {name} loses qualified leads every week to more visible rivals."
    )


def create_app() -> FastAPI:
    from concurrent.futures import ThreadPoolExecutor as _TPE
    _thread_pool = _TPE(max_workers=APP_THREADPOOL_WORKERS, thread_name_prefix="lf-worker")
    allowed_cors_origins = get_allowed_cors_origins()
    allow_all_cors = str(os.environ.get("CORS_ALLOW_ALL", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}

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
        allow_origins=["*"] if allow_all_cors else allowed_cors_origins,
        allow_origin_regex=(
            r".*"
            if allow_all_cors
            else r"^https:\/\/([a-zA-Z0-9-]+\.)?vercel\.app$|^https:\/\/(www\.)?sniped\.io$|^https:\/\/sniped-production\.up\.railway\.app$|^http:\/\/localhost(:\d+)?$"
        ),
        allow_credentials=not allow_all_cors,
        # Explicitly include the critical cross-origin flow used by Vercel and localhost.
        allow_methods=["GET", "POST", "OPTIONS", "PUT", "PATCH", "DELETE"],
        allow_headers=["Content-Type", "Authorization", "Accept", "Origin"],
    )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logging.exception("Unhandled exception on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error.",
                "error_type": type(exc).__name__,
            },
        )

    @app.middleware("http")
    async def admin_api_guard(request: Request, call_next):
        path = str(request.url.path or "")
        # Always let OPTIONS (CORS preflight) through — CORSMiddleware handles it.
        if request.method == "OPTIONS":
            return await call_next(request)
        if path.startswith("/api/admin/"):
            origin = request.headers.get("origin", "")
            cors_headers = {
                "Access-Control-Allow-Origin": origin or "*",
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "GET, POST, PUT, PATCH, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, Accept, Origin",
            }
            try:
                token = require_authenticated_session(request)
                billing = load_user_billing_context(token, allow_stripe_recovery=False)
                requester_email = str(billing.get("email") or "").strip().lower()
                if requester_email not in DEFAULT_ADMIN_EMAILS:
                    return JSONResponse(status_code=403, content={"detail": "Admin access required."}, headers=cors_headers)
                request.state.current_user_email = requester_email
                request.state.is_admin = True
            except HTTPException as exc:
                return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=cors_headers)
            except Exception:
                return JSONResponse(status_code=401, content={"detail": "Authentication required."}, headers=cors_headers)
        return await call_next(request)

    async def _log_playwright_runtime_diagnostics() -> None:
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            logging.exception("[startup] Playwright import failed: %s", exc)
            return

        try:
            async with async_playwright() as playwright:
                chromium_path = playwright.chromium.executable_path
            exists = Path(chromium_path).exists() if chromium_path else False
            logging.info("[startup] Playwright Chromium executable ready=%s path=%s", exists, chromium_path)
            if not exists:
                logging.error(
                    "[startup] Chromium executable missing. Ensure image installs it with: "
                    "python -m playwright install --with-deps chromium"
                )
        except Exception as exc:
            logging.exception("[startup] Playwright runtime check failed: %s", exc)

    def _prewarm_scraper_browser() -> None:
        warm_enabled = str(os.environ.get("SCRAPE_WARM_BROWSER", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        if not warm_enabled:
            logging.info("[startup] SCRAPE_WARM_BROWSER disabled; skipping browser warm-up.")
            return
        logging.info("[startup] Skipping browser warm-up during startup to avoid sync Playwright usage inside the event loop.")

    @app.on_event("startup")
    async def startup_tasks() -> None:
        logging.basicConfig(
            level=getattr(logging, DEFAULT_LOG_LEVEL, logging.WARNING),
            format="%(asctime)s | %(levelname)s | %(message)s",
        )
        print(f"[startup] CORS allowed origins: {', '.join(allowed_cors_origins)}")
        supabase_settings = load_supabase_settings(DEFAULT_CONFIG_PATH)
        resolved_db_url = unquote(str(supabase_settings.get("resolved_database_url") or supabase_settings.get("database_url") or "").strip())
        resolved_db_url = _ensure_sslmode_require(resolved_db_url)

        if _is_railway_runtime():
            supabase_url = str(supabase_settings.get("url") or "").strip()
            if supabase_url:
                try:
                    supabase_host = str(urlparse(supabase_url).hostname or "")
                    if _looks_local_hostname(supabase_host):
                        raise RuntimeError(
                            "SUPABASE_URL points to a local address in Railway runtime. "
                            "Set SUPABASE_URL to the production Supabase project URL."
                        )
                except RuntimeError:
                    raise
                except Exception:
                    pass
            if resolved_db_url:
                try:
                    db_host = str(urlparse(resolved_db_url).hostname or "")
                    if _looks_local_hostname(db_host):
                        raise RuntimeError(
                            "DATABASE_URL/SUPABASE_DATABASE_URL points to a local address in Railway runtime. "
                            "Set it to the production Supabase Postgres URL."
                        )
                except RuntimeError:
                    raise
                except Exception:
                    pass

        if resolved_db_url:
            os.environ["DATABASE_URL"] = resolved_db_url
            os.environ["SUPABASE_DATABASE_URL"] = resolved_db_url
            parsed_host, parsed_port = _extract_db_host_port(resolved_db_url)

            if parsed_host:
                print(f"Attempting connection to: {parsed_host}")

            if parsed_port == 5432:
                logging.warning(
                    "[startup] DATABASE_URL is using port 5432 (direct). Prefer Supabase pooler transaction mode on port 6543."
                )
        if STATELESS_SUPABASE_ONLY and not supabase_settings.get("has_service_role"):
            print(
                "[startup] WARNING: STATELESS_SUPABASE_ONLY is set but SUPABASE_SERVICE_ROLE_KEY is missing. "
                "Backend write operations under RLS may fail. Set SUPABASE_SERVICE_ROLE_KEY in Railway env vars."
            )
        else:
            logging.info("[startup] Supabase service-role key detected for server-side writes.")
        await _log_playwright_runtime_diagnostics()
        _prewarm_scraper_browser()
        # â”€â”€ Env-var check â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        _required_env = {
            "SUPABASE_URL": "Supabase project URL (required for auth & DB)",
            "DATABASE_URL": "Supabase Postgres connection string (required for pgdb/worker/runtime access)",
            "AZURE_OPENAI_API_KEY": "Azure OpenAI key (required for enrichment & mail)",
            "AZURE_OPENAI_ENDPOINT": "Azure OpenAI endpoint (required for enrichment & mail)",
            "AZURE_OPENAI_DEPLOYMENT_NAME": "Azure OpenAI deployment name (required for enrichment & mail)",
        }
        _optional_env = {
            "SUPABASE_KEY": "Shared Supabase API key alias (alternative to service-role/publishable key)",
            "SUPABASE_SERVICE_ROLE_KEY": "Preferred Supabase service-role key for server-side writes",
            "SUPABASE_DATABASE_URL": "Optional alias for DATABASE_URL",
            "BACKEND_URL": "Public URL of this server (used by Vercel proxy)",
            "STRIPE_SECRET_KEY": "Stripe secret key (required for billing)",
            "SMTP_HOST": "Default SMTP host (optional, can be set per-user)",
        }
        for var, desc in _required_env.items():
            if not os.environ.get(var):
                print(f"[startup] ERROR: Missing required env var {var} â€” {desc}")
                logging.error("[startup] Missing required env var %s â€” %s", var, desc)
        for var, desc in _optional_env.items():
            if not os.environ.get(var):
                print(f"[startup] WARNING: Optional env var {var} not set â€” {desc}")
        # â”€â”€ DB init â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if not supabase_settings.get("enabled"):
            raise RuntimeError(
                "Supabase runtime is required. Set SUPABASE_URL plus SUPABASE_KEY "
                "(or SUPABASE_SERVICE_ROLE_KEY / SUPABASE_PUBLISHABLE_KEY)."
            )
        if not supabase_settings.get("has_database_url"):
            raise RuntimeError(
                "Postgres runtime is required. Set DATABASE_URL or SUPABASE_DATABASE_URL to the Supabase Postgres connection string."
            )

        print("[startup] Initialising Supabase tables...")
        try:
            await run_in_threadpool(ensure_supabase_users_table, DEFAULT_CONFIG_PATH)
            print("[startup] Supabase users table OK")
        except Exception as exc:
            logging.warning("[startup] Supabase table init skipped (non-fatal): %s", exc)
            print(f"[startup] WARNING: Supabase table init skipped: {exc}")
        try:
            await run_in_threadpool(ensure_system_tables, DEFAULT_DB_PATH)
            print("[startup] Core system tables OK")
        except Exception as exc:
            logging.warning("[startup] System table init skipped (non-fatal): %s", exc)
            print(f"[startup] WARNING: System table init skipped: {exc}")
        try:
            await run_in_threadpool(ensure_dashboard_indexes_startup, DEFAULT_DB_PATH)
            print("[startup] Dashboard indexes OK")
        except Exception as exc:
            logging.warning("[startup] Dashboard index init skipped (non-fatal): %s", exc)
            print(f"[startup] WARNING: Dashboard index init skipped: {exc}")
        start_scheduler(app)
        if RUN_STARTUP_JOBS:
            launch_detached_task(lambda _app, _payload: run_autopilot_cycle(_app), app, {})
            launch_detached_task(lambda _app, _payload: run_monthly_credit_reset_cycle(_app), app, {})
            if AUTO_DRIP_DISPATCH_ENABLED:
                launch_detached_task(lambda _app, _payload: run_drip_dispatch_cycle(_app), app, {})
        print("[startup] Scheduler started â€” app ready")

    @app.on_event("shutdown")
    def shutdown_tasks() -> None:
        app.state.mailer_stop_event.set()
        stop_scheduler(app)
        _thread_pool.shutdown(wait=False, cancel_futures=True)

    @app.get("/api/health")
    def health() -> dict:
        settings = load_supabase_settings(DEFAULT_CONFIG_PATH)
        resolved_db_url = str(settings.get("resolved_database_url") or settings.get("database_url") or "").strip()
        db_host, parsed_port = _extract_db_host_port(resolved_db_url)
        return {
            "ok": True,
            "database": "supabase",
            "supabase_enabled": bool(settings.get("enabled")),
            "supabase_primary": bool(settings.get("primary_mode")),
            "has_database_url": bool(settings.get("has_database_url")),
            "db_host": db_host,
            "db_port": parsed_port,
        }

    @app.get("/api/heartbeat")
    def heartbeat() -> dict:
        return {
            "ok": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": "leadgen-api",
        }

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
                with pgdb.connect(DEFAULT_DB_PATH) as conn:
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
        cache_key = str(session_token)
        cached = _get_cached_api("config", cache_key, _CONFIG_CACHE_TTL)
        if cached is not None:
            return cached
        try:
            with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
        _, dev_threshold = _get_developer_webhook_settings(DEFAULT_CONFIG_PATH)
        openai_key = str(cfg.get("openai", {}).get("api_key", "") or "")
        smtp_accounts = load_user_smtp_accounts(session_token=session_token, db_path=DEFAULT_DB_PATH)
        billing = load_user_billing_context(session_token, allow_stripe_recovery=False)
        plan_key = _normalize_plan_key((billing or {}).get("plan_key"), fallback=DEFAULT_PLAN_KEY)
        custom_smtp_allowed = plan_key != "free"
        system_smtp_account = get_system_smtp_account(DEFAULT_CONFIG_PATH)
        mailer_cfg = cfg.get("mailer", {}) if isinstance(cfg, dict) else {}
        safe_accounts = _safe_smtp_accounts(smtp_accounts)
        first_smtp = smtp_accounts[0] if smtp_accounts else {}
        supabase_settings = load_supabase_settings(DEFAULT_CONFIG_PATH)
        result = {
            "openai_api_key": "***" if openai_key and openai_key != "YOUR_OPENAI_API_KEY" else "",
            "smtp_host": first_smtp.get("host", ""),
            "smtp_port": first_smtp.get("port", 587),
            "smtp_email": first_smtp.get("email", ""),
            "smtp_password_set": bool(str(first_smtp.get("password", "") or "").strip()),
            "smtp_accounts": safe_accounts,
            "smtp_custom_allowed": bool(custom_smtp_allowed),
            "system_smtp_enabled": bool(system_smtp_account),
            "system_smtp_sender": str(system_smtp_account.get("email", "") or ""),
            "system_smtp_limit": int(SYSTEM_SMTP_DEFAULT_SEND_LIMIT),
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
            "developer_webhook_url": str(cfg.get("developer_webhook_url", "") or ""),
            "developer_score_drop_threshold": dev_threshold,
            "auto_weekly_report_email": bool(cfg.get("auto_weekly_report_email", True)),
            "auto_monthly_report_email": bool(cfg.get("auto_monthly_report_email", True)),
            "proxy_url": str(cfg.get("proxy_url", "") or ""),
            "proxy_urls": "\n".join(cfg.get("proxy_urls") or []),
            "supabase_url": str(supabase_settings.get("url", "") or ""),
            "supabase_publishable_key": "" if STATELESS_SUPABASE_ONLY else str((cfg.get("supabase", {}) if isinstance(cfg, dict) else {}).get("publishable_key", "") or ""),
            "supabase_service_role_key_set": bool(supabase_settings.get("has_service_role")),
            "supabase_primary_mode": bool(supabase_settings.get("primary_mode", False)),
        }
        _set_cached_api("config", cache_key, result)
        return result

    @app.put("/api/config")
    def update_config(payload: ConfigUpdateRequest, request: Request) -> dict:
        session_token = require_authenticated_session(request)
        try:
            with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}

        smtp_update_requested = payload.smtp_accounts is not None or any(
            value is not None
            for value in [payload.smtp_host, payload.smtp_port, payload.smtp_email, payload.smtp_password]
        )
        if smtp_update_requested:
            billing = load_user_billing_context(session_token, allow_stripe_recovery=False)
            plan_key = _normalize_plan_key((billing or {}).get("plan_key"), fallback=DEFAULT_PLAN_KEY)
            if plan_key == "free":
                raise HTTPException(status_code=403, detail="Custom SMTP is available on paid plans only.")

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

        if payload.developer_webhook_url is not None:
            cfg["developer_webhook_url"] = payload.developer_webhook_url.strip()

        if payload.developer_score_drop_threshold is not None:
            cfg["developer_score_drop_threshold"] = max(0.0, min(10.0, float(payload.developer_score_drop_threshold)))

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
            if STATELESS_SUPABASE_ONLY:
                raise HTTPException(
                    status_code=400,
                    detail="Supabase connection settings are managed by Railway environment variables in stateless mode.",
                )
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

        _invalidate_api_cache("config")

        return {"status": "saved", **load_config_health(DEFAULT_CONFIG_PATH)}

    @app.post("/api/config/test-smtp")
    def test_smtp_connection(payload: SMTPTestRequest, request: Request) -> dict:
        session_token = require_authenticated_session(request)
        billing = load_user_billing_context(session_token, allow_stripe_recovery=False)
        plan_key = _normalize_plan_key((billing or {}).get("plan_key"), fallback=DEFAULT_PLAN_KEY)
        if plan_key == "free":
            raise HTTPException(status_code=403, detail="SMTP testing is available on paid plans only.")
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

    def _record_open_tracking_token(tracking_token: str) -> None:
        safe_token = str(tracking_token or "").strip()
        if safe_token:
            now_iso = utc_now_iso()
            _mark_email_communication_opened(tracking_id=safe_token)
            try:
                ensure_mailer_campaign_tables(DEFAULT_DB_PATH)
                with pgdb.connect(DEFAULT_DB_PATH) as conn:
                    conn.row_factory = pgdb.Row
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

    @app.get("/api/track/open/{token}")
    def track_mail_open_legacy(token: str) -> Response:
        _record_open_tracking_token(token)

        return Response(
            content=TRACKING_PIXEL_GIF,
            media_type="image/gif",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.get("/api/tracks/open/{tracking_id}")
    def track_mail_open(tracking_id: str) -> Response:
        _record_open_tracking_token(tracking_id)

        return Response(
            content=TRACKING_PIXEL_GIF,
            media_type="image/gif",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.get("/api/tracking/open/{email_id}")
    def track_mail_open_email_history(email_id: str) -> Response:
        safe_id = str(email_id or "").strip()
        if safe_id.isdigit():
            _mark_email_communication_opened(communication_id=int(safe_id))
        else:
            _record_open_tracking_token(safe_id)

        return Response(
            content=TRACKING_PIXEL_GIF,
            media_type="image/gif",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.get("/api/tracks/click/{link_id}")
    def track_mail_click(link_id: str, request: Request) -> RedirectResponse:
        safe_link_id = str(link_id or "").strip()
        target_url = str(request.query_params.get("url") or request.query_params.get("u") or "").strip()
        parsed = urlparse(target_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise HTTPException(status_code=400, detail="Invalid redirect target.")

        if safe_link_id:
            try:
                ensure_mailer_campaign_tables(DEFAULT_DB_PATH)
                with pgdb.connect(DEFAULT_DB_PATH) as conn:
                    conn.row_factory = pgdb.Row
                    lead_row = conn.execute(
                        """
                        SELECT id, email, COALESCE(NULLIF(user_id, ''), 'legacy') AS user_id
                        FROM leads
                        WHERE open_tracking_token = ?
                        LIMIT 1
                        """,
                        (safe_link_id,),
                    ).fetchone()
                if lead_row is not None:
                    record_mailer_campaign_event(
                        DEFAULT_DB_PATH,
                        str(lead_row["user_id"] or "legacy"),
                        {
                            "lead_id": int(lead_row["id"]),
                            "email": str(lead_row["email"] or "").strip(),
                            "event_type": "click",
                            "metadata": {
                                "source": "click_redirect",
                                "token": safe_link_id,
                                "target_url": target_url,
                            },
                        },
                    )
            except Exception:
                logging.debug("Failed to update click tracking for token=%s", safe_link_id)

        return RedirectResponse(url=target_url, status_code=307)

    def _send_admin_reply_alert_email(
        *,
        lead_id: int,
        lead_email: str,
        subject_line: Optional[str],
        reason: Optional[str],
    ) -> None:
        account = get_system_smtp_account(DEFAULT_CONFIG_PATH)
        if not account:
            logging.info("Reply alert email skipped: system SMTP is not configured.")
            return

        safe_lead_email = str(lead_email or "").strip() or "unknown"
        safe_subject_line = str(subject_line or "").strip() or "(no subject)"
        safe_reason = str(reason or "").strip()
        subject = f"Reply detected: lead #{int(lead_id)}"
        text_body = (
            "A lead replied to a cold email.\n\n"
            f"Lead ID: {int(lead_id)}\n"
            f"Lead Email: {safe_lead_email}\n"
            f"Subject: {safe_subject_line}\n"
            f"Reason: {safe_reason or 'n/a'}\n\n"
            "Status was automatically set to 'replied' and the sequence was stopped."
        )

        for admin_email in sorted(DEFAULT_ADMIN_EMAILS):
            try:
                send_auth_email(account, admin_email, subject, text_body)
            except Exception as exc:
                logging.warning("Failed to send reply alert email to %s: %s", admin_email, exc)

    def _store_reply_dashboard_notification(*, lead_id: int, lead_email: str) -> None:
        payload = {
            "active": True,
            "message": f"Reply detected from {str(lead_email or '').strip() or 'a lead'} (lead #{int(lead_id)}). Jump in manually to close.",
            "updated_at": utc_now_iso(),
            "source": "reply_webhook",
        }
        try:
            set_runtime_value(DEFAULT_DB_PATH, "reply_detection_latest", json.dumps(payload, ensure_ascii=False))
        except Exception as exc:
            logging.warning("Failed to store reply dashboard notification: %s", exc)

    def _handle_reply_webhook(payload: IncomingEmailWebhookRequest, request: Request, *, source: str) -> dict:
        ensure_system_tables(DEFAULT_DB_PATH)
        cfg: dict[str, Any] = {}
        try:
            with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as handle:
                loaded = json.load(handle)
                cfg = loaded if isinstance(loaded, dict) else {}
        except Exception:
            cfg = {}

        expected_secret = str(
            os.environ.get("SNIPED_INBOUND_WEBHOOK_SECRET")
            or cfg.get("inbound_email_webhook_secret", "")
            or ""
        ).strip()
        if not expected_secret:
            raise HTTPException(status_code=503, detail="Inbound webhook secret is not configured.")

        provided_secret = str(
            request.headers.get("x-sniped-webhook-secret")
            or request.headers.get("x-webhook-secret")
            or ""
        ).strip()
        if not provided_secret or not hmac.compare_digest(provided_secret, expected_secret):
            raise HTTPException(status_code=401, detail="Invalid webhook secret.")

        event_type = _normalize_campaign_event_type(payload.event_type)
        if event_type != "reply":
            return {"status": "ignored", "reason": f"unsupported_event:{event_type}"}

        lead_row: Optional[pgdb.Row] = None
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            if payload.lead_id is not None:
                lead_row = conn.execute(
                    "SELECT id, email, COALESCE(NULLIF(user_id, ''), 'legacy') AS user_id FROM leads WHERE id = ? LIMIT 1",
                    (int(payload.lead_id),),
                ).fetchone()

            if lead_row is None and str(payload.thread_token or "").strip():
                lead_row = conn.execute(
                    """
                    SELECT id, email, COALESCE(NULLIF(user_id, ''), 'legacy') AS user_id
                    FROM leads
                    WHERE open_tracking_token = ?
                    LIMIT 1
                    """,
                    (str(payload.thread_token or "").strip(),),
                ).fetchone()

            if lead_row is None and str(payload.email or "").strip():
                lead_row = conn.execute(
                    """
                    SELECT id, email, COALESCE(NULLIF(user_id, ''), 'legacy') AS user_id
                    FROM leads
                    WHERE LOWER(COALESCE(email, '')) = ?
                    ORDER BY COALESCE(last_contacted_at, sent_at, created_at) DESC, id DESC
                    LIMIT 1
                    """,
                    (str(payload.email or "").strip().lower(),),
                ).fetchone()

        if lead_row is None:
            raise HTTPException(status_code=404, detail="No matching lead for inbound reply.")

        metadata = dict(payload.metadata or {})
        metadata.setdefault("source", source)
        if payload.thread_token:
            metadata.setdefault("thread_token", str(payload.thread_token).strip())
        if payload.from_email:
            metadata.setdefault("from_email", str(payload.from_email).strip())
        if payload.body_html:
            metadata.setdefault("body_html", str(payload.body_html))
        if payload.body_text:
            metadata.setdefault("body_text", str(payload.body_text))

        event = record_mailer_campaign_event(
            DEFAULT_DB_PATH,
            user_id=str(lead_row["user_id"] or "legacy"),
            payload={
                "lead_id": int(lead_row["id"]),
                "email": str(payload.email or lead_row["email"] or "").strip(),
                "event_type": "reply",
                "subject_line": str(payload.subject_line or "").strip() or None,
                "body_text": str(payload.body_text or "").strip() or None,
                "reason": str(payload.reason or "").strip() or None,
                "metadata": metadata,
            },
        )
        maybe_sync_supabase(DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH)
        _invalidate_leads_cache()
        _store_reply_dashboard_notification(
            lead_id=int(lead_row["id"]),
            lead_email=str(payload.email or lead_row["email"] or "").strip(),
        )
        _send_admin_reply_alert_email(
            lead_id=int(lead_row["id"]),
            lead_email=str(payload.email or lead_row["email"] or "").strip(),
            subject_line=str(payload.subject_line or "").strip() or None,
            reason=str(payload.reason or "").strip() or None,
        )

        return {
            "status": "recorded",
            "lead_id": int(lead_row["id"]),
            "pipeline_stage": "Replied",
            "event": event,
        }

    @app.post("/api/webhooks/incoming-email")
    def incoming_email_webhook(payload: IncomingEmailWebhookRequest, request: Request) -> dict:
        return _handle_reply_webhook(payload, request, source="incoming_email_webhook")

    @app.post("/api/webhooks/reply")
    def reply_webhook(payload: IncomingEmailWebhookRequest, request: Request) -> dict:
        return _handle_reply_webhook(payload, request, source="reply_webhook")

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
                "error": "Set supabase.url and supabase key (service role or publishable) in environment settings or env.",
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

    @app.get("/api/db-audit")
    def db_audit(request: Request) -> dict:
        """Diagnostic endpoint: count all leads rows in Supabase grouped by user_id.
        Returns total row count (no user filter) plus per-user breakdown.
        Useful to confirm data is actually reaching Supabase regardless of UI filters.
        """
        result: dict = {
            "total_rows": 0,
            "rows_per_user": {},
            "supabase_enabled": False,
            "supabase_primary": False,
            "local_db_total": 0,
            "local_db_for_caller": 0,
        }

        result["supabase_enabled"] = is_supabase_auth_enabled(DEFAULT_CONFIG_PATH)
        result["supabase_primary"] = is_supabase_primary_enabled(DEFAULT_CONFIG_PATH)

        # ── Local SQLite count ─────────────────────────────────────────
        try:
            engine = pg_get_engine(str(DEFAULT_DB_PATH))
            with engine.connect() as conn:
                result["local_db_total"] = int(conn.execute(text("SELECT COUNT(*) FROM leads")).scalar() or 0)
                try:
                    caller_uid = require_current_user_id(request)
                    result["local_db_for_caller"] = int(
                        conn.execute(
                            text("SELECT COUNT(*) FROM leads WHERE CAST(user_id AS TEXT) = :uid"),
                            {"uid": str(caller_uid)},
                        ).scalar() or 0
                    )
                    result["caller_user_id"] = caller_uid
                except Exception:
                    pass
                rows_by_user = conn.execute(
                    text(
                        "SELECT CAST(user_id AS TEXT) AS uid, COUNT(*) AS cnt "
                        "FROM leads GROUP BY CAST(user_id AS TEXT) ORDER BY cnt DESC LIMIT 20"
                    )
                ).fetchall()
                result["local_rows_per_user"] = {r[0]: r[1] for r in rows_by_user}
        except Exception as local_exc:
            result["local_db_error"] = str(local_exc)

        # ── Supabase count (no user filter) ────────────────────────────
        if result["supabase_enabled"]:
            try:
                sb = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
                if sb is not None:
                    all_ids = supabase_select_rows(sb, "leads", columns="user_id")
                    result["total_rows"] = len(all_ids)
                    user_counts: dict[str, int] = {}
                    for r in all_ids:
                        uid = str(r.get("user_id") or "")
                        user_counts[uid] = user_counts.get(uid, 0) + 1
                    result["rows_per_user"] = dict(sorted(user_counts.items(), key=lambda x: -x[1])[:20])
            except Exception as sb_exc:
                result["supabase_error"] = str(sb_exc)

        return result

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
            f"Sniped Monthly Report â€” {summary.get('month_label', 'Current Month')}",
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
        def _has_usable_recommendations(payload: Any) -> bool:
            if not isinstance(payload, dict):
                return False
            recs = payload.get("recommendations")
            if not isinstance(recs, list):
                return False
            for item in recs:
                if isinstance(item, dict) and str(item.get("keyword") or "").strip():
                    return True
            return False

        def _ensure_recommendation_shape(payload: dict[str, Any], selected_country: str) -> dict[str, Any]:
            recommendations = payload.get("recommendations") if isinstance(payload, dict) else []
            if not isinstance(recommendations, list):
                recommendations = []
            normalized_country = normalize_country_value(
                payload.get("selected_country_code") if isinstance(payload, dict) else None,
                selected_country,
            )
            usable = [
                item for item in recommendations
                if isinstance(item, dict) and str(item.get("keyword") or "").strip()
            ]
            if not usable:
                usable = default_market_recommendations(normalized_country)
            else:
                normalized_usable: list[dict[str, Any]] = []
                for item in usable:
                    normalized_item = dict(item)
                    normalized_item["country_code"] = normalize_country_value(
                        item.get("country_code"),
                        normalized_country,
                    )
                    if not str(normalized_item.get("location") or "").strip():
                        normalized_item["location"] = normalized_country
                    normalized_usable.append(normalized_item)
                usable = normalized_usable
            payload["recommendations"] = usable[:3]
            top_pick = payload.get("top_pick") if isinstance(payload, dict) else None
            if not isinstance(top_pick, dict) or not str(top_pick.get("keyword") or "").strip():
                payload["top_pick"] = usable[0]
            payload["selected_country_code"] = normalized_country
            payload.setdefault("generated_at", utc_now_iso())
            return payload

        session_token = ""
        user_id = "anonymous"
        billing_context: dict[str, Any] = {}
        credits_balance = 0
        credits_limit = DEFAULT_MONTHLY_CREDIT_LIMIT
        plan_key = DEFAULT_PLAN_KEY
        is_free_plan = True

        # Keep Market Intelligence available even if session/billing lookup is temporarily failing.
        try:
            auth_header = str(request.headers.get("Authorization", "") or "").strip()
            if auth_header.lower().startswith("bearer "):
                session_token = auth_header[7:].strip()
            if session_token and _session_token_exists(session_token):
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
        except Exception as exc:
            logging.warning("recommend-niche auth fallback activated: %s", exc)
        selected_country_code = normalize_country_value(
            request.query_params.get("country") or request.query_params.get("country_code"),
            None,
        )
        search_context = str(
            request.query_params.get("context_keyword")
            or request.query_params.get("keyword")
            or ""
        ).strip()
        force_refresh = str(request.query_params.get("refresh") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
        refresh_window_days = _niche_recommendation_refresh_window_days(is_free_plan)
        refresh_window_seconds = _niche_recommendation_refresh_window_seconds(is_free_plan)
        refresh_window_hours = round(refresh_window_seconds / 3600, 2)

        if not force_refresh:
            cached_result = _get_cached_niche_recommendation(user_id, selected_country_code)
            if _has_usable_recommendations(cached_result):
                cached_result = _ensure_recommendation_shape(dict(cached_result), selected_country_code)
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
        if _has_usable_recommendations(stored_result) and (is_free_plan or not force_refresh):
            stored_result = _ensure_recommendation_shape(dict(stored_result), selected_country_code)
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

        result = get_niche_recommendation(
            DEFAULT_DB_PATH,
            DEFAULT_CONFIG_PATH,
            country_code=selected_country_code,
            user_id=user_id,
            search_context=search_context,
        )
        if not isinstance(result, dict):
            result = {
                "generated_at": utc_now_iso(),
                "recommendations": [],
                "top_pick": {},
                "selected_country_code": selected_country_code,
            }
        result = _ensure_recommendation_shape(result, selected_country_code)
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

    @app.get("/api/ai/market-intelligence")
    def ai_market_intelligence(request: Request) -> dict:
        selected_country_code = normalize_country_value(
            request.query_params.get("country") or request.query_params.get("country_code"),
            None,
        )
        ai_signals_state = load_ai_signals_runtime_state(DEFAULT_DB_PATH)
        ai_key_configured = has_any_ai_api_key(DEFAULT_CONFIG_PATH)

        if not bool(ai_signals_state.get("enabled", True)):
            disabled_payload = build_market_intelligence_mock_response(
                selected_country_code,
                maintenance=True,
                maintenance_message="System Maintenance: AI Signals are temporarily disabled by admin.",
            )
            disabled_payload.update(
                {
                    "ai_signals_enabled": False,
                    "ai_key_configured": ai_key_configured,
                }
            )
            return disabled_payload

        if not ai_key_configured:
            raise HTTPException(
                status_code=503,
                detail=(
                    "AZURE_OPENAI_API_KEY is not configured for Market Intelligence. "
                    "Set AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, and AZURE_OPENAI_DEPLOYMENT_NAME in Railway environment variables."
                ),
            )

        try:
            response = recommend_niche(request)
            if isinstance(response, dict):
                response["ai_signals_enabled"] = True
                response["ai_key_configured"] = True
                response.setdefault("maintenance", False)
                response.setdefault("maintenance_message", "")
            return response
        except Exception as exc:
            logging.warning("Market intelligence endpoint fallback to mock: %s", exc)
            raise HTTPException(
                status_code=502,
                detail=(
                    "Market intelligence upstream failed. Check Railway logs for "
                    "'Market intelligence endpoint fallback to mock'."
                ),
            )

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
            client = get_supabase_request_client(request, DEFAULT_CONFIG_PATH)
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
        with pgdb.connect(db_path) as conn:
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

    @app.post("/api/revenue/won-deal")
    @auth_required
    def add_won_deal_revenue(payload: WonDealRevenueRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        currency = str(payload.currency or "EUR").upper().strip() or "EUR"
        note = str(payload.note or "").strip()
        now_iso = utc_now_iso()

        lead_name: Optional[str] = None
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_request_client(request, DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            lead_rows = client.table("leads").select("id,user_id,business_name").eq("id", int(payload.lead_id)).limit(1).execute().data or []
            if not lead_rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            lead_name = str(lead_rows[0].get("business_name") or "").strip() or None

            if supabase_table_available(DEFAULT_CONFIG_PATH, "revenue_logs"):
                client.table("revenue_logs").insert(
                    {
                        "user_id": user_id,
                        "lead_id": int(payload.lead_id),
                        "amount": float(payload.amount),
                        "currency": currency,
                        "event_type": "won_stage_manual",
                        "note": note or None,
                        "created_at": now_iso,
                    }
                ).execute()

            inserted = client.table("revenue_log").insert(
                {
                    "user_id": user_id,
                    "amount": float(payload.amount),
                    "service_type": "Won Deal",
                    "lead_name": lead_name,
                    "lead_id": int(payload.lead_id),
                    "is_recurring": 0,
                    "date": now_iso,
                }
            ).execute().data or []

            return {
                "id": int(inserted[0].get("id")) if inserted else None,
                "lead_id": int(payload.lead_id),
                "lead_name": lead_name,
                "amount": float(payload.amount),
                "currency": currency,
                "event_type": "won_stage_manual",
            }

        db_path = DEFAULT_DB_PATH
        ensure_revenue_log_table(db_path)
        ensure_revenue_logs_table(db_path)
        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            lead_row = conn.execute(
                "SELECT id, user_id, business_name FROM leads WHERE id = ? AND user_id = ? LIMIT 1",
                (int(payload.lead_id), user_id),
            ).fetchone()
            if lead_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            lead_name = str(lead_row["business_name"] or "").strip() or None

            conn.execute(
                """
                INSERT INTO revenue_logs (user_id, lead_id, amount, currency, event_type, note, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    int(payload.lead_id),
                    float(payload.amount),
                    currency,
                    "won_stage_manual",
                    note or None,
                    now_iso,
                ),
            )
            cursor = conn.execute(
                """
                INSERT INTO revenue_log (user_id, amount, service_type, lead_name, lead_id, is_recurring, date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    float(payload.amount),
                    "Won Deal",
                    lead_name,
                    int(payload.lead_id),
                    0,
                    now_iso,
                ),
            )
            conn.commit()

        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        return {
            "id": cursor.lastrowid,
            "lead_id": int(payload.lead_id),
            "lead_name": lead_name,
            "amount": float(payload.amount),
            "currency": currency,
            "event_type": "won_stage_manual",
        }

    @app.get("/api/revenue")
    @auth_required
    def get_revenue(request: Request, limit: int = Query(10, ge=1, le=100)) -> dict:
        user_id = require_current_user_id(request)
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_request_client(request, DEFAULT_CONFIG_PATH)
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
        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
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

    @app.get("/api/tasks/{task_id}")
    def task_by_id(task_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        reconcile_orphaned_active_tasks(app, DEFAULT_DB_PATH)
        task = fetch_task_by_id(DEFAULT_DB_PATH, int(task_id), user_id=user_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        status = str(task.get("status") or "idle").lower()
        return {
            **task,
            "running": status in ACTIVE_TASK_STATUSES,
            "result": task.get("result") if isinstance(task.get("result"), dict) else {},
            "status": task.get("status") or "idle",
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
            client = get_supabase_request_client(request, DEFAULT_CONFIG_PATH)
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

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
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
        debug_all: bool = Query(default=False),
    ) -> dict:
        user_id = require_current_user_id(request)
        if debug_all:
            logging.warning("[/api/leads] Ignoring client-provided debug_all for user_id=%s", user_id)
        debug_all = False
        logging.debug(
            "[/api/leads] user_id=%s status=%s search=%s sort=%s quick_filter=%s include_blacklisted=%s page=%s limit=%s",
            user_id,
            status,
            search,
            sort,
            quick_filter,
            include_blacklisted,
            page,
            limit,
        )
        page_size = max(1, min(int(limit or 50), 200))
        page_number = max(1, int(page or 1))
        offset = max(0, (page_number - 1) * page_size)
        normalized_status = str(status or "").strip().lower()
        normalized_search = str(search or "").strip().lower()
        normalized_sort = str(sort or "recent").strip().lower() or "recent"
        normalized_quick_filter = str(quick_filter or "all").strip().lower() or "all"

        cache_scope = f"{str(DEFAULT_DB_PATH)}:{int(is_supabase_primary_enabled(DEFAULT_CONFIG_PATH))}"
        cache_key = f"{cache_scope}:{user_id}:{page_size}:{page_number}:{normalized_status}:{normalized_search}:{normalized_sort}:{normalized_quick_filter}:{int(include_blacklisted)}:{int(debug_all)}"
        cached = _get_cached_leads(cache_key)
        if cached is not None:
            return cached

        def _json_safe_rows(raw_rows: list[dict]) -> list[dict]:
            # Ensure every value is JSON-serializable for FastAPI responses.
            return json.loads(json.dumps(raw_rows, default=str))

        def _quick_filter_sql_clause(quick_filter_value: str) -> str:
            if quick_filter_value == "qualified":
                return "(COALESCE(ai_score, 0) >= 7 OR LOWER(COALESCE(status, '')) IN ('queued_mail','emailed','interested','replied','meeting set','zoom scheduled','closed','paid','qualified_not_interested','qualified not interested'))"
            if quick_filter_value == "not_qualified":
                return "(COALESCE(ai_score, 0) < 7 AND LOWER(COALESCE(status, '')) NOT IN ('queued_mail','emailed','interested','replied','meeting set','zoom scheduled','closed','paid','qualified_not_interested','qualified not interested'))"
            if quick_filter_value == "mailed":
                return "(sent_at IS NOT NULL OR last_contacted_at IS NOT NULL OR NULLIF(last_sender_email, '') IS NOT NULL OR LOWER(COALESCE(status, '')) IN ('emailed','interested','replied','meeting set','zoom scheduled','closed','paid'))"
            if quick_filter_value == "opened":
                return "(COALESCE(open_count, 0) > 0 OR first_opened_at IS NOT NULL OR last_opened_at IS NOT NULL)"
            if quick_filter_value == "replied":
                return "(reply_detected_at IS NOT NULL OR LOWER(COALESCE(status, '')) IN ('replied','interested','meeting set','zoom scheduled','closed','paid','qualified_not_interested','qualified not interested'))"
            return ""

        def _post_process_rows(raw_rows: list[dict]) -> list[dict]:
            normalized_rows: list[dict] = []
            for raw in raw_rows:
                item = dict(raw)
                item["insecure_site"] = bool(item.get("insecure_site"))
                item.setdefault("enrichment_status", "pending")
                normalized_rows.append(_augment_lead_with_deep_intelligence(item))

            lead_ids = [int(row.get("id")) for row in normalized_rows if str(row.get("id") or "").isdigit()]
            if not lead_ids:
                return normalized_rows

            placeholders = ",".join(["?"] * len(lead_ids))
            history_map: dict[int, list[float]] = {int(lead_id): [] for lead_id in lead_ids}
            try:
                with pgdb.connect(DEFAULT_DB_PATH) as trend_conn:
                    trend_conn.row_factory = pgdb.Row
                    trend_rows = trend_conn.execute(
                        f"""
                        SELECT lead_id, seo_score, performance_score
                        FROM lead_history
                        WHERE user_id = ? AND lead_id IN ({placeholders})
                        ORDER BY captured_at ASC, id ASC
                        """,
                        [str(user_id or "legacy"), *lead_ids],
                    ).fetchall()
                for row in trend_rows:
                    lead_id = int(row["lead_id"])
                    if lead_id not in history_map:
                        continue
                    seo_val = _to_float_or_none(row["seo_score"])
                    perf_val = _to_float_or_none(row["performance_score"])
                    if seo_val is None and perf_val is None:
                        continue
                    if seo_val is not None and perf_val is not None:
                        history_map[lead_id].append(round((seo_val + perf_val) / 2.0, 1))
                    else:
                        history_map[lead_id].append(round(float(seo_val if seo_val is not None else perf_val), 1))
            except Exception:
                history_map = {int(lead_id): [] for lead_id in lead_ids}

            active_share_ids: set[int] = set()
            try:
                with pgdb.connect(DEFAULT_DB_PATH) as share_conn:
                    share_conn.row_factory = pgdb.Row
                    share_rows = share_conn.execute(
                        f"""
                        SELECT lead_id
                        FROM lead_reports
                        WHERE user_id = ? AND active = 1 AND lead_id IN ({placeholders})
                        GROUP BY lead_id
                        """,
                        [str(user_id or "legacy"), *lead_ids],
                    ).fetchall()
                active_share_ids = {int(row["lead_id"]) for row in share_rows}
            except Exception:
                active_share_ids = set()

            for item in normalized_rows:
                lead_id = int(item.get("id") or 0)
                seo_score, performance_score = _extract_scores_from_lead_payload(item)
                item["seo_score"] = seo_score
                item["performance_score"] = performance_score
                current_point = None
                if seo_score is not None and performance_score is not None:
                    current_point = round((float(seo_score) + float(performance_score)) / 2.0, 1)
                elif seo_score is not None:
                    current_point = float(seo_score)
                elif performance_score is not None:
                    current_point = float(performance_score)

                points = list(history_map.get(lead_id) or [])
                if current_point is not None:
                    points.append(round(current_point, 1))
                trend_points = _build_score_trend_points(points, limit=LEAD_TREND_POINTS_LIMIT)
                trend_direction, trend_delta = _resolve_trend_direction(trend_points)
                item["score_trend_points"] = trend_points
                item["score_trend_direction"] = trend_direction
                item["score_trend_delta"] = trend_delta
                item["has_active_report_share"] = lead_id in active_share_ids
            return normalized_rows

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            # SQL-only path: filtering, sorting, and pagination are performed by PostgreSQL.
            try:
                engine = pg_get_engine(str(DEFAULT_DB_PATH))
                where_clauses = ["CAST(user_id AS TEXT) = :uid"]
                sql_params: dict[str, Any] = {"uid": str(user_id), "limit": page_size, "offset": offset}

                if normalized_status:
                    where_clauses.append("LOWER(COALESCE(status, 'pending')) = :status")
                    sql_params["status"] = normalized_status

                if not include_blacklisted:
                    where_clauses.append("LOWER(COALESCE(status, 'pending')) NOT IN ('blacklisted', 'skipped (unsubscribed)')")

                if normalized_search:
                    where_clauses.append(
                        "(" 
                        "LOWER(COALESCE(business_name, '')) LIKE :search OR "
                        "LOWER(COALESCE(contact_name, '')) LIKE :search OR "
                        "LOWER(COALESCE(email, '')) LIKE :search OR "
                        "LOWER(COALESCE(website_url, '')) LIKE :search OR "
                        "LOWER(COALESCE(address, '')) LIKE :search OR "
                        "LOWER(COALESCE(search_keyword, '')) LIKE :search OR "
                        "LOWER(COALESCE(CAST(tech_stack AS TEXT), '')) LIKE :search"
                        ")"
                    )
                    sql_params["search"] = f"%{normalized_search}%"

                if normalized_quick_filter not in {"", "all"}:
                    quick_clause = _quick_filter_sql_clause(normalized_quick_filter)
                    if quick_clause:
                        where_clauses.append(quick_clause)

                where_fragment = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

                if normalized_sort == "name":
                    order_clause = "ORDER BY LOWER(COALESCE(business_name, '')) ASC, id DESC"
                elif normalized_sort in {"score", "best"}:
                    order_clause = "ORDER BY COALESCE(ai_score, 0) DESC, COALESCE(created_at, scraped_at) DESC, id DESC"
                else:
                    order_clause = "ORDER BY COALESCE(created_at, scraped_at) DESC, id DESC"

                count_sql = text(f"SELECT COUNT(*) AS c FROM leads{where_fragment}")
                page_sql = text(f"SELECT * FROM leads{where_fragment} {order_clause} LIMIT :limit OFFSET :offset")

                with engine.connect() as conn:
                    total = int(conn.execute(count_sql, sql_params).scalar() or 0)
                    raw_rows = conn.execute(page_sql, sql_params).fetchall()

                page_items = _json_safe_rows(_post_process_rows([dict(r._mapping) for r in raw_rows]))
                result = {
                    "count": len(page_items),
                    "total": total,
                    "page": page_number,
                    "page_size": page_size,
                    "has_more": offset + len(page_items) < total,
                    "items": page_items,
                    "leads": page_items,
                    "source": "sql-fastpath",
                }
                _set_cached_leads(cache_key, result)
                return result
            except Exception as sql_fast_exc:
                if _is_db_capacity_error(sql_fast_exc):
                    record_pool_saturation_event(sql_fast_exc)
                    logging.warning("[/api/leads] DB connection pool is saturated; returning 503")
                    raise HTTPException(status_code=503, detail="Database is temporarily busy. Please retry in a few seconds.")
                logging.exception("[/api/leads] SQL-only Supabase query failed")
                raise HTTPException(status_code=500, detail=f"Leads query failed: {sql_fast_exc}")

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
                google_claimed,
                linkedin_url,
                instagram_url,
                facebook_url,
                tiktok_url,
                ig_link,
                fb_link,
                has_pixel,
                tech_stack,
                rating,
                review_count,
                address,
                search_keyword,
                insecure_site,
                main_shortcoming,
                ai_description,
                ai_score,
                qualification_score,
                seo_score,
                performance_score,
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
            -- user_id is enforced in composed where_fragment for non-debug requests
        """
        where_clauses: list[str] = []
        params: list[Any] = []

        if not debug_all:
            where_clauses.append("user_id = ?")
            params.append(user_id)

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
                "LOWER(COALESCE(search_keyword, '')) LIKE ? OR "
                "LOWER(COALESCE(tech_stack, '')) LIKE ?"
                ")"
            )
            params.extend([search_like] * 7)

        if normalized_quick_filter not in {"", "all"}:
            quick_clause = _quick_filter_sql_clause(normalized_quick_filter)
            if quick_clause:
                where_clauses.append(quick_clause)

        order_clause = "COALESCE(created_at, scraped_at) DESC, id DESC"
        if normalized_sort == "name":
            order_clause = "LOWER(COALESCE(business_name, '')) ASC, id DESC"
        elif normalized_sort in {"score", "best"}:
            order_clause = "COALESCE(ai_score, 0) DESC, COALESCE(created_at, scraped_at) DESC, id DESC"

        where_sql = " AND ".join(where_clauses)
        where_fragment = f" WHERE {where_sql}" if where_sql else ""
        count_query = f"SELECT COUNT(*) FROM leads{where_fragment}"
        query = f"{select_clause}{where_fragment} ORDER BY {order_clause} LIMIT ? OFFSET ?"

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            total = int(conn.execute(count_query, params).fetchone()[0] or 0)
            rows = conn.execute(query, [*params, page_size, offset]).fetchall()

        page_items = _json_safe_rows(_post_process_rows([dict(row) for row in rows]))
        result = {
            "count": len(page_items),
            "total": total,
            "page": page_number,
            "page_size": page_size,
            "has_more": offset + len(page_items) < total,
            "items": page_items,
            "leads": page_items,
        }
        _set_cached_leads(cache_key, result)
        return result

    @app.get("/api/leads/{lead_id}/report", response_class=HTMLResponse)
    def get_lead_gap_report(lead_id: int, request: Request) -> HTMLResponse:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                """
                SELECT id, user_id, business_name, contact_name, website_url, insecure_site, main_shortcoming,
                       enrichment_data, seo_score, performance_score
                FROM leads
                WHERE id = ?
                """,
                (lead_id,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Lead not found")
        lead = dict(row)
        if str(lead.get("user_id") or "") != str(user_id):
            raise HTTPException(status_code=403, detail="Forbidden")

        seo_score, performance_score = _extract_scores_from_lead_payload(lead)
        report_html = _build_gap_report_html(
            lead=lead,
            seo_score=seo_score,
            performance_score=performance_score,
            report_title=f"{lead.get('business_name') or 'Lead'} - Gap Report",
        )
        return HTMLResponse(
            content=report_html,
            status_code=200,
            headers={"X-Robots-Tag": "noindex, nofollow, noarchive"},
        )

    @app.post("/api/leads/{lead_id}/report/share")
    def generate_shareable_lead_report(lead_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                """
                SELECT id, user_id, business_name, contact_name, website_url, insecure_site, main_shortcoming,
                       enrichment_data, seo_score, performance_score
                FROM leads
                WHERE id = ?
                """,
                (lead_id,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Lead not found")

        lead = dict(row)
        if str(lead.get("user_id") or "") != str(user_id):
            raise HTTPException(status_code=403, detail="Forbidden")

        seo_score, performance_score = _extract_scores_from_lead_payload(lead)
        report_html = _build_gap_report_html(
            lead=lead,
            seo_score=seo_score,
            performance_score=performance_score,
            report_title=f"{lead.get('business_name') or 'Lead'} - Shared Gap Report",
        )
        token = secrets.token_urlsafe(24)
        created_at = utc_now_iso()

        with pgdb.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO lead_reports (lead_id, user_id, token, report_html, created_at, active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (int(lead_id), str(user_id), token, report_html, created_at),
            )
            conn.commit()

        base = str(request.base_url).rstrip("/")
        share_url = f"{base}/public/report/{token}"
        return {
            "status": "ok",
            "lead_id": int(lead_id),
            "token": token,
            "share_url": share_url,
            "created_at": created_at,
        }

    @app.delete("/api/leads/{lead_id}/report/share")
    def revoke_shareable_lead_report(lead_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            lead_row = conn.execute(
                """
                SELECT id, user_id
                FROM leads
                WHERE id = ?
                """,
                (lead_id,),
            ).fetchone()
            if lead_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_row["user_id"] or "") != str(user_id):
                raise HTTPException(status_code=403, detail="Forbidden")

            active_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM lead_reports
                WHERE lead_id = ? AND user_id = ? AND active = 1
                """,
                (int(lead_id), str(user_id)),
            ).fetchone()
            active_before = int((active_row["total"] if active_row else 0) or 0)

            conn.execute(
                """
                UPDATE lead_reports
                SET active = 0
                WHERE lead_id = ? AND user_id = ? AND active = 1
                """,
                (int(lead_id), str(user_id)),
            )
            conn.commit()

        return {
            "status": "ok",
            "lead_id": int(lead_id),
            "revoked": active_before,
            "active": False,
        }

    @app.get("/public/report/{token}", response_class=HTMLResponse)
    def public_lead_gap_report(token: str) -> HTMLResponse:
        normalized_token = str(token or "").strip()
        if not normalized_token:
            raise HTTPException(status_code=404, detail="Report not found")

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                """
                SELECT report_html, active
                FROM lead_reports
                WHERE token = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (normalized_token,),
            ).fetchone()
        if row is None or int(row["active"] or 0) != 1:
            raise HTTPException(status_code=404, detail="Report not found")
        content = str(row["report_html"] or "").strip()
        if not content:
            raise HTTPException(status_code=404, detail="Report not found")

        return HTMLResponse(
            content=content,
            status_code=200,
            headers={"X-Robots-Tag": "noindex, nofollow, noarchive"},
        )

    @app.post("/api/leads/ai-filter")
    def ai_filter_leads(payload: AILeadFilterRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        prompt = str(payload.prompt or "").strip()
        if len(prompt) < 2:
            raise HTTPException(status_code=400, detail="Prompt is too short")

        llm_clause = _ai_prompt_sql_where_clause(prompt)
        sanitized_clause = _sanitize_ai_where_clause(llm_clause or "") if llm_clause else None
        if not sanitized_clause:
            return {
                "prompt": prompt,
                "source": "llm-unavailable",
                "sql_condition": "",
                "total_candidates": 0,
                "matched_count": 0,
                "lead_ids": [],
                "preview_businesses": [],
                "assistant_message": AI_FILTER_EMPTY_MESSAGE,
            }

        page_size = min(max(int(payload.limit or 5000), 1), 10000)
        where_clauses = ["CAST(user_id AS TEXT) = :uid", f"({sanitized_clause})"]
        sql_params: dict[str, Any] = {"uid": str(user_id), "limit": page_size, "offset": 0}

        if not bool(payload.include_blacklisted):
            where_clauses.append("LOWER(COALESCE(status, 'pending')) NOT IN ('blacklisted', 'skipped (unsubscribed)')")

        where_fragment = " AND ".join(where_clauses)
        count_sql = text(f"SELECT COUNT(*) AS c FROM leads WHERE {where_fragment}")
        rows_sql = text(
            f"""
            SELECT id, business_name
            FROM leads
            WHERE {where_fragment}
            ORDER BY COALESCE(ai_score, 0) DESC, COALESCE(created_at, scraped_at) DESC, id DESC
            LIMIT :limit OFFSET :offset
            """
        )

        try:
            engine = pg_get_engine(str(DEFAULT_DB_PATH))
            with engine.connect() as conn:
                matched_count = int(conn.execute(count_sql, sql_params).scalar() or 0)
                matched_rows = conn.execute(rows_sql, sql_params).fetchall()
        except Exception as exc:
            logging.exception("[/api/leads/ai-filter] SQL execution failed")
            raise HTTPException(status_code=500, detail=f"AI filter query failed: {exc}")

        lead_ids = [int(row._mapping.get("id")) for row in matched_rows if row._mapping.get("id") is not None]
        preview_names = [str(row._mapping.get("business_name") or "Unnamed").strip() for row in matched_rows[:5]]
        assistant_message = (
            AI_FILTER_EMPTY_MESSAGE
            if matched_count <= 0
            else f"I found {matched_count} lead(s) matching your request."
        )

        return {
            "prompt": prompt,
            "source": "llm-sql",
            "sql_condition": sanitized_clause,
            "total_candidates": matched_count,
            "matched_count": matched_count,
            "lead_ids": lead_ids,
            "preview_businesses": preview_names,
            "assistant_message": assistant_message,
        }

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

        with pgdb.connect(db_path) as conn:
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

    @app.post("/api/leads/bulk-delete")
    def bulk_delete_leads(payload: BulkDeleteLeadsRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        lead_ids = [int(x) for x in (payload.lead_ids or []) if int(x) > 0][:500]
        if not lead_ids:
            raise HTTPException(status_code=400, detail="No lead IDs provided.")

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_request_client(request, DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            existing_rows = (
                client.table("leads")
                .select("id")
                .eq("user_id", user_id)
                .in_("id", lead_ids)
                .limit(1000)
                .execute()
                .data
                or []
            )
            allowed_ids = [int(row.get("id")) for row in existing_rows if row.get("id") is not None]
            deleted = 0
            if allowed_ids:
                deleted_rows = (
                    client.table("leads")
                    .delete()
                    .eq("user_id", user_id)
                    .in_("id", allowed_ids)
                    .execute()
                    .data
                    or []
                )
                deleted = len(deleted_rows)
            _invalidate_leads_cache()
            return {"status": "deleted", "deleted": int(deleted), "requested": len(lead_ids)}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        placeholders = ", ".join(["?"] * len(lead_ids))
        with pgdb.connect(db_path) as conn:
            cursor = conn.execute(
                f"DELETE FROM leads WHERE user_id = ? AND id IN ({placeholders})",
                [user_id, *lead_ids],
            )
            conn.commit()
        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)
        _invalidate_leads_cache()
        return {"status": "deleted", "deleted": int(cursor.rowcount or 0), "requested": len(lead_ids)}

    @app.post("/api/leads/repair-hardcoded-user")
    def repair_hardcoded_user_leads(
        request: Request,
        apply: bool = Query(default=False),
        lookback_minutes: int = Query(default=180, ge=5, le=24 * 60),
    ) -> dict:
        user_id = require_current_user_id(request)
        now_utc = datetime.now(timezone.utc)
        cutoff_iso = (now_utc - timedelta(minutes=int(lookback_minutes))).isoformat()
        repaired_supabase = 0
        repaired_local = 0
        candidate_supabase = 0
        candidate_local = 0

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            try:
                admin_client = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
                if admin_client is not None:
                    supabase_query = (
                        admin_client.table("leads")
                        .select("id", count="exact")
                        .eq("user_id", "1")
                        .gte("created_at", cutoff_iso)
                    )
                    candidate_response = supabase_query.execute()
                    candidate_supabase = int(getattr(candidate_response, "count", 0) or 0)
                    if apply:
                        response = (
                            admin_client.table("leads")
                            .update({"user_id": str(user_id)})
                            .eq("user_id", "1")
                            .gte("created_at", cutoff_iso)
                            .execute()
                        )
                        repaired_supabase = len(getattr(response, "data", None) or [])
            except Exception as exc:
                logging.warning("Failed to repair hardcoded Supabase user_id rows: %s", exc)

        try:
            db_path = DEFAULT_DB_PATH
            ensure_system_tables(db_path)
            with pgdb.connect(db_path) as conn:
                candidate_local = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM leads
                        WHERE CAST(user_id AS TEXT) = '1'
                          AND COALESCE(created_at, scraped_at, '') >= ?
                        """,
                        (cutoff_iso,),
                    ).fetchone()[0]
                    or 0
                )
                if apply:
                    repaired_local = conn.execute(
                        """
                        UPDATE leads
                        SET user_id = ?
                        WHERE CAST(user_id AS TEXT) = '1'
                          AND COALESCE(created_at, scraped_at, '') >= ?
                        """,
                        (str(user_id), cutoff_iso),
                    ).rowcount or 0
                conn.commit()
        except Exception as exc:
            logging.warning("Failed to repair hardcoded local user_id rows: %s", exc)

        if apply:
            _invalidate_leads_cache()
        logging.info(
            "Hardcoded lead owner check for user_id=%s apply=%s lookback_minutes=%s candidates_supabase=%s candidates_local=%s repaired_supabase=%s repaired_local=%s",
            user_id,
            apply,
            lookback_minutes,
            candidate_supabase,
            candidate_local,
            repaired_supabase,
            repaired_local,
        )
        return {
            "status": "ok",
            "user_id": str(user_id),
            "dry_run": not bool(apply),
            "lookback_minutes": int(lookback_minutes),
            "cutoff_iso": cutoff_iso,
            "candidates_supabase": int(candidate_supabase),
            "candidates_local": int(candidate_local),
            "repaired_supabase": int(repaired_supabase),
            "repaired_local": int(repaired_local),
        }

    @app.post("/api/leads/score")
    async def score_leads_endpoint(payload: BulkLeadScoreRequest, request: Request):
        payload_dict = payload.dict()
        session_token = require_authenticated_session(request, fallback_token=payload_dict.get("token", ""))
        require_feature_access(session_token, "ai_lead_scoring", db_path=str(DEFAULT_DB_PATH))

        client, model_name = load_openai_client(DEFAULT_CONFIG_PATH)
        if client is None:
            raise HTTPException(status_code=503, detail="Azure OpenAI deployment is not configured.")

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
                deduct_credits_on_success(
                    user_id=str(require_current_user_id(request)),
                    credits_to_deduct=scored_count,
                    db_path=DEFAULT_DB_PATH,
                    action_type="lead_scoring",
                    metadata={
                        "scored_count": int(scored_count),
                        "shown_count": int(shown_count),
                    },
                )
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

        normalized_email = str(email or "").strip().lower()
        target_user_ids: set[str] = set()

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is not None and normalized_email:
                rows = (
                    client.table("leads")
                    .select("user_id")
                    .eq("email", normalized_email)
                    .limit(1000)
                    .execute()
                    .data
                    or []
                )
                target_user_ids = {str(row.get("user_id") or "").strip() for row in rows if str(row.get("user_id") or "").strip()}
        else:
            with pgdb.connect(db_path) as conn:
                rows = conn.execute("SELECT DISTINCT user_id FROM leads WHERE LOWER(COALESCE(email, '')) = ?", (normalized_email,)).fetchall()
            target_user_ids = {str(row[0] or "").strip() for row in rows if str(row[0] or "").strip()}

        if not target_user_ids:
            target_user_ids = {"legacy"}

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            for target_user_id in sorted(target_user_ids):
                add_blacklist_entry_supabase(
                    DEFAULT_CONFIG_PATH,
                    user_id=target_user_id,
                    kind="email",
                    value=normalized_email,
                    reason="Unsubscribe link",
                )
        else:
            for target_user_id in sorted(target_user_ids):
                add_blacklist_entry(
                    db_path,
                    user_id=target_user_id,
                    kind="email",
                    value=normalized_email,
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
        user_id = require_current_user_id(request)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_request_client(request, DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            items = (
                client.table("lead_blacklist")
                .select("id,kind,value,reason,created_at")
                .eq("user_id", user_id)
                .order("created_at", desc=True)
                .limit(200)
                .execute()
                .data
                or []
            )
            return {"items": items, "count": len(items)}

        db_path = DEFAULT_DB_PATH
        ensure_blacklist_table(db_path)
        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            rows = conn.execute(
                "SELECT id, kind, value, reason, created_at FROM lead_blacklist WHERE user_id = ? ORDER BY created_at DESC, id DESC LIMIT 200",
                (user_id,),
            ).fetchall()
        return {
            "items": [dict(row) for row in rows],
            "count": len(rows),
        }

    @app.post("/api/blacklist")
    def create_blacklist_entry(payload: BlacklistEntryRequest, request: Request) -> dict:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            return add_blacklist_entry_supabase(
                DEFAULT_CONFIG_PATH,
                user_id=user_id,
                kind=payload.kind,
                value=payload.value,
                reason=payload.reason or "Manual blacklist",
            )

        return add_blacklist_entry(
            db_path,
            user_id=user_id,
            kind=payload.kind,
            value=payload.value,
            reason=payload.reason or "Manual blacklist",
        )

    @app.delete("/api/blacklist")
    def delete_blacklist_entry(request: Request, kind: str = Query(...), value: str = Query(...)) -> dict:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            return remove_blacklist_entry_supabase(DEFAULT_CONFIG_PATH, user_id=user_id, kind=kind, value=value)

        return remove_blacklist_entry(db_path, user_id=user_id, kind=kind, value=value)

    @app.post("/api/leads/{lead_id}/blacklist")
    def blacklist_lead(lead_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_request_client(request, DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            rows = client.table("leads").select("id,user_id").eq("id", lead_id).limit(1).execute().data or []
            if not rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            return blacklist_lead_and_matches_supabase(lead_id, "Manual blacklist", DEFAULT_CONFIG_PATH)

        with pgdb.connect(db_path) as conn:
            owner_row = conn.execute("SELECT user_id FROM leads WHERE id = ? AND user_id = ?", (lead_id, user_id)).fetchone()
            if owner_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")

        return blacklist_lead_and_matches(db_path, lead_id, "Manual blacklist", user_id=user_id)

    @app.delete("/api/leads/{lead_id}/blacklist")
    def unblacklist_lead(lead_id: int, request: Request) -> dict:
        user_id = require_current_user_id(request)
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_request_client(request, DEFAULT_CONFIG_PATH)
            if client is None:
                raise HTTPException(status_code=500, detail="Supabase not configured")
            rows = client.table("leads").select("id,user_id").eq("id", lead_id).limit(1).execute().data or []
            if not rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            return remove_lead_blacklist_and_matches_supabase(lead_id, DEFAULT_CONFIG_PATH)

        with pgdb.connect(db_path) as conn:
            owner_row = conn.execute("SELECT user_id FROM leads WHERE id = ? AND user_id = ?", (lead_id, user_id)).fetchone()
            if owner_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")

        return remove_lead_blacklist_and_matches(db_path, lead_id, user_id=user_id)

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

            lead_rows = client.table("leads").select("id,user_id,business_name,status,pipeline_stage,paid_at,sent_at,last_contacted_at,reply_detected_at").eq("id", lead_id).limit(1).execute().data or []
            if not lead_rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")

            existing_paid_at = lead_rows[0].get("paid_at")
            existing_sent_at = lead_rows[0].get("sent_at")
            existing_last_contacted_at = lead_rows[0].get("last_contacted_at")
            existing_reply_detected_at = lead_rows[0].get("reply_detected_at")
            previous_stage = _derive_pipeline_stage(
                status=str(lead_rows[0].get("status") or ""),
                pipeline_stage=str(lead_rows[0].get("pipeline_stage") or ""),
            )
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

            if previous_stage != "Replied" and pipeline_stage_value == "Replied":
                _dispatch_developer_webhook_event(
                    event_type="lead.moved_to_replied",
                    user_id=user_id,
                    lead_id=int(lead_id),
                    payload={
                        "lead_id": int(lead_id),
                        "business_name": str(lead_rows[0].get("business_name") or "").strip() or None,
                        "old_stage": previous_stage,
                        "new_stage": pipeline_stage_value,
                        "new_status": next_status,
                    },
                )

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
            with pgdb.connect(db_path) as conn:
                owner_row = conn.execute("SELECT user_id FROM leads WHERE id = ? AND user_id = ?", (lead_id, user_id)).fetchone()
                if owner_row is None:
                    raise HTTPException(status_code=404, detail="Lead not found")
            return blacklist_lead_and_matches(db_path, lead_id, "Manual status blacklist", user_id=user_id)

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            owner_row = conn.execute(
                "SELECT user_id, business_name, status, pipeline_stage, paid_at, sent_at, last_contacted_at, reply_detected_at FROM leads WHERE id = ? AND user_id = ?",
                (lead_id, user_id),
            ).fetchone()
            if owner_row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            previous_stage = _derive_pipeline_stage(
                status=str(owner_row["status"] or ""),
                pipeline_stage=str(owner_row["pipeline_stage"] or ""),
            )
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
                WHERE id = ? AND user_id = ?
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
                    user_id,
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

        if previous_stage != "Replied" and pipeline_stage_value == "Replied":
            _dispatch_developer_webhook_event(
                event_type="lead.moved_to_replied",
                user_id=user_id,
                lead_id=int(lead_id),
                payload={
                    "lead_id": int(lead_id),
                    "business_name": str(owner_row["business_name"] or "").strip() or None,
                    "old_stage": previous_stage,
                    "new_stage": pipeline_stage_value,
                    "new_status": next_status,
                },
            )

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
        try:
            user_id = require_current_user_id(request)
            if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "SavedSegments"):
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
        except HTTPException:
            raise
        except Exception as exc:
            logging.exception("Failed to load saved segments")
            raise HTTPException(status_code=500, detail=f"Saved segments error: {exc}")

    @app.post("/api/saved-segments")
    def save_segment_route(payload: SavedSegmentRequest, request: Request) -> dict:
        try:
            user_id = require_current_user_id(request)
            if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "SavedSegments"):
                client = get_supabase_client(DEFAULT_CONFIG_PATH)
                if client is None:
                    raise HTTPException(status_code=500, detail="Supabase not configured")
                saved_segments_table = resolve_supabase_table_name(client, "SavedSegments")
                now_iso = utc_now_iso()
                name = payload.name.strip()
                filters_json = json.dumps(payload.filters or {}, ensure_ascii=False)
                existing_rows = client.table(saved_segments_table).select("id").eq("user_id", user_id).eq("name", name).limit(1).execute().data or []
                if existing_rows:
                    segment_id = int(existing_rows[0].get("id"))
                    client.table(saved_segments_table).update({"filters_json": filters_json, "updated_at": now_iso}).eq("id", segment_id).eq("user_id", user_id).execute()
                    saved_rows = client.table(saved_segments_table).select("id,user_id,name,filters_json,created_at,updated_at").eq("id", segment_id).limit(1).execute().data or []
                else:
                    saved_rows = client.table(saved_segments_table).insert({
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
        except HTTPException:
            raise
        except Exception as exc:
            logging.exception("Failed to save segment")
            raise HTTPException(status_code=500, detail=f"Saved segments error: {exc}")

    @app.delete("/api/saved-segments/{segment_id}")
    def delete_saved_segment_route(segment_id: int, request: Request) -> dict:
        try:
            user_id = require_current_user_id(request)
            if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH) and supabase_table_available(DEFAULT_CONFIG_PATH, "SavedSegments"):
                client = get_supabase_client(DEFAULT_CONFIG_PATH)
                if client is None:
                    raise HTTPException(status_code=500, detail="Supabase not configured")
                saved_segments_table = resolve_supabase_table_name(client, "SavedSegments")
                existing_rows = client.table(saved_segments_table).select("id").eq("id", int(segment_id)).eq("user_id", user_id).limit(1).execute().data or []
                if not existing_rows:
                    raise HTTPException(status_code=404, detail="Saved segment not found")
                client.table(saved_segments_table).delete().eq("id", int(segment_id)).eq("user_id", user_id).execute()
                return {"status": "deleted", "id": int(segment_id)}

            return delete_saved_segment(DEFAULT_DB_PATH, user_id, segment_id)
        except HTTPException:
            raise
        except Exception as exc:
            logging.exception("Failed to delete saved segment")
            raise HTTPException(status_code=500, detail=f"Saved segments error: {exc}")

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
            client_folders_table = resolve_supabase_table_name(client, "ClientFolders")
            now_iso = utc_now_iso()
            inserted = client.table(client_folders_table).insert(
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
            client_folders_table = resolve_supabase_table_name(client, "ClientFolders")
            lead_rows = client.table("leads").select("id,user_id,business_name").eq("id", lead_id).limit(1).execute().data or []
            if not lead_rows:
                raise HTTPException(status_code=404, detail="Lead not found")
            if str(lead_rows[0].get("user_id") or "") != user_id:
                raise HTTPException(status_code=403, detail="Forbidden")
            folder_name = None
            if normalized_folder_id is not None:
                folder_rows = client.table(client_folders_table).select("id,name,user_id").eq("id", normalized_folder_id).limit(1).execute().data or []
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
        cache_key = str(user_id)
        cached = _get_cached_api("workers", cache_key, _WORKERS_CACHE_TTL)
        if cached is not None:
            return cached

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            result = get_workers_snapshot_supabase(DEFAULT_CONFIG_PATH, user_id=user_id)
            _set_cached_api("workers", cache_key, result)
            return result

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        result = get_workers_snapshot(db_path, user_id=user_id)
        _set_cached_api("workers", cache_key, result)
        return result

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
            _invalidate_api_cache("workers", str(user_id))
            return {"status": "created", "worker_id": worker_id}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        status_value = payload.status.strip().title()
        if status_value not in {"Active", "Idle"}:
            raise HTTPException(status_code=422, detail="Status must be Active or Idle")

        with pgdb.connect(db_path) as conn:
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
        _invalidate_api_cache("workers", str(user_id))

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
            _invalidate_api_cache("workers", str(user_id))
            return {"status": "updated", "worker_id": worker_id}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
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
                WHERE id = ? AND user_id = ?
                """,
                (worker_name, role, monthly_cost, status, comms_link, utc_now_iso(), worker_id, user_id),
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
        _invalidate_api_cache("workers", str(user_id))
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
            _invalidate_api_cache("workers", str(user_id))
            _invalidate_leads_cache()
            return {"status": "deleted", "worker_id": worker_id, "unassigned_leads": unassigned_leads}

        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
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
        _invalidate_api_cache("workers", str(user_id))
        _invalidate_leads_cache()

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
        with pgdb.connect(db_path) as conn:
            if worker_id is not None:
                row = conn.execute("SELECT id, user_id, worker_name FROM workers WHERE id = ?", (worker_id,)).fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail="Worker not found")
                if str(row[1] or "") != user_id:
                    raise HTTPException(status_code=403, detail="Forbidden")
                worker_name = row[2]

            lead_owner = conn.execute("SELECT user_id FROM leads WHERE id = ? AND user_id = ?", (lead_id, user_id)).fetchone()
            if lead_owner is None:
                raise HTTPException(status_code=404, detail="Lead not found")

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

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
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
                LEFT JOIN leads l ON l.id = dt.lead_id AND l.user_id = dt.user_id
                LEFT JOIN workers w ON w.id = dt.worker_id AND w.user_id = dt.user_id
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
                    COALESCE(dt.due_at, dt.created_at) ASC,
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

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                "SELECT id, user_id, worker_id, lead_id, business_name, status, notes, done_at FROM delivery_tasks WHERE id = ? AND user_id = ?",
                (task_id, user_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Delivery task not found")

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
                WHERE id = ? AND user_id = ?
                """,
                (worker_id, status_value, notes_value, done_at_value, utc_now_iso(), task_id, user_id),
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

        with pgdb.connect(db_path) as conn:
            row = conn.execute("SELECT user_id FROM leads WHERE id = ? AND user_id = ?", (lead_id, user_id)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            cursor = conn.execute(
                "UPDATE leads SET client_tier = ? WHERE id = ? AND user_id = ?",
                (tier_value, lead_id, user_id),
            )
            conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Lead not found")

        maybe_sync_supabase(db_path, DEFAULT_CONFIG_PATH)

        return {"status": "updated", "lead_id": lead_id, "new_tier": tier_value}

    # ------------------------------------------------------------------
    # Job Queue endpoints  (Supabase-first, legacy store fallback)
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

        # legacy store fallback via the compatibility DB wrapper
        db_path = DEFAULT_DB_PATH
        ensure_system_tables(db_path)
        ensure_jobs_queue_table(db_path)
        with pgdb.connect(db_path) as conn:
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

        # legacy store fallback
        db_path = DEFAULT_DB_PATH
        ensure_jobs_queue_table(db_path)
        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
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
        ensure_jobs_queue_table(db_path)
        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            rows = conn.execute(
                "SELECT id,user_id,type,status,error,created_at,started_at,completed_at FROM jobs WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
        return {"items": [dict(r) for r in rows]}

    @app.get("/api/scrape")
    @auth_required
    def list_scrape_jobs(
        request: Request,
        limit: int = Query(20, ge=1, le=200),
        page: int = Query(1, ge=1),
        status: Optional[str] = Query(default=None),
        sort: str = Query("recent"),
    ) -> dict:
        user_id = require_current_user_id(request)
        page_size = max(1, min(int(limit or 20), 200))
        page_number = max(1, int(page or 1))
        offset = max(0, (page_number - 1) * page_size)
        normalized_status = str(status or "").strip().lower()
        normalized_sort = str(sort or "recent").strip().lower() or "recent"

        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            try:
                engine = pg_get_engine(str(DEFAULT_DB_PATH))
                where_clauses = ["CAST(user_id AS TEXT) = :uid", "LOWER(COALESCE(type, '')) = 'scrape'"]
                sql_params: dict[str, Any] = {"uid": str(user_id), "limit": page_size, "offset": offset}

                if normalized_status:
                    where_clauses.append("LOWER(COALESCE(status, 'queued')) = :status")
                    sql_params["status"] = normalized_status

                where_fragment = f" WHERE {' AND '.join(where_clauses)}"
                order_clause = "ORDER BY COALESCE(created_at, updated_at) DESC, id DESC"
                if normalized_sort == "oldest":
                    order_clause = "ORDER BY COALESCE(created_at, updated_at) ASC, id ASC"
                elif normalized_sort == "status":
                    order_clause = "ORDER BY LOWER(COALESCE(status, 'queued')) ASC, COALESCE(created_at, updated_at) DESC, id DESC"

                count_sql = text(f"SELECT COUNT(*) AS c FROM jobs{where_fragment}")
                page_sql = text(
                    "SELECT id,user_id,type,status,error,created_at,started_at,completed_at,updated_at "
                    f"FROM jobs{where_fragment} {order_clause} LIMIT :limit OFFSET :offset"
                )
                with engine.connect() as conn:
                    total = int(conn.execute(count_sql, sql_params).scalar() or 0)
                    rows = conn.execute(page_sql, sql_params).fetchall()

                items = [dict(r._mapping) for r in rows]
                return {
                    "count": len(items),
                    "total": total,
                    "page": page_number,
                    "page_size": page_size,
                    "has_more": offset + len(items) < total,
                    "items": items,
                }
            except Exception as scrape_jobs_exc:
                if _is_db_capacity_error(scrape_jobs_exc):
                    record_pool_saturation_event(scrape_jobs_exc)
                    logging.warning("[/api/scrape] DB connection pool is saturated; returning 503")
                    raise HTTPException(status_code=503, detail="Database is temporarily busy. Please retry in a few seconds.")
                raise

        db_path = DEFAULT_DB_PATH
        ensure_jobs_queue_table(db_path)
        where_clauses = ["user_id = ?", "LOWER(COALESCE(type,'')) = 'scrape'"]
        params: list[Any] = [user_id]

        if normalized_status:
            where_clauses.append("LOWER(COALESCE(status, 'queued')) = ?")
            params.append(normalized_status)

        order_clause = "COALESCE(created_at, updated_at) DESC, id DESC"
        if normalized_sort == "oldest":
            order_clause = "COALESCE(created_at, updated_at) ASC, id ASC"
        elif normalized_sort == "status":
            order_clause = "LOWER(COALESCE(status, 'queued')) ASC, COALESCE(created_at, updated_at) DESC, id DESC"

        where_fragment = f" WHERE {' AND '.join(where_clauses)}"
        count_query = f"SELECT COUNT(*) FROM jobs{where_fragment}"
        query = (
            "SELECT id,user_id,type,status,error,created_at,started_at,completed_at,updated_at "
            f"FROM jobs{where_fragment} ORDER BY {order_clause} LIMIT ? OFFSET ?"
        )

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
            total = int(conn.execute(count_query, params).fetchone()[0] or 0)
            rows = conn.execute(query, [*params, page_size, offset]).fetchall()

        items = [dict(r) for r in rows]
        return {
            "count": len(items),
            "total": total,
            "page": page_number,
            "page_size": page_size,
            "has_more": offset + len(items) < total,
            "items": items,
        }

    @app.post("/api/scrape")
    def run_scrape(payload: ScrapeRequest, background_tasks: BackgroundTasks, request: Request) -> dict:
        logging.info("[scrape] POST /api/scrape | keyword=%r results=%s", payload.keyword, payload.results)
        _, billing, access = resolve_plan_access_context(
            request,
            feature_key="basic_search",
            allow_stripe_recovery=False,
        )
        user_id = require_current_user_id(request)
        logging.info("[scrape] request user_id=%s", user_id)
        db_path = resolve_path(payload.db_path, DEFAULT_DB_PATH)
        try:
            ensure_scrape_tables(db_path)
        except Exception as _db_exc:
            logging.exception("[scrape] DB init error: %s", _db_exc)
            raise HTTPException(status_code=500, detail=f"Database offline: {_db_exc}")

        available_credits = max(0, int(billing.get("credits_balance") or 0))
        if available_credits <= 0:
            raise HTTPException(status_code=403, detail="Out of credits. Please upgrade.")

        if bool(payload.export_targets):
            require_feature_access(access.get("plan_key"), "bulk_export")

        payload_data = payload.model_dump()
        # Hard-lock task ownership to authenticated user from request context.
        payload_data["user_id"] = user_id
        requested_results = max(1, int(payload.results or 25))
        required_scrape_credits = requested_results * SCRAPE_CREDIT_COST_PER_LEAD
        if available_credits < required_scrape_credits:
            # Cap to available credits rather than rejecting entirely.
            capped_results = max(1, available_credits // SCRAPE_CREDIT_COST_PER_LEAD)
            logging.info(
                "[scrape] Capping results %s → %s to match available credits %s",
                requested_results,
                capped_results,
                available_credits,
            )
            requested_results = capped_results
        payload_data["country"] = normalize_country_value(payload.country, payload.country_code)
        payload_data["results"] = requested_results
        payload_data["_queue_priority"] = bool(access.get("queue_priority"))
        logging.info(
            "[scrape] enqueueing task | user_id=%s keyword=%r results=%s headless=%s country=%s",
            user_id,
            payload.keyword,
            payload_data["results"],
            bool(payload.headless),
            payload_data["country"],
        )
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
                "owner_id": user_id,
                "request_user_id": user_id,
            },
        )

    @app.get("/api/scrape/smoke-test")
    async def scrape_smoke_test(request: Request) -> dict:
        user_id = require_current_user_id(request)
        timeout_seconds = max(10, int(os.environ.get("SCRAPE_SMOKE_TIMEOUT_SECONDS", "30") or "30"))
        launch_timeout_ms = timeout_seconds * 1000
        nav_timeout_ms = timeout_seconds * 1000

        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Playwright import failed: {exc}")

        browser = None
        title = ""
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(
                    headless=True,
                    args=["--disable-extensions", "--disable-gpu"],
                    timeout=launch_timeout_ms,
                )
                page = await browser.new_page()
                await page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=nav_timeout_ms)
                title = str(await page.title() or "")
            return {
                "ok": True,
                "user_id": user_id,
                "url": "https://www.google.com",
                "title": title,
                "timeout_seconds": timeout_seconds,
            }
        except Exception as exc:
            logging.exception("Scrape smoke test failed")
            raise HTTPException(status_code=502, detail=f"Smoke test failed: {exc}")
        finally:
            try:
                if browser is not None:
                    await browser.close()
            except Exception:
                pass

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

        jwt_user_id = resolve_supabase_auth_user_id(token, DEFAULT_CONFIG_PATH)
        if jwt_user_id:
            return True

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            sb_client = get_supabase_client_for_token(DEFAULT_CONFIG_PATH, token)
            if sb_client is None:
                return False
            try:
                response = sb_client.table("users").select("id").eq("token", token).limit(1).execute()
                rows = list(getattr(response, "data", None) or [])
                return bool(rows)
            except Exception:
                return False

        auth_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(auth_db_path)
        with pgdb.connect(auth_db_path) as conn:
            row = conn.execute("SELECT 1 FROM users WHERE token = ? LIMIT 1", (token,)).fetchone()
        return bool(row)

    def _session_token_block_reason(session_token: str, db_path: Optional[Path] = None) -> Optional[str]:
        token = str(session_token or "").strip()
        if not token:
            return None

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            sb_client = get_supabase_client_for_token(DEFAULT_CONFIG_PATH, token)
            if sb_client is not None:
                try:
                    rows = (
                        sb_client.table("users")
                        .select("is_blocked,blocked_reason")
                        .eq("token", token)
                        .limit(1)
                        .execute()
                        .data
                        or []
                    )
                    if rows and bool(rows[0].get("is_blocked") or False):
                        reason = str(rows[0].get("blocked_reason") or "").strip()
                        return reason or "Your account is temporarily blocked."
                except Exception:
                    pass

        auth_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(auth_db_path)
        with pgdb.connect(auth_db_path) as conn:
            conn.row_factory = pgdb.Row
            try:
                row = conn.execute(
                    "SELECT COALESCE(is_blocked, FALSE) AS is_blocked, blocked_reason FROM users WHERE token = ? LIMIT 1",
                    (token,),
                ).fetchone()
            except Exception as exc:
                if "does not exist" in str(exc).lower():
                    # Older Railway schemas may not have blocking columns yet.
                    row = conn.execute("SELECT 0 AS is_blocked, '' AS blocked_reason FROM users WHERE token = ? LIMIT 1", (token,)).fetchone()
                else:
                    raise
        if row is None:
            return None
        if bool(row["is_blocked"] or False):
            reason = str(row["blocked_reason"] or "").strip()
            return reason or "Your account is temporarily blocked."
        return None

    def require_authenticated_session(request: Optional[Request] = None, fallback_token: Optional[str] = None) -> str:
        token = _resolve_session_token(request=request, fallback_token=fallback_token)
        if not _session_token_exists(token):
            raise HTTPException(status_code=401, detail="Authentication required.")
        block_reason = _session_token_block_reason(token)
        if block_reason:
            raise HTTPException(status_code=403, detail=block_reason)
        return token

    def resolve_user_id_from_session_token(session_token: str, db_path: Optional[Path] = None) -> str:
        token = str(session_token or "").strip()
        if not token:
            raise HTTPException(status_code=401, detail="Authentication required.")

        jwt_user_id = resolve_supabase_auth_user_id(token, DEFAULT_CONFIG_PATH)
        if jwt_user_id:
            return jwt_user_id
        if jwt_user_id:
            return jwt_user_id

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            sb_client = get_supabase_client_for_token(DEFAULT_CONFIG_PATH, token)
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

        auth_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(auth_db_path)
        with pgdb.connect(auth_db_path) as conn:
            conn.row_factory = pgdb.Row
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
        allow_stripe_recovery: bool = True,
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        session_token = require_authenticated_session(request, fallback_token=fallback_token)
        billing = load_user_billing_context(session_token, allow_stripe_recovery=allow_stripe_recovery)
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

    def load_user_billing_context(session_token: str, *, allow_stripe_recovery: bool = True) -> dict:
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
            if allow_stripe_recovery and not extras_is_paid:
                stripe_customer_id = str(extras.get("stripe_customer_id") or "").strip()
                stripe_recovered = recover_billing_snapshot_from_stripe(
                    user_email=base_row.get("email") if stripe_customer_id else None,
                    stripe_customer_id=stripe_customer_id,
                    fallback_plan_key="pro",
                    config_path=DEFAULT_CONFIG_PATH,
                    allow_email_lookup=False,
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

            stripe_customer_id = str(extras.get("stripe_customer_id") or "").strip()
            pending_topup = {}
            if allow_stripe_recovery:
                pending_topup = recover_pending_topup_credits_from_stripe(
                    user_id=base_row.get("id"),
                    user_email=base_row.get("email") if stripe_customer_id else None,
                    stripe_customer_id=stripe_customer_id,
                    updated_at_raw=extras.get("updated_at"),
                    config_path=DEFAULT_CONFIG_PATH,
                    allow_email_lookup=False,
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

            payload = {
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
            return _apply_admin_billing_override(payload, email=base_row.get("email"))

        ensure_users_table(DEFAULT_DB_PATH)
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                f"""
                SELECT
                    id,
                    email,
                    COALESCE(credits_balance, 0) AS credits_balance,
                    COALESCE(NULLIF(monthly_quota, 0), NULLIF(monthly_limit, 0), NULLIF(credits_limit, 0), {DEFAULT_MONTHLY_CREDIT_LIMIT}) AS monthly_limit,
                    COALESCE(topup_credits_balance, 0) AS topup_credits_balance,
                    COALESCE(subscription_start_date, '') AS subscription_start_date,
                    COALESCE(subscription_active, FALSE) AS subscription_active,
                    COALESCE(subscription_status, '') AS subscription_status,
                    COALESCE(subscription_cancel_at, '') AS subscription_cancel_at,
                    COALESCE(subscription_cancel_at_period_end, FALSE) AS subscription_cancel_at_period_end,
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
            stripe_customer_id = str(row_data.get("stripe_customer_id") or "").strip()
            stripe_recovered = recover_billing_snapshot_from_stripe(
                user_email=row_data.get("email") if stripe_customer_id else None,
                stripe_customer_id=stripe_customer_id,
                fallback_plan_key="pro",
                config_path=DEFAULT_CONFIG_PATH,
                allow_email_lookup=False,
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
                with pgdb.connect(DEFAULT_DB_PATH) as conn:
                    conn.execute(
                        """
                        UPDATE users
                        SET stripe_customer_id = ?,
                            plan_key = ?,
                            subscription_active = TRUE,
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
                            bool(stripe_recovered.get("subscription_cancel_at_period_end")),
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
                        "subscription_active": True,
                        "subscription_status": str(stripe_recovered.get("subscription_status") or "active").strip().lower(),
                        "subscription_cancel_at": stripe_recovered.get("subscription_cancel_at") or "",
                        "subscription_cancel_at_period_end": bool(stripe_recovered.get("subscription_cancel_at_period_end")),
                        "monthly_limit": recovered_limit,
                        "topup_credits_balance": existing_topup,
                        "credits_balance": recovered_balance,
                        "subscription_start_date": str(stripe_recovered.get("subscription_start_date") or utc_now_iso()),
                        "updated_at": utc_now_iso(),
                    }
                )
                store_runtime_billing_snapshot(row_data.get("id"), row_data.get("email"), row_data)

        stripe_customer_id = str(row_data.get("stripe_customer_id") or "").strip()
        pending_topup = recover_pending_topup_credits_from_stripe(
            user_id=row_data.get("id"),
            user_email=row_data.get("email") if stripe_customer_id else None,
            stripe_customer_id=stripe_customer_id,
            updated_at_raw=row_data.get("updated_at"),
            config_path=DEFAULT_CONFIG_PATH,
            allow_email_lookup=False,
        )
        recovered_topup_delta = max(0, _safe_int(pending_topup.get("credits_delta"), 0))
        if recovered_topup_delta > 0:
            next_topup_balance = max(0, _safe_int(row_data.get("topup_credits_balance"), 0)) + recovered_topup_delta
            next_balance = max(0, _safe_int(row_data.get("credits_balance"), 0)) + recovered_topup_delta
            now_iso = utc_now_iso()
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
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
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
                conn.execute(
                    """
                    UPDATE users
                    SET plan_key = 'free',
                        subscription_active = FALSE,
                        subscription_status = 'expired',
                        subscription_cancel_at_period_end = FALSE,
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

        payload = {
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
        return _apply_admin_billing_override(payload, email=row_data.get("email"))

    def create_stripe_billing_portal_session(customer_id: str, return_url: str) -> str:
        secret_key = get_stripe_secret_key(DEFAULT_CONFIG_PATH)
        if not secret_key:
            raise HTTPException(status_code=503, detail="Stripe is not configured. Add `stripe.secret_key` in `environment settings` or set `STRIPE_SECRET_KEY`.")

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

    def _require_stripe_sdk_client() -> Any:
        if stripe is None:
            raise HTTPException(status_code=503, detail="Stripe SDK is not installed on the backend.")
        secret_key = get_stripe_secret_key(DEFAULT_CONFIG_PATH)
        if not secret_key:
            raise HTTPException(status_code=503, detail="Stripe is not configured. Add `stripe.secret_key` in `environment settings` or set `STRIPE_SECRET_KEY`.")
        stripe.api_key = secret_key
        return stripe

    def _stripe_field(value: Any, key: str, default: Any = None) -> Any:
        if isinstance(value, dict):
            return value.get(key, default)
        try:
            return value.get(key, default)
        except Exception:
            return getattr(value, key, default)

    def _int_or_zero(value: Any) -> int:
        try:
            return int(value or 0)
        except Exception:
            return 0

    def _pick_manageable_subscription(subscription_rows: list[Any]) -> Optional[Any]:
        if not subscription_rows:
            return None

        def _rank(item: Any) -> tuple[int, int]:
            status = str(_stripe_field(item, "status", "") or "").strip().lower()
            rank = {
                "active": 0,
                "trialing": 1,
                "past_due": 2,
                "unpaid": 3,
                "incomplete": 4,
                "canceled": 5,
                "incomplete_expired": 6,
            }.get(status, 9)
            period_end = _int_or_zero(_stripe_field(item, "current_period_end", 0))
            return (rank, -period_end)

        ranked = sorted(subscription_rows, key=_rank)
        for candidate in ranked:
            status = str(_stripe_field(candidate, "status", "") or "").strip().lower()
            if status not in {"canceled", "incomplete_expired"}:
                return candidate
        return ranked[0]

    def _set_subscription_cancel_at_period_end(customer_id: str, *, cancel_at_period_end: bool) -> dict[str, Any]:
        stripe_client = _require_stripe_sdk_client()
        try:
            subscriptions = stripe_client.Subscription.list(customer=customer_id, status="all", limit=10)
            rows = list(getattr(subscriptions, "data", None) or [])
        except HTTPException:
            raise
        except Exception as exc:
            logging.exception("Stripe subscription listing failed for customer=%s", customer_id)
            raise HTTPException(status_code=502, detail=f"Could not load Stripe subscription: {exc}")

        selected = _pick_manageable_subscription(rows)
        if selected is None:
            raise HTTPException(status_code=404, detail="No Stripe subscription found for this customer.")

        subscription_id = str(_stripe_field(selected, "id", "") or "").strip()
        if not subscription_id:
            raise HTTPException(status_code=502, detail="Stripe subscription is missing an id.")

        already_flag = bool(_stripe_field(selected, "cancel_at_period_end", False))
        try:
            updated = (
                selected
                if already_flag == bool(cancel_at_period_end)
                else stripe_client.Subscription.modify(subscription_id, cancel_at_period_end=bool(cancel_at_period_end))
            )
        except Exception as exc:
            logging.exception("Stripe subscription update failed for subscription=%s", subscription_id)
            raise HTTPException(status_code=502, detail=f"Could not update Stripe subscription: {exc}")

        effective_cancel_at_period_end = bool(_stripe_field(updated, "cancel_at_period_end", cancel_at_period_end))
        current_period_end = _int_or_zero(_stripe_field(updated, "current_period_end", 0))
        cancel_at = _int_or_zero(_stripe_field(updated, "cancel_at", 0))
        cancel_ts = cancel_at or current_period_end
        cancel_iso: Optional[str] = None
        if effective_cancel_at_period_end and cancel_ts > 0:
            try:
                cancel_iso = datetime.fromtimestamp(cancel_ts, tz=timezone.utc).isoformat()
            except Exception:
                cancel_iso = None

        return {
            "subscription_id": subscription_id,
            "subscription_status": str(_stripe_field(updated, "status", "") or "").strip().lower() or "active",
            "subscription_cancel_at": cancel_iso,
            "subscription_cancel_at_period_end": effective_cancel_at_period_end,
        }

    def _persist_manual_subscription_state(
        billing: dict[str, Any],
        *,
        subscription_status: str,
        subscription_cancel_at: Optional[str],
        subscription_cancel_at_period_end: bool,
        stripe_customer_id: Optional[str] = None,
    ) -> dict[str, Any]:
        user_id = str(billing.get("id") or "").strip()
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")

        user_email = str(billing.get("email") or "").strip().lower()
        plan_key = _normalize_plan_key(billing.get("plan_key"), fallback="pro")
        if plan_key == "free":
            plan_key = "pro"

        now_iso = utc_now_iso()
        payload = {
            "subscription_active": True,
            "subscription_status": str(subscription_status or "active").strip().lower(),
            "subscription_cancel_at": subscription_cancel_at if subscription_cancel_at_period_end else None,
            "subscription_cancel_at_period_end": bool(subscription_cancel_at_period_end),
            "plan_key": plan_key,
            "updated_at": now_iso,
        }
        normalized_customer_id = str(stripe_customer_id or billing.get("stripe_customer_id") or "").strip()
        if normalized_customer_id:
            payload["stripe_customer_id"] = normalized_customer_id

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            execute_supabase_update_with_retry(
                sb_client,
                "users",
                payload,
                eq_filters={"id": user_id},
                operation_name="manual_subscription_state_update",
            )
        else:
            ensure_users_table(DEFAULT_DB_PATH)
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
                conn.execute(
                    """
                    UPDATE users
                    SET subscription_active = TRUE,
                        subscription_status = ?,
                        subscription_cancel_at = ?,
                        subscription_cancel_at_period_end = ?,
                        plan_key = ?,
                        stripe_customer_id = COALESCE(NULLIF(?, ''), stripe_customer_id),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        payload["subscription_status"],
                        payload["subscription_cancel_at"],
                        int(payload["subscription_cancel_at_period_end"]),
                        plan_key,
                        normalized_customer_id,
                        now_iso,
                        user_id,
                    ),
                )
                conn.commit()

        snapshot = {
            "id": user_id,
            "email": user_email,
            "credits_balance": max(0, _safe_int(billing.get("credits_balance"), 0)),
            "credits_limit": max(1, _safe_int(billing.get("credits_limit") or billing.get("monthly_limit") or billing.get("monthly_quota"), DEFAULT_MONTHLY_CREDIT_LIMIT)),
            "monthly_limit": max(1, _safe_int(billing.get("monthly_limit") or billing.get("monthly_quota") or billing.get("credits_limit"), DEFAULT_MONTHLY_CREDIT_LIMIT)),
            "monthly_quota": max(1, _safe_int(billing.get("monthly_quota") or billing.get("monthly_limit") or billing.get("credits_limit"), DEFAULT_MONTHLY_CREDIT_LIMIT)),
            "topup_credits_balance": max(0, _safe_int(billing.get("topup_credits_balance"), 0)),
            "subscription_start_date": str(billing.get("subscription_start_date") or "").strip() or now_iso,
            "subscription_active": True,
            "subscription_status": payload["subscription_status"],
            "subscription_cancel_at": payload["subscription_cancel_at"],
            "subscription_cancel_at_period_end": bool(payload["subscription_cancel_at_period_end"]),
            "plan_key": plan_key,
            "stripe_customer_id": normalized_customer_id,
            "updated_at": now_iso,
        }
        store_runtime_billing_snapshot(user_id, user_email, snapshot)
        return snapshot

    def _resolve_or_create_stripe_customer(
        user_id: str,
        user_email: str,
        stripe_customer_id: Optional[str],
    ) -> str:
        """Return a guaranteed-valid Stripe customer ID.

        If the stored ID doesn't exist in the current Stripe environment
        (live vs. test key mismatch, deleted customer, etc.) the stale ID
        is wiped from our database, a fresh customer is created, and the
        new ID is persisted before being returned.
        """
        secret_key = get_stripe_secret_key(DEFAULT_CONFIG_PATH)
        if not secret_key:
            return str(stripe_customer_id or "").strip()

        safe_customer_id = str(stripe_customer_id or "").strip()
        safe_user_id = str(user_id or "").strip()
        safe_user_email = str(user_email or "").strip().lower()

        def _create_new_stripe_customer() -> str:
            form: list[tuple[str, str]] = []
            if safe_user_email:
                form.append(("email", safe_user_email))
            if safe_user_id:
                form.append(("metadata[user_id]", safe_user_id))
            req = urllib.request.Request(
                "https://api.stripe.com/v1/customers",
                data=urlencode(form).encode("utf-8"),
                method="POST",
                headers={
                    "Authorization": f"Bearer {secret_key}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                new_id = str(data.get("id") or "").strip()
                logging.info("Created new Stripe customer %s for user_id=%s", new_id, safe_user_id)
                return new_id
            except Exception as exc:
                logging.warning("Could not create new Stripe customer for user_id=%s: %s", safe_user_id, exc)
                return ""

        def _nullify_and_refresh_customer_id(new_cid: str) -> None:
            now_iso = utc_now_iso()
            try:
                with pgdb.connect(DEFAULT_DB_PATH) as conn:
                    conn.execute(
                        "UPDATE users SET stripe_customer_id = ?, updated_at = ? WHERE id = ?",
                        (new_cid or None, now_iso, safe_user_id),
                    )
                    conn.commit()
            except Exception:
                logging.debug("Could not update local stripe_customer_id for user_id=%s", safe_user_id)
            if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
                sb_client = get_supabase_client(DEFAULT_CONFIG_PATH)
                if sb_client is not None:
                    try:
                        execute_supabase_update_with_retry(
                            sb_client,
                            "users",
                            {"stripe_customer_id": new_cid or None, "updated_at": now_iso},
                            eq_filters={"id": safe_user_id},
                            operation_name=f"nullify stripe_customer_id user={safe_user_id}",
                        )
                    except Exception:
                        logging.debug("Could not update Supabase stripe_customer_id for user_id=%s", safe_user_id)

        if safe_customer_id:
            # Validate that the customer actually exists in the current Stripe environment.
            req = urllib.request.Request(
                f"https://api.stripe.com/v1/customers/{safe_customer_id}",
                method="GET",
                headers={"Authorization": f"Bearer {secret_key}"},
            )
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    resp.read()  # customer exists, all good
                return safe_customer_id
            except urllib.error.HTTPError as exc:
                raw = ""
                try:
                    raw = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    pass
                err_code = ""
                try:
                    err_code = str((json.loads(raw).get("error") or {}).get("code") or "").strip().lower()
                except Exception:
                    pass
                is_no_such_customer = (
                    getattr(exc, "code", None) in (404,)
                    or err_code in ("resource_missing", "no_such_customer")
                    or "no such customer" in raw.lower()
                )
                if is_no_such_customer:
                    logging.warning(
                        "Stripe customer %s does not exist in current environment — creating a new one for user_id=%s",
                        safe_customer_id,
                        safe_user_id,
                    )
                    new_cid = _create_new_stripe_customer()
                    _nullify_and_refresh_customer_id(new_cid)
                    return new_cid
                # Any other Stripe error: don't block checkout, fall through with original ID.
                logging.warning("Stripe customer validation returned unexpected error for %s: %s", safe_customer_id, raw[:400])
                return safe_customer_id
            except Exception as exc:
                logging.warning("Stripe customer validation failed (network?) for %s: %s", safe_customer_id, exc)
                return safe_customer_id

        # No stored customer ID — create a new one.
        new_cid = _create_new_stripe_customer()
        if new_cid:
            _nullify_and_refresh_customer_id(new_cid)
        return new_cid

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
            raise HTTPException(status_code=503, detail="Stripe is not configured. Add `stripe.secret_key` in `environment settings` or set `STRIPE_SECRET_KEY`.")

        plan_key = str(plan_id or "").strip().lower()
        plan = STRIPE_SUBSCRIPTION_PLANS.get(plan_key)
        if not plan:
            raise HTTPException(status_code=400, detail="Invalid subscription plan.")

        price_id = str(plan.get("price_id") or "").strip()
        monthly_credits = int(plan.get("credits") or 0)
        if not price_id or monthly_credits <= 0:
            logging.error(
                "Stripe subscription plan misconfigured: plan_id=%s price_id=%s monthly_credits=%s",
                plan_key,
                price_id,
                monthly_credits,
            )
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

        resolved_customer_id = _resolve_or_create_stripe_customer(user_id, user_email, stripe_customer_id)
        if resolved_customer_id:
            form_items.append(("customer", resolved_customer_id))
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
        except urllib.error.HTTPError as exc:
            stripe_message, raw_body = extract_stripe_http_error_details(exc)
            logging.error(
                "Stripe subscription checkout session creation failed: status=%s plan_id=%s price_id=%s success_url=%s cancel_url=%s stripe_message=%s stripe_body=%s",
                getattr(exc, "code", None),
                plan_key,
                price_id,
                success_url,
                cancel_url,
                stripe_message,
                raw_body[:1500],
            )
            raise HTTPException(
                status_code=502,
                detail=f"Could not create Stripe subscription checkout session. Stripe error: {stripe_message}",
            )
        except Exception as exc:
            logging.exception(
                "Stripe subscription checkout session creation failed unexpectedly: plan_id=%s price_id=%s success_url=%s cancel_url=%s",
                plan_key,
                price_id,
                success_url,
                cancel_url,
            )
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
            raise HTTPException(status_code=503, detail="Stripe is not configured. Add `stripe.secret_key` in `environment settings` or set `STRIPE_SECRET_KEY`.")

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

        resolved_customer_id = _resolve_or_create_stripe_customer(user_id, user_email, stripe_customer_id)
        if resolved_customer_id:
            form_items.append(("customer", resolved_customer_id))
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
        except urllib.error.HTTPError as exc:
            stripe_message, raw_body = extract_stripe_http_error_details(exc)
            logging.error(
                "Stripe top-up checkout session creation failed: status=%s package_id=%s price_id=%s success_url=%s cancel_url=%s stripe_message=%s stripe_body=%s",
                getattr(exc, "code", None),
                package_key,
                price_id,
                success_url,
                cancel_url,
                stripe_message,
                raw_body[:1500],
            )
            raise HTTPException(
                status_code=502,
                detail=f"Could not create Stripe checkout session. Stripe error: {stripe_message}",
            )
        except Exception as exc:
            logging.exception(
                "Stripe top-up checkout session creation failed unexpectedly: package_id=%s price_id=%s success_url=%s cancel_url=%s",
                package_key,
                price_id,
                success_url,
                cancel_url,
            )
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
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
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

        auth_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(auth_db_path)
        with pgdb.connect(auth_db_path) as conn:
            conn.row_factory = pgdb.Row
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

        auth_db_path = db_path or DEFAULT_DB_PATH
        ensure_users_table(auth_db_path)
        with pgdb.connect(auth_db_path) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                "SELECT niche FROM users WHERE id = ?",
                (target_user_id,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="Authenticated user does not exist.")
        return str(row["niche"] or "").strip()

    @app.post("/api/enrich")
    def run_enrichment(payload: EnrichRequest, background_tasks: BackgroundTasks, request: Request) -> JSONResponse:
        print(f"[enrich] POST /api/enrich â€” limit={payload.limit}")
        db_path = resolve_path(payload.db_path, DEFAULT_DB_PATH)
        config_path = resolve_path(None, DEFAULT_CONFIG_PATH)
        if not has_any_ai_api_key(config_path):
            raise HTTPException(status_code=503, detail="Missing AI API Key")
        try:
            ensure_system_tables(db_path)
        except Exception as _db_exc:
            print(f"[enrich] DB init error: {_db_exc}")
            raise HTTPException(status_code=500, detail="Database offline")

        payload_data = payload.model_dump()
        # Force unlock enrichment for all plans: keep authentication, bypass plan checks.
        session_token = require_authenticated_session(request, fallback_token=payload_data.get("token"))
        billing: dict[str, Any] = {}
        try:
            billing = load_user_billing_context(session_token, allow_stripe_recovery=False)
        except Exception:
            billing = {}
        access = get_plan_feature_access(_normalize_plan_key((billing or {}).get("plan_key"), fallback=DEFAULT_PLAN_KEY))
        user_id = require_current_user_id(request, fallback_token=payload_data.get("token"), db_path=db_path)
        session_token = require_authenticated_session(request, fallback_token=payload_data.pop("token", ""))
        requested_niche = str(payload_data.get("user_niche") or "").strip()
        profile_niche = str(resolve_user_niche_from_user_id(user_id, db_path=db_path) or "").strip()
        payload_data["user_niche"] = requested_niche or profile_niche
        payload_data["_ai_model"] = str(access.get("ai_model") or DEFAULT_AI_MODEL)
        payload_data["_queue_priority"] = bool(access.get("queue_priority"))
        payload_data["_plan_type"] = str(access.get("plan_type") or billing.get("plan_key") or DEFAULT_PLAN_KEY)
        requested_limit = int(payload_data.get("limit") or 50)
        effective_limit = max(1, min(requested_limit, 200))
        requested_lead_ids = payload_data.get("lead_ids") or []
        if isinstance(requested_lead_ids, list) and requested_lead_ids:
            effective_limit = min(effective_limit, len(requested_lead_ids))
        required_enrich_credits = max(1, effective_limit) * ENRICH_CREDIT_COST_PER_LEAD
        current_credits = max(0, int((billing or {}).get("credits_balance") or 0))
        if current_credits < required_enrich_credits:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Insufficient credits for enrichment. Required: {required_enrich_credits}, "
                    f"available: {current_credits}."
                ),
            )
        payload_data["limit"] = effective_limit
        payload_data["requested_limit"] = requested_limit
        payload_data["_credits_per_success"] = ENRICH_CREDIT_COST_PER_LEAD
        raw_lead_ids = payload_data.get("lead_ids") or []
        if isinstance(raw_lead_ids, list):
            payload_data["lead_ids"] = [int(x) for x in raw_lead_ids if str(x).strip().isdigit()][:500]
        else:
            payload_data["lead_ids"] = []
        print(f"DEBUG: Received {len(payload_data['lead_ids'])} for enrichment: {payload_data['lead_ids']}")

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
        enricher = _create_lead_enricher(
            db_path=str(db_path),
            headless=True,
            config_path=str(DEFAULT_CONFIG_PATH),
            user_id=user_id,
        )
        exported = enricher.export_ai_mailer_ready(output_csv=str(output_path))
        return {"exported": exported, "output_csv": str(output_path)}

    @app.post("/api/mailer/send")
    def run_mailer(payload: MailerRequest, background_tasks: BackgroundTasks, request: Request) -> dict:
        print(f"[mailer] POST /api/mailer/send â€” limit={payload.limit}")
        session_token, billing, access = resolve_plan_access_context(request, allow_stripe_recovery=False)
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
        smtp_resolution = resolve_mailer_smtp_accounts_for_send(
            session_token=session_token,
            user_id=user_id,
            billing=billing,
            requested_limit=requested_limit,
            db_path=db_path,
        )
        payload_data["limit"] = min(
            requested_limit,
            available_credits,
            int(smtp_resolution.get("effective_limit") or requested_limit),
        )
        payload_data["_queue_priority"] = bool(access.get("queue_priority"))
        payload_data["_ai_model"] = str(access.get("ai_model") or DEFAULT_AI_MODEL)
        payload_data["_credit_capped"] = bool(payload_data["limit"] < requested_limit)
        payload_data["_smtp_accounts_override"] = list(smtp_resolution.get("accounts") or [])
        payload_data["_smtp_source"] = str(smtp_resolution.get("source") or "custom")
        payload_data["_smtp_quota_limit"] = int(smtp_resolution.get("system_quota_limit") or SYSTEM_SMTP_DEFAULT_SEND_LIMIT)
        payload_data["_smtp_quota_remaining_before"] = int(smtp_resolution.get("system_quota_remaining") or 0)
        payload_data["_smtp_quota_capped"] = bool(int(smtp_resolution.get("effective_limit") or requested_limit) < requested_limit)
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

    @app.get("/api/leads/{lead_id}/email-history")
    def get_lead_email_history(lead_id: int, request: Request, limit: int = Query(default=100, ge=1, le=500)) -> dict:
        user_id = require_current_user_id(request)
        lead_id_value = int(lead_id)

        owns_lead = False
        if is_supabase_primary_enabled(DEFAULT_CONFIG_PATH):
            client = get_supabase_client(DEFAULT_CONFIG_PATH)
            if client is not None:
                try:
                    rows = (
                        client.table("leads")
                        .select("id")
                        .eq("id", lead_id_value)
                        .eq("user_id", str(user_id))
                        .limit(1)
                        .execute()
                        .data
                        or []
                    )
                    owns_lead = bool(rows)
                except Exception:
                    owns_lead = False

        if not owns_lead:
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
                row = conn.execute(
                    "SELECT id FROM leads WHERE id = ? AND COALESCE(NULLIF(user_id, ''), 'legacy') = ? LIMIT 1",
                    (lead_id_value, str(user_id or "legacy")),
                ).fetchone()
                owns_lead = row is not None

        if not owns_lead:
            raise HTTPException(status_code=404, detail="Lead not found")

        items = list_email_communications_for_lead(
            lead_id=lead_id_value,
            user_id=str(user_id or "legacy"),
            limit=int(limit),
        )
        return {
            "status": "ok",
            "lead_id": lead_id_value,
            "count": len(items),
            "items": items,
        }

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
        ensure_user_mailer_smtp_ready(session_token=session_token, user_id=user_id, db_path=DEFAULT_DB_PATH)
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
            raise HTTPException(status_code=503, detail="Azure OpenAI deployment is not configured.")

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

    # â”€â”€ Lead Qualifier â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    @app.get("/api/leads/qualify")
    def qualify_leads(request: Request) -> dict:
        """
                Dynamic Lead Qualifier (Niche-Agnostic).

                Uses user selected niche + context benchmark instead of fixed city-only logic.
                Buckets:
                    1. ghost            â€“ digital presence exists, but critical niche signal is missing
                    2. invisible_giant  â€“ operationally large, digitally quiet
                    3. tech_debt        â€“ technical stack drags conversion and visibility

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
        requested_niche = str(request.query_params.get("niche") or "").strip()
        if requested_niche in NICHES:
            selected_niche = requested_niche

        excluded_statuses = {
            "blacklisted", "closed", "low_priority", "paid",
            "qualified_not_interested",
        }

        qualifier_scope = "user"

        with pgdb.connect(db_path) as conn:
            conn.row_factory = pgdb.Row
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

        # Build city â†’ review counts (kept as part of context benchmark)
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

        no_website: list[dict] = []
        traffic_opportunity: list[dict] = []
        competitor_gap: list[dict] = []
        site_speed: list[dict] = []

        for lead in leads_raw:
            try:
                lead_id = _qualifier_to_int(lead.get("id"), default=0)
                if lead_id <= 0:
                    continue

                website = str(lead.get("website_url") or "").strip().lower()
                has_website = bool(website and website not in ("none", ""))
                rating = lead.get("rating")
                review_count = _qualifier_to_int(lead.get("review_count"), default=0)
                city = _qualifier_extract_city(lead.get("address", ""))
                city_max = max(city_reviews.get(city, [0]))
                insecure = bool(lead.get("insecure_site"))
                ai_score = _qualifier_to_float(lead.get("ai_score"), default=0.0)
                metrics = _qualifier_extract_metrics(lead)

                raw_enrichment = lead.get("enrichment_data")
                enrichment_obj = _qualifier_parse_enrichment(raw_enrichment)
                competitive_hook = str(enrichment_obj.get("competitive_hook", "") or "")

                # Gap signals aligned with sniped-email-templates categories.
                pagespeed_score = _qualifier_to_float(
                    lead.get("performance_score"),
                    default=_qualifier_to_float(metrics.get("pagespeed_score"), default=100.0),
                )
                seo_score = _qualifier_to_float(
                    lead.get("seo_score"),
                    default=max(0.0, min(100.0, (metrics.get("authority", 0.0) * 2.0) + (metrics.get("organic_traffic", 0.0) / 25.0))),
                )
                best_signal = _lead_compute_best_score(lead, enrichment_obj)
                high_potential = bool(ai_score >= 6.0 or best_signal >= 60.0 or review_count >= 15)

                missing_website_gap = not has_website
                site_speed_gap = pagespeed_score < 50.0

                authority = _qualifier_to_float(metrics.get("authority"), default=0.0)
                competitor_avg = _qualifier_to_float(metrics.get("competitor_avg"), default=0.0)
                competitor_text = " ".join(
                    [
                        str(lead.get("main_shortcoming") or ""),
                        str(lead.get("ai_description") or ""),
                        competitive_hook,
                    ]
                ).lower()
                competitor_gap_flag = bool(
                    (competitor_avg > 0 and authority < competitor_avg)
                    or any(term in competitor_text for term in ["outrank", "competitor", "behind competitors", "ranking gap"])
                )

                low_seo_or_perf = bool(
                    seo_score < 55.0
                    or pagespeed_score < 65.0
                    or _qualifier_to_int(metrics.get("backlink_count"), default=0) < 20
                    or _qualifier_to_float(metrics.get("organic_traffic"), default=0.0) < 120.0
                )
                traffic_opportunity_gap = bool(has_website and high_potential and low_seo_or_perf)

                gap_scores: list[tuple[str, float]] = []
                if missing_website_gap:
                    gap_scores.append(("no_website", 100.0))
                if site_speed_gap:
                    gap_scores.append(("site_speed", 75.0 + max(0.0, (50.0 - pagespeed_score) * 0.4)))
                if competitor_gap_flag:
                    gap_scores.append(("competitor_gap", 72.0 + max(0.0, (competitor_avg - authority) * 0.8)))
                if traffic_opportunity_gap:
                    gap_scores.append(("traffic_opportunity", 70.0 + max(0.0, (60.0 - seo_score) * 0.35)))

                if not gap_scores:
                    continue

                top_gap, top_gap_score = sorted(gap_scores, key=lambda item: item[1], reverse=True)[0]

                if top_gap == "no_website":
                    opportunity_pitch = f"This lead is a prime candidate for a {selected_niche} pitch because they lack a website."
                elif top_gap == "traffic_opportunity":
                    opportunity_pitch = f"This lead is a prime candidate for a {selected_niche} pitch due to high potential with weak SEO/performance coverage."
                elif top_gap == "competitor_gap":
                    opportunity_pitch = f"This lead is a prime candidate for a {selected_niche} pitch because competitors are outranking them."
                else:
                    opportunity_pitch = f"This lead is a prime candidate for a {selected_niche} pitch because their site speed is below 50%."

                latest_review_rating = _qualifier_to_float(
                    enrichment_obj.get("latest_review_rating", enrichment_obj.get("last_review_rating", rating)),
                    default=_qualifier_to_float(rating, default=0.0),
                )
                latest_review_days_ago = _qualifier_to_int(
                    enrichment_obj.get("latest_review_days_ago", enrichment_obj.get("last_review_days_ago", 999)),
                    default=999,
                )
                competitor_recent_launch = _qualifier_to_bool(
                    enrichment_obj.get("competitor_recent_launch", enrichment_obj.get("competitor_new_website", False))
                )
                social_inactive_days = _qualifier_to_int(
                    enrichment_obj.get("social_inactive_days", enrichment_obj.get("last_social_post_days", 0)),
                    default=0,
                )

                score_breakdown: list[dict[str, Any]] = []
                if missing_website_gap:
                    score_breakdown.append({"signal": "No mobile-friendly website", "points": 3})
                if traffic_opportunity_gap:
                    score_breakdown.append({"signal": "High potential traffic being missed", "points": 2})
                if competitor_gap_flag:
                    score_breakdown.append({"signal": "Competitors are outranking this lead", "points": 2})
                if site_speed_gap:
                    score_breakdown.append({"signal": "Site performance below 50%", "points": 3})
                if latest_review_rating > 0 and latest_review_rating <= 3.0:
                    score_breakdown.append({"signal": "Recent low review sentiment", "points": 1})
                if social_inactive_days >= 30:
                    score_breakdown.append({"signal": f"Social media inactive for {social_inactive_days} days", "points": 1})

                raw_total_score = sum(_qualifier_to_int(entry.get("points"), default=0) for entry in score_breakdown)
                qualifier_score = max(2, min(10, raw_total_score))

                if qualifier_score >= 8:
                    tier_code = "bleeding"
                    tier_label = "Contact Today"
                elif qualifier_score >= 5:
                    tier_code = "warm"
                    tier_label = "Contact This Week"
                else:
                    tier_code = "nurture"
                    tier_label = "Follow up in 30 days"

                if latest_review_rating > 0 and latest_review_rating <= 2.5 and latest_review_days_ago <= 14:
                    urgency_signal = f"Last Google review was {latest_review_rating:.1f} star, unanswered — {latest_review_days_ago} days ago."
                elif competitor_recent_launch:
                    urgency_signal = "Competitor just launched a new website and is likely taking search demand right now."
                elif social_inactive_days >= 30:
                    urgency_signal = f"Social media inactive for {social_inactive_days} days, signaling stale acquisition channels."
                elif top_gap == "site_speed":
                    urgency_signal = f"Performance score is {round(pagespeed_score, 1)}/100; visitors are dropping before conversion."
                elif top_gap == "no_website":
                    urgency_signal = "No website presence means this lead is losing buyer intent daily."
                else:
                    urgency_signal = "Strong commercial gap identified — delaying outreach increases competitor advantage."

                pitch_components: list[str] = []
                if top_gap == "no_website":
                    pitch_components.append("missing website foundation")
                elif top_gap == "traffic_opportunity":
                    pitch_components.append("traffic opportunity from weak SEO/performance")
                elif top_gap == "competitor_gap":
                    pitch_components.append("competitor ranking pressure")
                else:
                    pitch_components.append("site speed drag")
                if competitor_gap_flag and top_gap != "competitor_gap":
                    pitch_components.append("competitor outranking")
                if site_speed_gap and top_gap != "site_speed":
                    pitch_components.append("mobile performance issues")
                pitch_angle = f"Pitch angle: Lead with {' + '.join(pitch_components[:2])}."

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
                    "opportunity_pitch": opportunity_pitch,
                    "tier_code": tier_code,
                    "tier_label": tier_label,
                    "qualifier_score": qualifier_score,
                    "urgency_signal": urgency_signal,
                    "score_breakdown": score_breakdown,
                    "pitch_angle": pitch_angle,
                    "gold_mine_gap": top_gap,
                    "gold_mine_score": round(top_gap_score, 1),
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
                    },
                    "signals": {
                        "seo_score": round(seo_score, 1),
                        "performance_score": round(pagespeed_score, 1),
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

                if top_gap == "no_website":
                    no_website.append(out)
                elif top_gap == "traffic_opportunity":
                    traffic_opportunity.append(out)
                elif top_gap == "competitor_gap":
                    competitor_gap.append(out)
                elif top_gap == "site_speed":
                    site_speed.append(out)
            except Exception as lead_exc:
                logging.debug("Qualifier skipped lead due to malformed data: %s", lead_exc)
                continue

        # Backward compatibility keys for older clients.
        ghost = no_website
        invisible_local = traffic_opportunity
        invisible_giant = competitor_gap
        tech_debt = site_speed
        low_authority = site_speed

        no_website.sort(key=lambda item: -_qualifier_to_float(item.get("qualifier_score"), default=0.0))
        traffic_opportunity.sort(key=lambda item: -_qualifier_to_float(item.get("qualifier_score"), default=0.0))
        competitor_gap.sort(key=lambda item: -_qualifier_to_float(item.get("qualifier_score"), default=0.0))
        site_speed.sort(key=lambda item: -_qualifier_to_float(item.get("qualifier_score"), default=0.0))

        total_count = len(no_website) + len(traffic_opportunity) + len(competitor_gap) + len(site_speed)

        return {
            "selected_niche": selected_niche,
            "scope": qualifier_scope,
            "context_benchmark": {
                "niche_avg_score": round(niche_avg_score, 2),
                "city_reviews": city_reviews,
            },
            "gap_buckets": {
                "no_website": no_website,
                "traffic_opportunity": traffic_opportunity,
                "competitor_gap": competitor_gap,
                "site_speed": site_speed,
            },
            "no_website": no_website,
            "traffic_opportunity": traffic_opportunity,
            "competitor_gap": competitor_gap,
            "site_speed": site_speed,
            "ghost": ghost,
            "invisible_local": invisible_local,
            "invisible_giant": invisible_giant,
            "tech_debt": tech_debt,
            "low_authority": low_authority,
            "total": total_count,
            "counts": {
                "no_website": len(no_website),
                "traffic_opportunity": len(traffic_opportunity),
                "competitor_gap": len(competitor_gap),
                "site_speed": len(site_speed),
                "ghost": len(ghost),
                "invisible_local": len(invisible_local),
                "invisible_giant": len(invisible_giant),
                "tech_debt": len(tech_debt),
                "low_authority": len(low_authority),
            },
        }

    # â”€â”€ Auth â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

        if auth_email_exists(email, config_path=DEFAULT_CONFIG_PATH, db_path=DEFAULT_DB_PATH):
            raise HTTPException(status_code=409, detail="An account with this email already exists.")

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

        try:
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
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
        except pgdb.IntegrityError:
            raise HTTPException(status_code=409, detail="An account with this email already exists.")
        return {"token": token, "niche": req.niche, "email": email, "display_name": display_name}

    @app.get("/api/auth/check-email")
    def auth_check_email(email: str = Query(..., min_length=3, max_length=320)) -> dict:
        normalized_email = str(email or "").strip().lower()
        if not normalized_email or "@" not in normalized_email:
            raise HTTPException(status_code=400, detail="Invalid email address.")

        exists = auth_email_exists(normalized_email, config_path=DEFAULT_CONFIG_PATH, db_path=DEFAULT_DB_PATH)
        return {
            "ok": True,
            "email": normalized_email,
            "available": not exists,
            "detail": "" if not exists else "An account with this email already exists.",
        }

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
                    "id,password_hash,salt,niche,token,display_name,contact_name,account_type,is_blocked,blocked_reason"
                ).eq("email", email).limit(1).execute()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Supabase login query failed: {exc}")
            rows = list(getattr(response, "data", None) or [])
            if not rows:
                raise HTTPException(status_code=401, detail="Invalid email or password.")
            row = rows[0]
            if bool(row.get("is_blocked") or False):
                block_reason = str(row.get("blocked_reason") or "").strip() or "Your account has been blocked by admin."
                raise HTTPException(status_code=403, detail=block_reason)
            expected = _hash_password(req.password, str(row.get("salt") or ""))
            if not secrets.compare_digest(expected, str(row.get("password_hash") or "")):
                raise HTTPException(status_code=401, detail="Invalid email or password.")
            token = str(row.get("token") or "") or str(uuid.uuid4())
            now_iso = utc_now_iso()
            try:
                sb_client.table("users").update(
                    {
                        "token": token,
                        "last_login_at": now_iso,
                        "updated_at": now_iso,
                    }
                ).eq("id", row.get("id")).execute()
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
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            try:
                row = conn.execute(
                    "SELECT id, password_hash, salt, niche, token, display_name, contact_name, account_type, COALESCE(is_blocked, FALSE) AS is_blocked, blocked_reason FROM users WHERE email = ?",
                    (email,),
                ).fetchone()
            except Exception as exc:
                if "does not exist" in str(exc).lower():
                    row = conn.execute(
                        "SELECT id, password_hash, salt, niche, token, display_name, contact_name, account_type, 0 AS is_blocked, '' AS blocked_reason FROM users WHERE email = ?",
                        (email,),
                    ).fetchone()
                else:
                    raise
        if row is None:
            raise HTTPException(status_code=401, detail="Invalid email or password.")
        if bool(row["is_blocked"] or False):
            block_reason = str(row["blocked_reason"] or "").strip() or "Your account has been blocked by admin."
            raise HTTPException(status_code=403, detail=block_reason)
        expected = _hash_password(req.password, row["salt"])
        if not secrets.compare_digest(expected, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid email or password.")
        token = row["token"] or str(uuid.uuid4())
        now_iso = utc_now_iso()
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.execute(
                "UPDATE users SET token = ?, last_login_at = ?, updated_at = ? WHERE id = ?",
                (token, now_iso, now_iso, row["id"]),
            )
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
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
                conn.row_factory = pgdb.Row
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
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
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
        ensure_supabase_runtime("auth profile")
        billing = load_user_billing_context(token)
        jwt_user_id = resolve_supabase_auth_user_id(token, DEFAULT_CONFIG_PATH)

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            if not ensure_supabase_users_table(DEFAULT_CONFIG_PATH):
                raise HTTPException(
                    status_code=503,
                    detail="Supabase users table is missing. Run supabase_schema.sql in Supabase SQL Editor.",
                )
            sb_client = get_supabase_client_for_token(DEFAULT_CONFIG_PATH, token)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            row_dict = None
            # Try full select first, fall back to minimal columns if schema is incomplete
            for attempt_cols in [
                "email,niche,display_name,contact_name,account_type,quickstart_completed,average_deal_value,is_admin,last_login_at",
                "email,niche,display_name,contact_name,account_type,is_admin,last_login_at",
                "email,niche,display_name,contact_name,account_type",
            ]:
                try:
                    query = sb_client.table("users").select(attempt_cols)
                    if jwt_user_id:
                        query = query.eq("auth_user_id", jwt_user_id)
                    else:
                        query = query.eq("token", token)
                    response = query.limit(1).execute()
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
            credit_snapshot = _load_user_credit_snapshot(str(jwt_user_id or billing.get("id") or "").strip(), db_path=DEFAULT_DB_PATH)
            return {
                "email": row_dict.get("email") or "",
                "niche": row_dict.get("niche") or "",
                "display_name": row_dict.get("display_name") or "",
                "contact_name": row_dict.get("contact_name") or "",
                "account_type": row_dict.get("account_type") or "entrepreneur",
                "is_admin": bool(row_dict.get("is_admin") or False),
                "last_login_at": row_dict.get("last_login_at"),
                "credits_balance": int(credit_snapshot.get("credits_balance") or 0),
                "credits_limit": int(credit_snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
                "monthly_limit": int(credit_snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
                "monthly_quota": int(credit_snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
                "topup_credits_balance": int(credit_snapshot.get("topup_credits_balance") or 0),
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

        # Fallback to legacy store if Supabase not available
        ensure_users_table(DEFAULT_DB_PATH)
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                "SELECT email, niche, display_name, contact_name, account_type, quickstart_completed, average_deal_value, is_admin, last_login_at FROM users WHERE token = ?",
                (token,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="Invalid or expired session token.")
        plan_key = str(billing.get("plan_key") or "free").strip().lower()
        is_subscribed = bool(billing.get("subscription_active"))
        access = get_plan_feature_access(plan_key)
        credit_snapshot = _load_user_credit_snapshot(str(billing.get("id") or resolve_user_id_from_session_token(token) or "").strip(), db_path=DEFAULT_DB_PATH)
        return {
            "email": row["email"] or "",
            "niche": row["niche"] or "",
            "display_name": row["display_name"] or "",
            "contact_name": row["contact_name"] or "",
            "account_type": row["account_type"] or "entrepreneur",
            "is_admin": bool(row["is_admin"] or False),
            "last_login_at": row["last_login_at"],
            "credits_balance": int(credit_snapshot.get("credits_balance") or 0),
            "credits_limit": int(credit_snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
            "monthly_limit": int(credit_snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
            "monthly_quota": int(credit_snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT),
            "topup_credits_balance": int(credit_snapshot.get("topup_credits_balance") or 0),
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

    @app.get("/api/auth/me")
    def auth_me(request: Request) -> dict:
        token = require_authenticated_session(request)
        return auth_profile(SessionTokenRequest(token=token))

    @app.get("/api/user/credits")
    def user_credits(request: Request) -> dict:
        user_id = require_current_user_id(request)
        snapshot = _load_user_credit_snapshot(user_id, db_path=DEFAULT_DB_PATH)
        credits_balance = max(0, int(snapshot.get("credits_balance") or 0))
        credits_limit = max(1, int(snapshot.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT))
        topup_credits_balance = max(0, int(snapshot.get("topup_credits_balance") or 0))
        return {
            "user_id": str(snapshot.get("user_id") or user_id),
            "credits_balance": credits_balance,
            "credits_limit": credits_limit,
            "monthly_limit": credits_limit,
            "monthly_quota": credits_limit,
            "topup_credits_balance": topup_credits_balance,
            "updated_at": utc_now_iso(),
        }

    @app.get("/api/admin/overview")
    def admin_overview(request: Request) -> dict:
        ensure_users_table(DEFAULT_DB_PATH)
        ensure_system_task_table(DEFAULT_DB_PATH)
        ensure_revenue_log_table(DEFAULT_DB_PATH)
        ensure_credit_logs_table(DEFAULT_DB_PATH)

        users_rows: list[dict[str, Any]] = []
        user_email_map: dict[str, str] = {}
        total_users = 0
        total_leads = 0
        mrr_value = 0.0
        total_revenue_value = 0.0
        latest_scrape_status = "unknown"
        latest_scrape_error = ""
        latest_scrape_updated_at = ""
        transactions: list[dict[str, Any]] = []
        top_scrapers: list[dict[str, Any]] = []
        logs: list[dict[str, Any]] = []
        lead_quality = {
            "success_rate": 0.0,
            "successful": 0,
            "attempted": 0,
        }

        def _compute_lead_quality(leads_rows: list[dict[str, Any]]) -> dict[str, Any]:
            attempted = 0
            successful = 0
            for lead in leads_rows:
                status_text = str(lead.get("enrichment_status") or "").strip().lower()
                if not status_text or status_text in {"pending", "queued", "idle", "new"}:
                    continue
                attempted += 1
                if status_text in {"enriched", "success", "completed"}:
                    successful += 1
            rate = round((successful / attempted) * 100.0, 2) if attempted > 0 else 0.0
            return {
                "success_rate": rate,
                "successful": successful,
                "attempted": attempted,
            }

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            admin_client = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
            if admin_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")

            users_rows = supabase_select_rows(
                admin_client,
                "users",
                columns="id,email,plan_key,subscription_active,credits_balance,monthly_quota,monthly_limit,credits_limit,is_admin,is_blocked,blocked_at,blocked_reason,last_login_at,created_at,updated_at",
                order_by="created_at",
                desc=True,
                limit=5000,
            )
            total_users = len(users_rows)
            user_email_map = {
                str(row.get("id") or "").strip(): str(row.get("email") or "").strip().lower()
                for row in users_rows
                if str(row.get("id") or "").strip()
            }

            try:
                total_leads_resp = admin_client.table("leads").select("id", count="exact").limit(1).execute()
                total_leads = int(getattr(total_leads_resp, "count", 0) or 0)
            except Exception:
                total_leads = len(supabase_select_rows(admin_client, "leads", columns="id", limit=500000))

            lead_rows = supabase_select_rows(
                admin_client,
                "leads",
                columns="id,user_id,enrichment_status",
                limit=200000,
            )
            lead_quality = _compute_lead_quality(lead_rows)
            scraper_counts: dict[str, int] = {}
            for row in lead_rows:
                owner = str(row.get("user_id") or "").strip()
                if not owner:
                    continue
                scraper_counts[owner] = int(scraper_counts.get(owner, 0) + 1)
            top_scrapers = [
                {
                    "user_id": user_id,
                    "email": user_email_map.get(user_id, ""),
                    "scraped_count": count,
                }
                for user_id, count in sorted(scraper_counts.items(), key=lambda item: item[1], reverse=True)[:10]
            ]

            if supabase_table_available(DEFAULT_CONFIG_PATH, "revenue_log"):
                revenue_rows = supabase_select_rows(admin_client, "revenue_log", columns="amount,is_recurring", limit=100000)
                total_revenue_value = float(sum(float(r.get("amount") or 0) for r in revenue_rows))
                mrr_value = float(sum(float(r.get("amount") or 0) for r in revenue_rows if int(r.get("is_recurring") or 0) == 1))
                transaction_rows = supabase_select_rows(
                    admin_client,
                    "revenue_log",
                    columns="id,user_id,amount,service_type,lead_name,lead_id,is_recurring,date",
                    order_by="id",
                    desc=True,
                    limit=50,
                )
                transactions = [
                    {
                        "id": int(row.get("id") or 0),
                        "user_id": str(row.get("user_id") or "").strip(),
                        "email": user_email_map.get(str(row.get("user_id") or "").strip(), ""),
                        "amount": float(row.get("amount") or 0),
                        "service_type": str(row.get("service_type") or "").strip(),
                        "lead_name": str(row.get("lead_name") or "").strip(),
                        "is_recurring": bool(int(row.get("is_recurring") or 0)),
                        "date": row.get("date"),
                    }
                    for row in transaction_rows
                ]

            if supabase_table_available(DEFAULT_CONFIG_PATH, "system_tasks"):
                task_rows = supabase_select_rows(
                    admin_client,
                    "system_tasks",
                    columns="id,task_type,status,error,finished_at,started_at,created_at",
                    filters={"task_type": "scrape"},
                    order_by="id",
                    desc=True,
                    limit=1,
                )
                if task_rows:
                    task = task_rows[0]
                    latest_scrape_status = str(task.get("status") or "unknown").strip().lower()
                    latest_scrape_error = str(task.get("error") or "").strip()
                    latest_scrape_updated_at = str(task.get("finished_at") or task.get("started_at") or task.get("created_at") or "").strip()
                recent_task_logs = supabase_select_rows(
                    admin_client,
                    "system_tasks",
                    columns="id,user_id,task_type,status,error,created_at,started_at,finished_at",
                    order_by="id",
                    desc=True,
                    limit=40,
                )
                logs.extend(
                    [
                        {
                            "kind": "task",
                            "id": int(row.get("id") or 0),
                            "user_id": str(row.get("user_id") or "").strip(),
                            "email": user_email_map.get(str(row.get("user_id") or "").strip(), ""),
                            "status": str(row.get("status") or "").strip(),
                            "message": str(row.get("error") or row.get("task_type") or "").strip(),
                            "created_at": row.get("finished_at") or row.get("started_at") or row.get("created_at"),
                        }
                        for row in recent_task_logs
                    ]
                )
            if supabase_table_available(DEFAULT_CONFIG_PATH, "credit_logs"):
                credit_log_rows = supabase_select_rows(
                    admin_client,
                    "credit_logs",
                    columns="id,user_id,amount,action_type,created_at",
                    order_by="id",
                    desc=True,
                    limit=40,
                )
                logs.extend(
                    [
                        {
                            "kind": "credits",
                            "id": int(row.get("id") or 0),
                            "user_id": str(row.get("user_id") or "").strip(),
                            "email": user_email_map.get(str(row.get("user_id") or "").strip(), ""),
                            "status": str(row.get("action_type") or "").strip(),
                            "message": f"{int(row.get('amount') or 0)} credits",
                            "created_at": row.get("created_at"),
                        }
                        for row in credit_log_rows
                    ]
                )
        else:
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
                conn.row_factory = pgdb.Row
                rows = conn.execute(
                    """
                    SELECT id,email,plan_key,subscription_active,credits_balance,
                           COALESCE(NULLIF(monthly_quota,0), NULLIF(monthly_limit,0), NULLIF(credits_limit,0), ?) AS credits_limit,
                           COALESCE(is_admin, FALSE) AS is_admin,
                           COALESCE(is_blocked, FALSE) AS is_blocked,
                           blocked_at,
                           blocked_reason,
                           last_login_at,created_at,updated_at
                    FROM users
                    ORDER BY COALESCE(created_at, updated_at) DESC, id DESC
                    """,
                    (DEFAULT_MONTHLY_CREDIT_LIMIT,),
                ).fetchall()
                users_rows = [dict(r) for r in rows]
                total_users = len(users_rows)
                user_email_map = {
                    str(row.get("id") or "").strip(): str(row.get("email") or "").strip().lower()
                    for row in users_rows
                    if str(row.get("id") or "").strip()
                }
                total_leads = int(conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] or 0)
                total_revenue_value = float(conn.execute("SELECT COALESCE(SUM(amount), 0) FROM revenue_log").fetchone()[0] or 0.0)
                mrr_value = float(conn.execute("SELECT COALESCE(SUM(amount), 0) FROM revenue_log WHERE COALESCE(is_recurring, 0) = 1").fetchone()[0] or 0.0)
                revenue_rows = conn.execute(
                    """
                    SELECT id,user_id,amount,service_type,lead_name,lead_id,is_recurring,date
                    FROM revenue_log
                    ORDER BY id DESC
                    LIMIT 50
                    """
                ).fetchall()
                transactions = [
                    {
                        "id": int(row["id"] or 0),
                        "user_id": str(row["user_id"] or "").strip(),
                        "email": user_email_map.get(str(row["user_id"] or "").strip(), ""),
                        "amount": float(row["amount"] or 0),
                        "service_type": str(row["service_type"] or "").strip(),
                        "lead_name": str(row["lead_name"] or "").strip(),
                        "is_recurring": bool(int(row["is_recurring"] or 0)),
                        "date": row["date"],
                    }
                    for row in revenue_rows
                ]
                leads_for_analytics = conn.execute(
                    "SELECT user_id,enrichment_status FROM leads"
                ).fetchall()
                lead_rows = [dict(row) for row in leads_for_analytics]
                lead_quality = _compute_lead_quality(lead_rows)
                scraper_counts: dict[str, int] = {}
                for row in lead_rows:
                    owner = str(row.get("user_id") or "").strip()
                    if not owner:
                        continue
                    scraper_counts[owner] = int(scraper_counts.get(owner, 0) + 1)
                top_scrapers = [
                    {
                        "user_id": user_id,
                        "email": user_email_map.get(user_id, ""),
                        "scraped_count": count,
                    }
                    for user_id, count in sorted(scraper_counts.items(), key=lambda item: item[1], reverse=True)[:10]
                ]
                last_task = conn.execute(
                    "SELECT status,error,finished_at,started_at,created_at FROM system_tasks WHERE task_type = 'scrape' ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if last_task is not None:
                    latest_scrape_status = str(last_task[0] or "unknown").strip().lower()
                    latest_scrape_error = str(last_task[1] or "").strip()
                    latest_scrape_updated_at = str(last_task[2] or last_task[3] or last_task[4] or "").strip()
                task_log_rows = conn.execute(
                    "SELECT id,user_id,task_type,status,error,created_at,started_at,finished_at FROM system_tasks ORDER BY id DESC LIMIT 40"
                ).fetchall()
                logs.extend(
                    [
                        {
                            "kind": "task",
                            "id": int(row["id"] or 0),
                            "user_id": str(row["user_id"] or "").strip(),
                            "email": user_email_map.get(str(row["user_id"] or "").strip(), ""),
                            "status": str(row["status"] or "").strip(),
                            "message": str(row["error"] or row["task_type"] or "").strip(),
                            "created_at": row["finished_at"] or row["started_at"] or row["created_at"],
                        }
                        for row in task_log_rows
                    ]
                )
                credit_log_rows = conn.execute(
                    "SELECT id,user_id,amount,action_type,created_at FROM credit_logs ORDER BY id DESC LIMIT 40"
                ).fetchall()
                logs.extend(
                    [
                        {
                            "kind": "credits",
                            "id": int(row["id"] or 0),
                            "user_id": str(row["user_id"] or "").strip(),
                            "email": user_email_map.get(str(row["user_id"] or "").strip(), ""),
                            "status": str(row["action_type"] or "").strip(),
                            "message": f"{int(row['amount'] or 0)} credits",
                            "created_at": row["created_at"],
                        }
                        for row in credit_log_rows
                    ]
                )

        normalized_users = []
        plan_counts: dict[str, int] = {}
        for row in users_rows:
            plan_key = _normalize_plan_key(row.get("plan_key"), fallback="free")
            plan_counts[plan_key] = int(plan_counts.get(plan_key, 0) + 1)
            resolved_limit = int(row.get("monthly_quota") or row.get("monthly_limit") or row.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT)
            normalized_users.append(
                {
                    "id": str(row.get("id") or ""),
                    "email": str(row.get("email") or "").strip().lower(),
                    "plan_key": plan_key,
                    "plan_name": str(PLAN_DISPLAY_NAMES.get(plan_key, plan_key.title())),
                    "subscription_active": bool(row.get("subscription_active") or False),
                    "credits_balance": int(row.get("credits_balance") or 0),
                    "credits_limit": max(1, resolved_limit),
                    "is_admin": bool(row.get("is_admin") or False),
                    "is_blocked": bool(row.get("is_blocked") or False),
                    "blocked_at": row.get("blocked_at") or None,
                    "blocked_reason": row.get("blocked_reason") or None,
                    "last_login_at": row.get("last_login_at") or row.get("updated_at") or None,
                    "created_at": row.get("created_at") or None,
                }
            )

        scraper_health = "healthy"
        if latest_scrape_status in {"running", "queued"}:
            scraper_health = "running"
        elif latest_scrape_status in {"failed", "cancelled", "stopped"}:
            scraper_health = "failing"

        notification_payload: dict[str, Any] = {
            "active": False,
            "message": "",
            "updated_at": None,
        }
        raw_notification = get_runtime_value(DEFAULT_DB_PATH, "global_notification_banner")
        if raw_notification:
            try:
                parsed_notification = json.loads(str(raw_notification))
                if isinstance(parsed_notification, dict):
                    notification_payload = {
                        "active": bool(parsed_notification.get("active") or False),
                        "message": str(parsed_notification.get("message") or "").strip(),
                        "updated_at": parsed_notification.get("updated_at"),
                    }
            except Exception:
                pass

        logs = sorted(
            logs,
            key=lambda row: str(row.get("created_at") or ""),
            reverse=True,
        )[:80]
        ai_signals_state = load_ai_signals_runtime_state(DEFAULT_DB_PATH)
        ai_signals_state["ai_key_configured"] = has_any_ai_api_key(DEFAULT_CONFIG_PATH)

        return {
            "stats": {
                "total_users": int(total_users),
                "mrr": float(mrr_value),
                "total_revenue": float(total_revenue_value),
                "total_leads": int(total_leads),
            },
            "scraper": {
                "health": scraper_health,
                "last_status": latest_scrape_status,
                "last_error": latest_scrape_error,
                "last_updated_at": latest_scrape_updated_at,
            },
            "plans": plan_counts,
            "users": normalized_users,
            "transactions": transactions,
            "top_scrapers": top_scrapers,
            "lead_quality": lead_quality,
            "logs": logs,
            "notification": notification_payload,
            "ai_signals": ai_signals_state,
        }

    @app.post("/api/admin/credits")
    def admin_update_user_credits(payload: AdminCreditUpdateRequest, request: Request) -> dict:
        action = str(payload.action or "add").strip().lower()
        if action not in {"add", "set", "reset"}:
            raise HTTPException(status_code=400, detail="action must be one of: add, set, reset")

        target_user_id = str(payload.user_id or "").strip()
        target_email = str(payload.email or "").strip().lower()
        if not target_user_id and not target_email:
            raise HTTPException(status_code=400, detail="user_id or email is required")

        note = str(payload.note or "").strip()
        now_iso = utc_now_iso()
        amount = int(payload.amount or 0)

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            admin_client = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
            if admin_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")

            query = admin_client.table("users").select(
                "id,email,credits_balance,monthly_quota,monthly_limit,credits_limit,topup_credits_balance"
            )
            if target_user_id:
                query = query.eq("id", target_user_id)
            else:
                query = query.eq("email", target_email)
            rows = list(getattr(query.limit(1).execute(), "data", None) or [])
            if not rows:
                raise HTTPException(status_code=404, detail="User not found")
            row = rows[0]
            resolved_id = str(row.get("id") or "").strip()
            current_balance = int(row.get("credits_balance") or 0)
            credits_limit = int(row.get("monthly_quota") or row.get("monthly_limit") or row.get("credits_limit") or DEFAULT_MONTHLY_CREDIT_LIMIT)
            topup_balance = int(row.get("topup_credits_balance") or 0)

            if action == "reset":
                next_balance = max(0, int(credits_limit) + max(0, int(topup_balance)))
            elif action == "set":
                next_balance = max(0, amount)
            else:
                next_balance = max(0, int(current_balance) + amount)

            admin_client.table("users").update(
                {
                    "credits": next_balance,
                    "credits_balance": next_balance,
                    "updated_at": now_iso,
                }
            ).eq("id", resolved_id).execute()

            _append_credit_log(
                resolved_id,
                int(next_balance - current_balance),
                f"admin_{action}",
                {
                    "note": note,
                    "admin_email": str(getattr(request.state, "current_user_email", "") or "").strip().lower(),
                    "credits_before": current_balance,
                    "credits_after": next_balance,
                },
                db_path=DEFAULT_DB_PATH,
            )

            return {
                "status": "ok",
                "user_id": resolved_id,
                "email": str(row.get("email") or "").strip().lower(),
                "action": action,
                "credits_before": current_balance,
                "credits_after": next_balance,
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            if target_user_id:
                row = conn.execute(
                    "SELECT id,email,credits_balance,COALESCE(NULLIF(monthly_quota,0),NULLIF(monthly_limit,0),NULLIF(credits_limit,0),?) AS credits_limit,COALESCE(topup_credits_balance,0) AS topup_credits_balance FROM users WHERE id = ? LIMIT 1",
                    (DEFAULT_MONTHLY_CREDIT_LIMIT, target_user_id),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT id,email,credits_balance,COALESCE(NULLIF(monthly_quota,0),NULLIF(monthly_limit,0),NULLIF(credits_limit,0),?) AS credits_limit,COALESCE(topup_credits_balance,0) AS topup_credits_balance FROM users WHERE LOWER(email) = ? LIMIT 1",
                    (DEFAULT_MONTHLY_CREDIT_LIMIT, target_email),
                ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="User not found")

            resolved_id = str(row["id"])
            current_balance = int(row["credits_balance"] or 0)
            credits_limit = int(row["credits_limit"] or DEFAULT_MONTHLY_CREDIT_LIMIT)
            topup_balance = int(row["topup_credits_balance"] or 0)

            if action == "reset":
                next_balance = max(0, int(credits_limit) + max(0, int(topup_balance)))
            elif action == "set":
                next_balance = max(0, amount)
            else:
                next_balance = max(0, int(current_balance) + amount)

            conn.execute(
                "UPDATE users SET credits = ?, credits_balance = ?, updated_at = ? WHERE id = ?",
                (next_balance, next_balance, now_iso, resolved_id),
            )
            conn.commit()

        _append_credit_log(
            resolved_id,
            int(next_balance - current_balance),
            f"admin_{action}",
            {
                "note": note,
                "admin_email": str(getattr(request.state, "current_user_email", "") or "").strip().lower(),
                "credits_before": current_balance,
                "credits_after": next_balance,
            },
            db_path=DEFAULT_DB_PATH,
        )

        return {
            "status": "ok",
            "user_id": resolved_id,
            "email": str(row["email"] or "").strip().lower(),
            "action": action,
            "credits_before": current_balance,
            "credits_after": next_balance,
        }

    @app.get("/api/admin/reply-alerts")
    def admin_reply_alerts(limit: int = Query(default=25, ge=1, le=200)) -> dict:
        ensure_mailer_campaign_tables(DEFAULT_DB_PATH)
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            rows = conn.execute(
                """
                SELECT
                    e.id,
                    e.lead_id,
                    e.user_id,
                    e.email,
                    e.subject_line,
                    e.metadata_json,
                    e.occurred_at,
                    l.business_name,
                    l.status,
                    l.pipeline_stage,
                    l.reply_detected_at,
                    l.last_contacted_at
                FROM CampaignEvents e
                LEFT JOIN leads l ON l.id = e.lead_id
                WHERE LOWER(COALESCE(e.event_type, '')) = 'reply'
                ORDER BY e.id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

        items: list[dict[str, Any]] = []
        for row in rows:
            parsed_metadata = deserialize_json(row["metadata_json"]) or {}
            if not isinstance(parsed_metadata, dict):
                parsed_metadata = {}
            items.append(
                {
                    "id": int(row["id"] or 0),
                    "lead_id": int(row["lead_id"] or 0) if row["lead_id"] is not None else None,
                    "user_id": str(row["user_id"] or "").strip(),
                    "email": str(row["email"] or "").strip(),
                    "business_name": str(row["business_name"] or "").strip(),
                    "subject_line": str(row["subject_line"] or "").strip(),
                    "status": str(row["status"] or "").strip(),
                    "pipeline_stage": str(row["pipeline_stage"] or "").strip(),
                    "reply_detected_at": row["reply_detected_at"],
                    "last_contacted_at": row["last_contacted_at"],
                    "occurred_at": row["occurred_at"],
                    "metadata": parsed_metadata,
                }
            )

        return {
            "items": items,
            "count": len(items),
        }

    @app.post("/api/admin/users/{user_id}/block")
    def admin_block_user(user_id: str, payload: AdminBlockUserRequest, request: Request) -> dict:
        target_user_id = str(user_id or "").strip()
        if not target_user_id:
            raise HTTPException(status_code=400, detail="Missing user id")

        blocked = bool(payload.blocked)
        reason = str(payload.reason or "").strip()
        now_iso = utc_now_iso()

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            admin_client = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
            if admin_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            rows = (
                admin_client.table("users")
                .select("id,email")
                .eq("id", target_user_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if not rows:
                raise HTTPException(status_code=404, detail="User not found")
            row = rows[0]
            update_payload: dict[str, Any] = {
                "is_blocked": blocked,
                "blocked_at": now_iso if blocked else None,
                "blocked_reason": reason if blocked else None,
                "updated_at": now_iso,
            }
            if blocked:
                update_payload["token"] = None
            admin_client.table("users").update(update_payload).eq("id", target_user_id).execute()
            return {
                "status": "ok",
                "user_id": target_user_id,
                "email": str(row.get("email") or "").strip().lower(),
                "is_blocked": blocked,
                "blocked_reason": reason if blocked else "",
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute("SELECT id,email FROM users WHERE id = ? LIMIT 1", (target_user_id,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="User not found")
            if blocked:
                conn.execute(
                    "UPDATE users SET is_blocked = TRUE, blocked_at = ?, blocked_reason = ?, token = NULL, updated_at = ? WHERE id = ?",
                    (now_iso, reason or None, now_iso, target_user_id),
                )
            else:
                conn.execute(
                    "UPDATE users SET is_blocked = FALSE, blocked_at = NULL, blocked_reason = NULL, updated_at = ? WHERE id = ?",
                    (now_iso, target_user_id),
                )
            conn.commit()
            email = str(row["email"] or "").strip().lower()

        return {
            "status": "ok",
            "user_id": target_user_id,
            "email": email,
            "is_blocked": blocked,
            "blocked_reason": reason if blocked else "",
        }

    @app.post("/api/admin/users/{user_id}/impersonate")
    def admin_impersonate_user(user_id: str, request: Request) -> dict:
        target_user_id = str(user_id or "").strip()
        if not target_user_id:
            raise HTTPException(status_code=400, detail="Missing user id")

        now_iso = utc_now_iso()
        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            admin_client = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
            if admin_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            rows = (
                admin_client.table("users")
                .select("id,email,token,is_blocked")
                .eq("id", target_user_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if not rows:
                raise HTTPException(status_code=404, detail="User not found")
            row = rows[0]
            if bool(row.get("is_blocked") or False):
                raise HTTPException(status_code=400, detail="Cannot impersonate blocked user")
            token = str(row.get("token") or "").strip() or str(uuid.uuid4())
            admin_client.table("users").update({"token": token, "last_login_at": now_iso, "updated_at": now_iso}).eq("id", target_user_id).execute()
            return {
                "status": "ok",
                "token": token,
                "user_id": target_user_id,
                "email": str(row.get("email") or "").strip().lower(),
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            try:
                row = conn.execute(
                    "SELECT id,email,token,COALESCE(is_blocked, FALSE) AS is_blocked FROM users WHERE id = ? LIMIT 1",
                    (target_user_id,),
                ).fetchone()
            except Exception as exc:
                if "does not exist" in str(exc).lower():
                    row = conn.execute(
                        "SELECT id,email,token,0 AS is_blocked FROM users WHERE id = ? LIMIT 1",
                        (target_user_id,),
                    ).fetchone()
                else:
                    raise
            if row is None:
                raise HTTPException(status_code=404, detail="User not found")
            if bool(row["is_blocked"] or False):
                raise HTTPException(status_code=400, detail="Cannot impersonate blocked user")
            token = str(row["token"] or "").strip() or str(uuid.uuid4())
            conn.execute(
                "UPDATE users SET token = ?, last_login_at = ?, updated_at = ? WHERE id = ?",
                (token, now_iso, now_iso, target_user_id),
            )
            conn.commit()
            email = str(row["email"] or "").strip().lower()

        return {
            "status": "ok",
            "token": token,
            "user_id": target_user_id,
            "email": email,
        }

    @app.post("/api/admin/users/{user_id}/reset-password")
    def admin_reset_user_password(user_id: str, payload: AdminResetPasswordRequest, request: Request) -> dict:
        target_user_id = str(user_id or "").strip()
        if not target_user_id:
            raise HTTPException(status_code=400, detail="Missing user id")

        reset_token = secrets.token_urlsafe(32)
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        now_iso = utc_now_iso()

        target_email = ""
        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            admin_client = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
            if admin_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            rows = (
                admin_client.table("users")
                .select("id,email")
                .eq("id", target_user_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if not rows:
                raise HTTPException(status_code=404, detail="User not found")
            target_email = str(rows[0].get("email") or "").strip().lower()
            admin_client.table("users").update(
                {
                    "reset_token": reset_token,
                    "reset_token_expires_at": expires_at,
                    "updated_at": now_iso,
                }
            ).eq("id", target_user_id).execute()
        else:
            ensure_users_table(DEFAULT_DB_PATH)
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
                conn.row_factory = pgdb.Row
                row = conn.execute("SELECT id,email FROM users WHERE id = ? LIMIT 1", (target_user_id,)).fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail="User not found")
                target_email = str(row["email"] or "").strip().lower()
                conn.execute(
                    "UPDATE users SET reset_token = ?, reset_token_expires_at = ?, updated_at = ? WHERE id = ?",
                    (reset_token, expires_at, now_iso, target_user_id),
                )
                conn.commit()

        reset_base_url = str(payload.reset_base_url or "").strip().rstrip("/")
        if not reset_base_url:
            reset_base_url = str(os.environ.get("RESET_PASSWORD_BASE_URL") or "https://sniped-one.vercel.app/reset-password").strip().rstrip("/")
        reset_link = f"{reset_base_url}?token={quote_plus(reset_token)}"

        smtp_account: dict[str, Any] = {}
        try:
            smtp_account = get_primary_user_smtp_account(user_id=target_user_id, db_path=DEFAULT_DB_PATH)
        except Exception:
            smtp_account = get_system_smtp_account(DEFAULT_CONFIG_PATH)
        if not smtp_account:
            raise HTTPException(status_code=503, detail="No SMTP account available to send reset email.")

        text_body = (
            "Sniped password reset\n\n"
            "An admin initiated a password reset for your account.\n"
            f"Reset link: {reset_link}\n\n"
            "This link expires in 1 hour."
        )
        html_body = (
            "<p>An admin initiated a password reset for your account.</p>"
            f"<p><a href=\"{reset_link}\">Reset your password</a></p>"
            "<p>This link expires in 1 hour.</p>"
        )
        try:
            send_auth_email(smtp_account, target_email, "Sniped password reset", text_body, html_body)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Password reset email failed: {classify_smtp_error(exc)}")

        return {
            "status": "ok",
            "user_id": target_user_id,
            "email": target_email,
            "expires_at": expires_at,
        }

    @app.post("/api/admin/users/{user_id}/plan")
    def admin_update_user_plan(user_id: str, payload: AdminPlanUpdateRequest, request: Request) -> dict:
        target_user_id = str(user_id or "").strip()
        plan_key = _normalize_plan_key(payload.plan_key, fallback=DEFAULT_PLAN_KEY)
        if plan_key not in PLAN_MONTHLY_QUOTAS:
            raise HTTPException(status_code=400, detail="Invalid plan key")

        monthly_limit = max(1, int(PLAN_MONTHLY_QUOTAS.get(plan_key, DEFAULT_MONTHLY_CREDIT_LIMIT)))
        now_iso = utc_now_iso()
        subscription_active = plan_key != "free"
        subscription_status = "active" if subscription_active else "free"

        if is_supabase_auth_enabled(DEFAULT_CONFIG_PATH):
            ensure_supabase_users_table(DEFAULT_CONFIG_PATH)
            admin_client = get_supabase_admin_client(DEFAULT_CONFIG_PATH) or get_supabase_client(DEFAULT_CONFIG_PATH)
            if admin_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")
            rows = (
                admin_client.table("users")
                .select("id,email,credits_balance,topup_credits_balance")
                .eq("id", target_user_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if not rows:
                raise HTTPException(status_code=404, detail="User not found")
            row = rows[0]
            current_balance = int(row.get("credits_balance") or 0)
            topup_balance = int(row.get("topup_credits_balance") or 0)
            next_balance = max(current_balance, monthly_limit + max(0, topup_balance))
            admin_client.table("users").update(
                {
                    "plan_key": plan_key,
                    "monthly_quota": monthly_limit,
                    "monthly_limit": monthly_limit,
                    "credits_limit": monthly_limit,
                    "subscription_active": subscription_active,
                    "subscription_status": subscription_status,
                    "credits_balance": next_balance,
                    "credits": next_balance,
                    "updated_at": now_iso,
                }
            ).eq("id", target_user_id).execute()
            return {
                "status": "ok",
                "user_id": target_user_id,
                "email": str(row.get("email") or "").strip().lower(),
                "plan_key": plan_key,
                "plan_name": str(PLAN_DISPLAY_NAMES.get(plan_key, plan_key.title())),
                "monthly_limit": monthly_limit,
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                "SELECT id,email,COALESCE(credits_balance,0) AS credits_balance,COALESCE(topup_credits_balance,0) AS topup_credits_balance FROM users WHERE id = ? LIMIT 1",
                (target_user_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="User not found")
            current_balance = int(row["credits_balance"] or 0)
            topup_balance = int(row["topup_credits_balance"] or 0)
            next_balance = max(current_balance, monthly_limit + max(0, topup_balance))
            conn.execute(
                """
                UPDATE users
                SET plan_key = ?,
                    monthly_quota = ?,
                    monthly_limit = ?,
                    credits_limit = ?,
                    subscription_active = ?,
                    subscription_status = ?,
                    credits_balance = ?,
                    credits = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    plan_key,
                    monthly_limit,
                    monthly_limit,
                    monthly_limit,
                    1 if subscription_active else 0,
                    subscription_status,
                    next_balance,
                    next_balance,
                    now_iso,
                    target_user_id,
                ),
            )
            conn.commit()
            email = str(row["email"] or "").strip().lower()

        return {
            "status": "ok",
            "user_id": target_user_id,
            "email": email,
            "plan_key": plan_key,
            "plan_name": str(PLAN_DISPLAY_NAMES.get(plan_key, plan_key.title())),
            "monthly_limit": monthly_limit,
        }

    @app.post("/api/admin/scrapers/restart")
    def admin_restart_scrapers(request: Request) -> dict:
        admin_email = str(getattr(request.state, "current_user_email", "") or "").strip().lower()
        payload = {
            "requested_by": admin_email,
            "requested_at": utc_now_iso(),
        }
        set_runtime_value(DEFAULT_DB_PATH, "admin_scraper_restart_requested", json.dumps(payload, ensure_ascii=False))
        reconcile_orphaned_active_tasks(app, DEFAULT_DB_PATH)
        latest_task = fetch_latest_task(DEFAULT_DB_PATH, "scrape")
        return {
            "status": "restart_requested",
            "requested_at": payload["requested_at"],
            "latest_scrape": latest_task,
        }

    @app.post("/api/admin/notification")
    def admin_set_global_notification(payload: AdminGlobalNotificationRequest, request: Request) -> dict:
        message = str(payload.message or "").strip()
        active = bool(payload.active and message)
        notification_payload = {
            "active": active,
            "message": message,
            "updated_at": utc_now_iso(),
            "updated_by": str(getattr(request.state, "current_user_email", "") or "").strip().lower(),
        }
        set_runtime_value(DEFAULT_DB_PATH, "global_notification_banner", json.dumps(notification_payload, ensure_ascii=False))
        return {
            "status": "ok",
            "notification": notification_payload,
        }

    @app.post("/api/admin/ai-signals")
    def admin_set_ai_signals(payload: AdminAiSignalsToggleRequest, request: Request) -> dict:
        admin_email = str(getattr(request.state, "current_user_email", "") or "").strip().lower()
        state = save_ai_signals_runtime_state(bool(payload.enabled), admin_email, db_path=DEFAULT_DB_PATH)
        return {
            "status": "ok",
            "ai_signals": state,
        }

    @app.get("/api/system/notification")
    def get_global_notification() -> dict:
        raw_value = get_runtime_value(DEFAULT_DB_PATH, "global_notification_banner")
        if not raw_value:
            reply_value = get_runtime_value(DEFAULT_DB_PATH, "reply_detection_latest")
            if not reply_value:
                return {"active": False, "message": "", "updated_at": None}
            try:
                parsed_reply = json.loads(str(reply_value))
                if isinstance(parsed_reply, dict):
                    return {
                        "active": bool(parsed_reply.get("active") or False),
                        "message": str(parsed_reply.get("message") or "").strip(),
                        "updated_at": parsed_reply.get("updated_at"),
                    }
            except Exception:
                pass
            return {"active": False, "message": "", "updated_at": None}
        try:
            parsed = json.loads(str(raw_value))
            if not isinstance(parsed, dict):
                return {"active": False, "message": "", "updated_at": None}
            return {
                "active": bool(parsed.get("active") or False),
                "message": str(parsed.get("message") or "").strip(),
                "updated_at": parsed.get("updated_at"),
            }
        except Exception:
            return {"active": False, "message": "", "updated_at": None}

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

    @app.post("/api/stripe/cancel-subscription")
    def stripe_cancel_subscription(request: Request) -> dict:
        session_token = require_authenticated_session(request)
        billing = load_user_billing_context(session_token)

        if not bool(billing.get("subscription_active")):
            raise HTTPException(status_code=400, detail="No active subscription to cancel.")

        stripe_customer_id = str(billing.get("stripe_customer_id") or "").strip()
        if not stripe_customer_id:
            raise HTTPException(status_code=400, detail="Stripe customer id is missing for this account.")

        stripe_state = _set_subscription_cancel_at_period_end(stripe_customer_id, cancel_at_period_end=True)
        persisted = _persist_manual_subscription_state(
            billing,
            subscription_status="cancelled_pending",
            subscription_cancel_at=stripe_state.get("subscription_cancel_at"),
            subscription_cancel_at_period_end=True,
            stripe_customer_id=stripe_customer_id,
        )

        return {
            "ok": True,
            "subscription_active": True,
            "isSubscribed": True,
            "subscription_status": str(persisted.get("subscription_status") or "cancelled_pending").strip().lower(),
            "subscription_cancel_at": persisted.get("subscription_cancel_at"),
            "subscription_cancel_at_period_end": bool(persisted.get("subscription_cancel_at_period_end")),
            "plan_key": str(persisted.get("plan_key") or "pro").strip().lower(),
            "currentPlanName": str(PLAN_DISPLAY_NAMES.get(str(persisted.get("plan_key") or "pro").strip().lower(), "Pro Plan")),
        }

    @app.post("/api/stripe/reactivate-subscription")
    def stripe_reactivate_subscription(request: Request) -> dict:
        session_token = require_authenticated_session(request)
        billing = load_user_billing_context(session_token)

        if not bool(billing.get("subscription_active")):
            raise HTTPException(status_code=400, detail="No active subscription to reactivate.")

        stripe_customer_id = str(billing.get("stripe_customer_id") or "").strip()
        if not stripe_customer_id:
            raise HTTPException(status_code=400, detail="Stripe customer id is missing for this account.")

        stripe_state = _set_subscription_cancel_at_period_end(stripe_customer_id, cancel_at_period_end=False)
        next_status = str(stripe_state.get("subscription_status") or "active").strip().lower() or "active"
        persisted = _persist_manual_subscription_state(
            billing,
            subscription_status=next_status,
            subscription_cancel_at=None,
            subscription_cancel_at_period_end=False,
            stripe_customer_id=stripe_customer_id,
        )

        return {
            "ok": True,
            "subscription_active": True,
            "isSubscribed": True,
            "subscription_status": str(persisted.get("subscription_status") or "active").strip().lower(),
            "subscription_cancel_at": None,
            "subscription_cancel_at_period_end": False,
            "plan_key": str(persisted.get("plan_key") or "pro").strip().lower(),
            "currentPlanName": str(PLAN_DISPLAY_NAMES.get(str(persisted.get("plan_key") or "pro").strip().lower(), "Pro Plan")),
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

        success_url = STRIPE_SETTINGS_SUCCESS_URL
        cancel_url = STRIPE_SETTINGS_CANCEL_URL

        logging.info(
            "Creating Stripe subscription checkout session: user_id=%s plan_id=%s price_id=%s success_url=%s cancel_url=%s stripe_customer_id_present=%s",
            user_id,
            plan_key,
            str(plan.get("price_id") or "").strip(),
            success_url,
            cancel_url,
            bool(str(billing.get("stripe_customer_id") or "").strip()),
        )

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

    @app.post("/api/create-checkout-session")
    def create_checkout_session_alias(payload: StripeSubscriptionSessionRequest, request: Request) -> dict:
        return stripe_create_subscription_session(payload, request)

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

        success_url = STRIPE_SETTINGS_SUCCESS_URL
        cancel_url = STRIPE_SETTINGS_CANCEL_URL

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
        jwt_user_id = resolve_supabase_auth_user_id(token, DEFAULT_CONFIG_PATH)

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
            sb_client = get_supabase_client_for_token(DEFAULT_CONFIG_PATH, token)
            if sb_client is None:
                raise HTTPException(status_code=503, detail="Supabase is not reachable.")

            try:
                query = sb_client.table("users").select(
                    "id,email,niche,display_name,contact_name,account_type,password_hash,salt,quickstart_completed,average_deal_value"
                )
                if jwt_user_id:
                    query = query.eq("auth_user_id", jwt_user_id)
                else:
                    query = query.eq("token", token)
                response = query.limit(1).execute()
            except Exception as exc:
                if "does not exist" in str(exc):
                    # Retry with minimal columns if schema is incomplete
                    logging.warning(f"Supabase column mismatch on profile update lookup: {exc}, retrying with fewer columns")
                    try:
                        query = sb_client.table("users").select(
                            "id,email,niche,display_name,contact_name,account_type,password_hash,salt"
                        )
                        if jwt_user_id:
                            query = query.eq("auth_user_id", jwt_user_id)
                        else:
                            query = query.eq("token", token)
                        response = query.limit(1).execute()
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
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
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
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
            row = conn.execute(
                "SELECT id, email, password_hash, salt FROM users WHERE token = ?",
                (token,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=401, detail="Invalid or expired session token.")
            expected = _hash_password(current_password, row["salt"])
            if not secrets.compare_digest(expected, row["password_hash"]):
                raise HTTPException(status_code=401, detail="Current password is incorrect.")
            ensure_jobs_queue_table(DEFAULT_DB_PATH)
            conn.execute("DELETE FROM jobs WHERE user_id IN (?, ?)", (row["email"], token))
            conn.execute("DELETE FROM users WHERE id = ?", (row["id"],))
            conn.commit()
        return {"ok": True, "message": "Account deleted successfully."}

    # â”€â”€ Cold Email Opener â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
                conn.row_factory = pgdb.Row
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

        if auth_email_exists(email, config_path=DEFAULT_CONFIG_PATH, db_path=DEFAULT_DB_PATH):
            raise HTTPException(status_code=409, detail="An account with this email already exists.")

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

        try:
            with pgdb.connect(DEFAULT_DB_PATH) as conn:
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
        except pgdb.IntegrityError:
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
            (event_type in {"invoice.payment_succeeded", "invoice.paid"} and billing_reason in {"subscription_create", "subscription_update", "subscription_cycle"})
            or (event_type == "checkout.session.completed" and checkout_mode == "subscription")
            or event_type == "customer.subscription.created"
        )
        is_subscription_state_event = event_type in {
            "customer.subscription.created",
            "customer.subscription.updated",
            "customer.subscription.deleted",
            "invoice.payment_failed",
        }

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
                payload["subscription_active"] = True
                payload["subscription_status"] = "active"
                payload["subscription_cancel_at"] = None
                payload["subscription_cancel_at_period_end"] = False
                payload["plan_key"] = _resolve_plan_key_from_limit(next_monthly_limit, fallback="pro")
                if billing_reason == "subscription_update" and next_monthly_limit > current_monthly_limit:
                    # Mid-month upgrade: immediately add only the credit difference
                    payload["credits_balance"] = current_balance + (next_monthly_limit - current_monthly_limit)
                else:
                    # New subscription or monthly renewal: reset to full plan quota
                    payload["credits_balance"] = next_monthly_limit + topup_balance

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
                    payload["subscription_status"] = "active"
                    payload["subscription_cancel_at"] = cancel_effective_at
                    payload["subscription_cancel_at_period_end"] = True
                else:
                    payload["monthly_quota"] = next_monthly_limit
                    payload["monthly_limit"] = next_monthly_limit
                    payload["credits_limit"] = next_monthly_limit
                    payload["credits_balance"] = next_monthly_limit + topup_balance
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
                _append_credit_log(resolved_user_id, max(0, credits_delta), "stripe_topup", {
                    "credits_after": final_credits_balance,
                    "topup_credits_after": final_topup_balance,
                    "event_type": event_type,
                }, db_path=DEFAULT_DB_PATH)
                mark_stripe_topup_payments_applied(
                    [str(event_obj.get("payment_intent") or event_obj.get("id") or "").strip()],
                    user_id=resolved_user_id,
                    credits_delta=max(0, credits_delta),
                )
            elif is_subscription_cycle_event or (is_subscription_state_event and final_subscription_active):
                _append_credit_log(resolved_user_id, 0, "subscription_sync", {
                    "credits_after": final_credits_balance,
                    "credits_limit": final_monthly_limit,
                    "topup_credits_after": final_topup_balance,
                    "plan_key": final_plan_key,
                    "event_type": event_type,
                }, db_path=DEFAULT_DB_PATH)
            return {
                "ok": True,
                "user_id": resolved_user_id,
                "credits_delta": max(0, credits_delta),
                "monthly_refill_applied": bool(is_subscription_cycle_event and not is_topup_event),
                "subscription_state_applied": bool(is_subscription_state_event),
                "updated_at": now_iso,
            }

        ensure_users_table(DEFAULT_DB_PATH)
        with pgdb.connect(DEFAULT_DB_PATH) as conn:
            conn.row_factory = pgdb.Row
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
            subscription_active_to_store = bool(_coerce_subscription_flag(row["subscription_active"]))
            subscription_status_to_store: Optional[str] = None
            subscription_cancel_at_to_store: Optional[str] = None
            subscription_cancel_at_period_end_to_store = False
            plan_key_to_store = current_plan_key

            if is_topup_event:
                credits_balance_to_store = current_balance + credits_delta
                topup_balance_to_store = topup_balance + credits_delta

            if is_subscription_cycle_event and not is_topup_event:
                monthly_limit_to_store = next_monthly_limit
                credits_limit_to_store = next_monthly_limit
                subscription_start_date_to_store = now_iso
                subscription_active_to_store = True
                subscription_status_to_store = "active"
                subscription_cancel_at_to_store = None
                subscription_cancel_at_period_end_to_store = False
                plan_key_to_store = _resolve_plan_key_from_limit(next_monthly_limit, fallback="pro")
                if billing_reason == "subscription_update" and next_monthly_limit > current_monthly_limit:
                    # Mid-month upgrade: immediately add only the credit difference
                    credits_balance_to_store = current_balance + (next_monthly_limit - current_monthly_limit)
                else:
                    # New subscription or monthly renewal: reset to full plan quota
                    credits_balance_to_store = next_monthly_limit + topup_balance

            if is_subscription_state_event:
                if deleted_or_terminal and not has_paid_access_until_end:
                    subscription_active_to_store = False
                    subscription_status_to_store = "expired"
                    subscription_cancel_at_to_store = cancel_effective_at or now_iso
                    subscription_cancel_at_period_end_to_store = False
                    plan_key_to_store = "free"
                    monthly_limit_to_store = free_quota
                    credits_limit_to_store = free_quota
                    credits_balance_to_store = free_quota + topup_balance_to_store
                    subscription_start_date_to_store = now_iso
                elif cancel_at_period_end or (deleted_or_terminal and has_paid_access_until_end):
                    subscription_active_to_store = True
                    subscription_status_to_store = "active"
                    subscription_cancel_at_to_store = cancel_effective_at
                    subscription_cancel_at_period_end_to_store = True
                    plan_key_to_store = current_plan_key if current_plan_key != "free" else "pro"
                else:
                    monthly_limit_to_store = next_monthly_limit
                    credits_limit_to_store = next_monthly_limit
                    credits_balance_to_store = next_monthly_limit + topup_balance_to_store
                    subscription_active_to_store = True
                    subscription_status_to_store = subscription_status_event or "active"
                    subscription_cancel_at_to_store = None
                    subscription_cancel_at_period_end_to_store = False
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
            _append_credit_log(
                resolved_user_id,
                max(0, credits_delta),
                "stripe_topup",
                {
                    "credits_after": credits_balance_to_store,
                    "topup_credits_after": topup_balance_to_store,
                    "event_type": event_type,
                },
                db_path=DEFAULT_DB_PATH,
            )
            mark_stripe_topup_payments_applied(
                [str(event_obj.get("payment_intent") or event_obj.get("id") or "").strip()],
                user_id=resolved_user_id,
                credits_delta=max(0, credits_delta),
            )
        elif is_subscription_cycle_event or (is_subscription_state_event and bool(subscription_active_to_store)):
            _append_credit_log(
                resolved_user_id,
                0,
                "subscription_sync",
                {
                    "credits_after": credits_balance_to_store,
                    "credits_limit": monthly_limit_to_store,
                    "topup_credits_after": topup_balance_to_store,
                    "plan_key": plan_key_to_store,
                    "event_type": event_type,
                },
                db_path=DEFAULT_DB_PATH,
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

