import os
import time
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

from dotenv import load_dotenv
from sqlalchemy import JSON, Boolean, DateTime, Float, Index, Integer, Text, UniqueConstraint, asc, create_engine, desc, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .models import Lead


load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class LeadRecord(Base):
    __tablename__ = "leads"
    __table_args__ = (
        UniqueConstraint("user_id", "business_name", "address", name="uq_leads_user_business_address"),
        Index("idx_leads_user_created_at", "user_id", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(Text, nullable=False)
    business_name: Mapped[str] = mapped_column(Text, nullable=False)
    website_url: Mapped[Optional[str]] = mapped_column(Text)
    maps_url: Mapped[Optional[str]] = mapped_column(Text)
    phone_number: Mapped[Optional[str]] = mapped_column(Text)
    rating: Mapped[Optional[float]] = mapped_column(Float)
    review_count: Mapped[Optional[int]] = mapped_column(Integer)
    address: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    search_keyword: Mapped[Optional[str]] = mapped_column(Text)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    contact_name: Mapped[Optional[str]] = mapped_column(Text)
    email: Mapped[Optional[str]] = mapped_column(Text)
    google_claimed: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    linkedin_url: Mapped[Optional[str]] = mapped_column(Text)
    instagram_url: Mapped[Optional[str]] = mapped_column(Text)
    facebook_url: Mapped[Optional[str]] = mapped_column(Text)
    tiktok_url: Mapped[Optional[str]] = mapped_column(Text)
    twitter_url: Mapped[Optional[str]] = mapped_column(Text)
    youtube_url: Mapped[Optional[str]] = mapped_column(Text)
    ig_link: Mapped[Optional[str]] = mapped_column(Text)
    fb_link: Mapped[Optional[str]] = mapped_column(Text)
    has_pixel: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    tech_stack: Mapped[Optional[str]] = mapped_column(Text)
    insecure_site: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    main_shortcoming: Mapped[Optional[str]] = mapped_column(Text)
    ai_description: Mapped[Optional[str]] = mapped_column(Text)
    enriched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    enrichment_data: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    status: Mapped[Optional[str]] = mapped_column(Text)
    enrichment_status: Mapped[str] = mapped_column(Text, nullable=False, default="pending", server_default="pending")
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    generated_email_body: Mapped[Optional[str]] = mapped_column(Text)
    crm_comment: Mapped[Optional[str]] = mapped_column(Text)
    status_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_sender_email: Mapped[Optional[str]] = mapped_column(Text)
    last_contacted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    follow_up_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    ai_score: Mapped[Optional[float]] = mapped_column(Float)
    client_tier: Mapped[str] = mapped_column(Text, nullable=False, default="standard", server_default="standard")
    next_mail_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    is_ads_client: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    is_website_client: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    worker_id: Mapped[Optional[int]] = mapped_column(Integer)
    assigned_worker_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    open_tracking_token: Mapped[Optional[str]] = mapped_column(Text)
    open_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    first_opened_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_opened_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    campaign_sequence_id: Mapped[Optional[int]] = mapped_column(Integer)
    campaign_step: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    ab_variant: Mapped[Optional[str]] = mapped_column(Text)
    last_subject_line: Mapped[Optional[str]] = mapped_column(Text)
    reply_detected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    bounced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    bounce_reason: Mapped[Optional[str]] = mapped_column(Text)
    phone_formatted: Mapped[Optional[str]] = mapped_column(Text)
    phone_type: Mapped[Optional[str]] = mapped_column(Text)
    pipeline_stage: Mapped[str] = mapped_column(Text, nullable=False, default="Scraped", server_default="Scraped")
    client_folder_id: Mapped[Optional[int]] = mapped_column(Integer)
    qualification_score: Mapped[Optional[float]] = mapped_column(Float)


_ENGINE_CACHE: dict[str, Any] = {}
_SESSION_FACTORY_CACHE: dict[str, sessionmaker[Session]] = {}
_ENGINE_CONNECT_COOLDOWN_UNTIL = 0.0
_ENGINE_CONNECT_LAST_ERROR: Optional[Exception] = None

# ---------------------------------------------------------------------------
# Connection-limit monitoring
# ---------------------------------------------------------------------------
# Keeps timestamps (monotonic) of each pool-saturation event so we can
# emit a rate-limited CRITICAL alert when the 200-connection ceiling is
# being approached in production.
_POOL_SATURATION_EVENTS: list[float] = []
_POOL_SATURATION_WINDOW_SECS = 60          # look-back window for rate limiting
_POOL_SATURATION_CRITICAL_THRESHOLD = 5    # CRITICAL log after N events/window


def record_pool_saturation_event(exc: Optional[Exception] = None) -> None:
    """Call this whenever a DB capacity / EMAXCONN error is caught.

    Logs a WARNING immediately and escalates to CRITICAL when the event
    rate exceeds *_POOL_SATURATION_CRITICAL_THRESHOLD* in the last minute.
    This gives Railway log alerts an easy keyword to grep: ``POOL_SATURATION``.
    """
    now = time.monotonic()
    _POOL_SATURATION_EVENTS.append(now)
    # Purge events outside the look-back window.
    cutoff = now - _POOL_SATURATION_WINDOW_SECS
    while _POOL_SATURATION_EVENTS and _POOL_SATURATION_EVENTS[0] < cutoff:
        _POOL_SATURATION_EVENTS.pop(0)

    recent_count = len(_POOL_SATURATION_EVENTS)
    err_str = f" ({exc})" if exc else ""
    if recent_count >= _POOL_SATURATION_CRITICAL_THRESHOLD:
        logging.critical(
            "[POOL_SATURATION] DB pool saturated %d times in the last %ds%s. "
            "Consider increasing SUPABASE_POOLER_POOL_SIZE_CAP or upgrading the Supabase plan.",
            recent_count,
            _POOL_SATURATION_WINDOW_SECS,
            err_str,
        )
    else:
        logging.warning(
            "[POOL_SATURATION] DB pool saturation event #%d in last %ds%s.",
            recent_count,
            _POOL_SATURATION_WINDOW_SECS,
            err_str,
        )
DEFAULT_DB_POOL_SIZE = max(1, int(os.environ.get("DB_POOL_SIZE", "4")))
DEFAULT_DB_MAX_OVERFLOW = max(0, int(os.environ.get("DB_MAX_OVERFLOW", "8")))
DEFAULT_DB_POOL_RECYCLE = max(60, int(os.environ.get("DB_POOL_RECYCLE", "900")))
DEFAULT_DB_POOL_TIMEOUT = max(2, int(os.environ.get("DB_POOL_TIMEOUT", "10")))
DEFAULT_DB_CONNECT_TIMEOUT = max(3, int(os.environ.get("DB_CONNECT_TIMEOUT", "10")))
DEFAULT_DB_CONNECT_RETRIES = max(1, int(os.environ.get("DB_CONNECT_RETRIES", "2")))
DEFAULT_DB_CONNECT_RETRY_DELAY = max(1, int(os.environ.get("DB_CONNECT_RETRY_DELAY", "2")))
DEFAULT_DB_CONNECT_FAILURE_COOLDOWN = max(1, int(os.environ.get("DB_CONNECT_FAILURE_COOLDOWN", "2")))
DB_POOL_SIZE_CAP = max(1, int(os.environ.get("DB_POOL_SIZE_CAP", "12")))
DB_MAX_OVERFLOW_CAP = max(0, int(os.environ.get("DB_MAX_OVERFLOW_CAP", "24")))
# Per-replica caps for the shared Supabase pooler.
# Math: (pool_size_cap + max_overflow_cap) × replica_count <= 180 (leaving 20 for Supabase internals).
# Default: (10 + 20) × 4 replicas = 120 < 180 — safe for 4 replicas.
SUPABASE_POOLER_POOL_SIZE_CAP = max(1, int(os.environ.get("SUPABASE_POOLER_POOL_SIZE_CAP", "10")))
SUPABASE_POOLER_MAX_OVERFLOW_CAP = max(0, int(os.environ.get("SUPABASE_POOLER_MAX_OVERFLOW_CAP", "20")))
ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_FILE = ROOT_DIR / "environment settings"


def _effective_pool_settings(engine_url: str) -> tuple[int, int]:
    pool_size = min(DEFAULT_DB_POOL_SIZE, DB_POOL_SIZE_CAP)
    max_overflow = min(DEFAULT_DB_MAX_OVERFLOW, DB_MAX_OVERFLOW_CAP)

    try:
        host = str(urlparse(engine_url).hostname or "").strip().lower()
    except ValueError:
        host = ""

    # Shared Supabase pooler has strict global limits; keep per-process pools small.
    if host.endswith(".pooler.supabase.com"):
        pool_size = min(pool_size, SUPABASE_POOLER_POOL_SIZE_CAP)
        max_overflow = min(max_overflow, SUPABASE_POOLER_MAX_OVERFLOW_CAP)

    return max(1, pool_size), max(0, max_overflow)


def _load_database_url_from_config_file() -> str:
    config_path = str(os.environ.get("SNIPED_CONFIG_PATH") or "").strip()
    path = Path(config_path) if config_path else DEFAULT_CONFIG_FILE
    try:
        with path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
    except Exception:
        return ""
    supabase_cfg = cfg.get("supabase", {}) if isinstance(cfg, dict) else {}
    return str(supabase_cfg.get("database_url", "") or "").strip()


def _with_default_query_params(url: str, params: dict[str, str]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        if key not in query or not str(query.get(key) or "").strip():
            query[key] = value
    return urlunparse(parsed._replace(query=urlencode(query)))


def _sanitize_database_url(raw_url: str) -> str:
    value = str(raw_url or "").strip()
    if not value:
        return ""

    # Remove accidental whitespace/newlines from copied env values.
    value = value.replace("\n", "").replace("\r", "")
    value = value.replace(":// ", "://").replace(" @", "@").replace("@ ", "@")

    if "://" not in value:
        return value

    scheme, rest = value.split("://", 1)
    authority, sep, path_tail = rest.partition("/")
    authority = authority.strip()
    if not authority:
        return value

    if "@" in authority:
        creds, host_port = authority.rsplit("@", 1)
        host_port = host_port.strip()
        if host_port.startswith("[") and "]" in host_port:
            inside = host_port[1:host_port.index("]")]
            host_suffix = host_port[host_port.index("]") + 1 :]
            # Unwrap brackets only for non-IPv6 hosts.
            if ":" not in inside:
                host_port = f"{inside}{host_suffix}"
        authority = f"{creds}@{host_port}"
    else:
        host_port = authority
        if host_port.startswith("[") and "]" in host_port:
            inside = host_port[1:host_port.index("]")]
            host_suffix = host_port[host_port.index("]") + 1 :]
            if ":" not in inside:
                host_port = f"{inside}{host_suffix}"
        authority = host_port

    rebuilt = f"{scheme}://{authority}"
    if sep:
        rebuilt = f"{rebuilt}/{path_tail}"
    return rebuilt
def _with_port(url: str, new_port: int) -> str:
    parsed = urlparse(url)
    replacement_netloc = parsed.netloc
    if "@" in replacement_netloc:
        credentials, host_port = replacement_netloc.rsplit("@", 1)
        host_only = host_port.split(":", 1)[0]
        replacement_netloc = f"{credentials}@{host_only}:{new_port}"
    else:
        host_only = replacement_netloc.split(":", 1)[0]
        replacement_netloc = f"{host_only}:{new_port}"
    return urlunparse(parsed._replace(netloc=replacement_netloc))
def _engine_connect_url(raw_url: str) -> str:
    return unquote(str(raw_url or "").strip())


def _project_ref_from_supabase_url() -> str:
    raw = str(os.environ.get("SUPABASE_URL") or "").strip()
    if not raw:
        return ""
    try:
        host = str(urlparse(raw).hostname or "").strip().lower()
    except ValueError:
        return ""
    if not host.endswith(".supabase.co"):
        return ""
    return host.split(".", 1)[0].strip()


def _ensure_pooler_tenant_username(url: str) -> str:
    try:
        parsed = urlparse(url)
    except ValueError:
        return url

    host = str(parsed.hostname or "").strip().lower()
    username = str(parsed.username or "").strip()
    if not host.endswith(".pooler.supabase.com"):
        return url
    if not username or "." in username:
        return url

    project_ref = _project_ref_from_supabase_url()
    if not project_ref:
        return url

    tenant_username = f"{username}.{project_ref}"
    raw_password = ""
    raw_netloc = str(parsed.netloc or "")
    if "@" in raw_netloc:
        raw_credentials = raw_netloc.rsplit("@", 1)[0]
        if ":" in raw_credentials:
            raw_password = raw_credentials.split(":", 1)[1]

    host_only = str(parsed.hostname or "")
    if not host_only:
        return url
    if ":" in host_only and not host_only.startswith("["):
        host_only = f"[{host_only}]"

    try:
        parsed_port = parsed.port
    except ValueError:
        parsed_port = None

    userinfo = quote(tenant_username, safe="")
    if raw_password:
        userinfo = f"{userinfo}:{raw_password}"
    netloc = f"{userinfo}@{host_only}"
    if parsed_port:
        netloc = f"{netloc}:{parsed_port}"

    logging.warning("Rewriting pooler username to tenant format for Supabase shared pooler host.")
    return urlunparse(parsed._replace(netloc=netloc))


def _prefer_supabase_pooler_url(raw_url: str) -> str:
    url = _sanitize_database_url(raw_url)
    if not url:
        return url

    try:
        parsed = urlparse(url)
    except ValueError:
        return url
    host = str(parsed.hostname or "").strip().lower()
    try:
        port = parsed.port
    except ValueError:
        return url

    # If the standard Supabase direct host/port is used, switch to pooler port.
    if host.startswith("db.") and host.endswith(".supabase.co") and (port in (None, 5432)):
        replacement_netloc = parsed.netloc
        if "@" in replacement_netloc:
            credentials, host_port = replacement_netloc.rsplit("@", 1)
            if ":" in host_port:
                host_only = host_port.split(":", 1)[0]
            else:
                host_only = host_port
            replacement_netloc = f"{credentials}@{host_only}:6543"
        else:
            host_only = replacement_netloc.split(":", 1)[0]
            replacement_netloc = f"{host_only}:6543"
        logging.warning("DATABASE_URL uses direct Supabase port 5432; switching to pooler port 6543.")
        url = urlunparse(parsed._replace(netloc=replacement_netloc))

    return url
def _build_database_url_candidates() -> list[str]:
    raw_primary = str(
        os.environ.get("DATABASE_URL")
        or os.environ.get("SUPABASE_DATABASE_URL")
        or ""
    ).strip()
    raw_pooler = str(
        os.environ.get("SUPABASE_DB_POOLER_URL")
        or os.environ.get("SUPABASE_POOLER_URL")
        or ""
    ).strip()
    raw_config = _load_database_url_from_config_file()

    raw_candidates = [c for c in [raw_primary, raw_pooler, raw_config] if c]
    if not raw_candidates:
        return []

    query_defaults = {
        "sslmode": "require",
        "connect_timeout": str(DEFAULT_DB_CONNECT_TIMEOUT),
        "application_name": "sniped-backend",
        "keepalives": "1",
        "keepalives_idle": "30",
        "keepalives_interval": "10",
        "keepalives_count": "5",
    }

    candidates: list[str] = []
    seen: set[str] = set()

    def _push(url_value: str) -> None:
        if url_value and url_value not in seen:
            seen.add(url_value)
            candidates.append(url_value)

    for raw in raw_candidates:
        normalized = _normalize_database_url(raw)
        pooler_candidate = _ensure_pooler_tenant_username(_prefer_supabase_pooler_url(normalized))

        try:
            parsed = urlparse(pooler_candidate)
        except ValueError:
            logging.warning("Skipping malformed DATABASE_URL candidate")
            continue
        host = str(parsed.hostname or "").strip()
        try:
            parsed_port = parsed.port
        except ValueError:
            parsed_port = None

        base_with_defaults = _with_default_query_params(pooler_candidate, query_defaults)
        _push(base_with_defaults)

        # Try transaction pooler port for direct Supabase host fallback.
        if host.lower().startswith("db.") and host.lower().endswith(".supabase.co") and (parsed_port in (None, 5432)):
            pooled = _with_default_query_params(_with_port(pooler_candidate, 6543), query_defaults)
            _push(pooled)

    return candidates


def _normalize_database_url(raw_url: str) -> str:
    url = _sanitize_database_url(raw_url)
    if not url:
        raise RuntimeError("SUPABASE_DATABASE_URL or DATABASE_URL is required.")
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg2://", 1)
    if url.startswith("postgresql://") and "+" not in url.split("://", 1)[0]:
        return url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def get_database_url(db_path: Optional[str] = None) -> str:
    del db_path
    candidates = _build_database_url_candidates()
    if candidates:
        return candidates[0]
    raise RuntimeError(
        "Database URL is missing. Set SUPABASE_DB_POOLER_URL or SUPABASE_POOLER_URL "
        "(preferred), or DATABASE_URL / SUPABASE_DATABASE_URL."
    )


def get_engine(db_path: Optional[str] = None) -> Any:
    global _ENGINE_CONNECT_COOLDOWN_UNTIL, _ENGINE_CONNECT_LAST_ERROR
    primary_url = get_database_url(db_path)
    candidates = _build_database_url_candidates() or [primary_url]

    for candidate_url in candidates:
        engine_url = _engine_connect_url(candidate_url)
        cached = _ENGINE_CACHE.get(engine_url)
        if cached is not None:
            return cached

    now_ts = time.monotonic()
    if now_ts < _ENGINE_CONNECT_COOLDOWN_UNTIL:
        raise RuntimeError(
            "Database connection is cooling down after recent failures. "
            f"Last error: {_ENGINE_CONNECT_LAST_ERROR}"
        )

    last_error: Optional[Exception] = None
    for candidate_url in candidates:
        for attempt in range(1, DEFAULT_DB_CONNECT_RETRIES + 1):
            engine_url = _engine_connect_url(candidate_url)
            pool_size, max_overflow = _effective_pool_settings(engine_url)
            try:
                parsed_engine_url = urlparse(engine_url)
                db_host = str(parsed_engine_url.hostname or "").strip()
            except ValueError:
                db_host = ""
            if db_host:
                logging.debug("Attempting connection to: %s", db_host)
            engine = create_engine(
                engine_url,
                future=True,
                pool_pre_ping=True,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=DEFAULT_DB_POOL_TIMEOUT,
                pool_recycle=DEFAULT_DB_POOL_RECYCLE,
                pool_use_lifo=True,
                connect_args={"sslmode": "require"},
            )
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                _ENGINE_CACHE[engine_url] = engine
                _ENGINE_CONNECT_LAST_ERROR = None
                _ENGINE_CONNECT_COOLDOWN_UNTIL = 0.0
                return engine
            except Exception as exc:
                last_error = exc
                _ENGINE_CONNECT_LAST_ERROR = exc
                try:
                    engine.dispose()
                except Exception:
                    pass
                if attempt < DEFAULT_DB_CONNECT_RETRIES:
                    try:
                        candidate_netloc = urlparse(candidate_url).netloc
                    except ValueError:
                        candidate_netloc = "<malformed-url>"
                    logging.warning(
                        "DB connect attempt %s/%s failed for candidate %s: %s",
                        attempt,
                        DEFAULT_DB_CONNECT_RETRIES,
                        candidate_netloc,
                        exc,
                    )
                    time.sleep(DEFAULT_DB_CONNECT_RETRY_DELAY)

    _ENGINE_CONNECT_COOLDOWN_UNTIL = time.monotonic() + DEFAULT_DB_CONNECT_FAILURE_COOLDOWN

    raise RuntimeError(
        "Database connection failed after retries. Check SUPABASE_DB_POOLER_URL/SUPABASE_POOLER_URL "
        "(port 6543) or DATABASE_URL/SUPABASE_DATABASE_URL, plus network routing and credentials. "
        f"Last error: {last_error}"
    )


def dispose_cached_engines() -> None:
    for engine in list(_ENGINE_CACHE.values()):
        try:
            engine.dispose()
        except Exception:
            pass
    _ENGINE_CACHE.clear()
    _SESSION_FACTORY_CACHE.clear()


def get_session_factory(db_path: Optional[str] = None) -> sessionmaker[Session]:
    database_url = get_database_url(db_path)
    cached = _SESSION_FACTORY_CACHE.get(database_url)
    if cached is not None:
        return cached
    factory = sessionmaker(bind=get_engine(db_path), autoflush=False, autocommit=False, expire_on_commit=False, future=True)
    _SESSION_FACTORY_CACHE[database_url] = factory
    return factory


def _lead_record_to_dict(record: LeadRecord) -> dict[str, Any]:
    return {
        "id": record.id,
        "user_id": record.user_id,
        "business_name": record.business_name,
        "website_url": record.website_url,
        "maps_url": record.maps_url,
        "phone_number": record.phone_number,
        "email": record.email,
        "google_claimed": record.google_claimed,
        "linkedin_url": record.linkedin_url,
        "instagram_url": record.instagram_url,
        "facebook_url": record.facebook_url,
        "tiktok_url": record.tiktok_url,
        "twitter_url": record.twitter_url,
        "youtube_url": record.youtube_url,
        "ig_link": record.ig_link,
        "fb_link": record.fb_link,
        "has_pixel": bool(record.has_pixel) if record.has_pixel is not None else False,
        "tech_stack": record.tech_stack,
        "rating": record.rating,
        "review_count": record.review_count,
        "address": record.address,
        "search_keyword": record.search_keyword,
        "scraped_at": record.scraped_at.isoformat() if record.scraped_at else None,
        "created_at": record.created_at.isoformat() if record.created_at else None,
        "main_shortcoming": record.main_shortcoming or "",
        "ai_score": float(record.ai_score or 0),
        "qualification_score": float(record.qualification_score or 0),
        "status": record.status or "",
        "enriched_at": record.enriched_at.isoformat() if record.enriched_at else "",
    }


def _to_bool_flag(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", "", "none", "null"}:
        return False
    return True


def _to_int_flag(value: Any) -> Optional[int]:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().lower()
    if text in {"true", "yes", "on"}:
        return 1
    if text in {"false", "no", "off", "", "none", "null"}:
        return 0
    try:
        return int(float(text))
    except Exception:
        return 0


LEAD_INT_FLAG_FIELDS = (
    "google_claimed",
    "has_pixel",
    "insecure_site",
    "is_ads_client",
    "is_website_client",
    "follow_up_count",
    "open_count",
    "campaign_step",
)


def _coerce_bool_flags_for_lead_payload(payload: dict[str, Any]) -> dict[str, Any]:
    # PostgreSQL bigint columns reject boolean bind values; force bool -> int explicitly.
    for key in LEAD_INT_FLAG_FIELDS:
        if key in payload:
            value = payload.get(key)
            payload[key] = int(value) if isinstance(value, bool) else value
    return payload


def _lead_identity_from_payload(payload: dict[str, Any]) -> tuple[str, str]:
    return (
        str(payload.get("business_name") or "").strip(),
        str(payload.get("address") or "").strip(),
    )


def _is_duplicate_lead_identity_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "uq_leads_user_business_address" in message
        or "duplicate key value violates unique constraint" in message
        or "unique constraint failed: leads.user_id, leads.business_name, leads.address" in message
    )


LEAD_UPSERT_CONFLICT_COLUMNS = ("user_id", "business_name", "address")
LEAD_UPSERT_IMMUTABLE_COLUMNS = {
    "id",
    "created_at",
    *LEAD_UPSERT_CONFLICT_COLUMNS,
}


def _build_lead_upsert_statement(payloads: list[dict[str, Any]], dialect_name: str) -> Any:
    if dialect_name != "postgresql":
        return None
    stmt = pg_insert(LeadRecord).values(payloads)

    updatable_columns = [
        column.name
        for column in LeadRecord.__table__.columns
        if column.name not in LEAD_UPSERT_IMMUTABLE_COLUMNS
    ]
    update_set = {column_name: getattr(stmt.excluded, column_name) for column_name in updatable_columns}
    return stmt.on_conflict_do_update(
        index_elements=list(LEAD_UPSERT_CONFLICT_COLUMNS),
        set_=update_set,
    )


def _clean_address(value: Any) -> str:
    raw = str(value or "").replace("\r", "\n")
    lines = [re.sub(r"\s+", " ", line).strip() for line in raw.split("\n")]
    lines = [line for line in lines if line]
    if not lines:
        return "Unknown Address"

    junk_tokens = {
        "rating",
        "hours",
        "all filters",
        "results",
        "website",
        "directions",
        "call",
        "open",
        "closed",
        "overview",
        "photos",
        "reviews",
    }

    candidates: list[str] = []
    for line in lines:
        lowered = line.lower()
        if any(token in lowered for token in junk_tokens):
            continue
        candidates.append(line)

    if not candidates:
        candidates = lines

    for line in candidates:
        if re.search(r"\d", line) and (
            "," in line
            or re.search(r"\b(st|street|ave|avenue|rd|road|blvd|boulevard|dr|drive|ln|lane|way|suite|ste|unit|#)\b", line, flags=re.IGNORECASE)
        ):
            return line[:240]

    return candidates[0][:240] if candidates else "Unknown Address"


def _to_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_text(value: Any, max_len: Optional[int] = None) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).replace("\x00", "").strip()
    if max_len is not None and max_len > 0 and len(text_value) > max_len:
        return text_value[:max_len]
    return text_value


_URL_BLOCKLIST_PREFIXES = (
    "http://schema.org",
    "https://schema.org",
    "http://www.schema.org",
    "https://www.schema.org",
)


def _safe_url(value: Any, max_len: Optional[int] = None) -> Optional[str]:
    """Return a sanitised URL string, or None if invalid/schema.org/non-http(s)."""
    text = _safe_text(value, max_len)
    if not text:
        return None
    lower = text.lower()
    for prefix in _URL_BLOCKLIST_PREFIXES:
        if lower.startswith(prefix):
            return None
    if not (lower.startswith("http://") or lower.startswith("https://")):
        return None
    return text


def _log_payload_types(context: str, payload: dict[str, Any]) -> None:
    try:
        type_map = {k: type(v).__name__ for k, v in payload.items()}
        logging.error("Lead persistence type map (%s): %s", context, json.dumps(type_map, ensure_ascii=False, sort_keys=True))
        logging.error("Lead persistence payload (%s): %s", context, json.dumps(payload, ensure_ascii=False, default=str))
    except Exception:
        logging.exception("Failed to log lead payload type map for context=%s", context)


def _normalize_required_user_id(user_id: Any) -> str:
    normalized = str(user_id or "").strip()
    if not normalized:
        raise ValueError("Missing user_id for lead persistence")
    if normalized.lower() == "legacy":
        raise ValueError("Invalid user_id 'legacy' for lead persistence")
    return normalized


def _lead_to_create_payload(lead: Lead, user_id: str) -> dict[str, Any]:
    normalized_user_id = _normalize_required_user_id(user_id)
    payload: dict[str, Any] = {
        "user_id": normalized_user_id,
        # Keep identity fields bounded to avoid oversized-index row failures.
        "business_name": _safe_text(getattr(lead, "business_name", ""), 255) or "Unknown Business",
        "website_url": _safe_url(getattr(lead, "website_url", None), 2048),
        "maps_url": _safe_url(getattr(lead, "maps_url", None), 2048),
        "phone_number": _safe_text(getattr(lead, "phone_number", None), 128),
        "google_claimed": _to_int_flag(lead.google_claimed),
        "linkedin_url": _safe_text(getattr(lead, "linkedin_url", None), 2048),
        "instagram_url": _safe_text(getattr(lead, "instagram_url", None), 2048),
        "facebook_url": _safe_text(getattr(lead, "facebook_url", None), 2048),
        "rating": lead.rating,
        "review_count": lead.review_count,
        "address": _safe_text(_clean_address(getattr(lead, "address", None)), 512) or "Unknown Address",
        "search_keyword": _safe_text(getattr(lead, "search_keyword", None), 255),
        "is_ads_client": _to_int_flag(getattr(lead, "is_ads_client", 0)),
        "is_website_client": _to_int_flag(getattr(lead, "is_website_client", 0)),
        "follow_up_count": _to_int_flag(getattr(lead, "follow_up_count", 0)),
        "open_count": _to_int_flag(getattr(lead, "open_count", 0)),
        "campaign_step": _to_int_flag(getattr(lead, "campaign_step", 1)) or 1,
    }

    optional_fields = {
        "tiktok_url": _safe_text(getattr(lead, "tiktok_url", None), 2048),
        "twitter_url": _safe_text(getattr(lead, "twitter_url", None), 2048),
        "youtube_url": _safe_text(getattr(lead, "youtube_url", None), 2048),
        "ig_link": _safe_text(getattr(lead, "ig_link", None), 2048),
        "fb_link": _safe_text(getattr(lead, "fb_link", None), 2048),
        "has_pixel": _to_int_flag(lead.has_pixel),
        "insecure_site": _to_int_flag(getattr(lead, "insecure_site", None)),
        "tech_stack": _safe_text(getattr(lead, "tech_stack", None), 4000),
        "email": _safe_text(getattr(lead, "email", None), 320),
        "qualification_score": _to_optional_float(lead.qualification_score),
    }
    for key, value in optional_fields.items():
        if value is not None:
            payload[key] = value

    return _coerce_bool_flags_for_lead_payload(payload)


def _apply_lead_updates(record: LeadRecord, updates: dict[str, Any]) -> None:
    updates = _coerce_bool_flags_for_lead_payload(dict(updates or {}))
    for field_name, value in updates.items():
        if field_name in {
            "google_claimed",
            "has_pixel",
            "insecure_site",
            "is_ads_client",
            "is_website_client",
            "follow_up_count",
            "open_count",
            "campaign_step",
        }:
            value = _to_int_flag(value)
        elif field_name == "address":
            value = _clean_address(value)
        elif field_name == "qualification_score" and value is not None:
            try:
                value = float(value)
            except Exception:
                value = None
        if hasattr(record, field_name):
            setattr(record, field_name, value)


def init_db(db_path: Optional[str] = None) -> None:
    Base.metadata.create_all(get_engine(db_path))


def create_lead(lead: Lead, user_id: str, db_path: Optional[str] = None) -> dict[str, Any]:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        payload = _lead_to_create_payload(lead, user_id)
        business_name, address = _lead_identity_from_payload(payload)
        existing = session.execute(
            select(LeadRecord).where(
                LeadRecord.user_id == str(user_id),
                LeadRecord.business_name == business_name,
                LeadRecord.address == address,
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise ValueError("Lead already exists.")
        record = LeadRecord(**payload)
        session.add(record)
        try:
            session.commit()
        except Exception as exc:
            session.rollback()
            if _is_duplicate_lead_identity_error(exc):
                raise ValueError("Lead already exists.") from exc
            _log_payload_types("create_lead", payload)
            raise
        session.refresh(record)
        return _lead_record_to_dict(record)


def get_lead(lead_id: int, db_path: Optional[str] = None) -> Optional[dict[str, Any]]:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        record = session.get(LeadRecord, int(lead_id))
        return _lead_record_to_dict(record) if record is not None else None


def update_lead(lead_id: int, updates: dict[str, Any], db_path: Optional[str] = None) -> Optional[dict[str, Any]]:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        record = session.get(LeadRecord, int(lead_id))
        if record is None:
            return None
        _apply_lead_updates(record, dict(updates or {}))
        session.add(record)
        session.commit()
        session.refresh(record)
        return _lead_record_to_dict(record)


def delete_lead(lead_id: int, db_path: Optional[str] = None) -> bool:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        record = session.get(LeadRecord, int(lead_id))
        if record is None:
            return False
        session.delete(record)
        session.commit()
        return True


def upsert_lead(lead: Lead, db_path: Optional[str] = None, user_id: str = "") -> bool:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        normalized_user_id = _normalize_required_user_id(user_id)
        logging.info("[upsert_lead] using user_id=%s business_name=%s", normalized_user_id, str(getattr(lead, "business_name", "") or ""))
        payload = _lead_to_create_payload(lead, normalized_user_id)
        statement = _build_lead_upsert_statement([payload], session.bind.dialect.name if session.bind is not None else "")
        try:
            if statement is None:
                # Fallback for unsupported dialects.
                session.merge(LeadRecord(**payload))
            else:
                session.execute(statement)
            session.commit()
        except Exception as exc:
            session.rollback()
            _log_payload_types("upsert_lead", payload)
            raise
        return True


def batch_upsert_leads(leads: Sequence[Lead], db_path: Optional[str] = None, user_id: str = "") -> int:
    if not leads:
        return 0

    normalized_user_id = _normalize_required_user_id(user_id)
    logging.info("[batch_upsert_leads] using user_id=%s leads=%s", normalized_user_id, len(leads))
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        # Deduplicate within current batch to avoid conflicting updates on the same key in one statement.
        payload_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for lead in leads:
            business_name = str(getattr(lead, "business_name", "") or "").strip() or "<unknown>"
            print(f"DEBUG: Attempting to save {business_name} to DB...", flush=True)
            logging.info("DEBUG: Attempting to save %s to DB...", business_name)
            payload = _lead_to_create_payload(lead, normalized_user_id)
            key = _lead_identity_from_payload(payload)
            payload_by_key[key] = payload

        payloads = list(payload_by_key.values())
        if not payloads:
            return 0

        statement = _build_lead_upsert_statement(payloads, session.bind.dialect.name if session.bind is not None else "")
        try:
            if statement is None:
                for payload in payloads:
                    session.merge(LeadRecord(**payload))
            else:
                session.execute(statement)
            session.commit()
        except Exception:
            session.rollback()
            inserted_count = 0
            for idx, payload in enumerate(payloads, start=1):
                try:
                    fallback_stmt = _build_lead_upsert_statement(
                        [payload],
                        session.bind.dialect.name if session.bind is not None else "",
                    )
                    if fallback_stmt is None:
                        session.merge(LeadRecord(**payload))
                    else:
                        session.execute(fallback_stmt)
                    session.commit()
                    inserted_count += 1
                except Exception:
                    session.rollback()
                    _log_payload_types(f"batch_upsert_leads#{idx}", payload)
                    logging.exception("Skipping invalid lead payload at batch index=%s", idx)
            return inserted_count
        return len(payloads)


def fetch_target_leads(db_path: Optional[str] = None, min_score: float = 7.0, user_id: Optional[str] = None) -> list[dict[str, Any]]:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        query = select(LeadRecord).where((LeadRecord.ai_score.is_not(None)) & (LeadRecord.ai_score >= float(min_score)))
        if user_id:
            query = query.where(LeadRecord.user_id == str(user_id))
        query = query.order_by(desc(LeadRecord.ai_score), desc(LeadRecord.review_count), asc(LeadRecord.business_name))
        records = session.execute(query).scalars().all()
        return [_lead_record_to_dict(record) for record in records]
