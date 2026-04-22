import os
import time
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

from sqlalchemy import JSON, Boolean, DateTime, Float, Index, Integer, Text, UniqueConstraint, asc, create_engine, desc, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .models import Lead


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
    user_id: Mapped[str] = mapped_column(Text, nullable=False, default="legacy", server_default="legacy")
    business_name: Mapped[str] = mapped_column(Text, nullable=False)
    website_url: Mapped[Optional[str]] = mapped_column(Text)
    phone_number: Mapped[Optional[str]] = mapped_column(Text)
    rating: Mapped[Optional[float]] = mapped_column(Float)
    review_count: Mapped[Optional[int]] = mapped_column(Integer)
    address: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    search_keyword: Mapped[Optional[str]] = mapped_column(Text)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    contact_name: Mapped[Optional[str]] = mapped_column(Text)
    email: Mapped[Optional[str]] = mapped_column(Text)
    google_claimed: Mapped[Optional[bool]] = mapped_column(Boolean)
    linkedin_url: Mapped[Optional[str]] = mapped_column(Text)
    instagram_url: Mapped[Optional[str]] = mapped_column(Text)
    facebook_url: Mapped[Optional[str]] = mapped_column(Text)
    tiktok_url: Mapped[Optional[str]] = mapped_column(Text)
    ig_link: Mapped[Optional[str]] = mapped_column(Text)
    fb_link: Mapped[Optional[str]] = mapped_column(Text)
    has_pixel: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    tech_stack: Mapped[Optional[str]] = mapped_column(Text)
    insecure_site: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
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
    is_ads_client: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_website_client: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
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
DEFAULT_DB_POOL_SIZE = max(1, int(os.environ.get("DB_POOL_SIZE", "1")))
DEFAULT_DB_MAX_OVERFLOW = max(0, int(os.environ.get("DB_MAX_OVERFLOW", "0")))
DEFAULT_DB_POOL_RECYCLE = max(60, int(os.environ.get("DB_POOL_RECYCLE", "1800")))
DEFAULT_DB_CONNECT_TIMEOUT = max(3, int(os.environ.get("DB_CONNECT_TIMEOUT", "10")))
DEFAULT_DB_CONNECT_RETRIES = max(1, int(os.environ.get("DB_CONNECT_RETRIES", "4")))
DEFAULT_DB_CONNECT_RETRY_DELAY = max(1, int(os.environ.get("DB_CONNECT_RETRY_DELAY", "2")))
ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_FILE = ROOT_DIR / "environment settings"


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
    primary_url = get_database_url(db_path)
    candidates = _build_database_url_candidates() or [primary_url]

    for candidate_url in candidates:
        engine_url = _engine_connect_url(candidate_url)
        cached = _ENGINE_CACHE.get(engine_url)
        if cached is not None:
            return cached

    last_error: Optional[Exception] = None
    for candidate_url in candidates:
        for attempt in range(1, DEFAULT_DB_CONNECT_RETRIES + 1):
            engine_url = _engine_connect_url(candidate_url)
            try:
                parsed_engine_url = urlparse(engine_url)
                db_host = str(parsed_engine_url.hostname or "").strip()
            except ValueError:
                db_host = ""
            if db_host:
                print(f"Attempting connection to: {db_host}")
            engine = create_engine(
                engine_url,
                future=True,
                pool_pre_ping=True,
                pool_size=DEFAULT_DB_POOL_SIZE,
                max_overflow=DEFAULT_DB_MAX_OVERFLOW,
                pool_recycle=DEFAULT_DB_POOL_RECYCLE,
                pool_use_lifo=True,
                connect_args={"sslmode": "require"},
            )
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                _ENGINE_CACHE[engine_url] = engine
                return engine
            except Exception as exc:
                last_error = exc
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
        "phone_number": record.phone_number,
        "email": record.email,
        "google_claimed": record.google_claimed,
        "linkedin_url": record.linkedin_url,
        "instagram_url": record.instagram_url,
        "facebook_url": record.facebook_url,
        "tiktok_url": record.tiktok_url,
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


def _lead_to_create_payload(lead: Lead, user_id: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "user_id": user_id,
        "business_name": lead.business_name,
        "website_url": lead.website_url,
        "phone_number": lead.phone_number,
        "google_claimed": lead.google_claimed,
        "linkedin_url": lead.linkedin_url,
        "instagram_url": lead.instagram_url,
        "facebook_url": lead.facebook_url,
        "rating": lead.rating,
        "review_count": lead.review_count,
        "address": lead.address,
        "search_keyword": lead.search_keyword,
    }

    optional_fields = {
        "tiktok_url": lead.tiktok_url,
        "ig_link": lead.ig_link,
        "fb_link": lead.fb_link,
        "has_pixel": lead.has_pixel,
        "tech_stack": lead.tech_stack,
        "email": lead.email,
        "qualification_score": lead.qualification_score,
    }
    for key, value in optional_fields.items():
        if value is not None:
            payload[key] = value

    return payload


def _apply_lead_updates(record: LeadRecord, updates: dict[str, Any]) -> None:
    for field_name, value in updates.items():
        if hasattr(record, field_name):
            setattr(record, field_name, value)


def init_db(db_path: str = "runtime-db") -> None:
    Base.metadata.create_all(get_engine(db_path))


def create_lead(lead: Lead, user_id: str = "legacy", db_path: str = "runtime-db") -> dict[str, Any]:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        existing = session.execute(
            select(LeadRecord).where(
                LeadRecord.user_id == str(user_id),
                LeadRecord.business_name == lead.business_name,
                LeadRecord.address == lead.address,
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise ValueError("Lead already exists.")
        record = LeadRecord(**_lead_to_create_payload(lead, user_id))
        session.add(record)
        session.commit()
        session.refresh(record)
        return _lead_record_to_dict(record)


def get_lead(lead_id: int, db_path: str = "runtime-db") -> Optional[dict[str, Any]]:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        record = session.get(LeadRecord, int(lead_id))
        return _lead_record_to_dict(record) if record is not None else None


def update_lead(lead_id: int, updates: dict[str, Any], db_path: str = "runtime-db") -> Optional[dict[str, Any]]:
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


def delete_lead(lead_id: int, db_path: str = "runtime-db") -> bool:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        record = session.get(LeadRecord, int(lead_id))
        if record is None:
            return False
        session.delete(record)
        session.commit()
        return True


def upsert_lead(lead: Lead, db_path: str = "runtime-db", user_id: str = "legacy") -> bool:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        existing = session.execute(
            select(LeadRecord.id).where(
                LeadRecord.user_id == str(user_id),
                LeadRecord.business_name == lead.business_name,
                LeadRecord.address == lead.address,
            )
        ).scalar_one_or_none()
        if existing is not None:
            return False
        session.add(LeadRecord(**_lead_to_create_payload(lead, user_id)))
        session.commit()
        return True


def batch_upsert_leads(leads: Sequence[Lead], db_path: str = "runtime-db", user_id: str = "legacy") -> int:
    if not leads:
        return 0

    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        existing_records = session.execute(
            select(LeadRecord.business_name, LeadRecord.address).where(LeadRecord.user_id == str(user_id))
        ).all()
        existing_keys = {(row[0], row[1]) for row in existing_records}
        inserted = 0
        for lead in leads:
            key = (lead.business_name, lead.address)
            if key in existing_keys:
                continue
            session.add(LeadRecord(**_lead_to_create_payload(lead, user_id)))
            existing_keys.add(key)
            inserted += 1
        if inserted:
            session.commit()
        return inserted


def fetch_target_leads(db_path: str = "runtime-db", min_score: float = 7.0, user_id: Optional[str] = None) -> list[dict[str, Any]]:
    init_db(db_path)
    session_factory = get_session_factory(db_path)
    with session_factory() as session:
        query = select(LeadRecord).where((LeadRecord.ai_score.is_not(None)) & (LeadRecord.ai_score >= float(min_score)))
        if user_id:
            query = query.where(LeadRecord.user_id == str(user_id))
        query = query.order_by(desc(LeadRecord.ai_score), desc(LeadRecord.review_count), asc(LeadRecord.business_name))
        records = session.execute(query).scalars().all()
        return [_lead_record_to_dict(record) for record in records]
