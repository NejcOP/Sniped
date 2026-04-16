import os
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from sqlalchemy import JSON, Boolean, DateTime, Float, Index, Integer, Text, UniqueConstraint, asc, create_engine, desc, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .models import Lead


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class LeadRecord(Base):
    __tablename__ = "leads"
    __table_args__ = (
        UniqueConstraint("business_name", "address", name="uq_leads_business_name_address"),
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


_ENGINE_CACHE: dict[str, Any] = {}
_SESSION_FACTORY_CACHE: dict[str, sessionmaker[Session]] = {}


def _normalize_database_url(raw_url: str) -> str:
    url = str(raw_url or "").strip()
    if not url:
        raise RuntimeError("SUPABASE_DATABASE_URL or DATABASE_URL is required.")
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg2://", 1)
    if url.startswith("postgresql://") and "+" not in url.split("://", 1)[0]:
        return url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def get_database_url(db_path: Optional[str] = None) -> str:
    del db_path
    configured = str(
        os.environ.get("SUPABASE_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or ""
    ).strip()
    if configured:
        return _normalize_database_url(configured)
    raise RuntimeError("SUPABASE_DATABASE_URL or DATABASE_URL is required.")


def get_engine(db_path: Optional[str] = None) -> Any:
    database_url = get_database_url(db_path)
    cached = _ENGINE_CACHE.get(database_url)
    if cached is not None:
        return cached
    engine = create_engine(database_url, future=True, pool_pre_ping=True)
    _ENGINE_CACHE[database_url] = engine
    return engine


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
        "rating": record.rating,
        "review_count": record.review_count,
        "address": record.address,
        "search_keyword": record.search_keyword,
        "scraped_at": record.scraped_at.isoformat() if record.scraped_at else None,
        "created_at": record.created_at.isoformat() if record.created_at else None,
        "main_shortcoming": record.main_shortcoming or "",
        "ai_score": float(record.ai_score or 0),
        "status": record.status or "",
        "enriched_at": record.enriched_at.isoformat() if record.enriched_at else "",
    }


def _lead_to_create_payload(lead: Lead, user_id: str) -> dict[str, Any]:
    return {
        "user_id": user_id,
        "business_name": lead.business_name,
        "website_url": lead.website_url,
        "phone_number": lead.phone_number,
        "rating": lead.rating,
        "review_count": lead.review_count,
        "address": lead.address,
        "search_keyword": lead.search_keyword,
    }


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
        existing_records = session.execute(select(LeadRecord.business_name, LeadRecord.address)).all()
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
