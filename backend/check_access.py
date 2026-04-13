from __future__ import annotations

from typing import Any

from fastapi import HTTPException

DEFAULT_PLAN_KEY = "free"

PLAN_TYPE_LABELS: dict[str, str] = {
    "free": "Starter",
    "hustler": "Hustler",
    "growth": "Growth",
    "scale": "Scale",
    "empire": "Empire",
    "pro": "Growth",
}

PLAN_KEY_ALIASES: dict[str, str] = {
    "starter": "free",
    "basic": "hustler",
    "business": "scale",
    "elite": "empire",
}

KNOWN_PLAN_KEYS = set(PLAN_TYPE_LABELS.keys())


def normalize_plan_key(plan_key_raw: Any, fallback: str = DEFAULT_PLAN_KEY) -> str:
    value = str(plan_key_raw or "").strip().lower()
    value = PLAN_KEY_ALIASES.get(value, value)
    if value in KNOWN_PLAN_KEYS:
        return value
    normalized_fallback = PLAN_KEY_ALIASES.get(str(fallback or "").strip().lower(), fallback)
    return normalized_fallback if normalized_fallback in KNOWN_PLAN_KEYS else DEFAULT_PLAN_KEY


def get_plan_feature_access(plan_key_raw: Any) -> dict[str, Any]:
    normalized = normalize_plan_key(plan_key_raw)
    access: dict[str, Any] = {
        "plan_key": normalized,
        "plan_type": PLAN_TYPE_LABELS.get(normalized, "Starter"),
        "basic_search": True,
        "mailer_send": True,
        "deep_analysis": False,
        "bulk_export": False,
        "drip_campaigns": False,
        "ai_lead_scoring": False,
        "webhooks": False,
        "advanced_reporting": False,
        "client_success_dashboard": False,
        "queue_priority": False,
        "ai_model": "gpt-4o-mini",
    }

    match normalized:
        case "free":
            return access
        case "hustler":
            access.update({
                "ai_lead_scoring": True,
            })
            return access
        case "growth" | "pro":
            access.update({
                "deep_analysis": True,
                "bulk_export": True,
                "drip_campaigns": True,
                "ai_lead_scoring": True,
                "ai_model": "gpt-4o",
            })
            return access
        case "scale":
            access.update({
                "deep_analysis": True,
                "bulk_export": True,
                "drip_campaigns": True,
                "ai_lead_scoring": True,
                "webhooks": True,
                "advanced_reporting": True,
                "client_success_dashboard": True,
                "ai_model": "gpt-4o",
            })
            return access
        case "empire":
            access.update({
                "deep_analysis": True,
                "bulk_export": True,
                "drip_campaigns": True,
                "ai_lead_scoring": True,
                "webhooks": True,
                "advanced_reporting": True,
                "client_success_dashboard": True,
                "queue_priority": True,
                "ai_model": "gpt-4o",
            })
            return access
        case _:
            return access


def require_feature_access(plan_key_raw: Any, feature_key: str) -> dict[str, Any]:
    feature = str(feature_key or "").strip().lower()
    access = get_plan_feature_access(plan_key_raw)

    match feature:
        case "basic_search" | "mailer_send":
            return access
        case "deep_analysis" | "drip_campaigns" | "bulk_export":
            if not bool(access.get(feature)):
                raise HTTPException(status_code=403, detail="This feature is available on Growth and above.")
            return access
        case "ai_lead_scoring":
            if not bool(access.get(feature)):
                raise HTTPException(status_code=403, detail="This feature is available on Hustler and above.")
            return access
        case "webhooks" | "advanced_reporting" | "client_success_dashboard":
            if not bool(access.get(feature)):
                raise HTTPException(status_code=403, detail="This feature is available on Scale and above.")
            return access
        case "queue_priority":
            if not bool(access.get(feature)):
                raise HTTPException(status_code=403, detail="This feature is available on Empire.")
            return access
        case _:
            if not bool(access.get(feature)):
                raise HTTPException(status_code=403, detail="This feature is not available on your current plan.")
            return access
