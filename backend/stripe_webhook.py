from typing import Any


def extract_payment_refresh_payload(event: dict[str, Any]) -> dict[str, Any]:
    event_type = str(event.get("type") or "").strip().lower()
    if event_type not in {
        "checkout.session.completed",
        "invoice.payment_succeeded",
        "invoice.paid",
        "invoice.payment_failed",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    }:
        return {
            "should_process": False,
            "event_type": event_type,
        }

    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    obj = data.get("object") if isinstance(data.get("object"), dict) else {}
    metadata = obj.get("metadata") if isinstance(obj.get("metadata"), dict) else {}
    checkout_mode = str(obj.get("mode") or "").strip().lower()
    billing_reason = str(obj.get("billing_reason") or "").strip().lower()

    user_id = str(metadata.get("user_id") or obj.get("client_reference_id") or "").strip()
    user_email = str(
        metadata.get("email")
        or obj.get("customer_email")
        or (obj.get("customer_details") or {}).get("email")
        or ""
    ).strip().lower()
    stripe_customer_id = str(obj.get("customer") or "").strip()

    credits_delta_raw = metadata.get("credits_added") or metadata.get("credits") or 0
    try:
        credits_delta = int(credits_delta_raw)
    except Exception:
        credits_delta = 0

    monthly_limit_raw = metadata.get("monthly_limit") or metadata.get("credits_limit") or 0
    try:
        monthly_limit = int(monthly_limit_raw)
    except Exception:
        monthly_limit = 0

    cancel_at_period_end = bool(obj.get("cancel_at_period_end"))
    subscription_status = str(obj.get("status") or "").strip().lower()

    def _as_int(value: Any) -> int:
        try:
            return int(value or 0)
        except Exception:
            return 0

    current_period_end = _as_int(obj.get("current_period_end"))
    cancel_at = _as_int(obj.get("cancel_at"))
    canceled_at = _as_int(obj.get("canceled_at"))
    ended_at = _as_int(obj.get("ended_at"))

    # Stripe lifecycle events usually do not include checkout metadata, so resolve identity from object first.
    if event_type.startswith("customer.subscription"):
        user_id = str(
            metadata.get("user_id")
            or obj.get("client_reference_id")
            or ""
        ).strip()
        stripe_customer_id = str(obj.get("customer") or stripe_customer_id).strip()

    return {
        "should_process": True,
        "event_type": event_type,
        "checkout_mode": checkout_mode,
        "billing_reason": billing_reason,
        "user_id": user_id,
        "user_email": user_email,
        "stripe_customer_id": stripe_customer_id,
        "credits_delta": max(0, credits_delta),
        "monthly_limit": max(0, monthly_limit),
        "subscription_status": subscription_status,
        "cancel_at_period_end": cancel_at_period_end,
        "current_period_end": max(0, current_period_end),
        "cancel_at": max(0, cancel_at),
        "canceled_at": max(0, canceled_at),
        "ended_at": max(0, ended_at),
        "metadata": metadata,
        "object": obj,
    }
