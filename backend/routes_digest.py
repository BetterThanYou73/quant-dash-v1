"""
Email digest routes — /api/digest/*.

Endpoints:
    GET    /api/digest/prefs        -> {enabled, include_ai, last_sent_utc, configured}
    PUT    /api/digest/prefs        -> update opt-in + AI inclusion
    POST   /api/digest/preview      -> render HTML for current user (no send)
    POST   /api/digest/send_test    -> render + send to current user now
    POST   /api/digest/run_daily    -> cron: send to all subscribers
                                       (header X-Cron-Secret required)

Cron model:
    Heroku Scheduler (free addon) hits /api/digest/run_daily once a day
    at ~21:30 UTC (≈ market close + 30min during EDT). The endpoint is
    protected by DIGEST_CRON_SECRET — anyone hitting it without the
    matching header gets 401, so we don't have to expose a public
    no-auth endpoint.

    The daily run is idempotent within the same UTC day: each send
    stamps users.email_digest_last_sent, and the run skips users who
    already got mail today. Re-running the cron is safe.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field

from core import users_db as udb
from core import email_sender
from core import digest_builder
from backend.routes_auth import get_current_user_id


router = APIRouter(prefix="/api/digest", tags=["digest"])


def _require_user(request: Request) -> int:
    uid = get_current_user_id(request)
    if uid is None:
        raise HTTPException(status_code=401, detail="Sign in to manage email digests.")
    return uid


# ---- prefs ---------------------------------------------------------------

class PrefsIn(BaseModel):
    enabled: bool = Field(...)
    include_ai: bool = Field(default=True)


@router.get("/prefs")
def get_prefs(request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"
    prefs = udb.get_digest_prefs(uid)
    return {
        **prefs,
        # If the operator hasn't set RESEND_API_KEY/EMAIL_FROM, the
        # frontend disables the toggle and shows an explanation.
        "configured": email_sender.is_configured(),
    }


@router.put("/prefs")
def set_prefs(body: PrefsIn, request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"
    if body.enabled and not email_sender.is_configured():
        raise HTTPException(
            status_code=503,
            detail="Email sending isn't configured on this server yet. Try again later.",
        )
    prefs = udb.set_digest_prefs(uid, body.enabled, body.include_ai)
    return {**prefs, "configured": email_sender.is_configured()}


# ---- preview / test send -------------------------------------------------

@router.post("/preview")
def preview(request: Request, response: Response) -> dict:
    """Build the digest HTML for the current user without sending it.
    Use the include_ai query param to override the saved pref. Returns
    {subject, html} so the frontend can render it inside an iframe."""
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"
    user = udb.find_user_by_id(uid)
    if not user:
        raise HTTPException(status_code=404, detail="user not found")

    # Use the saved AI pref by default. We deliberately do NOT call AI
    # for previews unless the user has opted in — preview shouldn't
    # silently spend their key.
    prefs = udb.get_digest_prefs(uid)
    digest = digest_builder.build_digest(
        user_id=uid,
        user_email=user["email"],
        display_name=user.get("display_name"),
        include_ai=bool(prefs.get("include_ai")),
    )
    return {
        "subject": digest["subject"],
        "html": digest["html"],
        "has_data": digest["has_data"],
        "ai_used": digest["ai_used"],
        "alerts_count": digest["alerts_count"],
    }


@router.post("/send_test")
def send_test(request: Request, response: Response) -> dict:
    """Build + send the digest to the current user immediately. Useful
    to verify deliverability before enabling the daily cron."""
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"
    if not email_sender.is_configured():
        raise HTTPException(status_code=503, detail="Email sending not configured.")
    user = udb.find_user_by_id(uid)
    if not user:
        raise HTTPException(status_code=404, detail="user not found")

    prefs = udb.get_digest_prefs(uid)
    digest = digest_builder.build_digest(
        user_id=uid,
        user_email=user["email"],
        display_name=user.get("display_name"),
        include_ai=bool(prefs.get("include_ai")),
    )
    try:
        result = email_sender.send_email(
            to=user["email"],
            subject=digest["subject"],
            html=digest["html"],
            text=digest.get("text"),
        )
    except email_sender.EmailSendError as e:
        raise HTTPException(status_code=502, detail=str(e))

    udb.mark_digest_sent(uid)
    return {
        "ok": True,
        "to": user["email"],
        "subject": digest["subject"],
        "ai_used": digest["ai_used"],
        "provider_id": result.get("id"),
    }


# ---- cron ----------------------------------------------------------------

def _today_utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _last_sent_today(last_sent_utc: Optional[str]) -> bool:
    """True if the timestamp string falls on the current UTC date.
    Tolerant of either ISO-with-T or postgres' space-separated form."""
    if not last_sent_utc:
        return False
    today = _today_utc_date()
    return last_sent_utc.startswith(today)


@router.post("/run_daily")
def run_daily(
    request: Request,
    response: Response,
    x_cron_secret: Optional[str] = Header(default=None, alias="X-Cron-Secret"),
) -> dict:
    """Iterate all opted-in users and mail them today's digest.

    Authentication:
        Requires header `X-Cron-Secret: <DIGEST_CRON_SECRET>`. Returns
        401 otherwise. We do NOT allow signed-in users to trigger this
        on behalf of others — only the cron owns it.

    Idempotency:
        Skips users whose `email_digest_last_sent` is on today's UTC
        date. Re-running the same day is a no-op.
    """
    response.headers["Cache-Control"] = "no-store"

    expected = os.environ.get("DIGEST_CRON_SECRET", "")
    if not expected:
        raise HTTPException(status_code=503, detail="Cron secret not configured.")
    if not x_cron_secret or x_cron_secret != expected:
        raise HTTPException(status_code=401, detail="bad cron secret")

    if not email_sender.is_configured():
        raise HTTPException(status_code=503, detail="Email sending not configured.")

    started = time.time()
    subscribers = udb.list_digest_subscribers()
    sent: list[str] = []
    skipped: list[dict] = []
    failed: list[dict] = []

    for sub in subscribers:
        uid = sub["id"]
        email = sub["email"]
        if _last_sent_today(sub.get("last_sent_utc")):
            skipped.append({"user_id": uid, "reason": "already_sent_today"})
            continue
        try:
            digest = digest_builder.build_digest(
                user_id=uid,
                user_email=email,
                display_name=sub.get("display_name"),
                include_ai=bool(sub.get("include_ai")),
            )
            email_sender.send_email(
                to=email,
                subject=digest["subject"],
                html=digest["html"],
                text=digest.get("text"),
            )
            udb.mark_digest_sent(uid)
            sent.append(email)
        except email_sender.EmailSendError as e:
            failed.append({"user_id": uid, "error": str(e)[:200]})
        except Exception as e:
            failed.append({"user_id": uid, "error": f"build_or_send: {type(e).__name__}: {str(e)[:160]}"})

    elapsed = round(time.time() - started, 2)
    return {
        "ok": True,
        "sent_count": len(sent),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "subscriber_total": len(subscribers),
        "elapsed_seconds": elapsed,
        "failed": failed,    # surface errors in cron output for ops
        "skipped": skipped,
    }
