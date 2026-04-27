"""
Thin Resend REST client.

Why stdlib urllib instead of `requests`/`httpx`:
    Resend's API is one POST. Adding a dependency for a single ~30-line
    function is wasteful, and our slug already includes anthropic +
    cryptography + pandas + numpy + scipy. Keep email pluggable but
    minimal.

Required env vars:
    RESEND_API_KEY    Get from https://resend.com/api-keys
    EMAIL_FROM        e.g. "Quant Dash <digest@quantdash.tech>"
                      For initial testing without a verified domain you
                      can use "onboarding@resend.dev" — Resend lets the
                      account owner email themselves only with that.

The "send" function is intentionally synchronous and short-timeouts:
the cron will fan out one user at a time and we'd rather skip a
slow recipient than block the whole batch.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Optional


_RESEND_URL = "https://api.resend.com/emails"
_TIMEOUT_SECONDS = 12.0


class EmailSendError(RuntimeError):
    """Raised when Resend rejects the message or the network call fails."""


def is_configured() -> bool:
    """True if both RESEND_API_KEY and EMAIL_FROM are set. The frontend
    uses this (via /api/digest/prefs) to decide whether to even show
    the opt-in toggle."""
    return bool(os.environ.get("RESEND_API_KEY")) and bool(os.environ.get("EMAIL_FROM"))


def send_email(to: str, subject: str, html: str, text: Optional[str] = None) -> dict:
    """Send a single email. Raises EmailSendError on any failure.

    Returns the parsed Resend response (contains an `id` field on success).
    """
    api_key = os.environ.get("RESEND_API_KEY")
    sender = os.environ.get("EMAIL_FROM")
    if not api_key or not sender:
        raise EmailSendError("RESEND_API_KEY and EMAIL_FROM must be set")
    if not to or "@" not in to:
        raise EmailSendError(f"invalid recipient: {to!r}")

    payload: dict = {
        "from": sender,
        "to": [to],
        "subject": subject,
        "html": html,
    }
    if text:
        payload["text"] = text

    req = urllib.request.Request(
        _RESEND_URL,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(body) if body else {"ok": True}
            except json.JSONDecodeError:
                return {"raw": body}
    except urllib.error.HTTPError as e:
        # Resend returns JSON error bodies — surface them but never echo
        # the API key (we never put it in the URL or in error text below).
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = ""
        raise EmailSendError(f"Resend HTTP {e.code}: {err_body[:300]}") from None
    except urllib.error.URLError as e:
        raise EmailSendError(f"Network error contacting Resend: {e.reason}") from None
    except Exception as e:  # pragma: no cover — defensive
        raise EmailSendError(f"Unexpected email error: {e}") from None
