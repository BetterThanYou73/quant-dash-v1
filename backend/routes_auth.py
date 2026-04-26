"""
Routes for user authentication — /api/auth/*.

Endpoints:
    POST   /api/auth/signup    -> create account, sign in, migrate device portfolio
    POST   /api/auth/login     -> sign in, migrate device portfolio
    POST   /api/auth/logout    -> clear the session cookie
    GET    /api/auth/me        -> current user info (or null)

Identity model:
    A signed JWT in an httpOnly cookie `qd_session` carries the user id.
    The token has a 7-day rolling lifetime — every authenticated request
    extends it. Logout deletes the cookie; the JWT is also stateless so
    no server-side blacklist is needed for the MVP. (If we later need
    revocation, we can add a `password_hash`-derived `version` claim to
    the JWT and bump it on password change.)

    The `qd_device` cookie set by routes_portfolio is *additive*: a user
    can be both authenticated and have a device id. On login we run
    `users_db.migrate_device_portfolios_to_user()` so the portfolio they
    were just looking at follows them into their account.

Cookie security:
    - httponly=True       — JS can't steal the token (XSS-resistant)
    - samesite='lax'      — mitigates CSRF on top-level navigations
    - secure=False        — Heroku terminates TLS at the router; the
                            request to our app is internal http so a
                            blanket secure=True would break the cookie
                            on Heroku. Same caveat as routes_portfolio.

JWT secret:
    SESSION_SECRET env var on Heroku. Falls back to a dev-only constant
    locally so smoke tests work without env setup.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional

import jwt
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from core import users_db as udb


router = APIRouter(prefix="/api/auth", tags=["auth"])


_SESSION_COOKIE = "qd_session"
_SESSION_TTL_SECONDS = 60 * 60 * 24 * 7   # 7 days
_JWT_ALGO = "HS256"


def _secret() -> str:
    """Lazy lookup so tests can monkeypatch the env."""
    s = os.environ.get("SESSION_SECRET")
    if s:
        return s
    # Local dev fallback. NOT used on Heroku — we set SESSION_SECRET there.
    return "dev-only-do-not-use-in-prod-quantdash-2026"


def _make_token(user_id: int) -> str:
    # NB: time.time() is real UTC unix seconds. We deliberately avoid
    # datetime.utcnow().timestamp() because that treats naive datetimes
    # as local time and produces wrong epochs in non-UTC timezones,
    # which trips PyJWT's ImmatureSignatureError on decode.
    now = int(time.time())
    payload = {
        "sub": str(user_id),
        "iat": now,
        "exp": now + _SESSION_TTL_SECONDS,
    }
    return jwt.encode(payload, _secret(), algorithm=_JWT_ALGO)


def _decode_token(token: str) -> Optional[int]:
    """Returns user_id if valid, None otherwise. Never raises."""
    if not token:
        return None
    try:
        payload = jwt.decode(token, _secret(), algorithms=[_JWT_ALGO])
        return int(payload["sub"])
    except (jwt.InvalidTokenError, KeyError, ValueError):
        return None


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=_SESSION_COOKIE,
        value=token,
        max_age=_SESSION_TTL_SECONDS,
        httponly=True,
        samesite="lax",
        secure=False,  # see module docstring re: Heroku TLS termination
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(_SESSION_COOKIE)


def get_current_user_id(request: Request) -> Optional[int]:
    """Public helper used by other routers to scope reads/writes by user
    when the session cookie is present. Returns None if anonymous.
    """
    return _decode_token(request.cookies.get(_SESSION_COOKIE) or "")


# ---- bodies -------------------------------------------------------------

class Credentials(BaseModel):
    email: str = Field(..., max_length=254)
    password: str = Field(..., min_length=1, max_length=128)


class SignupBody(Credentials):
    # Optional friendly name shown in nav. Not used as identity.
    display_name: Optional[str] = Field(default=None, max_length=80)


# ---- routes -------------------------------------------------------------

@router.post("/signup")
def signup(body: SignupBody, request: Request, response: Response) -> dict[str, Any]:
    """Create a new account, sign in immediately, and migrate the
    caller's device-scoped portfolio into the new account."""
    try:
        user = udb.create_user(body.email, body.password, display_name=body.display_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Carry over device portfolio if any.
    device_id = request.cookies.get("qd_device") or ""
    migrated = udb.migrate_device_portfolios_to_user(device_id, user["id"]) if device_id else 0

    token = _make_token(user["id"])
    _set_session_cookie(response, token)
    return {
        "ok": True,
        "user": {"id": user["id"], "email": user["email"], "display_name": user.get("display_name")},
        "migrated_portfolios": migrated,
    }


@router.post("/login")
def login(body: Credentials, request: Request, response: Response) -> dict[str, Any]:
    """Verify credentials, set the session cookie, and migrate the
    caller's device-scoped portfolio if the account doesn't already
    have one."""
    user = udb.find_user_by_email(body.email)
    if not user or not udb.verify_password(body.password, user["password_hash"]):
        # Same error for both cases — don't leak which one failed.
        raise HTTPException(status_code=401, detail="invalid email or password")

    device_id = request.cookies.get("qd_device") or ""
    migrated = udb.migrate_device_portfolios_to_user(device_id, user["id"]) if device_id else 0

    token = _make_token(user["id"])
    _set_session_cookie(response, token)
    return {
        "ok": True,
        "user": {"id": user["id"], "email": user["email"], "display_name": user.get("display_name")},
        "migrated_portfolios": migrated,
    }


@router.post("/logout")
def logout(response: Response) -> dict[str, Any]:
    _clear_session_cookie(response)
    return {"ok": True}


@router.get("/me")
def me(request: Request) -> dict[str, Any]:
    """Returns the logged-in user, or {user: null} for anonymous callers.

    The frontend uses this to decide whether to show "Sign In" or the
    user's email in the nav.
    """
    uid = get_current_user_id(request)
    if uid is None:
        return {"user": None}
    user = udb.find_user_by_id(uid)
    if not user:
        # Stale cookie (user was deleted). Treat as anonymous.
        return {"user": None}
    return {"user": {"id": user["id"], "email": user["email"], "display_name": user.get("display_name")}}
