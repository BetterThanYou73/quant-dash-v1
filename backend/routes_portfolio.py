"""
Routes for portfolio persistence \u2014 /api/portfolio/*.

Identity model (Phase 2a):
  Each request must carry a `qd_device` cookie containing a UUID. If the
  cookie is absent, this layer mints one and sets it on the response.
  All portfolio rows are scoped by (owner_kind='device', owner_id=<uuid>).

  Phase 2b will add real auth: a logged-in request will use
  (owner_kind='user', owner_id=<user_id>) and a one-shot migration will
  reassign device-owned rows to the user on first sign-in.

Why a cookie and not localStorage:
  - Cookies are sent with every request automatically; the frontend
    doesn't have to remember to attach a header.
  - SameSite=Lax keeps it CSRF-safe for our GET-heavy traffic.
  - HttpOnly stops malicious scripts from stealing the device id.
  - It's first-party, no CORS issues since we're same-origin.

Limits (anti-abuse):
  - Max 100 positions per device. Real users have <50 holdings; this
    just stops a runaway script from eating Postgres rows.
  - Ticker length \u2264 10. Shares > 0. avg_cost \u2265 0.
"""

from __future__ import annotations

import re
import uuid
from typing import Any, Optional

from fastapi import APIRouter, Cookie, HTTPException, Request, Response
from pydantic import BaseModel, Field

from core import portfolio_db as pdb


router = APIRouter(prefix="/api/portfolio", tags=["portfolio"])


# Cookie name + lifetime. 1 year is long enough that users rarely lose
# their device id; if they do, they can re-import from the localStorage
# fallback the frontend keeps as a backup.
_COOKIE_NAME = "qd_device"
_COOKIE_MAX_AGE = 60 * 60 * 24 * 365  # 1 year

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_MAX_POSITIONS = 100


def _resolve_device_id(request: Request, response: Response) -> str:
    """Read or mint the device cookie. Always sets the cookie on the
    response so even cached/CDN-served HTML eventually carries one."""
    raw = request.cookies.get(_COOKIE_NAME)
    if raw and len(raw) == 36:
        try:
            uuid.UUID(raw)  # validate format; reject garbage
            return raw
        except ValueError:
            pass
    new = str(uuid.uuid4())
    response.set_cookie(
        key=_COOKIE_NAME,
        value=new,
        max_age=_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        # secure=True only in production over HTTPS. Local dev runs http
        # so we leave it off here; Heroku terminates TLS at the router
        # which means the request to our app is http internally anyway.
        secure=False,
    )
    return new


def _validate_ticker(t: str) -> str:
    sym = (t or "").strip().upper()
    if not _TICKER_RE.match(sym):
        raise HTTPException(status_code=400, detail=f"invalid ticker: {t!r}")
    return sym


# ---- request bodies ------------------------------------------------------

class PositionIn(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10)
    shares: float = Field(..., gt=0)
    avg_cost: float = Field(..., ge=0)


class BulkPositions(BaseModel):
    items: list[PositionIn] = Field(default_factory=list, max_length=_MAX_POSITIONS)


# ---- routes --------------------------------------------------------------

@router.get("")
def list_positions(request: Request, response: Response) -> dict[str, Any]:
    """List all positions for the calling device."""
    did = _resolve_device_id(request, response)
    items = pdb.list_positions("device", did)
    return {
        "owner_kind": "device",
        "device_id_hint": did[:8],   # first 8 chars only, for debug \u2014 don't echo full id
        "count": len(items),
        "positions": items,
    }


@router.post("")
def add_position(body: PositionIn, request: Request, response: Response) -> dict[str, Any]:
    """Add a single position. If the ticker already exists in the
    portfolio, blends the cost basis (weighted average)."""
    did = _resolve_device_id(request, response)
    sym = _validate_ticker(body.ticker)

    # Cap total positions per device to stop runaway abuse.
    existing = pdb.list_positions("device", did)
    if len(existing) >= _MAX_POSITIONS and not any(p["ticker"] == sym for p in existing):
        raise HTTPException(status_code=409, detail=f"max {_MAX_POSITIONS} positions per portfolio")

    try:
        result = pdb.upsert_position("device", did, sym, body.shares, body.avg_cost)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "position": result}


@router.delete("/{ticker}")
def delete_position(ticker: str, request: Request, response: Response) -> dict[str, Any]:
    """Remove a single position by ticker."""
    did = _resolve_device_id(request, response)
    sym = _validate_ticker(ticker)
    deleted = pdb.delete_position("device", did, sym)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"{sym} not in portfolio")
    return {"ok": True, "deleted": sym}


@router.put("")
def replace_all(body: BulkPositions, request: Request, response: Response) -> dict[str, Any]:
    """Atomically replace the entire portfolio.

    Used for one-shot localStorage \u2192 Postgres migration on first load,
    and for "Save All" in the portfolio modal.
    """
    did = _resolve_device_id(request, response)
    items = [p.model_dump() for p in body.items]
    # Validate all tickers up front so we don't write a partial bad batch.
    for p in items:
        _validate_ticker(p["ticker"])
    n = pdb.replace_all_positions("device", did, items)
    return {"ok": True, "written": n}
