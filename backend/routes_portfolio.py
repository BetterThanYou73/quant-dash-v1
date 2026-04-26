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

import math
import re
import threading
import time
import uuid
from typing import Any, Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Cookie, HTTPException, Request, Response
from pydantic import BaseModel, Field

from core import data_engine as de
from core import portfolio_db as pdb
from core import signals as sig
from backend.routes_auth import get_current_user_id


router = APIRouter(prefix="/api/portfolio", tags=["portfolio"])


# Cookie name + lifetime. 1 year is long enough that users rarely lose
# their device id; if they do, they can re-import from the localStorage
# fallback the frontend keeps as a backup.
_COOKIE_NAME = "qd_device"
_COOKIE_MAX_AGE = 60 * 60 * 24 * 365  # 1 year

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_MAX_POSITIONS = 100


def _resolve_owner(request: Request, response: Response) -> tuple[str, str]:
    """Return (owner_kind, owner_id) for the calling user.

    If a valid `qd_session` JWT is present, returns ('user', '<id>').
    Otherwise mints/reads the `qd_device` cookie and returns
    ('device', '<uuid>'). This keeps anonymous browsing fully
    functional while letting signed-in users persist across devices.
    """
    uid = get_current_user_id(request)
    if uid is not None:
        return ("user", str(uid))
    return ("device", _resolve_device_id(request, response))


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
    """List all positions for the calling user (or device, if anonymous)."""
    kind, oid = _resolve_owner(request, response)
    items = pdb.list_positions(kind, oid)
    return {
        "owner_kind": kind,
        "device_id_hint": oid[:8],   # first 8 chars only, for debug \u2014 don't echo full id
        "count": len(items),
        "positions": items,
    }


@router.post("")
def add_position(body: PositionIn, request: Request, response: Response) -> dict[str, Any]:
    """Add a single position. If the ticker already exists in the
    portfolio, blends the cost basis (weighted average)."""
    kind, oid = _resolve_owner(request, response)
    sym = _validate_ticker(body.ticker)

    # Cap total positions per portfolio to stop runaway abuse.
    existing = pdb.list_positions(kind, oid)
    if len(existing) >= _MAX_POSITIONS and not any(p["ticker"] == sym for p in existing):
        raise HTTPException(status_code=409, detail=f"max {_MAX_POSITIONS} positions per portfolio")

    try:
        result = pdb.upsert_position(kind, oid, sym, body.shares, body.avg_cost)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _bump_analytics_cache(f"{kind}:{oid}")
    return {"ok": True, "position": result}


@router.delete("/{ticker}")
def delete_position(ticker: str, request: Request, response: Response) -> dict[str, Any]:
    """Remove a single position by ticker."""
    kind, oid = _resolve_owner(request, response)
    sym = _validate_ticker(ticker)
    deleted = pdb.delete_position(kind, oid, sym)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"{sym} not in portfolio")
    _bump_analytics_cache(f"{kind}:{oid}")
    return {"ok": True, "deleted": sym}


@router.put("")
def replace_all(body: BulkPositions, request: Request, response: Response) -> dict[str, Any]:
    """Atomically replace the entire portfolio.

    Used for one-shot localStorage \u2192 Postgres migration on first load,
    and for "Save All" in the portfolio page.
    """
    kind, oid = _resolve_owner(request, response)
    items = [p.model_dump() for p in body.items]
    # Validate all tickers up front so we don't write a partial bad batch.
    for p in items:
        _validate_ticker(p["ticker"])
    n = pdb.replace_all_positions(kind, oid, items)
    _bump_analytics_cache(f"{kind}:{oid}")
    return {"ok": True, "written": n}


# ---- analytics ----------------------------------------------------------
#
# /api/portfolio/analytics computes everything the UI (and the future
# Advisor) needs in a single request: per-position price + day-change,
# portfolio totals, P&L, weighted beta vs SPY, weighted composite-z, and
# sector exposure. Expensive bits are reused from the same DataFrame the
# rest of the dashboard uses, so the marginal cost is small.
#
# A thin 60-second in-process cache keyed by (device_id, position-hash)
# absorbs the obvious "user keeps re-opening the modal" pattern. The
# cache is invalidated whenever the user adds/removes/replaces positions
# (via _bump_analytics_cache below).

_BENCHMARK_TICKER = "SPY"
_ANALYTICS_TTL = 60.0
_analytics_cache: dict[str, tuple[float, dict]] = {}
_analytics_lock = threading.Lock()


def _clean(v):
    if v is None: return None
    if isinstance(v, (np.floating,)):
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, float):
        return None if math.isnan(v) or math.isinf(v) else v
    return v


def _positions_key(positions: list[dict]) -> str:
    """Stable hash of (ticker, shares, avg_cost) tuples for cache keying."""
    parts = sorted(f"{p['ticker']}|{p['shares']}|{p['avg_cost']}" for p in positions)
    return "|".join(parts)


def _bump_analytics_cache(prefix: str) -> None:
    """Invalidate cached analytics for an owner.

    `prefix` should be either the legacy device_id (back-compat) or
    the new \"kind:oid\" form. We match on startswith() so both work.
    """
    with _analytics_lock:
        for k in [k for k in _analytics_cache if k.startswith(prefix + ":") or k.startswith(prefix)]:
            _analytics_cache.pop(k, None)


def _compute_analytics(positions: list[dict]) -> dict:
    """Pure compute: positions in -> analytics dict out."""
    if not positions:
        return {
            "totals": {
                "value": 0.0, "cost": 0.0, "unrealized_pl": 0.0,
                "unrealized_pl_pct": None, "day_change": 0.0,
                "weighted_beta": None, "weighted_composite_z": None,
            },
            "positions": [],
            "sector_exposure": [],
            "as_of_utc": None,
            "diagnostics": {"missing_prices": [], "missing_factors": []},
        }

    data, cache_ts = de.get_market_data()
    if data.empty:
        raise HTTPException(status_code=503, detail="No cached market data")

    close_all = sig.extract_close_prices(data)
    vols_all = sig.extract_volumes(data)
    if close_all.empty or _BENCHMARK_TICKER not in close_all.columns:
        raise HTTPException(status_code=503, detail="cache missing benchmark or close prices")

    benchmark_prices = close_all[_BENCHMARK_TICKER].dropna()
    universe_close = close_all.drop(columns=[_BENCHMARK_TICKER])
    universe_vols = vols_all.drop(columns=[_BENCHMARK_TICKER], errors="ignore")

    portfolio_tickers = sorted({p["ticker"] for p in positions})

    # Run the composite scoring across the full universe, but only return
    # rows for our portfolio tickers. Skip scoring entirely if the
    # universe is too small (cold cache).
    factor_map: dict[str, dict] = {}
    if universe_close.shape[1] >= 30:
        try:
            signal_df, _skipped = sig.build_composite_signals(
                close_prices=universe_close,
                volumes=universe_vols if not universe_vols.empty else None,
                benchmark_prices=benchmark_prices,
                watchlist=portfolio_tickers,
            )
            for row in signal_df.to_dict(orient="records"):
                factor_map[row["Ticker"]] = row
        except Exception:
            factor_map = {}  # fail soft \u2014 prices still work without factors

    # Sector + name lookup
    meta = de.get_ticker_metadata()
    sector_map = meta.set_index("Symbol")["Sector"].to_dict() if "Sector" in meta.columns else {}
    name_map = meta.set_index("Symbol")["Name"].to_dict() if "Name" in meta.columns else {}
    for sym, info in de.read_user_meta().items():
        sector_map.setdefault(sym, info.get("sector") or "Unknown")
        name_map.setdefault(sym, info.get("name") or sym)

    rows: list[dict] = []
    missing_prices: list[str] = []
    missing_factors: list[str] = []
    total_value = 0.0
    total_cost = 0.0
    total_day_change = 0.0
    weighted_beta_num = 0.0
    weighted_z_num = 0.0
    weighted_beta_denom = 0.0
    weighted_z_denom = 0.0

    for p in positions:
        sym = p["ticker"]
        shares = float(p["shares"])
        avg_cost = float(p["avg_cost"])
        cost = shares * avg_cost

        price = None
        prev_close = None
        if sym in close_all.columns:
            series = close_all[sym].dropna()
            if len(series) >= 1:
                price = float(series.iloc[-1])
            if len(series) >= 2:
                prev_close = float(series.iloc[-2])

        value = price * shares if price is not None else None
        day_change_pct = None
        day_change_dollar = None
        if price is not None and prev_close is not None and prev_close > 0:
            day_change_pct = (price / prev_close) - 1.0
            day_change_dollar = (price - prev_close) * shares

        upl = (value - cost) if value is not None else None
        upl_pct = (upl / cost) if (upl is not None and cost > 0) else None

        if value is None:
            missing_prices.append(sym)
        else:
            total_value += value
            total_cost += cost
            if day_change_dollar is not None:
                total_day_change += day_change_dollar

        f = factor_map.get(sym)
        beta = float(f["Beta"]) if f and f.get("Beta") is not None and not (isinstance(f.get("Beta"), float) and math.isnan(f["Beta"])) else None
        comp_z = float(f["Composite_Z"]) if f and f.get("Composite_Z") is not None and not (isinstance(f.get("Composite_Z"), float) and math.isnan(f["Composite_Z"])) else None
        comp_pct = float(f["Composite_Percentile"]) if f and f.get("Composite_Percentile") is not None else None
        signal_label = f.get("Signal") if f else None
        momentum = float(f["Momentum_12_1"]) if f and f.get("Momentum_12_1") is not None else None
        sortino = float(f["Sortino"]) if f and f.get("Sortino") is not None else None

        if value is not None:
            if beta is not None:
                weighted_beta_num += value * beta
                weighted_beta_denom += value
            else:
                missing_factors.append(sym)
            if comp_z is not None:
                weighted_z_num += value * comp_z
                weighted_z_denom += value

        rows.append({
            "ticker": sym,
            "name": name_map.get(sym, sym),
            "sector": sector_map.get(sym, "Unknown"),
            "shares": _clean(shares),
            "avg_cost": _clean(avg_cost),
            "price": _clean(price),
            "prev_close": _clean(prev_close),
            "value": _clean(value),
            "cost": _clean(cost),
            "day_change_pct": _clean(day_change_pct),
            "day_change_dollar": _clean(day_change_dollar),
            "unrealized_pl": _clean(upl),
            "unrealized_pl_pct": _clean(upl_pct),
            "weight": None,  # filled below once total_value is known
            "beta": _clean(beta),
            "composite_z": _clean(comp_z),
            "composite_percentile": _clean(comp_pct),
            "momentum_12_1": _clean(momentum),
            "sortino": _clean(sortino),
            "signal": signal_label,
        })

    # Now that we know totals, fill weight and sector exposure.
    sector_value: dict[str, float] = {}
    for r in rows:
        v = r["value"]
        if v is not None and total_value > 0:
            r["weight"] = v / total_value
            sector_value[r["sector"]] = sector_value.get(r["sector"], 0.0) + v

    sector_exposure = [
        {"sector": s, "value": _clean(v), "weight": _clean(v / total_value if total_value > 0 else None)}
        for s, v in sorted(sector_value.items(), key=lambda kv: -kv[1])
    ]

    upl_total = total_value - total_cost
    upl_total_pct = (upl_total / total_cost) if total_cost > 0 else None

    return {
        "totals": {
            "value": _clean(total_value),
            "cost": _clean(total_cost),
            "unrealized_pl": _clean(upl_total),
            "unrealized_pl_pct": _clean(upl_total_pct),
            "day_change": _clean(total_day_change),
            "weighted_beta": _clean(weighted_beta_num / weighted_beta_denom) if weighted_beta_denom > 0 else None,
            "weighted_composite_z": _clean(weighted_z_num / weighted_z_denom) if weighted_z_denom > 0 else None,
        },
        "positions": rows,
        "sector_exposure": sector_exposure,
        "as_of_utc": cache_ts,
        "benchmark": _BENCHMARK_TICKER,
        "diagnostics": {
            "missing_prices": missing_prices,
            "missing_factors": missing_factors,
        },
    }


@router.get("/analytics")
def analytics(request: Request, response: Response) -> dict[str, Any]:
    """Server-computed portfolio analytics: totals, weights, P&L,
    weighted beta vs SPY, weighted composite-z, sector exposure, and
    per-position factor scores."""
    kind, oid = _resolve_owner(request, response)
    positions = pdb.list_positions(kind, oid)
    cache_key = f"{kind}:{oid}:{_positions_key(positions)}"
    now = time.time()
    with _analytics_lock:
        hit = _analytics_cache.get(cache_key)
        if hit and (now - hit[0]) < _ANALYTICS_TTL:
            response.headers["Cache-Control"] = "private, max-age=30"
            return hit[1]

    payload = _compute_analytics(positions)

    with _analytics_lock:
        _analytics_cache[cache_key] = (now, payload)
        # Bound the cache so a churning user can't grow it without limit.
        if len(_analytics_cache) > 256:
            # drop oldest
            oldest = sorted(_analytics_cache.items(), key=lambda kv: kv[1][0])[:64]
            for k, _ in oldest:
                _analytics_cache.pop(k, None)

    response.headers["Cache-Control"] = "private, max-age=30"
    return payload
