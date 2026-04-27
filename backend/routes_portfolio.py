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


def _hydrate_meta_if_missing(sym: str) -> None:
    """Look up name + sector from yfinance and persist if not already known.

    Tries the bare symbol first, then .TO / .V suffix variants for foreign
    listings (matches _ensure_ticker_cached's fallback ladder). Cheap and
    silent — never raises. Called both when a brand-new ticker is added
    and when /api/portfolio/refresh hits a ticker whose price column is
    already in the cache but whose meta row is missing (e.g. positions
    added before user_meta moved to Postgres).
    """
    try:
        existing_meta = de.read_user_meta() or {}
        entry = existing_meta.get(sym, {}) or {}
        if entry.get("name") and entry.get("sector"):
            return  # already complete

        import yfinance as yf

        candidates = [sym]
        if "." not in sym:
            candidates.extend([f"{sym}.TO", f"{sym}.V"])

        for cand in candidates:
            try:
                info = yf.Ticker(cand).info or {}
            except Exception:
                info = {}
            name = info.get("longName") or info.get("shortName")
            sector = info.get("sector") or info.get("category")
            if name or sector:
                de.upsert_user_meta(sym, name=name or sym, sector=sector or "ETF")
                return
    except Exception:
        return


def _ensure_ticker_cached(sym: str) -> None:
    """Fetch `sym` into the price cache if it isn't already there.

    Called when a user adds a position whose ticker isn't in the S&P 500
    base universe (e.g. ETFs like XEQT, foreign listings like TLO.TO,
    leveraged like SOXL). Synchronous yfinance call, ~1-2s for one
    ticker. Failures are swallowed: the position row is already saved,
    analytics will just show '\u2014' until the next worker refresh.

    If the bare symbol returns no data on Yahoo, we transparently try
    Canadian-suffix variants (.TO, .V) and merge those rows under the
    BARE symbol so analytics keys (which use the position's stored
    ticker) still match. This lets a user type "TLO" or "XEQT" and get
    Toronto-listed prices without having to know the exchange suffix.
    """
    try:
        existing, _ = de.load_cached_market_data()
        if not existing.empty:
            cols = existing.columns
            if hasattr(cols, "get_level_values"):
                try:
                    have = set(cols.get_level_values(-1).unique())
                except Exception:
                    have = set()
            else:
                have = set(cols)
            if sym in have:
                try:
                    de.add_user_tickers([sym])
                except Exception:
                    pass
                # Even when the price column already exists, the metadata
                # row (name + sector) might not — earlier deploys lost
                # user_meta.json on dyno restart. Backfill it here so
                # /api/portfolio/refresh repairs older positions.
                _hydrate_meta_if_missing(sym)
                return

        # Try the bare symbol first, then Canadian fallbacks. Stop at
        # the first variant yfinance actually returns rows for.
        candidates = [sym]
        if "." not in sym:
            candidates.extend([f"{sym}.TO", f"{sym}.V"])

        added: list[str] = []
        used_variant: str | None = None
        for cand in candidates:
            added_v, _failed_v = de.merge_tickers_into_cache([cand], period="2y")
            if added_v:
                added = added_v
                used_variant = cand
                break

        if not added or not used_variant:
            return

        # If the variant differs from the bare symbol, alias the columns
        # back to the bare symbol so analytics finds them under the
        # ticker the user typed. We re-load, rename, and re-save.
        if used_variant != sym:
            try:
                df, _ = de.load_cached_market_data()
                if not df.empty and isinstance(df.columns, pd.MultiIndex):
                    new_cols = []
                    seen_bare = False
                    for col in df.columns:
                        # MultiIndex levels: (field, ticker)
                        if col[-1] == used_variant:
                            new_cols.append(tuple([*col[:-1], sym]))
                            seen_bare = True
                        else:
                            new_cols.append(col)
                    if seen_bare:
                        df.columns = pd.MultiIndex.from_tuples(new_cols)
                        # Drop any duplicate (field, sym) pairs left over
                        df = df.loc[:, ~df.columns.duplicated(keep="last")]
                        de.save_market_data_cache(df)
            except Exception:
                pass

        try:
            de.add_user_tickers([sym])
        except Exception:
            pass
        # CRITICAL: drop the in-process memo so the very next
        # /api/portfolio/analytics call sees the freshly-merged ticker.
        try:
            de.invalidate_memo()
        except Exception:
            pass
        # Best-effort name + sector lookup using the variant that worked
        # (its .info has real metadata; the bare symbol's wouldn't).
        try:
            import yfinance as yf

            info = yf.Ticker(used_variant).info or {}
            name = info.get("longName") or info.get("shortName") or sym
            sector = info.get("sector") or info.get("category") or "ETF"
            de.upsert_user_meta(sym, name=name, sector=sector)
        except Exception:
            pass
    except Exception:
        # Never let a cache hydration failure break the add-position flow.
        pass


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
    portfolio, blends the cost basis (weighted average).

    If the ticker isn't already in the price cache (i.e. it's outside
    the S&P 500 universe — ETFs like XEQT, leveraged like SOXL, foreign
    listings like TLO.TO), we synchronously hydrate it from yfinance so
    the analytics call that runs right after this returns real numbers
    instead of em-dashes. The fetch is ~1-2s for a single ticker, which
    is acceptable add-position latency.
    """
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

    # Auto-hydrate the cache for tickers outside the S&P 500. Best-effort:
    # if yfinance is down or the ticker is bogus, the position is still
    # saved — analytics will just show '—' for price until the next
    # worker run picks it up.
    _ensure_ticker_cached(sym)

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


class PositionPatch(BaseModel):
    shares: float = Field(..., gt=0)


@router.patch("/{ticker}")
def patch_position(ticker: str, body: PositionPatch, request: Request, response: Response) -> dict[str, Any]:
    """Set the share count for an existing position to an exact number.

    Used by the partial-sell flow in the UI: if a user sells some but
    not all of their shares, the cost basis stays the same (you're not
    averaging in, you're trimming) so we just update `shares`. Selling
    everything goes through DELETE instead.
    """
    kind, oid = _resolve_owner(request, response)
    sym = _validate_ticker(ticker)
    existing = pdb.list_positions(kind, oid)
    cur = next((p for p in existing if p["ticker"] == sym), None)
    if cur is None:
        raise HTTPException(status_code=404, detail=f"{sym} not in portfolio")
    if body.shares >= float(cur["shares"]):
        # Don't let PATCH grow the position \u2014 use POST for that so cost
        # basis is recomputed correctly via weighted average.
        raise HTTPException(
            status_code=400,
            detail=f"PATCH only reduces shares; you own {cur['shares']}, asked for {body.shares}. Use POST to add.",
        )
    # Replace via the same upsert path: delete + re-insert with the
    # original avg_cost. (upsert_position would weighted-average if the
    # row exists, which we don't want here.)
    pdb.delete_position(kind, oid, sym)
    result = pdb.upsert_position(kind, oid, sym, body.shares, float(cur["avg_cost"]))
    _bump_analytics_cache(f"{kind}:{oid}")
    return {"ok": True, "position": result}


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

    # Hydrate any tickers outside the S&P 500 cache in one batched fetch
    # so the analytics call right after this returns real prices for
    # ETFs / foreign listings instead of em-dashes.
    for sym in {p["ticker"].strip().upper() for p in items}:
        _ensure_ticker_cached(sym)

    _bump_analytics_cache(f"{kind}:{oid}")
    return {"ok": True, "written": n}


@router.post("/refresh")
def refresh_prices(request: Request, response: Response) -> dict[str, Any]:
    """Force-rehydrate prices for every position in the user's portfolio.

    Useful when:
      - A position was added during a yfinance hiccup and never got a
        price (the column never made it into the cache).
      - A position used a bare symbol like "TLO" before .TO fallback
        existed; this endpoint will retry with suffix variants and
        alias the columns to the bare ticker.

    Returns per-ticker status so the UI can show a small toast.
    """
    kind, oid = _resolve_owner(request, response)
    positions = pdb.list_positions(kind, oid)
    if not positions:
        return {"ok": True, "refreshed": [], "missing": [], "n": 0}

    refreshed: list[str] = []
    missing: list[str] = []
    for p in positions:
        sym = p["ticker"]
        # Force a re-fetch by checking cache state, then merging if absent.
        # _ensure_ticker_cached short-circuits if already cached, so to
        # actually retry a missing ticker we drop the alias check and
        # call merge directly via the helper.
        before, _ = de.load_cached_market_data()
        had = False
        if not before.empty:
            cols = before.columns
            try:
                have = set(cols.get_level_values(-1).unique()) if hasattr(cols, "get_level_values") else set(cols)
            except Exception:
                have = set()
            had = sym in have

        _ensure_ticker_cached(sym)

        after, _ = de.load_cached_market_data()
        ok = False
        if not after.empty:
            try:
                have2 = set(after.columns.get_level_values(-1).unique()) if hasattr(after.columns, "get_level_values") else set(after.columns)
            except Exception:
                have2 = set()
            ok = sym in have2

        if ok and not had:
            refreshed.append(sym)
        elif not ok:
            missing.append(sym)

    # Drop analytics + history caches since prices may have moved.
    de.invalidate_memo()
    _bump_analytics_cache(f"{kind}:{oid}")
    return {"ok": True, "refreshed": refreshed, "missing": missing, "n": len(positions)}


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

# Process-wide memo of the FULL composite-signals table. The computation
# scans the entire universe (~500 tickers × 500 days) and is identical
# for all callers as long as the underlying market-data cache hasn't
# changed. Caching by cache_ts means the first portfolio render of a
# new market-data tick pays the ~3-5s cost; every subsequent render
# (any user, any portfolio) reuses the same DataFrame in microseconds.
_signals_memo: dict[str, Any] = {"cache_ts": None, "df": None, "built_at": 0.0}
_signals_lock = threading.Lock()
_SIGNALS_MEMO_MAX_AGE = 600.0  # also bound by wallclock so a stuck ts can't pin stale data


def _get_universe_signals(universe_close, universe_vols, benchmark_prices, cache_ts):
    """Return the universe-wide composite-signals DataFrame, cached by cache_ts.

    `universe_close`, `universe_vols`, `benchmark_prices` are only used
    on a cache miss — pass them lazily if you can.
    """
    now = time.time()
    with _signals_lock:
        memo_ts = _signals_memo.get("cache_ts")
        memo_df = _signals_memo.get("df")
        memo_age = now - _signals_memo.get("built_at", 0.0)
        if memo_df is not None and memo_ts == cache_ts and memo_age < _SIGNALS_MEMO_MAX_AGE:
            return memo_df

    # Compute outside the lock so concurrent first-callers don't all
    # serialize on a 5-second op (worst case: a couple do redundant work
    # — accepted to keep the lock short).
    df, _skipped = sig.build_composite_signals(
        close_prices=universe_close,
        volumes=universe_vols if universe_vols is not None and not universe_vols.empty else None,
        benchmark_prices=benchmark_prices,
        watchlist=None,  # full universe — we filter per portfolio downstream
    )

    with _signals_lock:
        _signals_memo["cache_ts"] = cache_ts
        _signals_memo["df"] = df
        _signals_memo["built_at"] = now
    return df


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
    """Invalidate cached analytics + history for an owner.

    `prefix` should be either the legacy device_id (back-compat) or
    the new \"kind:oid\" form. We match on startswith() so both work.
    """
    with _analytics_lock:
        for k in [k for k in _analytics_cache if k.startswith(prefix + ":") or k.startswith(prefix)]:
            _analytics_cache.pop(k, None)
    # History cache is keyed the same way (kind:oid:period:hash) — drop
    # any matching entries so the equity curve refreshes after edits.
    try:
        with _history_lock:
            for k in [k for k in _history_cache if k.startswith(prefix + ":") or k.startswith(prefix)]:
                _history_cache.pop(k, None)
    except NameError:
        # _history_lock isn't defined yet during early imports \u2014 first
        # call after module load will see it.
        pass


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
            # Universe-wide signals are cached by cache_ts so this only
            # actually computes once per market-data refresh tick. Per-
            # portfolio cost reduces to a dict lookup.
            signal_df = _get_universe_signals(
                universe_close, universe_vols, benchmark_prices, cache_ts
            )
            if signal_df is not None and not signal_df.empty:
                wanted = set(portfolio_tickers)
                for row in signal_df.to_dict(orient="records"):
                    if row.get("Ticker") in wanted:
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


# ---- equity curve -------------------------------------------------------
#
# /api/portfolio/history backtests the user's CURRENT shares against the
# cached daily close prices to produce a portfolio-value time series for
# charting on the portfolio page. We don't have a transaction history
# yet (Phase 2c will add it), so this is a "what would my current basket
# have done" view, not true historical equity. We disclose that in the
# response (`mode: "current_basket_backtest"`).
#
# Cheap: O(rows × tickers) numpy. ~5ms for 50 positions × 250 days.

_HISTORY_TTL = 60.0
_history_cache: dict[str, tuple[float, dict]] = {}
_history_lock = threading.Lock()

_PERIOD_DAYS = {"1m": 22, "3m": 66, "6m": 132, "1y": 252, "2y": 504, "5y": 1260, "max": 100000}


@router.get("/history")
def portfolio_history(
    request: Request,
    response: Response,
    period: str = "1y",
) -> dict[str, Any]:
    """Daily portfolio value series + benchmark comparison.

    Uses CURRENT shares applied backward against cached close prices.
    Tickers without enough history are skipped (and listed in
    `diagnostics.skipped`); the curve still renders for the rest.
    """
    kind, oid = _resolve_owner(request, response)
    period = (period or "1y").lower()
    if period not in _PERIOD_DAYS:
        raise HTTPException(status_code=400, detail=f"invalid period: {period}")

    positions = pdb.list_positions(kind, oid)
    cache_key = f"{kind}:{oid}:{period}:{_positions_key(positions)}"
    now = time.time()
    with _history_lock:
        hit = _history_cache.get(cache_key)
        if hit and (now - hit[0]) < _HISTORY_TTL:
            response.headers["Cache-Control"] = "private, max-age=30"
            return hit[1]

    if not positions:
        payload = {
            "period": period,
            "mode": "current_basket_backtest",
            "series": [],
            "benchmark": _BENCHMARK_TICKER,
            "diagnostics": {"skipped": [], "n_days": 0},
        }
        response.headers["Cache-Control"] = "private, max-age=30"
        return payload

    data, _ts = de.get_market_data()
    if data.empty:
        raise HTTPException(status_code=503, detail="No cached market data")
    close_all = sig.extract_close_prices(data)
    if close_all.empty:
        raise HTTPException(status_code=503, detail="cache missing close prices")

    days = _PERIOD_DAYS[period]
    # Tail to the requested window. SP500.csv goes back ~2y in the
    # default fetch, so "max" effectively == "2y" for now.
    if days < len(close_all):
        close_window = close_all.iloc[-days:]
    else:
        close_window = close_all

    # Build a (days × tickers) frame aligned to the requested holdings.
    skipped: list[str] = []
    held: dict[str, float] = {}
    for p in positions:
        sym = p["ticker"]
        if sym in close_window.columns:
            s = close_window[sym].dropna()
            # Need at least half the window to be useful, else skip
            # (e.g. a brand-new IPO won't have a full year of data).
            if len(s) >= max(5, len(close_window) // 2):
                held[sym] = float(p["shares"])
                continue
        skipped.append(sym)

    if not held:
        payload = {
            "period": period,
            "mode": "current_basket_backtest",
            "series": [],
            "benchmark": _BENCHMARK_TICKER,
            "diagnostics": {"skipped": skipped, "n_days": 0,
                            "note": "No held tickers have enough history for this period"},
        }
        response.headers["Cache-Control"] = "private, max-age=30"
        return payload

    held_close = close_window[list(held.keys())].copy()
    # Forward-fill so a single missing day doesn't punch a hole in the
    # curve. Then drop any leading rows that are still all-NaN.
    held_close = held_close.ffill().dropna(how="all")

    # Portfolio value per day = sum_i (shares_i * close_i_day). NaNs
    # become 0 contributions (rare after ffill), so the curve is robust
    # to ticker-specific gaps.
    shares_vec = np.array([held[c] for c in held_close.columns], dtype=float)
    values = (held_close.values * shares_vec).sum(axis=1)

    # Benchmark: rebase SPY to start at the same dollar value as the
    # portfolio's first day, so the two lines are visually comparable.
    bench_series = None
    if _BENCHMARK_TICKER in close_window.columns:
        b = close_window[_BENCHMARK_TICKER].reindex(held_close.index).ffill().bfill()
        if not b.empty and b.iloc[0] and b.iloc[0] > 0 and values[0] > 0:
            scale = float(values[0]) / float(b.iloc[0])
            bench_series = (b.values * scale).tolist()

    series = []
    for i, idx in enumerate(held_close.index):
        date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        row = {"date": date_str, "value": _clean(float(values[i]))}
        if bench_series is not None:
            row["benchmark"] = _clean(float(bench_series[i]))
        series.append(row)

    payload = {
        "period": period,
        "mode": "current_basket_backtest",
        "series": series,
        "benchmark": _BENCHMARK_TICKER,
        "diagnostics": {"skipped": skipped, "n_days": len(series)},
    }

    with _history_lock:
        _history_cache[cache_key] = (now, payload)
        if len(_history_cache) > 256:
            oldest = sorted(_history_cache.items(), key=lambda kv: kv[1][0])[:64]
            for k, _ in oldest:
                _history_cache.pop(k, None)

    response.headers["Cache-Control"] = "private, max-age=30"
    return payload
