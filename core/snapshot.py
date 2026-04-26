"""
Precomputed dashboard snapshot.

Why this module exists
----------------------
The v1 dashboard fires ~10 parallel HTTP requests on page load
(/api/signals, /api/sectors, /api/regime, /api/macro, /api/quote/* x6,
/api/pairs). On a 512 MB Heroku Basic dyno with one worker, those requests
queue and the slowest one trips the 30 s router timeout — especially on a
cold start when the 5 MB pickle cache has just been unpickled.

This module computes ALL of those payloads ONCE per refresh interval and
bundles them into a single dict. The web layer exposes that dict at
`GET /api/snapshot` with a long Cache-Control. The frontend fetches the
snapshot first; every dashboard card renders from that one object.

Result: page loads in one HTTP round-trip and the dyno does ~1/Nth the work
when N users hit the site within a TTL window.

Cache layers
------------
1. In-process memo (this module): fastest, but lost on dyno restart.
2. Postgres BYTEA blob (snapshot_cache table): survives restarts so the
   web dyno doesn't have to recompute on boot if a fresh snapshot was
   built by the worker/release-phase or a sibling dyno.

The snapshot is intentionally a *shallow* aggregator — it calls the
existing route handlers as plain Python functions. They're decorated with
@router.get(...) which registers them with FastAPI but doesn't change
them; they're still normal functions returning dicts. Per-card errors
(HTTPException, anything else) are caught and recorded under
`snapshot["errors"][card_name]` so one broken card never poisons the
whole payload.
"""

from __future__ import annotations

import io
import os
import pickle
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from core import data_engine as de


# How long an in-memory snapshot is considered fresh. Matches the 5-min
# memo TTL on the underlying market-data cache so the two layers don't
# fight each other.
SNAPSHOT_TTL_SECONDS = 300.0

# Postgres table for cross-dyno snapshot persistence. Keyed by integer 1
# (single-row table) — same pattern as market_data_cache.
_PG_TABLE = "dashboard_snapshot"

# Tickers the snapshot pre-bakes. Must stay small — every ticker here adds
# work to every snapshot rebuild. Keep this in sync with the frontend's
# default watchlist + ticker bar.
DEFAULT_WATCHLIST = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "INTC", "AMD", "NVDA", "TSLA", "SNDK",
]
TICKER_BAR_SYMBOLS = ["SPY", "QQQ", "AAPL", "NVDA", "TSLA", "INTC"]
DEFAULT_PAIR = ("KO", "PEP")


# --- In-process cache -----------------------------------------------------
_LOCK = threading.Lock()
_MEMO: dict[str, Any] = {"snapshot": None, "built_at_monotonic": 0.0}


# --- Postgres persistence -------------------------------------------------

def _pg_ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_PG_TABLE} (
                id INTEGER PRIMARY KEY,
                payload BYTEA NOT NULL,
                built_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()


def _pg_save(snapshot: dict) -> None:
    """Persist the latest snapshot to Postgres so other dynos / restarts
    can pick it up without recomputing."""
    if de._cache_backend() != "postgres":
        return
    try:
        conn = de._pg_conn()
        try:
            _pg_ensure_schema(conn)
            buf = io.BytesIO()
            pickle.dump(snapshot, buf, protocol=pickle.HIGHEST_PROTOCOL)
            blob = buf.getvalue()
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {_PG_TABLE} (id, payload, built_at)
                    VALUES (1, %s, now())
                    ON CONFLICT (id) DO UPDATE
                    SET payload = EXCLUDED.payload,
                        built_at = EXCLUDED.built_at
                    """,
                    (blob,),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        # Persisting is best-effort; the in-memory copy is still good.
        print(f"[snapshot] pg_save failed (non-fatal): {exc}")


def _pg_load() -> tuple[dict | None, float | None]:
    """Try to load a previously-built snapshot from Postgres. Returns
    (snapshot, age_seconds) or (None, None) if unavailable."""
    if de._cache_backend() != "postgres":
        return None, None
    try:
        conn = de._pg_conn()
        try:
            _pg_ensure_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT payload, EXTRACT(EPOCH FROM (now() - built_at)) "
                    f"FROM {_PG_TABLE} WHERE id = 1"
                )
                row = cur.fetchone()
                if not row:
                    return None, None
                blob, age_s = row
                snap = pickle.loads(bytes(blob))
                return snap, float(age_s)
        finally:
            conn.close()
    except Exception as exc:
        print(f"[snapshot] pg_load failed (non-fatal): {exc}")
        return None, None


# --- Builder --------------------------------------------------------------

def _safe(out: dict, key: str, fn: Callable[[], Any]) -> None:
    """Run a card-builder; on failure record the error and continue."""
    try:
        out[key] = fn()
    except Exception as exc:
        # HTTPException (from FastAPI) has a .detail attribute; everything
        # else uses str(exc).
        detail = getattr(exc, "detail", None) or str(exc)
        out["errors"][key] = f"{type(exc).__name__}: {detail}"


def build_snapshot() -> dict:
    """Compute every dashboard card payload. Pure function — no caching here.

    This is the expensive call. It does the equivalent of ~10 user requests
    back-to-back on a single thread. Expect ~5-15 s on a Basic dyno.
    """
    # Imports here (not at module top) to avoid circular imports — the route
    # modules import core.* which would import this file at startup.
    from backend import (
        routes_signals,
        routes_sectors,
        routes_regime,
        routes_macro,
        routes_quote,
        routes_pairs,
    )

    df, cache_ts = de.get_market_data()

    snapshot: dict[str, Any] = {
        "schema_version": 1,
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "cache_as_of_utc": cache_ts,
        "default_watchlist": list(DEFAULT_WATCHLIST),
        "ticker_bar_symbols": list(TICKER_BAR_SYMBOLS),
        "default_pair": {"a": DEFAULT_PAIR[0], "b": DEFAULT_PAIR[1]},
        "errors": {},
        # Card payloads — populated below.
        "signals": None,
        "sectors": None,
        "regime_spy": None,
        "macro": None,
        "quotes": {},
        "pair_default": None,
    }

    if df is None or df.empty:
        snapshot["errors"]["cache"] = "Market data cache is empty; nothing to compute."
        return snapshot

    # Each card is independent; one failing must not abort the snapshot.
    _safe(snapshot, "signals", lambda: routes_signals.get_signals(
        watchlist=",".join(DEFAULT_WATCHLIST)
    ))
    _safe(snapshot, "sectors", lambda: routes_sectors.get_sectors(min_tickers=3))
    _safe(snapshot, "regime_spy", lambda: routes_regime.get_regime(ticker="SPY", lookback=504))
    _safe(snapshot, "macro", lambda: routes_macro.get_macro())
    _safe(snapshot, "pair_default", lambda: routes_pairs.get_pair(
        a=DEFAULT_PAIR[0], b=DEFAULT_PAIR[1],
        lookback=252, z_window=30, entry=2.0, exit_=0.5,
    ))

    # Ticker bar — each quote is small, but slow individually because each
    # one extracts a column from the 3030-col DataFrame. Bake them all.
    for sym in TICKER_BAR_SYMBOLS:
        try:
            snapshot["quotes"][sym] = routes_quote.get_quote(ticker=sym, lookback=21)
        except Exception as exc:
            detail = getattr(exc, "detail", None) or str(exc)
            snapshot["errors"][f"quote_{sym}"] = f"{type(exc).__name__}: {detail}"

    return snapshot


# --- Public API -----------------------------------------------------------

def get_snapshot(force_rebuild: bool = False) -> dict:
    """Return the current snapshot, rebuilding if stale.

    Lookup order:
      1. In-process memo (if fresh and not force_rebuild).
      2. Postgres-persisted snapshot (if fresh).
      3. Rebuild from scratch (slow path).

    Thread-safe: rebuild is guarded by a lock so concurrent first-requests
    don't all kick off their own rebuild.
    """
    if not force_rebuild:
        cached = _MEMO.get("snapshot")
        age = time.monotonic() - _MEMO.get("built_at_monotonic", 0.0)
        if cached is not None and age < SNAPSHOT_TTL_SECONDS:
            return cached

    with _LOCK:
        # Re-check after acquiring the lock — another thread may have
        # rebuilt while we were waiting.
        cached = _MEMO.get("snapshot")
        age = time.monotonic() - _MEMO.get("built_at_monotonic", 0.0)
        if not force_rebuild and cached is not None and age < SNAPSHOT_TTL_SECONDS:
            return cached

        # Try Postgres before doing the expensive rebuild — useful at boot
        # so the web dyno doesn't recompute when a worker just built one.
        if not force_rebuild:
            pg_snap, pg_age = _pg_load()
            if pg_snap is not None and pg_age is not None and pg_age < SNAPSHOT_TTL_SECONDS:
                _MEMO["snapshot"] = pg_snap
                _MEMO["built_at_monotonic"] = time.monotonic() - pg_age
                return pg_snap

        # Slow path: compute everything fresh.
        t0 = time.monotonic()
        snap = build_snapshot()
        elapsed = time.monotonic() - t0
        snap["build_seconds"] = round(elapsed, 3)
        print(
            f"[snapshot] rebuilt in {elapsed:.2f}s "
            f"(errors={len(snap.get('errors', {}))}) "
            f"signals={'ok' if snap.get('signals') else 'missing'}"
        )

        _MEMO["snapshot"] = snap
        _MEMO["built_at_monotonic"] = time.monotonic()

        # Best-effort persist for sibling dynos / boot reuse.
        _pg_save(snap)
        return snap


def invalidate() -> None:
    """Drop the in-memory snapshot. The next get_snapshot() call rebuilds."""
    with _LOCK:
        _MEMO["snapshot"] = None
        _MEMO["built_at_monotonic"] = 0.0
