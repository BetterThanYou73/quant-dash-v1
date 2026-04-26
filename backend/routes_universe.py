"""
Routes for ticker discovery + on-demand cache hydration.

Endpoints
---------
GET  /api/universe?q=apple        → fuzzy search the SP500 list (+ user-added).
                                    Returns up to 25 matches for autocomplete.

POST /api/cache/ensure            → make sure given tickers are in the cache.
                                    If missing, fetch them from yfinance and
                                    merge into the local pickle. Persists the
                                    symbol so the worker keeps them fresh.
                                    Also looks up company name/sector via
                                    yfinance Ticker.info as a best-effort.

The autocomplete endpoint is read-only and cheap. The ensure endpoint may
hit the network and is the slow one — frontend should show a spinner.
"""

from __future__ import annotations

from typing import List, Optional

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from core import data_engine as de
from core import signals as sig


router = APIRouter(prefix="/api", tags=["universe"])


# ----- /api/universe (search) -------------------------------------------

@router.get("/universe")
def search_universe(
    q: Optional[str] = Query(None, description="Substring to match against symbol or name"),
    limit: int = Query(25, ge=1, le=100),
):
    """Fuzzy search the SP500 universe + user-added tickers.

    Ranking: exact symbol match > symbol prefix > name prefix > substring.
    This is a simple, deterministic ranking — no fuzzy library needed.
    """
    meta_df = de.get_ticker_metadata()  # Symbol, Name, Sector
    items = [
        {"symbol": row["Symbol"], "name": row.get("Name") or row["Symbol"], "sector": row.get("Sector") or "Unknown"}
        for _, row in meta_df.iterrows()
    ]

    # Fold in user-added tickers that aren't in SP500.csv
    sp500_syms = {r["symbol"] for r in items}
    user_meta = de.read_user_meta()
    for sym in de.read_user_tickers():
        if sym in sp500_syms:
            continue
        info = user_meta.get(sym, {})
        items.append({
            "symbol": sym,
            "name": info.get("name") or sym,
            "sector": info.get("sector") or "Unknown",
        })

    if not q:
        # No query → return the first `limit` items, alphabetical.
        items.sort(key=lambda x: x["symbol"])
        return {"count": len(items), "results": items[:limit]}

    qu = q.strip().upper()
    ql = q.strip().lower()

    def score(item):
        sym_u = item["symbol"].upper()
        name_l = item["name"].lower()
        if sym_u == qu:                  return 0   # exact symbol
        if sym_u.startswith(qu):         return 1   # symbol prefix
        if name_l.startswith(ql):        return 2   # name prefix
        if qu in sym_u:                  return 3   # symbol substring
        if ql in name_l:                 return 4   # name substring
        return 99

    scored = [(score(x), x) for x in items]
    scored = [(s, x) for s, x in scored if s < 99]
    scored.sort(key=lambda t: (t[0], t[1]["symbol"]))

    return {"count": len(scored), "results": [x for _, x in scored[:limit]]}


# ----- POST /api/cache/ensure -------------------------------------------

class EnsureRequest(BaseModel):
    tickers: List[str]
    period: str = "1y"


@router.post("/cache/ensure")
def ensure_cached(req: EnsureRequest):
    """Ensure the given tickers are present in the local price cache.

    Workflow:
      1. Diff against what's already in the cache.
      2. Fetch any missing ones from yfinance and merge into the pickle.
      3. Best-effort lookup of company name/sector for unknown symbols
         (so the UI can render them properly).
      4. Persist the symbols so the background worker refreshes them later.

    Returns lists of: already_cached, newly_added, failed.
    """
    syms = sorted({str(t).strip().upper() for t in req.tickers if str(t).strip()})
    if not syms:
        raise HTTPException(status_code=400, detail="tickers list is empty")

    # 1. What's already in the cache?
    cached, _ = de.load_cached_market_data()
    if not cached.empty:
        existing = set(sig.extract_close_prices(cached).columns)
    else:
        existing = set()

    already = [s for s in syms if s in existing]
    to_fetch = [s for s in syms if s not in existing]

    added: List[str] = []
    failed: List[str] = []

    # 2. Fetch + merge
    if to_fetch:
        added, failed = de.merge_tickers_into_cache(to_fetch, period=req.period)

    # 3. Persist for the worker. We persist `added` only — no point telling
    # the worker about tickers yfinance refused to return.
    if added:
        de.add_user_tickers(added)

    # 4. Best-effort metadata fetch for ANY of the requested symbols that
    # aren't in the SP500 list. yfinance Ticker.info is slow (~1-2s each)
    # so we only call it for unknowns. Failures are silent.
    sp500_syms = set(de.get_ticker_metadata()["Symbol"].astype(str).str.upper())
    unknowns = [s for s in syms if s not in sp500_syms]
    for sym in unknowns:
        try:
            info = yf.Ticker(sym).info or {}
            name = info.get("longName") or info.get("shortName") or sym
            sector = info.get("sector") or "Unknown"
            de.upsert_user_meta(sym, name=name, sector=sector)
        except Exception:
            # Network hiccup or unknown ticker — leave metadata blank.
            pass

    return {
        "requested": syms,
        "already_cached": already,
        "newly_added": added,
        "failed": failed,
    }
