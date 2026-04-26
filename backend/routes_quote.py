"""
Routes for single-ticker price history — /api/quote/{ticker}.

Powers the big price chart in the dashboard. Returns a list of
(date, close) points plus simple summary stats (latest, change, % change).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from backend._helpers import clean_for_json, load_close_prices

router = APIRouter(prefix="/api", tags=["quote"])


@router.get("/quote/{ticker}")
def get_quote(
    ticker: str,
    lookback: int = Query(126, ge=5, le=504, description="Trading days of history to return"),
):
    """Return historical close prices for a single ticker.

    Path param (`ticker`) for the symbol because it identifies the resource.
    Query param (`lookback`) for the window because it shapes the response.
    That distinction is a REST convention worth keeping consistent.
    """
    ticker = ticker.upper().strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")

    # load_close_prices raises 404 if the ticker isn't in the cache
    close = load_close_prices([ticker], lookback)
    series = close[ticker].dropna()

    if series.empty:
        raise HTTPException(status_code=422, detail=f"No price data for {ticker}")

    # Build the time series for the frontend chart.
    # We send ISO date strings — Chart.js / lightweight-charts both handle that.
    points = [
        {"date": idx.strftime("%Y-%m-%d"), "close": clean_for_json(val)}
        for idx, val in series.items()
    ]

    # Summary metrics so the UI can render the header (price + change badge)
    # without having to recompute them client-side.
    latest = float(series.iloc[-1])
    first = float(series.iloc[0])
    change_abs = latest - first
    change_pct = (change_abs / first) if first else None

    return {
        "ticker": ticker,
        "lookback_days": lookback,
        "point_count": len(points),
        "latest": clean_for_json(latest),
        "change_abs": clean_for_json(change_abs),
        "change_pct": clean_for_json(change_pct),
        "series": points,
    }


@router.get("/quotes")
def get_quotes(
    tickers: str = Query(..., description="Comma-separated ticker symbols (e.g. AAPL,MSFT,NVDA)"),
):
    """Batch latest-price snapshot for many tickers in one round-trip.

    Used by the Portfolio panel to value many positions without firing N
    separate /api/quote/{ticker} requests. Returns latest close + 1-day
    change for each requested symbol that exists in the cache. Missing
    symbols are reported in a separate `missing` list rather than aborting
    the whole call (so a typo in one position doesn't blank the table).
    """
    syms = sorted({t.strip().upper() for t in tickers.split(",") if t.strip()})
    if not syms:
        raise HTTPException(status_code=400, detail="tickers query param is required")
    if len(syms) > 200:
        # Hard cap to keep this from being abused as a bulk-download endpoint.
        raise HTTPException(status_code=400, detail="Max 200 tickers per call.")

    # Pull the last 2 trading days so we can compute a 1d change cheaply.
    close = load_close_prices(syms, lookback=2)
    out = []
    for sym in syms:
        if sym not in close.columns:
            continue
        series = close[sym].dropna()
        if series.empty:
            continue
        latest = float(series.iloc[-1])
        prev = float(series.iloc[-2]) if len(series) >= 2 else None
        change_pct = ((latest - prev) / prev) if prev else None
        out.append({
            "ticker": sym,
            "price": clean_for_json(latest),
            "change_pct": clean_for_json(change_pct),
        })
    found = {q["ticker"] for q in out}
    missing = [s for s in syms if s not in found]
    return {"count": len(out), "results": out, "missing": missing}

