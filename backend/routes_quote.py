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
