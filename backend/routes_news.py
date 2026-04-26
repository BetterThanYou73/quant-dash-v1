"""
Routes for news headlines — /api/news.

Uses yfinance's free `Ticker.news` endpoint. No API key, no signup, no
rate-limit headaches. yfinance scrapes Yahoo Finance's news widget which
aggregates Reuters, Bloomberg syndication, MarketWatch, etc.

Caveats (be honest about what this gives you):
  - Yahoo's selection algorithm is opaque. Headlines skew toward retail
    interest (TSLA / NVDA), not toward what's most important.
  - No sentiment scoring here. Phase 2 will plug FinBERT (or a small LLM
    classifier) into a separate route to add per-headline sentiment.
  - The timestamps are publish times in UTC; the link is the original
    publisher's URL.
  - yfinance can return an empty list for sleepy / unfollowed tickers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query


router = APIRouter(prefix="/api", tags=["news"])


# Hard cap. Yahoo typically returns 8-12 items per call; more wouldn't fit
# the news card anyway and we don't want to encourage the frontend to
# render an unbounded list.
MAX_LIMIT = 20


def _normalize_item(raw: dict) -> Optional[dict]:
    """Map a yfinance news entry to a stable shape.

    yfinance has changed its news payload twice in the last year. We
    handle both the v1 (`title` / `link` / `providerPublishTime` / `publisher`)
    and v2 (`content`-nested) formats by trying both.
    """
    if not isinstance(raw, dict):
        return None

    # v2 format: fields live under raw["content"]
    content = raw.get("content") if isinstance(raw.get("content"), dict) else {}

    title = raw.get("title") or content.get("title")
    if not title:
        return None

    # Link can be at top level (v1) or nested (v2).
    link = (
        raw.get("link")
        or (content.get("canonicalUrl") or {}).get("url")
        or (content.get("clickThroughUrl") or {}).get("url")
    )

    publisher = (
        raw.get("publisher")
        or (content.get("provider") or {}).get("displayName")
        or "Unknown"
    )

    # Publish time: v1 uses unix seconds, v2 uses ISO strings.
    pub_time = None
    ts = raw.get("providerPublishTime") or content.get("pubDate")
    try:
        if isinstance(ts, (int, float)) and ts > 0:
            pub_time = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        elif isinstance(ts, str) and ts:
            pub_time = ts  # already ISO from v2
    except Exception:
        pub_time = None

    return {
        "title": title,
        "link": link,
        "publisher": publisher,
        "published_utc": pub_time,
    }


@router.get("/news")
def get_news(
    ticker: str = Query(..., description="Single ticker symbol"),
    limit: int = Query(8, ge=1, le=MAX_LIMIT),
):
    """Latest headlines for a single ticker.

    Returns up to `limit` items. Empty list is a valid response (some
    tickers genuinely have no recent news — we don't fabricate).
    """
    sym = ticker.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="ticker is required")

    try:
        raw_items = yf.Ticker(sym).news or []
    except Exception as exc:
        # yfinance rarely raises here, but if it does we tell the caller
        # rather than serving a confusing empty list.
        raise HTTPException(
            status_code=502,
            detail=f"Upstream news fetch failed for {sym}: {exc}",
        )

    items = [n for n in (_normalize_item(x) for x in raw_items) if n is not None]

    return {
        "ticker": sym,
        "count": len(items[:limit]),
        "results": items[:limit],
        "source": "Yahoo Finance (via yfinance)",
        "disclosure": (
            "Headlines are aggregated by Yahoo Finance with an opaque "
            "selection algorithm. No sentiment scoring is applied here; "
            "treat as awareness, not as a trading signal."
        ),
    }
