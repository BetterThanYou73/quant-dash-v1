"""
Routes for news headlines — /api/news.

Uses yfinance's free `Ticker.news` endpoint. No API key, no signup, no
rate-limit headaches. yfinance scrapes Yahoo Finance's news widget which
aggregates Reuters, Bloomberg syndication, MarketWatch, etc.

Caveats (be honest about what this gives you):
  - Yahoo's selection algorithm is opaque. Headlines skew toward retail
    interest (TSLA / NVDA), not toward what's most important.
  - We over-fetch and then keyword-filter to the requested ticker — Yahoo
    happily mixes in unrelated "industry mood" pieces (e.g. a CIEN
    request returning a GM earnings article). The filter trades a bit of
    recall for precision; if nothing matches we fall back to the raw feed
    and label it as such so the caller knows.
  - No sentiment scoring here. Phase 2 will plug FinBERT (or a small LLM
    classifier) into a separate route to add per-headline sentiment.
  - The timestamps are publish times in UTC; the link is the original
    publisher's URL.
  - yfinance can return an empty list for sleepy / unfollowed tickers.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query


router = APIRouter(prefix="/api", tags=["news"])


# Hard cap. Yahoo typically returns 8-12 items per call; more wouldn't fit
# the news card anyway and we don't want to encourage the frontend to
# render an unbounded list.
MAX_LIMIT = 20

# Generic words to strip from a company name when building keyword filters.
# Without this, the filter would match any "Inc" / "Corporation" / "Ltd"
# headline — defeating the point.
_NAME_STOPWORDS = {
    "inc", "inc.", "incorporated", "corp", "corp.", "corporation",
    "co", "co.", "company", "ltd", "ltd.", "limited", "plc",
    "holdings", "holding", "group", "the", "and", "&",
    "technologies", "technology", "systems", "international",
    "global", "industries", "industry", "enterprises",
}

# Tiny per-process cache for company names so we don't hammer yfinance
# on every news request. Cleared on process restart, which is fine.
_NAME_CACHE: dict[str, list[str]] = {}


def _company_keywords(sym: str) -> list[str]:
    """Return lowercase keywords identifying this ticker for headline matching.

    Always includes the symbol itself; tries to add the company's distinctive
    name words (e.g. "ciena" for CIEN). Falls back to just the symbol if
    yfinance can't supply a name.
    """
    if sym in _NAME_CACHE:
        return _NAME_CACHE[sym]

    keywords = [sym.lower()]
    try:
        info = yf.Ticker(sym).info or {}
        name = info.get("longName") or info.get("shortName") or ""
    except Exception:
        name = ""

    for word in re.split(r"[\s,/\-]+", name.lower()):
        word = word.strip(".,&")
        if word and word not in _NAME_STOPWORDS and len(word) >= 3 and word != sym.lower():
            keywords.append(word)

    _NAME_CACHE[sym] = keywords
    return keywords


def _is_relevant(title: str, keywords: list[str]) -> bool:
    """Headline is relevant if any keyword appears as a whole word in title."""
    text = f" {title.lower()} "
    return any(re.search(rf"\b{re.escape(k)}\b", text) for k in keywords)


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
    """Latest headlines for a single ticker, filtered for relevance.

    Returns up to `limit` items whose title mentions the ticker symbol or
    a distinctive word from the company name. If the relevance filter
    would leave us empty-handed, we serve the raw feed and set
    `filtered=False` so the caller can warn the user.
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

    all_items = [n for n in (_normalize_item(x) for x in raw_items) if n is not None]

    # Try the relevance filter first. If it leaves us with anything at
    # all, return only those. Otherwise fall back to the full feed so the
    # card isn't blank for tickers Yahoo only covers indirectly.
    keywords = _company_keywords(sym)
    relevant = [n for n in all_items if _is_relevant(n["title"], keywords)]
    filtered = bool(relevant)
    items = (relevant if relevant else all_items)[:limit]

    return {
        "ticker": sym,
        "count": len(items),
        "results": items,
        "filtered": filtered,
        "keywords": keywords,
        "source": "Yahoo Finance (via yfinance)",
        "disclosure": (
            "Headlines are aggregated by Yahoo Finance with an opaque "
            "selection algorithm and filtered to those mentioning the "
            "ticker or company name. No sentiment scoring is applied; "
            "treat as awareness, not as a trading signal."
        ),
    }

