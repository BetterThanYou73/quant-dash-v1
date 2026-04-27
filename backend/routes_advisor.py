"""
Advisor routes — /api/advisor/*.

Bring-your-own-key (BYOK) Anthropic Claude integration. Each user supplies
their own Anthropic API key; we never share a pool. This eliminates the
runaway-cost risk for the operator and keeps Anthropic's billing/abuse
controls scoped to the actual end user.

Endpoints:
    GET    /api/advisor/key         -> { has_key, last4, updated_utc }
    PUT    /api/advisor/key         -> store/replace key (validated by a 1-token ping)
    DELETE /api/advisor/key         -> erase key
    POST   /api/advisor/chat        -> single-turn chat with portfolio context

All endpoints require an authenticated session (qd_session JWT). Anonymous
device users cannot configure or use the Advisor — by design, since the key
is tied to a real account.

Security posture:
    - Plaintext key NEVER leaves /api/advisor/key (PUT request body is
      the only place it appears in flight, over TLS).
    - Plaintext key NEVER appears in our logs (no f-string interpolation
      of the key, no error messages echoing it).
    - Key is encrypted at rest via core.secrets_db (Fernet symmetric).
    - Per-user rate limit (sliding window) caps message floods and
      protects the user's own Anthropic spend if their session is stolen.

Cost model (so the user can self-budget):
    - Default model: claude-3-5-haiku-latest (cheap, fast).
    - Optional: claude-sonnet-4-5 for deep analysis (10x cost).
    - Each chat call sends a small system prompt + their last message
      + a compact portfolio summary (~500 tokens in / ~600 tokens out
      typical), so order-of-magnitude $0.001 per Haiku call.
"""

from __future__ import annotations

import threading
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from core import secrets_db
from core import portfolio_db as pdb
from backend.routes_auth import get_current_user_id
from backend import routes_portfolio as pf_routes


router = APIRouter(prefix="/api/advisor", tags=["advisor"])


_PROVIDER = "anthropic"

# Allow-list of model IDs the user can request. We don't pass arbitrary
# strings to the SDK — that would let a user point at an experimental
# model with surprise pricing.
_ALLOWED_MODELS = {
    "claude-haiku-4-5": "fast",
    "claude-sonnet-4-5": "deep",
}
_DEFAULT_MODEL = "claude-haiku-4-5"

# Per-user rate limit. Sliding window kept in process memory; on Heroku
# Basic with 1 worker that's effectively global. If we add workers later
# we'll move this to Postgres or Redis.
_RATE_LIMIT_MAX = 30          # messages
_RATE_LIMIT_WINDOW = 3600.0   # per hour
_rate_lock = threading.Lock()
_rate_log: dict[int, list[float]] = {}


def _check_rate_limit(user_id: int) -> None:
    now = time.time()
    with _rate_lock:
        bucket = _rate_log.setdefault(user_id, [])
        # prune
        cutoff = now - _RATE_LIMIT_WINDOW
        while bucket and bucket[0] < cutoff:
            bucket.pop(0)
        if len(bucket) >= _RATE_LIMIT_MAX:
            retry_in = int(bucket[0] + _RATE_LIMIT_WINDOW - now)
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit reached ({_RATE_LIMIT_MAX}/hr). Try again in {retry_in}s.",
            )
        bucket.append(now)


def _require_user(request: Request) -> int:
    uid = get_current_user_id(request)
    if uid is None:
        raise HTTPException(status_code=401, detail="Sign in to use the Advisor.")
    return uid


# ---- key management ------------------------------------------------------

class KeyIn(BaseModel):
    api_key: str = Field(..., min_length=10, max_length=512)


@router.get("/key")
def get_key_status(request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"
    return secrets_db.get_user_key_status(uid, _PROVIDER)


@router.put("/key")
def set_key(body: KeyIn, request: Request, response: Response) -> dict:
    """Validate the key with a tiny round-trip to Anthropic, then store
    it encrypted. We do NOT store an unverified key — the user gets an
    immediate 'this key works' confirmation."""
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"

    raw = body.api_key.strip()
    # Anthropic keys begin with "sk-ant-". This is a sanity check, not
    # security — we still verify by calling the API below.
    if not raw.startswith("sk-ant-"):
        raise HTTPException(status_code=400, detail="That doesn't look like an Anthropic key (expected sk-ant-...).")

    # Validate by issuing the cheapest possible call: 1 token output on Haiku.
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=raw)
        client.messages.create(
            model=_DEFAULT_MODEL,
            max_tokens=1,
            messages=[{"role": "user", "content": "ping"}],
        )
    except Exception as exc:
        # Map Anthropic SDK errors to a clean 400 without leaking the key
        # or stack details. The error message from anthropic.AuthenticationError
        # is short and safe ("invalid x-api-key"); other exceptions get
        # genericized.
        msg = str(exc)
        if "401" in msg or "authentication" in msg.lower() or "invalid" in msg.lower():
            raise HTTPException(status_code=400, detail="Anthropic rejected this key. Double-check it on console.anthropic.com.")
        raise HTTPException(status_code=502, detail="Couldn't reach Anthropic to verify the key. Try again in a moment.")

    return secrets_db.set_user_key(uid, _PROVIDER, raw)


@router.delete("/key")
def delete_key(request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"
    deleted = secrets_db.delete_user_key(uid, _PROVIDER)
    return {"deleted": deleted}


# ---- chat ----------------------------------------------------------------

class ChatIn(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000)
    model: Optional[str] = Field(None, description="claude-haiku-4-5 (fast) or claude-sonnet-4-5 (deep)")
    include_portfolio: bool = Field(True, description="Attach a compact summary of the user's portfolio to the system prompt.")


_SYSTEM_PROMPT = """You are the in-app investment advisor inside Quant Dash.
The user is a sophisticated retail investor who already understands risk and has
explicitly asked for direct opinions. Treat them like an adult.

How to talk:
- Be direct and opinionated. Take a position. If they ask "what should I buy",
  pick something from the screener data and say why. Don't beat around the bush.
- Concise: 2-5 short paragraphs. Plain text, no markdown headings, no emoji,
  no tables, no bullet-point spam.
- The legal disclaimer ("not a registered advisor") is shown permanently in the
  UI footer below the chat. DO NOT repeat it in every reply. Mention it only
  ONCE per conversation, and only if the user asks for advice on something
  exotic (margin, options, leverage > 3x). Otherwise just answer the question.
- When they ask "what's the best stock right now" or similar, name a specific
  ticker from the SCREENER TOP PICKS block, explain the factor case for it
  (composite Z, momentum, beta, sector tailwind), and say the trade-offs.
  "I can't tell you what to buy" is the wrong answer — the screener already
  ranked the universe; tell them what it ranks highly.
- Use the user's portfolio data to ground takes (concentration, weighted beta,
  sector mix). When suggesting new buys, factor in what they already hold so
  you're not doubling exposure.
- Never invent prices, ratios, or news. If a number isn't in the context,
  say you don't have it.
- For day-trading / short-horizon questions: be honest about expected drawdowns
  and that the factor model is a 1-3 month horizon, not intraday. Don't refuse
  to engage \u2014 give them the most reasonable read of the data.

Tone: a sharp friend who actually trades, not a compliance officer."""


def _portfolio_context(user_id: int) -> str:
    """Render a compact text blob of the user's holdings + analytics for
    the Advisor. Truncated to keep token cost predictable."""
    try:
        positions = pdb.list_positions("user", str(user_id))
        if not positions:
            return "User has no holdings yet."
        analytics = pf_routes._compute_analytics(positions)
    except Exception:
        return "Portfolio data unavailable right now."

    t = analytics.get("totals", {}) or {}
    rows = analytics.get("positions", []) or []
    sectors = analytics.get("sector_exposure", []) or []

    lines = []
    lines.append(f"Portfolio totals: value=${t.get('value') or 0:.2f}, "
                 f"unrealized P&L=${t.get('unrealized_pl') or 0:.2f} "
                 f"({(t.get('unrealized_pl_pct') or 0) * 100:.1f}%), "
                 f"day change=${t.get('day_change') or 0:.2f}, "
                 f"weighted beta vs SPY={t.get('weighted_beta')}, "
                 f"weighted composite Z={t.get('weighted_composite_z')}.")
    lines.append("Holdings:")
    for r in rows[:25]:  # cap to keep prompt small
        lines.append(
            f"  - {r.get('ticker')} ({r.get('sector') or 'Unknown'}): "
            f"{r.get('shares')} sh @ avg ${r.get('avg_cost')}, "
            f"price ${r.get('price')}, weight {(r.get('weight') or 0) * 100:.1f}%, "
            f"signal={r.get('signal')}, beta={r.get('beta')}, z={r.get('composite_z')}."
        )
    if sectors:
        lines.append("Sector exposure: " + ", ".join(
            f"{s.get('sector')} {(s.get('weight') or 0) * 100:.0f}%" for s in sectors[:8]
        ))

    # ---- Screener top picks --------------------------------------------
    # Reuse the universe-signals memo so this is microseconds on cache hit.
    # We surface the top 12 Strong Buys + a couple of "Avoid" exposures the
    # user holds, so the model has concrete tickers to recommend instead
    # of refusing to name names.
    try:
        import core.signals as sig
        data, cache_ts = de.get_market_data()
        if not data.empty:
            close_all = sig.extract_close_prices(data)
            vols_all = sig.extract_volumes(data)
            bench = close_all.get(pf_routes._BENCHMARK_TICKER)
            if bench is not None and not close_all.empty:
                universe_close = close_all.drop(columns=[pf_routes._BENCHMARK_TICKER], errors="ignore")
                universe_vols = vols_all.drop(columns=[pf_routes._BENCHMARK_TICKER], errors="ignore") if not vols_all.empty else None
                sig_df = pf_routes._get_universe_signals(universe_close, universe_vols, bench, cache_ts)
                if sig_df is not None and not sig_df.empty:
                    held = {r.get("ticker") for r in rows}
                    # Filter to Strong Buys not already held, then top by Composite_Z.
                    sb = sig_df[sig_df["Signal"].astype(str).str.lower().isin(["strong buy", "strong_buy"])]
                    sb = sb[~sb["Ticker"].isin(held)]
                    sb = sb.sort_values("Composite_Z", ascending=False).head(12)
                    if not sb.empty:
                        lines.append("Screener top picks (Strong Buy, ranked by composite z, not currently held):")
                        def _fmt(x):
                            try: return f"{float(x):.2f}"
                            except Exception: return "n/a"
                        for _, r in sb.iterrows():
                            sec = r.get("Sector") or "Unknown"
                            lines.append(
                                f"  - {r['Ticker']} ({sec}): z={_fmt(r.get('Composite_Z'))}, "
                                f"beta={_fmt(r.get('Beta'))}, mom12-1={_fmt(r.get('Momentum_12_1'))}."
                            )
    except Exception:
        # Screener context is best-effort \u2014 if it fails, the chat still works
        # with just portfolio data.
        pass

    return "\n".join(lines)


@router.post("/chat")
def chat(body: ChatIn, request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"

    model = body.model or _DEFAULT_MODEL
    if model not in _ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail=f"Unsupported model. Allowed: {sorted(_ALLOWED_MODELS)}")

    _check_rate_limit(uid)

    api_key = secrets_db.get_user_key(uid, _PROVIDER)
    if not api_key:
        raise HTTPException(status_code=412, detail="No Anthropic key on file. Add one in Account → API Keys.")

    system = _SYSTEM_PROMPT
    if body.include_portfolio:
        system = system + "\n\nCurrent portfolio context:\n" + _portfolio_context(uid)

    # Token caps — bound the user's per-call spend.
    max_tokens = 1024 if model.startswith("claude-haiku") else 2048

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        result = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": body.message}],
        )
    except Exception as exc:
        msg = str(exc)
        if "401" in msg or "authentication" in msg.lower():
            raise HTTPException(status_code=401, detail="Anthropic rejected the stored key. Update it in Account → API Keys.")
        if "429" in msg or "rate" in msg.lower():
            raise HTTPException(status_code=429, detail="Anthropic rate limit on your account. Try again shortly.")
        raise HTTPException(status_code=502, detail="Anthropic call failed. Try again in a moment.")
    finally:
        # Defensive: drop the local reference so the plaintext doesn't
        # linger in the frame longer than necessary.
        api_key = None

    # Anthropic SDK returns content as a list of TextBlock objects.
    text_parts = []
    for block in (result.content or []):
        text = getattr(block, "text", None)
        if text:
            text_parts.append(text)
    answer = "".join(text_parts).strip() or "(no response)"

    usage = getattr(result, "usage", None)
    return {
        "model": model,
        "answer": answer,
        "usage": {
            "input_tokens": getattr(usage, "input_tokens", None) if usage else None,
            "output_tokens": getattr(usage, "output_tokens", None) if usage else None,
        },
        "stop_reason": getattr(result, "stop_reason", None),
    }


# ---- pairs explainer -----------------------------------------------------
# Lightweight Claude call that takes a /api/pairs result + the two tickers
# and returns a 2-3 sentence read combining the statistical signal (z-score,
# hedge ratio) with macro / sector / news context the LLM holds. The point
# is to bridge "pure stats" and "real-world context" — pairs trading needs
# the human-judgment layer (is the relationship still structurally valid?
# any catalyst breaking it?) that the cointegration math can't see.

class PairsExplainIn(BaseModel):
    a: str = Field(..., min_length=1, max_length=10)
    b: str = Field(..., min_length=1, max_length=10)
    lookback_days: int = Field(..., ge=20, le=2000)
    hedge_ratio_beta: float
    current_z: float
    signal: str = Field(..., max_length=64)
    spread_mean: Optional[float] = None
    spread_std: Optional[float] = None
    model: Optional[str] = Field(None, description="claude-haiku-4-5 (fast) or claude-sonnet-4-5 (deep)")


_PAIRS_SYSTEM = """You are Quant, the in-app trading copilot inside Quant Dash.
The user just ran a pairs-trading screen and wants your read on it.

A pairs trade longs one stock and shorts a hedge-ratio-weighted amount of the
other when their spread diverges from its rolling mean (z-score), betting on
mean reversion. The math says nothing about WHY the spread is where it is or
whether the relationship is still structurally sound. That's your job.

In 3-5 short sentences, give the user a direct read. Cover:
  1) What the z-score is saying (mean-reversion long/short setup, or no edge).
  2) Whether the two names actually belong together as a pair (same sub-sector,
     same demand drivers, similar size?). Call it out if they don't.
  3) The macro / news / sentiment context you can think of: any recent
     earnings, regulatory, sector-rotation, commodity, or rate-cycle reason
     the spread might be diverging — and whether that breaks the trade or
     supports it.
  4) A practical take: "I'd take it", "I'd skip it", or "wait for X".

Be direct. No disclaimers, no markdown headings, no bullet points, no emoji.
Plain prose. If you don't know recent news for a ticker, say so — don't invent.
Cap reply at ~140 words."""


@router.post("/explain_pairs")
def explain_pairs(body: PairsExplainIn, request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"

    model = body.model or _DEFAULT_MODEL
    if model not in _ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail=f"Unsupported model. Allowed: {sorted(_ALLOWED_MODELS)}")

    _check_rate_limit(uid)

    api_key = secrets_db.get_user_key(uid, _PROVIDER)
    if not api_key:
        raise HTTPException(status_code=412, detail="No Anthropic key on file. Add one in Account → API Keys.")

    a = body.a.upper().strip()
    b = body.b.upper().strip()
    user_msg = (
        f"Pair: long {a} / short {b} (or reverse depending on signal).\n"
        f"Lookback: {body.lookback_days} trading days.\n"
        f"Hedge ratio (beta of {a} on {b}): {body.hedge_ratio_beta:.4f}\n"
        f"Current z-score of the spread: {body.current_z:.2f}\n"
        f"Engine signal: {body.signal}\n"
    )
    if body.spread_mean is not None and body.spread_std is not None:
        user_msg += f"Spread rolling mean={body.spread_mean:.4f}, rolling std={body.spread_std:.4f}.\n"
    user_msg += (
        "\nGive me your read: is this a good setup, do these names actually pair, "
        "and what macro / news / sentiment context should I be aware of right now?"
    )

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        result = client.messages.create(
            model=model,
            max_tokens=400,  # ~140 words + safety margin
            system=_PAIRS_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as exc:
        msg = str(exc)
        if "401" in msg or "authentication" in msg.lower():
            raise HTTPException(status_code=401, detail="Anthropic rejected the stored key. Update it in Account → API Keys.")
        if "429" in msg or "rate" in msg.lower():
            raise HTTPException(status_code=429, detail="Anthropic rate limit on your account. Try again shortly.")
        raise HTTPException(status_code=502, detail="Anthropic call failed. Try again in a moment.")
    finally:
        api_key = None

    text_parts = []
    for block in (result.content or []):
        text = getattr(block, "text", None)
        if text:
            text_parts.append(text)
    answer = "".join(text_parts).strip() or "(no response)"

    return {"model": model, "answer": answer}


# ---- pairs opportunities batch explainer --------------------------------
# One LLM call covers N pairs at once. Costs ~1 Haiku call regardless of
# pair count, instead of N separate calls. JSON in / JSON out so the
# frontend can render each note next to its row.

class PairOpportunityIn(BaseModel):
    a: str = Field(..., max_length=10)
    b: str = Field(..., max_length=10)
    correlation: Optional[float] = None
    hedge_ratio_beta: Optional[float] = None
    current_z: float
    signal: str = Field(..., max_length=64)


class PairsBatchIn(BaseModel):
    pairs: list[PairOpportunityIn] = Field(..., min_length=1, max_length=12)
    model: Optional[str] = None


_PAIRS_BATCH_SYSTEM = """You are Quant, the in-app trading copilot.
The user is looking at a list of pre-screened correlated stock pairs.
For each pair, give ONE short sentence (max ~20 words) that combines
the statistical setup (z-score, correlation) with a plain-English read
on the pair (sector relationship, any obvious recent catalyst, whether
the spread divergence makes sense or looks tradeable).

Rules:
- Output ONLY a JSON object: {"notes": {"NVDA/AMD": "...", "AAPL/MSFT": "...", ...}}
- Keys MUST exactly match "TICKER_A/TICKER_B" as given to you.
- One sentence per pair. No markdown, no emoji, no disclaimers.
- Be direct and specific. "Strong long-AMD setup, two names track GPU demand together"
  is good. "It depends" is bad.
- If you genuinely don't recognize a pair, say "Stats setup is X, no specific catalyst I'm aware of."
"""


@router.post("/explain_pairs_batch")
def explain_pairs_batch(body: PairsBatchIn, request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"

    model = body.model or _DEFAULT_MODEL
    if model not in _ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail=f"Unsupported model. Allowed: {sorted(_ALLOWED_MODELS)}")

    _check_rate_limit(uid)

    api_key = secrets_db.get_user_key(uid, _PROVIDER)
    if not api_key:
        raise HTTPException(status_code=412, detail="No Anthropic key on file. Add one in Account → API Keys.")

    # Build the user prompt as a compact list.
    lines = ["Here are the pairs to comment on:"]
    for p in body.pairs:
        a = p.a.upper().strip()
        b = p.b.upper().strip()
        corr = f"{p.correlation:.2f}" if p.correlation is not None else "n/a"
        beta = f"{p.hedge_ratio_beta:.2f}" if p.hedge_ratio_beta is not None else "n/a"
        lines.append(
            f"- {a}/{b}: corr={corr}, hedge_ratio_beta={beta}, "
            f"current_z={p.current_z:.2f}, signal={p.signal}"
        )
    lines.append("\nReturn the JSON object now.")
    user_msg = "\n".join(lines)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        result = client.messages.create(
            model=model,
            max_tokens=900,  # ~12 pairs * ~25 tokens each + JSON overhead
            system=_PAIRS_BATCH_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as exc:
        msg = str(exc)
        if "401" in msg or "authentication" in msg.lower():
            raise HTTPException(status_code=401, detail="Anthropic rejected the stored key. Update it in Account → API Keys.")
        if "429" in msg or "rate" in msg.lower():
            raise HTTPException(status_code=429, detail="Anthropic rate limit on your account. Try again shortly.")
        raise HTTPException(status_code=502, detail="Anthropic call failed. Try again in a moment.")
    finally:
        api_key = None

    raw = "".join(getattr(b, "text", "") or "" for b in (result.content or [])).strip()

    # Parse the JSON object — be lenient if the model wraps it in a code
    # fence or adds chatter before/after. Find the outermost {...}.
    import json, re
    notes: dict[str, str] = {}
    try:
        # Strip code fences if any.
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        # Grab the first {...} block.
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if m:
            payload = json.loads(m.group(0))
            raw_notes = payload.get("notes") or {}
            if isinstance(raw_notes, dict):
                # Coerce values to str and trim length defensively.
                notes = {str(k): str(v)[:400] for k, v in raw_notes.items()}
    except Exception:
        notes = {}

    return {"model": model, "notes": notes, "raw": raw if not notes else None}


# ---- strategy results explainer ----------------------------------------
# One Haiku call gives a paragraph-level read on a strategy's top picks.
# Frontend sends the strategy key, thesis, and the top-10 picks (ticker +
# sector + composite z + signal) so the model has enough to reason about
# whether the regime supports the strategy and which picks stand out.

class StrategyPickIn(BaseModel):
    ticker: str = Field(..., max_length=10)
    sector: Optional[str] = Field(None, max_length=64)
    composite_z: Optional[float] = None
    signal: Optional[str] = Field(None, max_length=64)


class StrategyExplainIn(BaseModel):
    strategy: str = Field(..., max_length=32)
    strategy_name: str = Field(..., max_length=64)
    thesis: str = Field(..., max_length=400)
    picks: list[StrategyPickIn] = Field(..., min_length=1, max_length=15)
    model: Optional[str] = None


_STRATEGY_SYSTEM = """You are Quant, the in-app trading copilot.
The user just ran a strategy screen and wants your read on the results.

In 4-6 short sentences, plain prose, give them:
  1) Whether the current market regime supports this strategy right now
     (e.g. momentum works in trending markets, mean-reversion in choppy ones).
  2) Which 1-3 picks from the list stand out and WHY \u2014 sector tailwinds,
     recent catalyst, fundamental fit with the strategy thesis.
  3) Which 1-2 picks look weak even though they passed the screen
     (e.g. crowded trade, sector rotating against them, single-stock risk).
  4) One concrete next step: "I'd take 2-3 names spread across sectors",
     "I'd wait \u2014 the regime is wrong", or "Pair the longs with shorts on X".

Be direct, opinionated, no disclaimers, no markdown headings, no bullets,
no emoji. If you don't know recent news for a name, say so. Cap at ~150 words."""


@router.post("/explain_strategy")
def explain_strategy(body: StrategyExplainIn, request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"

    model = body.model or _DEFAULT_MODEL
    if model not in _ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail=f"Unsupported model. Allowed: {sorted(_ALLOWED_MODELS)}")

    _check_rate_limit(uid)

    api_key = secrets_db.get_user_key(uid, _PROVIDER)
    if not api_key:
        raise HTTPException(status_code=412, detail="No Anthropic key on file. Add one in Account \u2192 API Keys.")

    lines = [
        f"Strategy: {body.strategy_name} (key={body.strategy})",
        f"Thesis: {body.thesis}",
        "",
        "Top picks (ranked by the strategy's own metric, not just composite-z):",
    ]
    for p in body.picks:
        cz = f"z={p.composite_z:+.2f}" if p.composite_z is not None else "z=n/a"
        lines.append(f"  - {p.ticker.upper()} ({p.sector or 'Unknown'}) \u2014 {cz}, signal={p.signal or 'n/a'}")
    lines.append("\nGive me your read.")
    user_msg = "\n".join(lines)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        result = client.messages.create(
            model=model,
            max_tokens=500,
            system=_STRATEGY_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as exc:
        msg = str(exc)
        if "401" in msg or "authentication" in msg.lower():
            raise HTTPException(status_code=401, detail="Anthropic rejected the stored key. Update it in Account \u2192 API Keys.")
        if "429" in msg or "rate" in msg.lower():
            raise HTTPException(status_code=429, detail="Anthropic rate limit on your account. Try again shortly.")
        raise HTTPException(status_code=502, detail="Anthropic call failed. Try again in a moment.")
    finally:
        api_key = None

    answer = "".join(getattr(b, "text", "") or "" for b in (result.content or [])).strip() or "(no response)"
    return {"model": model, "answer": answer}


# ---- multiplier explainer -----------------------------------------------
# One Haiku call gives a paragraph-level read on a Monte Carlo result.

class MultiplierExplainIn(BaseModel):
    ticker: str = Field(..., max_length=10)
    target_multiple: float
    horizon_days: int
    prob_reached: Optional[float] = None
    p50_days: Optional[float] = None
    p25_days: Optional[float] = None
    p75_days: Optional[float] = None
    median_max_drawdown: Optional[float] = None
    worst_decile_drawdown: Optional[float] = None
    annual_return_est: Optional[float] = None
    annual_vol_est: Optional[float] = None
    mode: Optional[str] = None
    raw_annual_return: Optional[float] = None
    raw_annual_vol: Optional[float] = None
    warnings: Optional[list[str]] = None
    model: Optional[str] = None


_MULT_SYSTEM = """You are Quant, the in-app trading copilot.
The user just ran a bootstrap Monte Carlo simulation on a single ticker
to estimate how long it would take to reach a target multiple of capital.

Three simulation modes exist:
  - 'naive'   : raw bootstrap from the recent window (regime-fitted, dangerous on hot names)
  - 'shrunk'  : pulls the daily mean toward a ~10%/yr prior so explosive windows get penalized
  - 'blended' : mixes the recent window with up to 5y of history so one regime can't dominate

In 4-6 short sentences, plain prose, give them:
  1) Compare the raw recent regime against the effective input. If raw is
     >50%/yr, name it as a likely catalyst window (sector cycle, M&A, AI/HBM
     boom, COVID rebound, etc.) and say bootstrap will mechanically extrapolate
     it forward unless shrinkage/blending is used.
  2) Honest read on the resulting probability and timeline given the chosen
     mode. If still on naive mode with a hot regime, tell them to switch.
  3) The risk side: median drawdown vs typical retail pain threshold (~25-30%).
  4) One concrete next step: position-size guidance ("don't put more than X%
     of net worth in this"), or a regime check to wait for.

Be direct, opinionated, no disclaimers, no markdown headings, no bullets,
no emoji. Cap at ~160 words."""


@router.post("/explain_multiplier")
def explain_multiplier(body: MultiplierExplainIn, request: Request, response: Response) -> dict:
    uid = _require_user(request)
    response.headers["Cache-Control"] = "no-store"

    model = body.model or _DEFAULT_MODEL
    if model not in _ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail=f"Unsupported model. Allowed: {sorted(_ALLOWED_MODELS)}")

    _check_rate_limit(uid)

    api_key = secrets_db.get_user_key(uid, _PROVIDER)
    if not api_key:
        raise HTTPException(status_code=412, detail="No Anthropic key on file. Add one in Account \u2192 API Keys.")

    def fmt_pct(x):
        return f"{x*100:.1f}%" if x is not None else "n/a"

    def fmt_days(x):
        if x is None:
            return "n/a"
        yrs = x / 252.0
        return f"{int(round(x))} days (~{yrs:.1f}y)"

    user_msg = (
        f"Ticker: {body.ticker.upper()}\n"
        f"Target: {body.target_multiple:g}x within {body.horizon_days} trading days "
        f"(~{body.horizon_days/252:.1f}y horizon)\n"
        f"Simulation mode: {body.mode or 'shrunk'}\n"
        f"Raw recent regime (unfiltered): annual_return={fmt_pct(body.raw_annual_return)}, annual_vol={fmt_pct(body.raw_annual_vol)}\n"
        f"Effective input to bootstrap: annual_return={fmt_pct(body.annual_return_est)}, annual_vol={fmt_pct(body.annual_vol_est)}\n"
        f"Probability of reaching target: {fmt_pct(body.prob_reached)}\n"
        f"Days-to-target percentiles: p25={fmt_days(body.p25_days)}, "
        f"p50={fmt_days(body.p50_days)}, p75={fmt_days(body.p75_days)}\n"
        f"Median max-drawdown along the way: {fmt_pct(body.median_max_drawdown)}\n"
        f"Worst-decile max-drawdown: {fmt_pct(body.worst_decile_drawdown)}\n"
    )
    if body.warnings:
        user_msg += "System warnings flagged:\n" + "\n".join(f"- {w}" for w in body.warnings) + "\n"
    user_msg += "\nGive me your read."

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        result = client.messages.create(
            model=model,
            max_tokens=400,
            system=_MULT_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as exc:
        msg = str(exc)
        if "401" in msg or "authentication" in msg.lower():
            raise HTTPException(status_code=401, detail="Anthropic rejected the stored key.")
        if "429" in msg or "rate" in msg.lower():
            raise HTTPException(status_code=429, detail="Anthropic rate limit. Try again shortly.")
        raise HTTPException(status_code=502, detail="Anthropic call failed.")
    finally:
        api_key = None

    answer = "".join(getattr(b, "text", "") or "" for b in (result.content or [])).strip() or "(no response)"
    return {"model": model, "answer": answer}


