"""
Daily portfolio digest — HTML email builder.

Architecture:
    Pure renderer. Takes a user_id, asks the existing pipeline for the
    same data the dashboard shows, and returns (subject, html, text).
    No I/O of its own except calling Anthropic (optional, gated by user
    pref + presence of their BYOK key).

What's in the email:
    1. Today's portfolio P&L line (value, day change $/%, YTD)
    2. Top + bottom movers from THEIR holdings
    3. Watchlist alerts (same logic as the in-app banner)
    4. Top-5 market movers (SP500 universe), with one-line takes
    5. AI's read of the day (optional, costs ~1-2 cents/day on Haiku)

Why HTML inline-styled (no external CSS):
    Email clients strip <style> blocks aggressively (especially Gmail
    on mobile). Inline styles are the only reliable styling. We keep
    the palette dark to match the app and accept that Outlook will
    butcher it slightly — the data still reads.
"""

from __future__ import annotations

import html
import os
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from core import data_engine as de
from core import portfolio_db as pdb
from core import secrets_db
from core import signals as sig


_BENCHMARK = "SPY"
_AI_PROVIDER = "anthropic"
_AI_MODEL = "claude-haiku-4-5"
_AI_MAX_TOKENS = 360


# ---- helpers -------------------------------------------------------------

def _fmt_money(v: Optional[float]) -> str:
    if v is None:
        return "—"
    sign = "-" if v < 0 else ""
    n = abs(v)
    if n >= 1e6:
        return f"{sign}${n/1e6:,.2f}M"
    return f"{sign}${n:,.2f}"


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v*100:+.2f}%"


def _color_for(v: Optional[float]) -> str:
    if v is None or v == 0:
        return "#9aa4b2"
    return "#22c55e" if v > 0 else "#ef4444"


# ---- top movers across the universe --------------------------------------

def _top_market_movers(close_all: pd.DataFrame, n: int = 5) -> dict:
    """Return {gainers: [...], losers: [...]} from latest 1-day return.

    Filters out the benchmark itself and rows missing a previous close.
    Uses the SAME cached prices as the dashboard so numbers reconcile.
    """
    out = {"gainers": [], "losers": []}
    if close_all is None or close_all.empty or close_all.shape[0] < 2:
        return out

    last2 = close_all.tail(2).copy()
    if last2.shape[0] < 2:
        return out
    prev = last2.iloc[0]
    cur = last2.iloc[1]
    rets = (cur / prev) - 1.0
    rets = rets.dropna()
    if _BENCHMARK in rets.index:
        rets = rets.drop(_BENCHMARK)
    if rets.empty:
        return out

    meta = de.get_ticker_metadata()
    name_map = meta.set_index("Symbol")["Name"].to_dict() if "Name" in meta.columns else {}
    sector_map = meta.set_index("Symbol")["Sector"].to_dict() if "Sector" in meta.columns else {}

    top = rets.nlargest(n)
    bot = rets.nsmallest(n)

    def _row(sym, ret):
        return {
            "ticker": sym,
            "name": name_map.get(sym, sym),
            "sector": sector_map.get(sym, "Unknown"),
            "price": float(cur[sym]) if sym in cur.index else None,
            "ret": float(ret),
        }

    out["gainers"] = [_row(s, r) for s, r in top.items()]
    out["losers"] = [_row(s, r) for s, r in bot.items()]
    return out


# ---- alerts (mirror of the in-app banner logic) --------------------------

def _build_alerts(analytics: dict) -> list[dict]:
    """Same triggers as the dashboard banner, computed server-side so the
    email and UI agree."""
    alerts: list[dict] = []
    totals = analytics.get("totals") or {}
    positions = analytics.get("positions") or []

    value = totals.get("value") or 0.0
    day_change = totals.get("day_change") or 0.0
    if value > 0:
        day_pct = day_change / value
        if abs(day_pct) >= 0.02:
            tone = "bad" if day_pct < 0 else "good"
            alerts.append({
                "tone": tone,
                "text": f"Portfolio moved {_fmt_pct(day_pct)} today ({_fmt_money(day_change)}).",
            })

    sharpe = totals.get("sharpe_ratio")
    if sharpe is not None and sharpe < 0:
        alerts.append({"tone": "warn", "text": f"Basket Sharpe is negative ({sharpe:.2f}). Risk-adjusted return underwater."})

    mdd = totals.get("max_drawdown")
    if mdd is not None and mdd <= -0.25:
        alerts.append({"tone": "warn", "text": f"Simulated 1y max drawdown ≤ {_fmt_pct(mdd)} on current weights."})

    for p in positions:
        wt = p.get("weight")
        upl_pct = p.get("unrealized_pl_pct")
        sym = p.get("ticker")
        if wt is not None and wt >= 0.30:
            alerts.append({"tone": "warn", "text": f"{sym} is {wt*100:.0f}% of book — concentration risk."})
        if upl_pct is not None and upl_pct <= -0.15:
            alerts.append({"tone": "bad", "text": f"{sym} down {_fmt_pct(upl_pct)} from cost. Decide: thesis or cut."})
        if upl_pct is not None and upl_pct >= 0.50:
            alerts.append({"tone": "good", "text": f"{sym} up {_fmt_pct(upl_pct)} from cost. Consider trimming."})

    return alerts


# ---- AI commentary -------------------------------------------------------

_AI_SYSTEM = """You write the 'Quant's Read' section at the bottom of a
daily portfolio email for a sophisticated retail investor.

Be direct, opinionated, and concrete. 4-6 short sentences total.
Cover: (1) one sentence on the day's portfolio move and what drove it,
(2) one sentence on the most interesting holding-level move,
(3) one tactical thought for tomorrow (e.g. position to watch, sector
rotating, an alert worth acting on).

No greetings, no sign-offs, no markdown, no bullet points, no emoji.
Plain text only. Never invent prices or news — only use the data given."""


def _ai_commentary(user_id: int, context_text: str) -> Optional[str]:
    """Call Claude with the user's BYOK key. Returns plain text or None
    if no key, no SDK, or the API fails. Never raises."""
    api_key = secrets_db.get_user_key(user_id, _AI_PROVIDER)
    if not api_key:
        return None
    try:
        import anthropic
    except Exception:
        return None
    try:
        client = anthropic.Anthropic(api_key=api_key, timeout=15.0)
        resp = client.messages.create(
            model=_AI_MODEL,
            max_tokens=_AI_MAX_TOKENS,
            system=_AI_SYSTEM,
            messages=[{"role": "user", "content": context_text}],
        )
        parts = []
        for block in resp.content or []:
            if getattr(block, "type", None) == "text":
                parts.append(block.text)
        text = "".join(parts).strip()
        return text or None
    except Exception:
        return None


# ---- HTML rendering ------------------------------------------------------

def _section(title: str, body_html: str) -> str:
    return (
        f'<div style="margin:24px 0;">'
        f'<div style="font-size:11px;letter-spacing:1.5px;color:#6b7280;'
        f'text-transform:uppercase;margin-bottom:8px;">{html.escape(title)}</div>'
        f'{body_html}</div>'
    )


def _movers_table(rows: list[dict]) -> str:
    if not rows:
        return '<div style="color:#9aa4b2;font-size:13px;">No data.</div>'
    cells = []
    for r in rows:
        ret = r.get("ret")
        color = _color_for(ret)
        sym = html.escape(str(r.get("ticker") or ""))
        name = html.escape(str(r.get("name") or "")[:32])
        sector = html.escape(str(r.get("sector") or ""))
        price = _fmt_money(r.get("price"))
        cells.append(
            f'<tr>'
            f'<td style="padding:6px 10px 6px 0;font-weight:600;color:#e6edf7;">{sym}</td>'
            f'<td style="padding:6px 10px 6px 0;color:#9aa4b2;font-size:12px;">{name}</td>'
            f'<td style="padding:6px 10px 6px 0;color:#6b7280;font-size:12px;">{sector}</td>'
            f'<td style="padding:6px 0;text-align:right;color:#e6edf7;">{price}</td>'
            f'<td style="padding:6px 0 6px 12px;text-align:right;font-weight:600;color:{color};">{_fmt_pct(ret)}</td>'
            f'</tr>'
        )
    return f'<table style="width:100%;border-collapse:collapse;font-size:13px;">{"".join(cells)}</table>'


def _holdings_table(positions: list[dict], limit: int = 10) -> str:
    if not positions:
        return '<div style="color:#9aa4b2;font-size:13px;">No holdings.</div>'
    sorted_p = sorted(
        [p for p in positions if p.get("value") is not None],
        key=lambda p: -(p.get("value") or 0),
    )[:limit]
    cells = []
    for p in sorted_p:
        sym = html.escape(p.get("ticker") or "")
        wt = p.get("weight")
        wt_text = f"{wt*100:.1f}%" if wt is not None else "—"
        day = p.get("day_change_pct")
        day_color = _color_for(day)
        upl = p.get("unrealized_pl_pct")
        upl_color = _color_for(upl)
        value = _fmt_money(p.get("value"))
        cells.append(
            f'<tr>'
            f'<td style="padding:6px 10px 6px 0;font-weight:600;color:#e6edf7;">{sym}</td>'
            f'<td style="padding:6px 0;text-align:right;color:#e6edf7;">{value}</td>'
            f'<td style="padding:6px 0 6px 12px;text-align:right;color:#9aa4b2;">{wt_text}</td>'
            f'<td style="padding:6px 0 6px 12px;text-align:right;color:{day_color};">{_fmt_pct(day)}</td>'
            f'<td style="padding:6px 0 6px 12px;text-align:right;color:{upl_color};">{_fmt_pct(upl)}</td>'
            f'</tr>'
        )
    header = (
        '<tr style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;">'
        '<th style="text-align:left;padding-bottom:6px;">Ticker</th>'
        '<th style="text-align:right;padding-bottom:6px;">Value</th>'
        '<th style="text-align:right;padding-bottom:6px;">Weight</th>'
        '<th style="text-align:right;padding-bottom:6px;">Day</th>'
        '<th style="text-align:right;padding-bottom:6px;">P&amp;L</th>'
        '</tr>'
    )
    return (
        f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
        f'{header}{"".join(cells)}</table>'
    )


def _alerts_block(alerts: list[dict]) -> str:
    if not alerts:
        return '<div style="color:#9aa4b2;font-size:13px;">No alerts. Boring is good.</div>'
    tones = {
        "good": ("#0b3a22", "#22c55e"),
        "warn": ("#3a2f0b", "#f59e0b"),
        "bad":  ("#3a0b14", "#ef4444"),
    }
    items = []
    for a in alerts:
        bg, fg = tones.get(a.get("tone") or "warn", tones["warn"])
        items.append(
            f'<div style="background:{bg};border-left:3px solid {fg};'
            f'padding:8px 12px;margin-bottom:6px;border-radius:4px;'
            f'color:#e6edf7;font-size:13px;">'
            f'{html.escape(a.get("text") or "")}'
            f'</div>'
        )
    return "".join(items)


def _ai_block(text: str) -> str:
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    if not paragraphs:
        paragraphs = [text]
    body = "".join(
        f'<p style="margin:0 0 10px 0;color:#e6edf7;font-size:14px;line-height:1.55;">'
        f'{html.escape(p)}</p>'
        for p in paragraphs
    )
    return (
        f'<div style="background:#1a1f2e;border:1px solid #2a3142;border-radius:6px;'
        f'padding:14px 16px;">{body}</div>'
    )


# ---- main entry ----------------------------------------------------------

def build_digest(user_id: int, user_email: str, display_name: Optional[str], include_ai: bool) -> dict:
    """Returns {subject, html, text, has_data} for one user.

    Never raises; on any failure returns has_data=False with an error
    message in the html body so the cron can still send (and the user
    sees something).
    """
    # Lazy import — avoids a circular import (routes_portfolio imports
    # users_db indirectly via auth in some paths).
    from backend import routes_portfolio as pf_routes

    try:
        positions = pdb.list_positions("user", str(user_id))
    except Exception as e:
        positions = []
        print(f"[digest] list_positions failed for {user_id}: {e}")

    try:
        analytics = pf_routes._compute_analytics(positions) if positions else {
            "totals": {}, "positions": [], "sector_exposure": [],
        }
    except Exception as e:
        print(f"[digest] _compute_analytics failed for {user_id}: {e}")
        analytics = {"totals": {}, "positions": [], "sector_exposure": []}

    totals = analytics.get("totals") or {}
    pos = analytics.get("positions") or []

    # Market data for top movers
    movers = {"gainers": [], "losers": []}
    try:
        df, _ = de.get_market_data()
        if df is not None and not df.empty:
            close_all = sig.extract_close_prices(df)
            movers = _top_market_movers(close_all, n=5)
    except Exception as e:
        print(f"[digest] market movers failed: {e}")

    alerts = _build_alerts(analytics)

    # Subject line: portfolio move if we have one, else market gainer
    value = totals.get("value") or 0.0
    day_change = totals.get("day_change") or 0.0
    day_pct = (day_change / value) if value > 0 else None
    today_str = datetime.now(timezone.utc).strftime("%b %d")
    if day_pct is not None:
        subject = f"Quant Dash · {today_str} · {_fmt_pct(day_pct)} ({_fmt_money(day_change)})"
    elif movers["gainers"]:
        g = movers["gainers"][0]
        subject = f"Quant Dash · {today_str} · {g['ticker']} +{g['ret']*100:.1f}%"
    else:
        subject = f"Quant Dash · {today_str} · daily digest"

    # Optional AI commentary
    ai_text = None
    if include_ai and (positions or movers["gainers"]):
        ctx_lines = []
        if positions:
            ctx_lines.append(f"Portfolio: value {_fmt_money(value)}, day {_fmt_pct(day_pct)} ({_fmt_money(day_change)}), YTD {_fmt_pct(totals.get('ytd_return'))}, weighted beta {totals.get('weighted_beta')}, Sharpe {totals.get('sharpe_ratio')}.")
            ctx_lines.append("Top holdings:")
            for p in sorted([p for p in pos if p.get("value")], key=lambda r: -(r.get("value") or 0))[:8]:
                ctx_lines.append(f"  {p['ticker']} ({p.get('sector') or '?'}): weight {(p.get('weight') or 0)*100:.1f}%, day {_fmt_pct(p.get('day_change_pct'))}, P&L {_fmt_pct(p.get('unrealized_pl_pct'))}")
        if movers["gainers"]:
            ctx_lines.append("Market top gainers:")
            for r in movers["gainers"]:
                ctx_lines.append(f"  {r['ticker']} ({r['sector']}): {_fmt_pct(r['ret'])}")
        if movers["losers"]:
            ctx_lines.append("Market top losers:")
            for r in movers["losers"]:
                ctx_lines.append(f"  {r['ticker']} ({r['sector']}): {_fmt_pct(r['ret'])}")
        if alerts:
            ctx_lines.append("Active alerts:")
            for a in alerts:
                ctx_lines.append(f"  - {a.get('text')}")
        ai_text = _ai_commentary(user_id, "\n".join(ctx_lines))

    # ---- assemble HTML ---------------------------------------------------
    name = html.escape(display_name or user_email.split("@")[0])

    # Header / portfolio summary card
    if positions:
        v_color = _color_for(day_change)
        ytd = totals.get("ytd_return")
        ytd_color = _color_for(ytd)
        summary_html = (
            f'<div style="background:#0e1422;border:1px solid #1d2535;border-radius:8px;'
            f'padding:18px 20px;">'
            f'<div style="font-size:12px;color:#6b7280;letter-spacing:1px;'
            f'text-transform:uppercase;margin-bottom:6px;">Portfolio Value</div>'
            f'<div style="font-size:28px;font-weight:700;color:#e6edf7;">{_fmt_money(value)}</div>'
            f'<div style="margin-top:10px;font-size:14px;color:{v_color};">'
            f'{_fmt_pct(day_pct)} today ({_fmt_money(day_change)})'
            f'</div>'
            f'<div style="margin-top:4px;font-size:13px;color:{ytd_color};">'
            f'{_fmt_pct(ytd)} YTD basket'
            f'</div>'
            f'</div>'
        )
    else:
        summary_html = (
            '<div style="background:#0e1422;border:1px solid #1d2535;border-radius:8px;'
            'padding:18px 20px;color:#9aa4b2;font-size:14px;">'
            'No holdings yet. Add positions in Quant Dash to see your portfolio digest here.'
            '</div>'
        )

    sections = [summary_html]

    if positions:
        sections.append(_section("Holdings", _holdings_table(pos, limit=10)))

    if alerts:
        sections.append(_section("Alerts", _alerts_block(alerts)))

    sections.append(_section("Top market gainers", _movers_table(movers["gainers"])))
    sections.append(_section("Top market losers", _movers_table(movers["losers"])))

    if ai_text:
        sections.append(_section("Quant's read", _ai_block(ai_text)))
    elif include_ai:
        sections.append(_section("Quant's read", (
            '<div style="color:#9aa4b2;font-size:13px;">'
            'AI commentary skipped — add an Anthropic key in Account → API Keys to enable.'
            '</div>'
        )))

    footer = (
        '<div style="margin-top:32px;padding-top:16px;border-top:1px solid #1d2535;'
        'color:#6b7280;font-size:11px;line-height:1.6;">'
        'Quant Dash daily digest · '
        '<a href="https://www.quantdash.tech" style="color:#6b7280;">quantdash.tech</a> · '
        'Manage email preferences in Account.<br>'
        'Not investment advice. Data sourced from public market feeds; figures may be delayed or revised.'
        '</div>'
    )

    body_html = (
        f'<!doctype html><html><body style="margin:0;padding:0;background:#0a0e1a;'
        f'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="background:#0a0e1a;padding:32px 16px;">'
        f'<tr><td align="center">'
        f'<table role="presentation" width="600" cellpadding="0" cellspacing="0" '
        f'style="max-width:600px;background:#0a0e1a;color:#e6edf7;">'
        f'<tr><td>'
        f'<div style="font-size:13px;color:#9aa4b2;margin-bottom:4px;">Hi {name},</div>'
        f'<div style="font-size:18px;color:#e6edf7;margin-bottom:16px;">'
        f'Here\'s your market wrap for {today_str}.</div>'
        f'{"".join(sections)}'
        f'{footer}'
        f'</td></tr>'
        f'</table>'
        f'</td></tr></table>'
        f'</body></html>'
    )

    # Plain-text fallback (Resend will use this if html fails to render)
    text_lines = [f"Quant Dash daily digest — {today_str}", ""]
    if positions:
        text_lines += [
            f"Portfolio: {_fmt_money(value)}",
            f"Day: {_fmt_pct(day_pct)} ({_fmt_money(day_change)})",
            f"YTD basket: {_fmt_pct(totals.get('ytd_return'))}",
            "",
        ]
    if alerts:
        text_lines.append("Alerts:")
        for a in alerts:
            text_lines.append(f"  - {a.get('text')}")
        text_lines.append("")
    if movers["gainers"]:
        text_lines.append("Top market gainers:")
        for r in movers["gainers"]:
            text_lines.append(f"  {r['ticker']}: {_fmt_pct(r['ret'])}")
        text_lines.append("")
    if ai_text:
        text_lines.append("Quant's read:")
        text_lines.append(ai_text)
    text_body = "\n".join(text_lines)

    return {
        "subject": subject,
        "html": body_html,
        "text": text_body,
        "has_data": bool(positions) or bool(movers["gainers"]),
        "ai_used": bool(ai_text),
        "alerts_count": len(alerts),
    }
