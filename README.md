# Quant Dash

A quantitative dashboard for exploring stock metrics, pair trades, signals,
risk, sector regimes, macro factors, and news on a custom universe of
tickers.

> **Status:** v2 is live. The Streamlit UI from v1 has been replaced
> with a FastAPI backend serving JSON to a static HTML/CSS/JS frontend.
> The legacy Streamlit app still runs from `legacy/` for reference. See [Roadmap](#roadmap).

## Features
- **Market Overview** — ranked composite-Z signals (momentum / volatility / skew / tail / drawdown / hit-rate) across the full S&P 500
- **Sector heatmap** — 24 GICS sector groupings with composite-Z averages
- **Regime + Volatility** card — trend / vol / breadth indicators with a regime label
- **Macro Factors** card — SPY/QQQ/VIX/DXY/TLT/GLD/USO/HYG/LQD with daily Δ
- **News** card — keyword-filtered Yahoo headlines per active ticker
- **Pairs Trading** — hedge ratio, spread, rolling z-score, prescriptive entry/exit signals
- **Correlation heatmap** — horizon-aware, themed scrollbars
- **Screener** modal — filter the universe by composite-Z / signal / sector
- **Portfolio** modal — weighted-avg cost basis, batch quote valuation (Phase 1 storage = `localStorage`; Phase 2 = Postgres)
- **Refresh Scheduler** in the frontend — single-flight heartbeat, page-visibility-aware, exponential backoff
- **Cache-first data flow** — the API never hits Yahoo at request time; the worker writes a pickle (Postgres BYTEA on Heroku, file locally)

## Project Structure
```
backend/                FastAPI app + per-card route modules
  main.py               app entrypoint, CORS, GZip, StaticFiles mount
  routes_*.py           one router per dashboard card / endpoint
core/                   pure-Python data + math layer (no web deps)
  data_engine.py        yfinance fetch, cache load/save (file or Postgres)
  metrics.py            returns, vol, skew, VaR/CVaR, hedge ratio, z-score
  signals.py            composite-Z signal model
  factors.py            macro / sector factor builders
  workers.py            cron-style cache refresher with --task=daily/intraday
web/                    static frontend (no build step)
  index.html            single-page dashboard
  app.js                vanilla JS, Chart.js via CDN
  styles.css            theme tokens + global themed scrollbars
  config.js             auto-detects dev vs production API base
data/SP500.csv          static ticker universe with sector metadata
legacy/                 v1 Streamlit app (read-only reference)
Procfile, app.json, runtime.txt   Heroku deploy manifests
requirements.txt        pip dependencies
HOSTING.md              full Heroku deploy walkthrough
```

## Quick Start (local)
```powershell
# 1. Virtual env
python -m venv venv
.\venv\Scripts\Activate.ps1

# 2. Dependencies
pip install -r requirements.txt

# 3. Fill the cache once (5–8 min, full S&P 500 from Yahoo)
python -m core.workers --once --task=daily

# 4. Run the API + frontend on one port
uvicorn backend.main:app --reload
# → open http://127.0.0.1:8000
```

That's it — the FastAPI app mounts `web/` at `/` so the dashboard, the
API (`/api/*`), and the OpenAPI explorer (`/docs`) all live on the same
origin. No CORS, no second server, no `config.js` editing.

If you prefer a separate static server (handy for cache-busting iteration):
```powershell
python -m http.server 5500 --directory web
# config.js auto-detects this and points at http://127.0.0.1:8000
```

## Concepts at a glance
- **Returns** — daily percent change of close prices
- **Volatility** — 20-day rolling standard deviation of returns, annualized by √252
- **Skewness** — asymmetry of the return distribution (negative = fat left tail)
- **VaR (5%) / CVaR (5%)** — typical and average loss on the worst 5% of days
- **Pairs trading** — when `spread = A − β·B` strays far from its mean (high |z|), bet on mean reversion
- **Profitability Score** — weighted percentile rank across momentum, volatility, skew, tail risk, drawdown, and hit rate

## Roadmap
- **Phase 1 (shipped):** FastAPI backend + static frontend, deployed to Heroku at [quantdash.tech](https://quantdash.tech) with same-origin StaticFiles, Postgres BYTEA cache, and Heroku Scheduler cron jobs.
- **Phase 2 (next):** GitHub OAuth, persistent portfolios in Postgres, dedicated `/portfolio` page (allocation donut, equity curve, suggestions tab driven by the screener), optional broker import (Plaid / SnapTrade / CSV).
- **Phase 3:** versioned REST API consumable by an LLM advisor that incorporates external signals (news, macro, weather, alt data).

## Hosting

Production lives on Heroku at **[quantdash.tech](https://quantdash.tech)**. Full deploy walkthrough in [HOSTING.md](HOSTING.md).

The short version:
- One Heroku app serves both the API and the frontend (same origin → no CORS).
- Heroku Postgres `essential-0` stores the worker's pickle as a single BYTEA row — dyno filesystems are ephemeral, so writing to disk doesn't survive restarts.
- Heroku Scheduler runs `python -m core.workers --once --task=daily` after the US close (22:00 UTC) and `--task=intraday` hourly for macro/regime tickers.
- Custom domain via Let's Encrypt (Heroku Automated Certs).
- Cost: $0 with GitHub Student Pack credits.
- Health probe: `GET /api/health`.

## Phase 2 Plan — Postgres + Portfolio Accounts

The localStorage portfolio in Phase 1 is a deliberate placeholder. Phase 2 replaces it with a real backend so positions sync across devices and feed the LLM advisor.

**Stack additions**
- **PostgreSQL** for relational data (users, portfolios, positions, snapshots, watchlists). Hosted on Supabase / Neon / Railway free tier.
- **SQLAlchemy 2.x + Alembic** for ORM and migrations.
- **Auth**: GitHub OAuth via `authlib`, JWT session cookies. No passwords stored.

**Schema sketch**
```
users           (id, email, github_id, created_at)
portfolios      (id, user_id, name, base_ccy, created_at)
positions       (id, portfolio_id, ticker, shares, avg_cost, opened_at)
transactions    (id, position_id, kind {buy,sell,div}, qty, price, ts)
watchlists      (id, user_id, name, symbols jsonb)
snapshots       (id, portfolio_id, ts, market_value, cost_basis, day_pnl)  -- daily MV history
```

**Migration path from Phase 1**
- Both `localStorage` keys (`qd.watchlist.v1`, `qd.portfolio.v1`) keep their `v1` suffix on purpose. On first sign-in, the frontend POSTs the local payload to `/api/portfolio/import` and the server writes initial rows. The `v1` data is left in place until the import succeeds, then cleared.

**New API surface (proposed)**
```
GET    /api/me                         → current user
GET    /api/portfolios                 → list user's portfolios
POST   /api/portfolios                 → create
GET    /api/portfolios/{id}            → positions + summary (server-side valuation)
POST   /api/portfolios/{id}/positions  → add/edit
DELETE /api/portfolios/{id}/positions/{ticker}
GET    /api/portfolios/{id}/history    → daily snapshots for charting
```


## Disclaimer
This project is **not financial advice**. It is for research and education only. Any investment decision is your responsibility.

## License
Proprietary — all rights reserved. See [LICENSE](./LICENSE).
