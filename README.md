# Quant Dash

A personal Python quantitative dashboard for exploring stock metrics, pair trades, and risk diagnostics on a custom universe of tickers.

> **Status:** v1 (Streamlit) is functional. A v2 revamp is in progress that replaces the Streamlit UI with a FastAPI backend and a static web frontend. See [Roadmap](#roadmap).

## Features (v1)
- **Market Overview** — ranked signals across the selected universe with a composite profitability score
- **Pairs Trading** — hedge ratio, spread, rolling z-score, and prescriptive entry/exit signals
- **Risk** — horizon-aware correlation heatmap and rolling pair correlation
- Sector-aware filtering driven by `SP500.csv`
- Cache-first data flow: the UI reads a local pickle cache instead of hitting Yahoo on every interaction
- Optional background worker that refreshes the cache on an interval

## Project Structure
```
app.py            Streamlit app entrypoint (v1 UI)
data_engine.py    yfinance fetch + local cache load/save + ticker metadata
metrics.py        Returns, volatility, skew, VaR/CVaR, hedge ratio, spread, z-score, pair signal
data_worker.py    Background process that periodically refreshes the cache
SP500.csv         Static ticker universe with sector metadata
requirements.txt  Pip dependencies
```

## Quick Start
```powershell
# 1. Create and activate a virtual environment (Windows PowerShell)
python -m venv venv
venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. (Optional) Run the cache worker in a separate terminal
python data_worker.py --interval 60 --period 1y

# 4. Run the dashboard
streamlit run app.py
```

If you skip step 3, you can refresh data manually from the sidebar via **Refresh Local Cache Now**.

## Concepts at a glance
- **Returns** — daily percent change of close prices
- **Volatility** — 20-day rolling standard deviation of returns, annualized by √252
- **Skewness** — asymmetry of the return distribution (negative = fat left tail)
- **VaR (5%) / CVaR (5%)** — typical and average loss on the worst 5% of days
- **Pairs trading** — when `spread = A − β·B` strays far from its mean (high |z|), bet on mean reversion
- **Profitability Score** — weighted percentile rank across momentum, volatility, skew, tail risk, drawdown, and hit rate

## Roadmap
- **Phase 1 (in progress):** replace Streamlit with a FastAPI backend serving JSON, and a static HTML/CSS/JS frontend hostable on GitHub Pages
- **Phase 2:** expose all dashboard data via a versioned REST API and pipe it into an LLM advisor (Claude / GPT / Gemini) that incorporates external signals (news, macro, weather, etc.)

## Hosting (Phase 1)

The backend (FastAPI/uvicorn) and the static frontend are deployed separately:

- **Frontend** (`web/`) → any static host: GitHub Pages, Cloudflare Pages, Netlify. No build step. Update `web/config.js` so `API_BASE` points at the deployed backend URL.
- **Backend** (`backend/` + `core/`) → a small Python container on Render / Fly.io / Railway. The cache pickle is rebuilt by `core/workers.py`; on free tiers run it as a scheduled job (e.g. daily). For dev: `uvicorn backend.main:app --reload`. For prod: `uvicorn backend.main:app --host 0.0.0.0 --port $PORT --workers 2`.
- **CORS** is currently `*` (read-only, no auth). When Phase 2 ships auth, lock origins to the deployed frontend domain.
- **GZip middleware** (`fastapi.middleware.gzip`) is enabled with a 1 KB threshold — JSON payloads compress ~75-85%, which matters on free-tier bandwidth caps.
- **Frontend refresh scheduler** (`web/app.js`) batches all polling into one heartbeat, runs single-flight per task, and pauses when the browser tab is hidden — drops idle API calls to zero.
- **Health probe**: `GET /api/health` returns 200 when the process is up; point the host's liveness check there.

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
