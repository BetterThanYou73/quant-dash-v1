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

## Disclaimer
This project is **not financial advice**. It is for research and education only. Any investment decision is your responsibility.

## License
Proprietary — all rights reserved. See [LICENSE](./LICENSE).
