# Quant Dash v2

A personal Python + Streamlit quantitative dashboard for market analysis, watchlisting, and stock idea generation.

## Features
- Tabbed information architecture:
	- `Market Overview`: ranked signals and quick diagnostics
	- `Pairs Trading`: spread, rolling z-score, and prescriptive pair signals
	- `Risk`: horizon-aware correlation and rolling pair correlation
- Sector-aware filtering for apples-to-apples comparison
- Cache-first data flow (UI reads local cache)
- Optional background worker that refreshes market cache every 60 seconds

## Project Structure
- `app.py` — Streamlit app entrypoint
- `data_engine.py` — data fetching, local cache load/save, and metadata helpers
- `metrics.py` — return/risk metrics plus pair trading math (hedge ratio, spread, z-score, signal)
- `data_worker.py` — background cache refresher process
- `requirements.txt` — pip dependencies

## Quick Start
1. Create and activate a virtual environment
2. Install dependencies
3. Run the background data worker (optional but recommended)
4. Run the app

```bash
python -m venv venv
# Windows PowerShell
venv\Scripts\Activate.ps1
pip install -r requirements.txt
python data_worker.py --interval 60 --period 1y
streamlit run app.py
```

If you do not run the worker, you can still refresh data from the app sidebar using `Refresh Local Cache Now`.

## Notes
- This is an educational/personal project focused on quant exploration workflows.
- Data quality and API availability depend on third-party sources.

## Important Disclaimer
This project is **not financial advice**. Use it for research and education only. Any investment decision is your responsibility.

## License
This project is proprietary and all rights are reserved. See [LICENSE](./LICENSE).
