# Quant Dash v1

A personal Python + Streamlit quantitative dashboard for market analysis, watchlisting, and stock idea generation.

## Features
- Pulls market data via `yfinance`
- Displays a market overview table (price, daily return, rolling volatility, skewness)
- Supports custom tickers from the sidebar
- Visualizes cross-ticker return correlations with a heatmap

## Project Structure
- `app.py` — Streamlit app entrypoint
- `data_engine.py` — data fetching and preparation
- `metrics.py` — return/risk metric calculations
- `requirements.txt` — pip dependencies

## Quick Start
1. Create and activate a virtual environment
2. Install dependencies
3. Run the app

```bash
python -m venv venv
# Windows PowerShell
venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

## Notes
- This is an educational/personal project focused on quant exploration workflows.
- Data quality and API availability depend on third-party sources.

## Important Disclaimer
This project is **not financial advice**. Use it for research and education only. Any investment decision is your responsibility.

## License
Licensed under the MIT License. See [LICENSE](./LICENSE).
