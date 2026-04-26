"""
Refresh data/SP500.csv from Wikipedia's canonical constituents table.

Run manually whenever the index changes (rare — a few times per year):
    python -m scripts.refresh_sp500_csv

Why Wikipedia: it's the de-facto public source for index membership,
updated within hours of any addition/removal. The official S&P list is
behind a paywall.

Why pandas.read_html: the page has a stable table with id="constituents".
pandas + lxml parses it without needing BeautifulSoup or a scraper.
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
OUT_PATH = Path(__file__).resolve().parents[1] / "data" / "SP500.csv"

# Wikipedia returns 403 to the default Python urllib UA.
# A normal browser-style UA is fine and conforms to their bot policy.
_UA = "Mozilla/5.0 (compatible; quant-dash-v1/1.0; +https://github.com/)"


def fetch_constituents() -> pd.DataFrame:
    """Scrape the constituents table and normalize columns + tickers."""
    # Fetch the HTML ourselves so we can set a UA header. Pass the result
    # to read_html via a StringIO buffer.
    req = Request(WIKI_URL, headers={"User-Agent": _UA})
    with urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    # read_html returns a list of all tables on the page; the first one
    # (id="constituents") is the current S&P 500 membership list.
    tables = pd.read_html(StringIO(html), attrs={"id": "constituents"})
    if not tables:
        raise RuntimeError("Wikipedia page did not contain a 'constituents' table")
    df = tables[0]

    # Wikipedia column names drift over the years. Normalize what we need.
    # Current columns (as of 2025): "Symbol", "Security", "GICS Sector", ...
    rename_map = {
        "Symbol": "Symbol",
        "Security": "Name",
        "GICS Sector": "Sector",
    }
    missing = [c for c in rename_map if c not in df.columns]
    if missing:
        raise RuntimeError(f"Expected Wikipedia columns missing: {missing}. Got: {list(df.columns)}")

    out = df[list(rename_map)].rename(columns=rename_map).copy()

    # yfinance uses '-' for class shares (BRK-B), not the dot form (BRK.B)
    # that Wikipedia and most index providers display. Convert to be safe.
    out["Symbol"] = (
        out["Symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(".", "-", regex=False)
    )
    out["Name"] = out["Name"].astype(str).str.strip()
    out["Sector"] = out["Sector"].astype(str).str.strip()

    # Drop any junk rows (duplicates or missing symbol)
    out = out.dropna(subset=["Symbol"]).drop_duplicates(subset=["Symbol"])
    out = out.sort_values("Symbol").reset_index(drop=True)
    return out


def main() -> None:
    print(f"[refresh] fetching {WIKI_URL}")
    df = fetch_constituents()
    print(f"[refresh] parsed {len(df)} constituents")

    # Backup the existing file in case the refresh produced something odd.
    if OUT_PATH.exists():
        backup = OUT_PATH.with_suffix(".csv.bak")
        OUT_PATH.replace(backup)
        print(f"[refresh] previous CSV backed up → {backup.name}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"[refresh] wrote {OUT_PATH} ({len(df)} rows)")
    print(f"[refresh] sample:\n{df.head(5).to_string(index=False)}")


if __name__ == "__main__":
    main()
