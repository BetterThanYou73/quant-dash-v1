import yfinance as yf
import pandas as pd

ticker_df = pd.read_csv("SP500.csv")
ticker_list = ticker_df["Symbol"].tolist()

def fetch_stock_data(ticker=None, period='1y'):
    """
    Fetch historical stock data for the given ticker and period.
    """
    tickers = ticker if ticker is not None else ticker_list

    if isinstance(tickers, str):
        tickers = [tickers]

    tickers = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    if not tickers:
        return pd.DataFrame()

    try:
        data = yf.download(
            tickers=" ".join(tickers),
            period=period,
            group_by="column",
            progress=False,
            auto_adjust=False,
            threads=False,
        )
    except Exception:
        return pd.DataFrame()

    return data if isinstance(data, pd.DataFrame) else pd.DataFrame()


