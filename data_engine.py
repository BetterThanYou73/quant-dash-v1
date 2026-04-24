import yfinance as yf
import pandas as pd

ticker_df = pd.read_csv("SP500.csv")
ticker_list = ticker_df["Symbol"].tolist()

def fetch_stock_data(ticker=ticker_list, period='1y'):
    """
    Fetch historical stock data for the given ticker and period.
    """
    data = yf.download(" ".join(ticker), period = period, group_by='ticker')
    return data


