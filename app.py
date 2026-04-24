import streamlit as st
import data_engine as de
import metrics
import plotly.express as px
import pandas as pd
from numbers import Real

st.set_page_config(layout="wide", page_title="Quantitative Dashboard")
st.title("S&P 500 Quant Tracker")


# initializing a sessions state for custom tickers
if "custom_tickers" not in st.session_state:
    st.session_state.custom_tickers = []

# builds the sidebar for the user to enter a stock of their choice
st.sidebar.header("Control")
ticker_input = st.sidebar.text_input("Add a Stock (e.g, SPY, JPM) :").upper()

if st.sidebar.button("Add to Dasboard"):
    if ticker_input and ticker_input not in st.session_state.custom_tickers:
        st.session_state.custom_tickers.append(ticker_input)
        st.rerun() # refresh the data to reflect new stocks

@st.cache_data(ttl=3600) # cache the data for 1 hour to avoid redundant API calls
def load_market_data(custom_list) -> pd.DataFrame:
    # Fetching some of the most important stocks in the S&P for analysis
    # Can be customized to include other stocks from the list
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', "INTC", "AMD", "NVDA", "TSLA"]

    all_tickers = list(set(tickers + custom_list)) # combines both custom and curated list
    fetched = de.fetch_stock_data(all_tickers)
    if fetched is None:
        return pd.DataFrame()
    return fetched


data = load_market_data(st.session_state.custom_tickers)

# extracting closing prices and calulating returns
if data.empty:
    st.error("No valid market data returned. Please try again later.")
    st.stop()

if isinstance(data.columns, pd.MultiIndex):
    level0 = set(data.columns.get_level_values(0))
    level1 = set(data.columns.get_level_values(1))

    if "Close" in level0:
        # group_by='column' -> first level contains Open/High/Low/Close/Volume
        close_prices = data["Close"]
    elif "Close" in level1:
        # group_by='ticker' -> second level contains Open/High/Low/Close/Volume
        close_prices = data.xs("Close", axis=1, level=1)
    else:
        st.error("Downloaded data does not contain close prices.")
        st.stop()
else:
    if "Close" not in data.columns:
        st.error("Downloaded data does not contain close prices.")
        st.stop()
    close_prices = data["Close"]

if isinstance(close_prices, pd.Series):
    close_prices = close_prices.to_frame()

# Market Overview and Risk metrics
st.subheader("Market Overview & Risk Metrics")

summary_data = []
skipped_tickers = []
for col in close_prices:
    stock_series = close_prices[col].dropna()

    # Skip tickers that have no usable history (common for invalid/delisted symbols)
    if stock_series.empty or len(stock_series) < 2:
        skipped_tickers.append(col)
        continue

    # calculates the returns, volatility, and skewness for each stock using the metrics module
    returns_series, vol_series, skewness = metrics.calculate_metrics(stock_series)

    if returns_series.empty:
        skipped_tickers.append(col)
        continue

    # gets us the most recent price, return, and volatility for the stock
    latest_price = stock_series.iloc[-1]
    daily_return = returns_series.iloc[-1]
    latest_volatility = vol_series.iloc[-1] 
    # vol series is a rolling window and this ets us the last one

    volatility_value = (
        float(latest_volatility)
        if pd.notna(latest_volatility) and isinstance(latest_volatility, Real)
        else float("nan")
    )
    skewness_value = (
        float(skewness)
        if pd.notna(skewness) and isinstance(skewness, Real)
        else float("nan")
    )

    summary_data.append({
        "Ticker": col,
        "Price": f"${latest_price:.2f}",
        "Daily Return Numeric": float(daily_return),
        "Daily Return": f"{daily_return:.2%}",
        "20d Volatility": round(volatility_value, 3),
        "20d Skewness": round(skewness_value, 3)
        })

if not summary_data:
    st.error("No valid ticker data available for metric calculation.")
    if skipped_tickers:
        st.info(f"Skipped tickers with insufficient data: {', '.join(sorted(set(skipped_tickers)))}")
    st.stop()

# displaying as an interactive table
summary_df = pd.DataFrame(summary_data).sort_values(by="Daily Return Numeric", ascending=False)
st.dataframe(summary_df.drop(columns=["Daily Return Numeric"]), use_container_width=True)

if skipped_tickers:
    st.warning(
        "Skipped tickers with insufficient/invalid data: "
        + ", ".join(sorted(set(skipped_tickers)))
    )


st.divider()



# creating correlation matrix
st.subheader("Correlation Matrix")
returns_df = close_prices.pct_change(fill_method=None)
returns_df = returns_df.dropna(axis=1, how="all").dropna(how="all")

if returns_df.empty:
    st.info("Not enough valid return history to compute a correlation matrix.")
    st.stop()

if returns_df.shape[1] == 1:
    corr_matrix = pd.DataFrame([[1.0]], index=returns_df.columns, columns=returns_df.columns)
else:
    corr_matrix = returns_df.corr()

fig = px.imshow(
    corr_matrix, 
    text_auto=True, 
    aspect="auto", 
    color_continuous_scale='RdYlGn'
)
fig.update_traces(texttemplate="%{z:.2f}")
st.plotly_chart(fig)
