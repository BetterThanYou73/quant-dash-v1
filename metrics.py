import numpy as np
import scipy.stats as stats

def calculate_metrics(price_series):

    returns = price_series.pct_change().dropna() # converts raw prices into percentages changes

    # 20-day rolling volatility (annualized)
    # it is a measure of how much the stock price fluctuates over time
    # higher volatility means more risk and lower means less
    volatility = returns.rolling(window=20).std() * np.sqrt(252)

    # current skewness over the last 20 days using scipy.stats
    # finding asymmetry of the return distribution
    current_skewness = returns.tail(20).skew()

    return returns, volatility, current_skewness