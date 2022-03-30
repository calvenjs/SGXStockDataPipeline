from google.cloud import bigquery
import pandas as pd
import yfinance as yf
import os
import numpy as np

# Derived Columns Functions - Transformations
'''
def MACD(DF, slow, fast, smooth):
    exp1 = DF["Close"].ewm(span = fast, adjust = False).mean()
    exp2 = DF["Close"].ewm(span = slow, adjust = False).mean()
    macd = exp1-exp2
    signal = macd.ewm(span = smooth, adjust = False).mean()
    DF["MACD"] = macd
    DF["MACD_SIG"] = signal
    return (DF["MACD"],DF["MACD_SIG"])

def OBV(DF):
    df = DF.copy()
    """function to calculate On Balance Volume"""
    df['daily_ret'] = df['Close'].pct_change()
    df['direction'] = np.where(df['daily_ret']>=0,1,-1)
    df['direction'][0] = 0
    df['vol_adj'] = df['Volume'] * df['direction']
    df['obv'] = df['vol_adj'].cumsum()
    return df['obv']

def trade_signal(df,i, ticker):
    "function to generate signal"
    if df[ticker]['OBV'][i] > df[ticker]['OBV_EMA'][i] and \
        df[ticker]['MACD'][i] > df[ticker]['MACD_SIG'][i]:
            df[ticker]['SIGNAL'][i] = "STRONG BUY"
    elif df[ticker]['OBV'][i] > df[ticker]['OBV_EMA'][i] or \
        df[ticker]['MACD'][i] > df[ticker]['MACD_SIG'][i]:
            df[ticker]['SIGNAL'][i] = "BUY"
    elif df[ticker]['OBV'][i] < df[ticker]['OBV_EMA'][i] and \
        df[ticker]['MACD'][i] < df[ticker]['MACD_SIG'][i]:
            df[ticker]['SIGNAL'][i] = "STRONG SELL"
    elif df[ticker]['OBV'][i] < df[ticker]['OBV_EMA'][i] or \
        df[ticker]['MACD'][i] < df[ticker]['MACD_SIG'][i]:
            df[ticker]['SIGNAL'][i] = "SELL"
    else:
        df[ticker]['SIGNAL'][i] = 'NEUTRAL'
'''

def getBacklog():
    credentials_path = 'C:/Users/gratz/OneDrive/Desktop/SGXStockDataPipeline/key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    df = pd.read_csv('STI_Component.csv')
    tickers = df['Ticker'].tolist()
    stock = df['Company name'].tolist()
    ohlcv_daily = {}
    i = 0
    for ticker in tickers:
        prices = yf.download(ticker, period = '1s').iloc[: , :6].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()
        prices.insert(loc = 1, column = 'Ticker', value = ticker)
        prices.insert(loc = 1, column = 'Stock', value = stock[i])
        prices = prices.rename({'Adj Close': 'Adj_Close'}, axis=1)
        i += 1
        data = pd.DataFrame(prices)
        ohlcv_daily[ticker] = data
        
    # Generate Technical Indicators
    for ticker in tickers:
        ohlcv_daily[ticker]['OBV'] = OBV(ohlcv_daily[ticker])
        ohlcv_daily[ticker]['OBV_EMA'] = ohlcv_daily[ticker]['OBV'].ewm(com=20).mean()
        ohlcv_daily[ticker]['MACD'] = MACD(ohlcv_daily[ticker],26,12,9)[0]
        ohlcv_daily[ticker]['MACD_SIG'] = MACD(ohlcv_daily[ticker],26,12,9)[1]
        ohlcv_daily[ticker]['SIGNAL'] = "None"
    # Generate Signals
    for ticker in tickers:
        for i in range(0, len(ohlcv_daily[ticker])):
            trade_signal(ohlcv_daily, i, ticker)
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    for key, value in ohlcv_daily.items():
        if not (value.empty):
            table_id = "bustling-brand-344211.STIData." + key.split(".")[0]
            job = client.load_table_from_dataframe(value, table_id)
            job.result()
    

def extract():
    df = pd.read_csv('STI_Component.csv')
    tickers = df['Ticker'].tolist()
    stock = df['Company name'].tolist()
    ohlcv_daily = {}
    i = 0
    for ticker in tickers:
        prices = yf.download(ticker, period = '5d').iloc[: , :6].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()
        prices.insert(loc = 1, column = 'Ticker', value = ticker)
        prices.insert(loc = 1, column = 'Stock', value = stock[i])
        prices = prices.rename({'Adj Close': 'Adj_Close'}, axis=1)
        i += 1
        data = pd.DataFrame(prices)
        ohlcv_daily[ticker] = data
    return ohlcv_daily

def transform(ohlcv_daily):
    return ohlcv_daily

def load(ohlcv_daily):
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    table_id = "bustling-brand-344211.Market_Staging.StockPrice_Staging"
    for key, value in ohlcv_daily.items():
        if not (value.empty):
            job = client.load_table_from_dataframe(value, table_id)
            job.result()
    query = """
    INSERT INTO `bustling-brand-344211.Market.StockPrice`
    SELECT *
    FROM (SELECT *, AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS MA_5day,
    CASE
    WHEN ((Close - AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) > 0.1) or (AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - Close) > 0.1 THEN 'Neutral'
    WHEN Close > AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) THEN 'Buy'
    WHEN Close < AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) THEN 'Sell'
    else 'Neutral'
    END AS Signal,
    FROM
    `bustling-brand-344211.Market_Staging.StockPrice_Staging` as p
    ) T
    where CAST(DATE as Date) = CURRENT_DATE()
    """
    query_job = client.query(query)

#code to run
load(transform(extract()))




