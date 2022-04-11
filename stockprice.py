import datetime
from google.cloud import bigquery
import pandas as pd
import yfinance as yf
import os

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
    
#Pipeline for Portfolio Manager
def stockprice_extract(ti):
    df = ti.xcom_pull(key='STIcomponents', task_ids=['STIExtract'])[0]
    df = pd.DataFrame(eval(df))
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
        ohlcv_daily[ticker] = data.to_json(orient='records')

    ti.xcom_push(key='ohlcv', value = ohlcv_daily)

def stockprice_staging(ti):
    ohlcv_daily = ti.xcom_pull(key='ohlcv', task_ids=['stockpriceExtract'])[0]
    for k,v in ohlcv_daily.items():
        df = pd.DataFrame(eval(v))
        df['Date'] = df['Date'].apply(lambda x: datetime.datetime.fromtimestamp(int(x) / 1000))
        print(df['Date'])
        ohlcv_daily[k] = df
    
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    table_id = "bustling-brand-344211.Market_Staging.StockPrice_Staging"
    client.delete_table(table_id, not_found_ok=True)
    for key, value in ohlcv_daily.items():
        if not (value.empty):
            job = client.load_table_from_dataframe(value, table_id)
            job.result()

# takes around 3min to run
def stockprice_load(): 
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    table_id = "bustling-brand-344211.Market_Staging.StockPrice_Staging"
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

    print('Successfully loaded stock prices')






