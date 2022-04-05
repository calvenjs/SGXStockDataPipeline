from google.cloud import bigquery
import pandas as pd
import yfinance as yf
import os
import numpy as np

#Extract raw stock price for QD pipeline
def stockprice_raw_extract(ti):
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


def stockprice_raw_load(ti):
    ohlcv_daily = ti.xcom_pull(key='ohlcv', task_ids=['stockpriceExtract'])[0]
    for k,v in ohlcv_daily.items():
        ohlcv_daily[k] = pd.DataFrame(eval(v))
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    table_id = "bustling-brand-344211.Market_Raw.StockPrice_Raw"
    for key, value in ohlcv_daily.items():
        if not (value.empty):
            job = client.load_table_from_dataframe(value, table_id)
            job.result()





