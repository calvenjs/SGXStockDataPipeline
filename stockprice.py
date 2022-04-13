import datetime
from google.cloud import bigquery
import pandas as pd
import yfinance as yf
import os
import json

#Pipeline for Portfolio Manager
def stockprice_extract(ti):
    df = ti.xcom_pull(key='STIcomponents', task_ids=['STIExtract'])[0]
    df = pd.DataFrame(eval(df))
    #Rename DF
    tickers = df['Ticker'].tolist()
    stock = df['Company name'].tolist()
    ohlcv_daily = pd.DataFrame()
    i = 0
    #Retrieve Stock Data from Yahoo Finance
    for ticker in tickers:
        prices = yf.download(ticker, period = '5d').iloc[: , :6].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()

        prices.insert(loc = 1, column = 'Ticker', value = ticker)
        prices.insert(loc = 1, column = 'Stock', value = stock[i])
        prices = prices.rename({'Adj Close': 'Adj_Close'}, axis=1)
        ohlcv_daily = pd.concat([ohlcv_daily, prices],ignore_index = True)
        i+= 1

    ohlcv_daily = ohlcv_daily.to_json(orient='records')
    ti.xcom_push(key='ohlcv', ohlcv_daily)
    
def stockprice_staging(ti):
    ohlcv_daily = ti.xcom_pull(key='ohlcv', task_ids=['stockpriceExtract'])[0]
    df = pd.DataFrame(eval(v))
    df['Date'] = df['Date'].apply(lambda x: datetime.datetime.fromtimestamp(int(x) / 1000))
    print(df['Date'])
    #Get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()

    #Connect to Bigquery
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()

    #Load To staging
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".Market_Staging.StockPrice_Staging"
    job = client.load_table_from_dataframe(ohlcv_daily, staging_table_id)
    job.result()
    
# takes around 3min to run
def stockprice_load():
    #Get Project ID
    openfile=open('testkey.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    staging_table_id = '`' + project_id + ".Market_Staging.StockPrice_Staging`"
    actual_table_id = "`" + project_id + ".Market.StockPrice`"
    
    #Setup
    credentials_path = 'testkey.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()

    #Load Data from Staging to Acutal
    query = f"""
    INSERT INTO {actual_table_id}
    SELECT *
    FROM (SELECT *, AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS MA_5day,
    CASE
    WHEN ((Close - AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) > 0.1) or (AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - Close) > 0.1 THEN 'Neutral'
    WHEN Close > AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) THEN 'Buy'
    WHEN Close < AVG(CAST(Close AS float64)) OVER (PARTITION BY p.Ticker ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) THEN 'Sell'
    else 'Neutral'
    END AS Signal,
    FROM
    {staging_table_id} as p
    ) T
    where CAST(DATE as Date) = CURRENT_DATE();

    DELETE FROM {staging_table_id} where True
    """
    query_job = client.query(query)
    print('Successfully loaded stock prices')






