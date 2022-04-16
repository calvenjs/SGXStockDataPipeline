from google.cloud import bigquery
import pandas as pd
import yfinance as yf
import os
import numpy as np
import datetime
import json 

# Pipeline for Quantitative Developers
def stockprice_raw_extract(ti):
    '''
    Extract stock information in STI Components for past five days
    Gets the OHLCV and Adjusted Close using Yahoo Finance API in Pandas Dataframe and push as JSON
    Input: List of Stock Tickers
    Output: None
    '''
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
    ti.xcom_push(key='ohlcv', value=ohlcv_daily)


def stockprice_raw_load(ti):
    '''
    Load Stock Data to Main Table
    Input: List of Stock Tickers
    Output: None
    '''
    ohlcv_daily = ti.xcom_pull(key='ohlcv', task_ids=['stockpriceRawExtract'])[0]
    df = pd.DataFrame(eval(ohlcv_daily))
    df['Date'] = df['Date'].apply(lambda x: datetime.datetime.fromtimestamp(int(x) / 1000))
    print(df['Date'])
    #Get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    table_id = project_id + ".Market_Raw.stockprice_raw"

    #Connect to Bigquery
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()

    #Load data to Bigquery
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
