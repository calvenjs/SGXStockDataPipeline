import pandas as pd
import yfinance as yf
from google.cloud import bigquery
import os
import requests
from dateutil.relativedelta import relativedelta
from datetime import datetime

def extract():
    startday = datetime(datetime.today().year, datetime.today().month, 1)
    endday = startday +  relativedelta(months=3)
    df = pd.read_csv('STI_Component.csv')
    tickers = df['Ticker'].tolist()
    stock = df['Company name'].tolist()
    dividend_quarterly = pd.DataFrame(columns=["EX_Date", 'Stock', 'Ticker', 'Dividends_Per_Share'])   
    i = 0
    for ticker in tickers:
        dividend = yf.Ticker(ticker).dividends
        if len(dividend) > 0:
            dividend = pd.DataFrame({'EX_Date':dividend.index, 'Dividends_Per_Share':dividend.values})
            #dividend = dividend[(dividend['EX_Date'] >= startday) & (dividend['EX_Date'] <= endday)]
            dividend = dividend[(dividend['EX_Date'] >= "2022-01-01") & (dividend['EX_Date'] <= "2022-04-01")]
            dividend.insert(loc = 1, column = 'Ticker', value = ticker)
            dividend.insert(loc = 1, column = 'Stock', value = stock[i])
            dividend_quarterly  =  dividend_quarterly.append(dividend, ignore_index=True)
        i += 1
    return dividend_quarterly

#def transform():
def load(df):
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    table_id = "bustling-brand-344211.Market_Staging.Dividend_Staging"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    query = """
    INSERT INTO `bustling-brand-344211.Market.Dividend`
    SELECT * FROM  `bustling-brand-344211.Market_Staging.Dividend_Staging` 
    """
    query_job = client.query(query)
  
#code to run
load(extract())