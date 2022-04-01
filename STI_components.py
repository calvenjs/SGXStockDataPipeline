import pandas as pd
import yfinance as yf
from google.cloud import bigquery
import os
import requests

def STIextraction(ti):
    url = 'https://sg.finance.yahoo.com/quote/%5ESTI/components/'
    r = requests.get(url, headers ={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
    payload=pd.read_html(r.text)
    df = payload[0]
    df = df[['Symbol', 'Company name']]
    df = df.rename({'Symbol': "Ticker"}, axis = 1)
    df.to_csv("STI_components.csv", index=False)
    df = df.to_json(orient='records')
    ti.xcom_push(key='STIcomponents', value=df)

#code to run
# STIextraction()
