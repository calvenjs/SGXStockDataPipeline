import pandas as pd
import yfinance as yf
from google.cloud import bigquery
import os
from dateutil.relativedelta import relativedelta
from datetime import datetime
import json

def dividend_extract(ti):
    #Initialize Start and Enddate
    startday = datetime(datetime.today().year, datetime.today().month, 1)
    endday = startday + relativedelta(months=1)
    
    #Get list of STI stock and convert it into a Dataframe
    df = ti.xcom_pull(key='STIcomponents', task_ids=['STIExtract'])[0]
    df = pd.DataFrame(eval(df))
    
    #Populate DF with  Tikcker and Company Name
    tickers = df['Ticker'].tolist()
    stock = df['Company name'].tolist()
    
    #Rename Column name in Dataframe
    dividend_quarterly = pd.DataFrame(columns=["EX_Date", 'Stock', 'Ticker', 'Dividends_Per_Share'])   

    #Populate Dataframe with dividend details
    i = 0
    for ticker in tickers:
        dividend = yf.Ticker(ticker).dividends
        if len(dividend) > 0:
            dividend = pd.DataFrame({'EX_Date':dividend.index, 'Dividends_Per_Share':dividend.values})
            # dividend = dividend[(dividend['EX_Date'] >= "2022-01-01") & (dividend['EX_Date'] <= "2022-04-01")] # for testing
            dividend = dividend[(dividend['EX_Date'] >= startday) & (dividend['EX_Date'] <= endday)]  
            dividend.insert(loc = 1, column = 'Ticker', value = ticker)
            dividend.insert(loc = 1, column = 'Stock', value = stock[i])
            dividend_quarterly  =  dividend_quarterly.append(dividend, ignore_index=True)
        i += 1
    dividend_quarterly = dividend_quarterly.to_json(orient='records')
    ti.xcom_push(key='dividend_quarterly', value=dividend_quarterly)


def dividend_staging(ti):
    df = ti.xcom_pull(key='dividend_quarterly', task_ids = ['dividendExtract'])[0]
    df = pd.DataFrame(eval(df))
    print(df)
    if len(df) == 0:
        print('no dividend entries')
        return
    
    #Get Project ID
    openfile=open('testkey.json')
    jsondata=json.load(openfile)
    openfile.close()
    
    #Connect to Bigquery
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()

    #Load To staging
    staging_table_id = jsondata['project_id'] + ".Market_Staging.Dividend_Staging"
    job = client.load_table_from_dataframe(df, staging_table_id)
    job.result()


def dividend_load():
    #Get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()
    staging_table_id = '`' + jsondata['project_id'] + ".Market_Staging.Dividend_Staging`"
    actual_table_id = "`" + jsondata['project_id'] + ".Market.Dividend`"
    
    #Connect to Bigquery
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    table_id = "bustling-brand-344211.Market_Staging.Dividend_Staging"
    
    #Transform and Load to Acutal Table
    query = f"""
    INSERT INTO {actual_table_id} SELECT DISTINCT * FROM {staging_table_id};
    DELETE FROM {staging_table_id} where True
    """
    query_job = client.query(query)
    print('Successfully loaded dividend details')

