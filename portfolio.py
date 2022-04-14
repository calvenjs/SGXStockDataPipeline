import datetime
from google.cloud import bigquery
import os
from os.path import exists
import pandas as pd
import json

def portfolio_extract(ti):
    #If portfolio file exist, start ETL Process
    if exists('Portfolio.csv'):
        df = pd.read_csv("Portfolio.csv")
        df = df.to_json(orient='records')

    ti.xcom_push(key='df',value=df)

def portfolio_transform(ti):
    df = ti.xcom_pull(key='df', task_ids = ['portfolioExtract'])[0]
    df = pd.DataFrame(eval(df))
    df['Date'] = df['Date'].apply(lambda x: ''.join(x.split('\\')))

    #Add Cost Column into DataFrame
    df['Cost'] = df['Avg_Price'] * df['Share']

    df = df.to_json(orient='records')
    ti.xcom_push(key='df',value=df)

def portfolio_staging(ti):
    df = ti.xcom_pull(key='df', task_ids = ['portfolioTransform'])[0]
    df = pd.DataFrame(eval(df))
    df['Date'] = df['Date'].apply(lambda x: ''.join(x.split('\\')))
    print(df['Date'])
    
    #Get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".Accounting_Staging.Position_Staging"

    #Setup BigQuery Connection
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()

    #Load Data to Staging
    job = client.load_table_from_dataframe(df, staging_table_id)
    job.result()

def portfolio_load():
    #Get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    staging_table_id = '`' + project_id + ".Accounting_Staging.Position_Staging`"
    actual_table_id = "`" + project_id + ".Accounting.Position`"
    stock_tabie_id = "`" + project_id + ".Market.StockPrice`"

    #Setup BigQuery Connection
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()

    #Load Data from Staging to Actual Data
    query = f"""
    Delete {actual_table_id} where True;
    INSERT INTO {actual_table_id}
    SELECT s.Date, p.Ticker, s.Stock, p.Avg_Price, P.Share, P.Cost, p.Share * s.Close AS Value, (p.Share * s.Close) - p.Cost as Return, p.Type
    FROM
    {staging_table_id} as p INNER JOIN
    {stock_tabie_id} as s on p.Ticker = s.Ticker
    where CAST(s.DATE as Date) = CURRENT_DATE();
     Delete {staging_table_id} where True;
    """
    query_job = client.query(query)
    print('Successfully loaded portfolio holdings')

