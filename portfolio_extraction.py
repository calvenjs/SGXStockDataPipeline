from google.cloud import bigquery
import os
from os.path import exists
import pandas as pd


credentials_path = 'C:/Users/gratz/OneDrive/Desktop/SGXStockDataPipeline/key.json'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path

def extract():
    if exists('portfolio.csv'):
        df = pd.read_csv("portfolio.csv")
    return df


def transform(df):
    df['Cost'] = df['Avg_Price'] * df['Share']
    return df

def load(df):
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()
    table_id = "bustling-brand-344211.Accounting_Staging.Position_Staging"
    client.delete_table(table_id, not_found_ok=True)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    query = """
    INSERT INTO `bustling-brand-344211.Accounting.Position`
    SELECT s.date, p.Ticker, s.Stock, p.Avg_Price,P.share, P.cost, p.Share * s.Close AS Value, (p.Share * s.Close) - p.Cost as Return
        FROM
        `bustling-brand-344211.Accounting_Staging.Position_Staging` as p INNER JOIN
        `bustling-brand-344211.Market.StockPrice` as s on p.Ticker = s.Ticker
        where CAST(s.DATE as Date) = CURRENT_DATE()
    """
    query_job = client.query(query)
#Code to run
load(transform(extract()))
