import datetime
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
import os
import json


def financials_extract(ti):
    '''
    Obtain the company info on yfinance based on the STI tickers
    
    Input: Tickers of STI companies (pulled from xcom)
    Output: Array of company info for all STI components
    '''

    # Get tickers for STIcomponets using xcom
    df = ti.xcom_pull(key='STIcomponents', task_ids=['STIExtract'])[0]
    df = pd.DataFrame(eval(df))

    STI_companies = df['Ticker'].tolist()
    # STI_companies = STI_companies[:3] # testing


    company_info = []

    for i in range(len(STI_companies)):
        company = yf.Ticker(STI_companies[i])

        if company.info.get('regularMarketPrice') != None:
            company_info.append(company.info)

    ti.xcom_push(key = 'company info', value = company_info)

    
def financials_transform(ti):
    '''
    Extract key information from the array of company info, such as Company_Name and Net_Income
    Store the result as a dataframe, and then transform into json format
    
    Input: Array of company info for all STI components (pulled from xcom)
    Output: Json file containing the key company information. 
    '''
    company_info = ti.xcom_pull(key = 'company info', task_ids=['financialsExtract'])
    numOfCompany = len(company_info[0])

    # Key information to be used
    dic = {'Company_Name': [company_info[0][0]['shortName']], 'Ticker': [company_info[0][0]['symbol']], 'Gross_Profits': [company_info[0][0]['grossProfits']],
           'Total_Debt': [company_info[0][0]['totalDebt']], 'Total_Cashflow': [company_info[0][0]['operatingCashflow']],
           'Total_Revenue': [company_info[0][0]['totalRevenue']], 'Net_Income': [company_info[0][0]['netIncomeToCommon']],
           'Return_On_Equity': [company_info[0][0]['returnOnEquity']], 'Book_per_Share': [company_info[0][0]['bookValue']]}
    
    # Convert into dataframe
    df = pd.DataFrame(dic)
    
    for i in range(1, numOfCompany):
        company = company_info[0][i]
        newDic = {'Company_Name': [company['shortName']], 'Ticker': [company['symbol']], 'Gross_Profits': [company['grossProfits']],
                  'Total_Debt': [company['totalDebt']], 'Total_Cashflow': [company['operatingCashflow']],
                  'Total_Revenue': [company['totalRevenue']], 'Net_Income': [company['netIncomeToCommon']],
                  'Return_On_Equity': [company['returnOnEquity']], 'Book_per_Share': [company['bookValue']]}

        newRow = pd.DataFrame(newDic)
        df = pd.concat([df, newRow], ignore_index=True)

    df = df.fillna(0)
    df['Date'] = df['Company_Name'].apply(lambda x: datetime.datetime.now(datetime.timezone.utc))
    df = df.to_json(orient='records')
    ti.xcom_push(key="processed_user_info", value=df)


def financials_staging(ti):
    '''
    Create financials table in the staging area if not exist
    Push the info for each company in the json into the table
    
    Input: Json file containing the key company information (pulled from xcom)
    Output: Staging table that organize key information of STI companies
    '''
    results = ti.xcom_pull(task_ids=["financialsTransform"], key="processed_user_info")
    df = pd.DataFrame(eval(results[0]))
    df['Date'] = df['Date'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)/1000))

    #Get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()
    
    #Connect to Bigquery
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()

    #Load To staging table
    staging_table_id = jsondata['project_id'] + ".Market_Staging.Financials_Staging"
    job = client.load_table_from_dataframe(df, staging_table_id)
    job.result()

    print('Successfully loaded company financials')

def financials_load():
    '''
    Load information in the staging table into the storage table
    
    Input: None
    Output: Storage table that organize key information of STI companies
    '''
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()

    #get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()
    staging_table_id = '`' + jsondata['project_id'] + ".Market_Staging.Financials_Staging`"
    actual_table_id = "`" + jsondata['project_id'] + ".Market.Financials`"

    #Load Data from Staging table to Acutal Table
    query = f"""
    INSERT INTO  {actual_table_id}
    SELECT DISTINCT * FROM  {staging_table_id};
    DELETE {staging_table_id} where True
    """
    query_job = client.query(query)
    print('Successfully loaded Financials details')
