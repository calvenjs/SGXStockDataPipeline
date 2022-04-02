import yfinance as yf
import pandas as pd
from google.cloud import bigquery
import os

def financials_extract(ti):

    df = ti.xcom_pull(key='STIcomponents', task_ids=['STIExtract'])[0]
    df = pd.DataFrame(eval(df))

    STI_companies = df['Ticker'].tolist()

    company_info = []

    for i in range(len(STI_companies)):
        company = yf.Ticker(STI_companies[i])

        if company.info.get('regularMarketPrice') != None:
            company_info.append(company.info)

    ti.xcom_push(key = 'company info', value = company_info)

def financials_transform(ti):
    company_info = ti.xcom_pull(key = 'company info', task_ids=['financialsExtract'])
    numOfCompany = len(company_info[0])

    # Need to decide what info to use
    dic = {'Company_Name': [company_info[0][0]['shortName']], 'Ticker': [company_info[0][0]['symbol']], 'Gross_Profits': [company_info[0][0]['grossProfits']],
           'Total_Debt': [company_info[0][0]['totalDebt']], 'Total_Cashflow': [company_info[0][0]['operatingCashflow']],
           'Total_Revenue': [company_info[0][0]['totalRevenue']], 'Net_Income': [company_info[0][0]['netIncomeToCommon']],
           'Return_On_Equity': [company_info[0][0]['returnOnEquity']], 'Book_per_Share': [company_info[0][0]['bookValue']]}
    
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
    df = df.to_json(orient='records')
    ti.xcom_push(key="processed_user_info", value=df)

def financials_load(ti):
    results = ti.xcom_pull(task_ids=["financialsTransform"], key="processed_user_info")
    df = pd.DataFrame(eval(results[0]))

    # Change the bigquery path and table name
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()
    table_id = "bustling-brand-344211.Market.Financials"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Company_Name", "STRING"),
            bigquery.SchemaField("Ticker", "STRING"),
            bigquery.SchemaField("Gross_Profits", "FLOAT"),
            bigquery.SchemaField("Total_Debt", "FLOAT"),
            bigquery.SchemaField("Total_Cashflow", "FLOAT"),
            bigquery.SchemaField("Total_Revenue", "FLOAT"),
            bigquery.SchemaField("Net_Income", "FLOAT"),
            bigquery.SchemaField("Return_On_Equity", "FLOAT"),
            bigquery.SchemaField("Book_per_Share", "FLOAT")
        ]
    )
    load_job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
    )

    load_job.result()  # Waits for the job to complete.

    print('Successfully loaded company financials')
