from datetime import timedelta

import yfinance as yf
import pandas as pd
from google.cloud import bigquery
import os


def financials_extract(ti):

    df = ti.xcom_pull(key='STIcomponents', task_ids=['STIExtraction'])[0]
    df = pd.DataFrame(eval(df))

    # CHANGE THIS LATER ON
    STI_companies = df['Ticker'].tolist()[:5]
    print(STI_companies)
    
    # STI_companies = ["C52.SI", "O39.SI", "U96.SI"]

    company_info = [0] * len(STI_companies)

    for i in range(len(STI_companies)):
        company = yf.Ticker(STI_companies[i])
        print(company.info[0][0]['shortName'])

        company_info[i] = company.info
        print("download info done")

    ti.xcom_push(key = 'company info', value = company_info)
    
    # return company_info


def financials_transform(ti):
    company_info = ti.xcom_pull(key = 'company info', task_ids=['financialsExtract'])
    # print(company_info)
    # Extract wanted values
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
    print("Process info done")
    print(df)
    df = df.fillna('-')
    df = df.to_json(orient='records')
    ti.xcom_push(key="processed_user_info", value=df)


def financials_load(ti):
    results = ti.xcom_pull(
        task_ids=["financialsDownload"], key="processed_user_info")
    print(results[0])
    df = pd.DataFrame(eval(results[0]))
    print(df)

    # # Change the bigquery path and table name
    # credentials_path = 'C:/Users/hewtu/Documents/GitHub/SGXStockDataPipeline/key.json'
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    # client = bigquery.Client()
    # table_id = "spatial-cargo-345307.company_info.financials_bigquery"

    # job_config = bigquery.LoadJobConfig(
    #     schema=[
    #         bigquery.SchemaField("Company_Name", "STRING"),
    #         bigquery.SchemaField("Ticker", "STRING"),
    #         bigquery.SchemaField("Gross_Profits", "FLOAT"),
    #         bigquery.SchemaField("Total_Debt", "FLOAT"),
    #         bigquery.SchemaField("Total_Cashflow", "FLOAT"),
    #         bigquery.SchemaField("Total_Revenue", "FLOAT"),
    #         bigquery.SchemaField("Net_Income", "FLOAT"),
    #         bigquery.SchemaField("Return_On_Equity", "FLOAT"),
    #         bigquery.SchemaField("Book_per_Share", "FLOAT")
    #     ]
    # )
    # load_job = client.load_table_from_dataframe(
    #     df,
    #     table_id,
    #     job_config=job_config,
    # )

    # load_job.result()  # Waits for the job to complete.

    # destination_table = client.get_table(table_id)
    # print("Loaded {} rows.".format(destination_table.num_rows))