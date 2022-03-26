from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
import os
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator


default_args = {
    'owner': 'lrqianqian',
    'depends_on_past': False,
    'email': ['lrqian2000@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def download_info():
    # This can be put into variables
    STI_companies = ["C52.SI", "O39.SI", "U96.SI", "N2IU.SI", "C07.SI", "T39.SI", "F34.SI", "C38U.SI", "BN4.SI",
                     "S63.SI", "S58.SI", "U11.SI", "M44U.SI", "C6L.SI", "G13.SI", "A17U.SI", "BS6.SI", "V03.SI",
                     "U14.SI", "Y92.SI", "Z74.SI", "S68.SI",  "D05.SI", "C09.SI", "H78.SI", "J36.SI", "D01.SI"]

    # STI_companies = ["C52.SI", "O39.SI", "U96.SI"]

    company_info = [0] * len(STI_companies)

    for i in range(len(STI_companies)):
        company = yf.Ticker(STI_companies[i])
        # print(company.info)

        company_info[i] = company.info
        print("download info done")
    return company_info


def process_info(ti):
    company_info = ti.xcom_pull(task_ids=['download_info'])
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
    df = df.to_json(orient='records')
    ti.xcom_push(key="processed_user_info", value=df)


def store_data(ti):
    results = ti.xcom_pull(
        task_ids=["process_info"], key="processed_user_info")
    print(results[0])
    df = pd.DataFrame(eval(results[0]))
    print(df)

    # Change the bigquery path and table name
    credentials_path = '/home/airflow/airflow/dags/support/keys/spatial-cargo-345307-759184d5e1ac.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()
    table_id = "spatial-cargo-345307.company_info.financials_bigquery"

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

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


with DAG(
    dag_id="Financials_BigQuery",
    schedule_interval="@quarterly",
    default_args=default_args,
    catchup=False,
    start_date=days_ago(2),
    tags=['lrqianqian'],
) as dag:

    dag.doc_md = """
    # DAG code starts here
    """

    download_info = PythonOperator(
        task_id="download_info",
        python_callable=download_info
    )

    process_info = PythonOperator(
        task_id="process_info",
        python_callable=process_info
    )

    store_data = PythonOperator(
        task_id="store_data",
        python_callable=store_data
    )

    download_info >> process_info >> store_data
