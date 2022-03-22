from concurrent.futures import process
from multiprocessing.sharedctypes import Value
from shutil import register_unpack_format
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from email.policy import default
import yfinance as yf
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json


default_args = {
    'owner': 'lrqianqian',
    'depends_on_past': False,
    'email': ['lrqian2000@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
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
    dic = {'Company Name': [company_info[0][0]['shortName']], 'Gross Profits': [company_info[0][0]['grossProfits']],
           'Ticker': [company_info[0][0]['symbol']], 'Total Debt': [company_info[0][0]['totalDebt']], 'Total Cashflow': [company_info[0][0]['operatingCashflow']],
           'Total Revenue': [company_info[0][0]['totalRevenue']], 'Net Income': [company_info[0][0]['netIncomeToCommon']],
           'Return On Equity': [company_info[0][0]['returnOnEquity']], 'Book per Share': [company_info[0][0]['bookValue']]}
    df = pd.DataFrame(dic)
    for i in range(1, numOfCompany):
        company = company_info[0][i]
        newDic = {'Company Name': [company['shortName']], 'Gross Profits': [company['grossProfits']],
                  'Ticker': [company['symbol']], 'Total Debt': [company['totalDebt']], 'Total Cashflow': [company['operatingCashflow']],
                  'Total Revenue': [company['totalRevenue']], 'Net Income': [company['netIncomeToCommon']],
                  'Return On Equity': [company['returnOnEquity']], 'Book per Share': [company['bookValue']]}
        newRow = pd.DataFrame(newDic)
        df = pd.concat([df, newRow], ignore_index=True)
    print("Process info done")
    print(df)
    df = df.to_json(orient='records')
    ti.xcom_push(key="processed_user_info", value=df)


def store_data(ti):
    results = ti.xcom_pull(
        task_ids=["process_info"], key="processed_user_info")
    result_json = json.loads(results[0])

    for result in result_json:
        info_needed = ['Company Name', 'Ticker', 'Gross Profits', 'Total Debt', 'Total Cashflow', 'Total Revenue',
                       'Net Income', 'Return On Equity', 'Book per Share']
        for info in info_needed:
            if result[info] == None:
                result[info] = 0
        print(result)
        query = f'''
        INSERT INTO FINANCIALS  (companyName, ticker, grossProfit, totalDebt, totalCashflow,
                                totalRevenue, netIncome, returnOnEquity, bookValue)
        VALUES ('{result["Company Name"]}', '{result["Ticker"]}', '{result["Gross Profits"]}',
                '{result["Total Debt"]}', '{result["Total Cashflow"]}', '{result["Total Revenue"]}', '{result["Net Income"]}',
                '{result["Return On Equity"]}', '{result["Book per Share"]}')
        ON CONFLICT (ticker) DO UPDATE
            SET grossProfit='{result["Gross Profits"]}', 
                totalDebt='{result["Total Debt"]}',
                totalCashflow='{result["Total Cashflow"]}', 
                totalRevenue='{result["Total Revenue"]}', 
                netIncome='{result["Net Income"]}',
                returnOnEquity='{result["Return On Equity"]}', 
                bookValue='{result["Book per Share"]}';
        '''
        print(query)
        execute_query_with_hook(query)
    print("Storing user task complete")


def execute_query_with_hook(query):
    hook = PostgresHook(postgres_conn_id='postgres_local')
    hook.run(query)


with DAG(
    dag_id="download_company_financial_stats",
    schedule_interval="@quarterly",
    default_args=default_args,
    catchup=False,
    start_date=days_ago(2),
    tags=['lrqianqian'],
) as dag:

    dag.doc_md = """
    # DAG code starts here
    """

    create_table = PostgresOperator(
        # Depend on the values wanted
        task_id="create_table",
        postgres_conn_id="postgres_local",
        sql='''
        CREATE TABLE IF NOT EXISTS FINANCIALS(
            companyName TEXT NOT NULL,
            ticker TEXT NOT NULL PRIMARY KEY,
            grossProfit FLOAT NOT NULL,
            totalDebt FLOAT NOT NULL,
            totalCashflow FLOAT NOT NULL,
            totalRevenue FLOAT NOT NULL,
            netIncome FLOAT NOT NULL,
            returnOnEquity FLOAT NOT NULL,
            bookValue FLOAT NOT NULL
        );
        '''
    )

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

    create_table >> download_info >> process_info >> store_data
