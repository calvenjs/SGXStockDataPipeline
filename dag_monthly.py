from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# from portfolio_extraction import extract
# from portfolio_extraction import transform
# from portfolio_extraction import load

from STI_components import STIextraction

from dividend import dividend_extract
from dividend import dividend_load

from financials import financials_extract
from financials import financials_transform
from financials import financials_load


with DAG(
    'Monthly_SGX_Stock_Data_Pipeline',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 3, 31),
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval= '@monthly',
    start_date= datetime(2022, 3, 31),
) as dag:

    financialsExtract = PythonOperator(
        task_id='financialsExtract',
        python_callable=financials_extract,
        dag=dag,  
    )

    financialsTransform = PythonOperator(
        task_id='financialsDownload',
        python_callable=financials_transform,
        dag=dag,  
    )

    financialsLoad = PythonOperator(
        task_id='financialsLoad',
        python_callable=financials_load,
        dag=dag,  
    )

    STIExtraction = PythonOperator(
        task_id='STIExtraction',
        python_callable=STIextraction,
        dag=dag,  
    )

    dividendExtract = PythonOperator(
        task_id='dividendExtract',
        python_callable=dividend_extract,
        dag=dag,  
    )

    dividendLoad = PythonOperator(
        task_id='dividendLoad',
        python_callable=dividend_load,
        dag=dag,  
    )

    # monthly
    STIExtraction >> financialsExtract >> financialsTransform >> financialsLoad
    STIExtraction >> dividendExtract >> dividendLoad