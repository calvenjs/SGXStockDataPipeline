from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from STI_components import STIextraction
from dividend import dividend_extract, dividend_load
from financials import financials_extract,financials_transform, financials_load
from portfolio_extraction import portfolio_extract, portfolio_transform, portfolio_load


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
        task_id='financialsTransform',
        python_callable=financials_transform,
        dag=dag,  
    )

    financialsLoad = PythonOperator(
        task_id='financialsLoad',
        python_callable=financials_load,
        dag=dag,  
    )

    STIExtraction = PythonOperator(
        task_id='STIExtract',
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

    portfolioExtract = PythonOperator(
        task_id='portfolioExtract',
        python_callable=portfolio_extract,
        dag=dag,  
    )
    portfolioTransform = PythonOperator(
        task_id='portfolioTransform',
        python_callable=portfolio_transform,
        dag=dag,  
    )
    portfolioLoad = PythonOperator(
        task_id='portfolioLoad',
        python_callable=portfolio_load,
        dag=dag,  
    )

    # monthly
    STIExtraction >> financialsExtract >> financialsTransform >> financialsLoad
    STIExtraction >> dividendExtract >> dividendLoad
    portfolioExtract >> portfolioTransform >> portfolioLoad