from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from financialnews_extraction import extract
from financialnews_extraction import transform
from financialnews_extraction import load
from portfolio_extraction import extract
from portfolio_extraction import transform
from portfolio_extraction import load
from STI_component_extraction import load
from dividend_extraction import extract
from dividend_extraction import load
from stockprice_extraction import extract
from stockprice_extraction import transform
from stockprice_extraction import load

with DAG(
    'financialnews_extraction',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 3, 11),
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval=timedelta(days=1),
    start_date= datetime(2021, 3, 21),
) as dag:

    financialNewsExtraction = PythonOperator(
        task_id='financialNewsExtraction',
        python_callable=extract,
        dag=dag,
    )

    financialNewsTransformation = PythonOperator(
        task_id='financialNewsTransformation',
        python_callable=transform,
        dag=dag,
    )

    financialNewsLoading = PythonOperator(
        task_id='financialNewsLoading',
        python_callable=load,
        dag=dag,  
    )

    financialNewsExtraction >> financialNewsTransformation >> financialNewsLoading
