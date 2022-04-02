from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from financialnews import financialnews_extract, financialnews_transform, financialnews_load
from STI_components import STIextraction
from stockprice import stockprice_extract, stockprice_transform, stockprice_load

with DAG(
    'Daily_SGX_Stock_Data_Pipeline',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 3, 11),
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval= '@daily',
    start_date= datetime(2022, 3, 31),
) as dag:

    financialNewsExtract = PythonOperator(
        task_id='financialNewsExtract',
        python_callable=financialnews_extract,
        dag=dag,
    )

    financialNewsTransform = PythonOperator(
        task_id='financialNewsTransform',
        python_callable=financialnews_transform,
        dag=dag,
    )

    financialNewsLoad = PythonOperator(
        task_id='financialNewsLoad',
        python_callable=financialnews_load,
        dag=dag,  
    )

    stockpriceExtract = PythonOperator(
        task_id='stockpriceExtract',
        python_callable=stockprice_extract,
        dag=dag,  
    )

    # stockpriceTransform = PythonOperator(
    #     task_id='stockpriceTransform',
    #     python_callable=stockprice_transform,
    #     dag=dag,  
    # )

    stockpriceLoad = PythonOperator(
        task_id='stockpriceLoad',
        python_callable=stockprice_load,
        dag=dag,  
    )

    STIExtraction = PythonOperator(
        task_id='STIExtract',
        python_callable=STIextraction,
        dag=dag,  
    )

    # daily 
    financialNewsExtract >> financialNewsTransform >> financialNewsLoad
    STIExtraction >> stockpriceExtract >> stockpriceLoad