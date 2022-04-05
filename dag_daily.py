from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from financialnews import financialnews_extract, financialnews_transform, financialnews_staging, financialnews_load
from STI_components import STIextraction
from stockprice_raw import stockprice_raw_extract, stockprice_raw_load
from stockprice import stockprice_extract, stockprice_staging, stockprice_load
from portfolio import portfolio_extract, portfolio_staging, portfolio_transform, portfolio_load

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

    STIExtraction = PythonOperator(
        task_id='STIExtract',
        python_callable=STIextraction,
        dag=dag,  
    )

    #####

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

    financialNewsStaging = PythonOperator(
        task_id='financialNewsStaging',
        python_callable=financialnews_staging,
        dag=dag,
    )

    financialNewsLoad = PythonOperator(
        task_id='financialNewsLoad',
        python_callable=financialnews_load,
        dag=dag,  
    )


    #####

    stockpriceExtract = PythonOperator(
        task_id='stockpriceExtract',
        python_callable=stockprice_extract,
        dag=dag,  
    )

    stockpriceStaging = PythonOperator(
        task_id='stockpriceStaging',
        python_callable=stockprice_staging,
        dag=dag,  
    )

    stockpriceLoad = PythonOperator(
        task_id='stockpriceLoad',
        python_callable=stockprice_load,
        dag=dag,  
    )


    #####

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

    portfolioStaging = PythonOperator(
        task_id='portfolioStaging',
        python_callable=portfolio_staging,
        dag=dag,  
    )

    portfolioLoad = PythonOperator(
        task_id='portfolioLoad',
        python_callable=portfolio_load,
        dag=dag,  
    )


    #####

    stockpriceRawExtract = PythonOperator(
        task_id='stockpriceRawExtract',
        python_callable=stockprice_raw_extract,
        dag=dag,  
    )

    stockpriceRawLoad = PythonOperator(
        task_id='stockpriceRawLoad',
        python_callable=stockprice_raw_load,
        dag=dag,  
    )

    STIExtraction >> stockpriceRawExtract >> stockpriceRawLoad   # DOESNT WORK


    financialNewsExtract >> financialNewsTransform >> financialNewsStaging >> financialNewsLoad  # works
    STIExtraction >> stockpriceExtract >> stockpriceStaging >> stockpriceLoad  # works
    portfolioExtract >> portfolioTransform >> portfolioStaging >> portfolioLoad  # works


    
