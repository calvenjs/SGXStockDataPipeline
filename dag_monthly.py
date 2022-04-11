from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from STI_components import STIextraction
from dividend import dividend_extract, dividend_staging, dividend_load
from financials import financials_extract,financials_transform, financials_staging, financials_load

import smtplib
from keys import EMAIL_PASSWORD

def send_email():
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login('is3107group32@gmail.com',EMAIL_PASSWORD)
    server.sendmail('is3107group32@gmail.com','is3107group32@gmail.com','The monthly pipeline has been executed successfully.')

with DAG(
    'Monthly_SGX_Stock_Data_Pipeline',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 3, 31),
        'email': ['is3107group32@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval= '@monthly',
    start_date= datetime(2022, 3, 31),
) as dag:

    STIExtraction = PythonOperator(
        task_id='STIExtract',
        python_callable=STIextraction,
        dag=dag,  
    )

    sendEmail = PythonOperator(
        task_id='sendEmail',
        python_callable=send_email,
        dag=dag,  
    )

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

    financialsStaging = PythonOperator(
        task_id='financialsStaging',
        python_callable=financials_staging,
        dag=dag,  
    )

    financialsLoad = PythonOperator(
        task_id='financialsLoad',
        python_callable=financials_load,
        dag=dag,  
    )

    #####

    dividendExtract = PythonOperator(
        task_id='dividendExtract',
        python_callable=dividend_extract,
        dag=dag,  
    )

    dividendStaging = PythonOperator(
        task_id='dividendStaging',
        python_callable=dividend_staging,
        dag=dag,  
    )

    dividendLoad = PythonOperator(
        task_id='dividendLoad',
        python_callable=dividend_load,
        dag=dag,  
    )

    # monthly
    STIExtraction >> financialsExtract >> financialsTransform >> financialsStaging >> financialsLoad >> sendEmail
    STIExtraction >> dividendExtract >> dividendStaging >> dividendLoad >> sendEmail