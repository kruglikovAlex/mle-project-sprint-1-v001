# dags/cost_estimate.py

import pandas as pd
import pendulum

from airflow import DAG
from airflow.models import XCom
from airflow.operators.python import PythonOperator

from steps.message import send_telegram_failure_message, send_telegram_success_message
from steps.cretl import create_table, extract, transform, load

with DAG(
    dag_id='cost_estimate',
    schedule='@once',
    start_date=pendulum.datetime(2025, 6, 14, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
) as dag:
    
    create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
        provide_context=True
    )

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
        provide_context=True
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
        provide_context=True
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load,
        provide_context=True
    )

    create_table >> extract_data >> transform_data >> load_data