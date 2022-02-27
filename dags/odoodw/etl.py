from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


from psycopg2.extras import execute_values
from odoodw.utils.dw_setup import DWSetup

# Import tasks
from odoodw.tasks.extract import extract_to_stage
from odoodw.tasks.hubs import load_hubs

import os


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 18),
    "email": ["airflowadmin@example.com"],  # TODO: This should be in .env file
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="odoo_dag",
    default_args=default_args,
    description="Odoo etl dag",
    schedule_interval=timedelta(days=1),
) as dag:

    extract = PythonOperator(
        task_id="Extract_to_staging", python_callable=extract_to_stage, dag=dag
    )

    load_dv_hubs = PythonOperator(
        task_id="Load_hubs", python_callable=load_hubs, dag=dag
    )

    extract >> load_dv_hubs
