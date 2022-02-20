from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import os


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 18),
    "email": ["airflowadmin@example.com"],  # This should be in .env file
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "odoo_dag",
    default_args=default_args,
    description="Odoo etl dag",
    schedule_interval=timedelta(days=1),
)


def hello_world():

    print("Hello world!")

    # Get source config
    print(os.getenv("AIRFLOW_CONN_ODOO_SOURCE_TEST", "Value not found"))

    ## Create file where is select statements from the source

    ## bulk copy data from source to stage & create stage table if not exists

    ## Parametrize stage load?

    ## Parametrize dw?


run_etl = PythonOperator(task_id="odoodw_etl", python_callable=hello_world, dag=dag)

run_etl
