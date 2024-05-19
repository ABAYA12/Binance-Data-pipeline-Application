"""
Module for orchestrating data workflow from Binance to S3, Snowflake, and PostgreSQL.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from binance_data import get_historical_klines
from binance_data import model_data
from s3_integration import upload_to_s3_and_grant_permissions
from snowflake_integration import persist_to_snowflake
from postgres_integration import persist_to_postgres
from main import model_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'binance_data_workflow',
    default_args=default_args,
    description='DAG to fetch data from Binance, upload to S3, and persist to Snowflake and PostgreSQL',
    schedule_interval=timedelta(hours=8),  # Run every 8 hours
)    

def fetch_and_process_data(**kwargs):
    """
    Fetches historical data from Binance and processes it.
    """
    historical_data = get_historical_klines()
    df = model_data(historical_data)
    return df


fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_and_process_data,
    provide_context=True,
    dag=dag,
)


def upload_to_s3(**kwargs):
    """
    Uploads processed data to S3.
    """
    df = kwargs['task_instance'].xcom_pull(task_ids='fetch_data')
    csv_data = df.to_csv(index=False)
    upload_to_s3_and_grant_permissions(
        csv_data, 'binance-data', 'history_data.csv')


upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

persist_to_snowflake_task = PythonOperator(
    task_id='persist_to_snowflake',
    python_callable=persist_to_snowflake,
    dag=dag,
)

persist_to_postgres_task = PythonOperator(
    task_id='persist_to_postgres',
    python_callable=persist_to_postgres,
    dag=dag,
)

fetch_data_task >> upload_to_s3_task >> [
    persist_to_snowflake_task, persist_to_postgres_task]
