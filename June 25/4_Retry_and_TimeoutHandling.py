from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def long_running_task():
    print("Starting long running task")
    time.sleep(120)  # Simulate work
    print("Task completed successfully")

with DAG(
    dag_id="retry_timeout_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    process = PythonOperator(
        task_id="long_running_process",
        python_callable=long_running_task,
        execution_timeout=timedelta(minutes=1),
        retries=3,
        retry_delay=timedelta(minutes=2)
    )