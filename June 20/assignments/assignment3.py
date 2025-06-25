from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

def flaky_api():
    if random.random() < 0.5:
        raise Exception("API call failed")
    print("API call successful")

def send_alert(context):
    print("Alert: API failed after all retries")

def follow_up():
    print("Post-success task executed")

with DAG(
    dag_id="assignment_3_retry_alert",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    api_call = PythonOperator(
        task_id="flaky_api_call",
        python_callable=flaky_api,
        retries=3,
        retry_delay=timedelta(seconds=15),
        retry_exponential_backoff=True,
        on_failure_callback=send_alert
    )

    success_task = PythonOperator(
        task_id="post_success_task",
        python_callable=follow_up,
        trigger_rule="all_success"
    )

    api_call >> success_task