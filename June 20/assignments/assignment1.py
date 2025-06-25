from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd
import logging

csv_path = 'data/customers.csv'
row_count = {"count": 0}

def check_file():
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File not found: {csv_path}")
    logging.info("CSV file exists.")

def count_rows():
    df = pd.read_csv(csv_path)
    row_count["count"] = len(df)
    logging.info(f"Row count: {row_count['count']}")
    if row_count["count"] > 100:
        return "high_count"

def log_count():
    logging.info(f"Final logged row count: {row_count['count']}")

with DAG(
    dag_id="csv_to_summary_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["assignment", "csv"],
) as dag:

    check_file_exists = PythonOperator(
        task_id="check_file_exists",
        python_callable=check_file,
    )

    count_csv_rows = PythonOperator(
        task_id="count_csv_rows",
        python_callable=count_rows,
    )

    log_row_count = PythonOperator(
        task_id="log_row_count",
        python_callable=log_count,
    )

    notify_high_count = BashOperator(
        task_id="notify_high_count",
        bash_command='echo "Row count exceeds 100"',
    )

    check_file_exists >> count_csv_rows >> [log_row_count, notify_high_count]