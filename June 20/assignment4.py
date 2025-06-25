from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os

FILE_PATH = "/opt/airflow/dags/data/inventory.csv"
SIZE_THRESHOLD = 500_000  # 500KB

def check_size():
    size = os.path.getsize(FILE_PATH)
    return "light_summary" if size < SIZE_THRESHOLD else "detailed_processing"

def light_summary():
    print("Light summary completed")

def detailed_processing():
    print("Detailed processing completed")

with DAG(
    dag_id="assignment_4_branching",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    check_size_task = BranchPythonOperator(
        task_id="check_file_size",
        python_callable=check_size
    )

    light_task = PythonOperator(
        task_id="light_summary",
        python_callable=light_summary
    )

    detailed_task = PythonOperator(
        task_id="detailed_processing",
        python_callable=detailed_processing
    )

    cleanup_task = EmptyOperator(
        task_id="cleanup",
        trigger_rule="none_failed_min_one_success"
    )

    check_size_task >> [light_task, detailed_task] >> cleanup_task