from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, time

def check_time():
    now = datetime.now().time()
    if now < time(12, 0):
        return "morning_task"
    return "afternoon_task"

def morning_work():
    print("Running morning routine")

def afternoon_work():
    print("Running afternoon routine")

with DAG(
    dag_id="time_based_workflow",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    check_time = BranchPythonOperator(
        task_id="check_current_time",
        python_callable=check_time
    )

    morning = PythonOperator(
        task_id="morning_task",
        python_callable=morning_work
    )

    afternoon = PythonOperator(
        task_id="afternoon_task",
        python_callable=afternoon_work
    )

    cleanup = EmptyOperator(task_id="cleanup")

    check_time >> [morning, afternoon] >> cleanup