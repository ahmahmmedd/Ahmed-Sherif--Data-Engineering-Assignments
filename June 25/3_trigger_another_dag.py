from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

def parent_task():
    print("Parent DAG task completed")

with DAG(
    dag_id="parent_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="parent_task",
        python_callable=parent_task
    )

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="child_dag",
        conf={"execution_date": "{{ ds }}"}
    )

    task1 >> trigger_child