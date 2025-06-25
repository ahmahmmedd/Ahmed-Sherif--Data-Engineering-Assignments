from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

def task_one():
    print("Task one completed")

def task_two():
    print("Task two completed")

with DAG(
    dag_id="email_notification_workflow",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        'email_on_failure': True,
        'email': 'admin@example.com'
    }
) as dag:

    task1 = PythonOperator(
        task_id="task_one",
        python_callable=task_one
    )

    task2 = PythonOperator(
        task_id="task_two",
        python_callable=task_two
    )

    email_success = EmailOperator(
        task_id="send_success_email",
        to="admin@example.com",
        subject="DAG Completed Successfully",
        html_content="All tasks completed successfully"
    )

    task1 >> task2 >> email_success