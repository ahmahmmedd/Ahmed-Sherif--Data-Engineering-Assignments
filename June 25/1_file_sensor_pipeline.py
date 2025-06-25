from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import shutil
import os

def process_file():
    print("Processing report.csv")
    shutil.move("data/incoming/report.csv", "data/archive/report.csv")

with DAG(
    dag_id="file_sensor_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_report",
        filepath="data/incoming/report.csv",
        poke_interval=30,
        timeout=600,
        mode="poke"
    )

    process = PythonOperator(
        task_id="process_report",
        python_callable=process_file
    )

    success = EmptyOperator(task_id="success")

    wait_for_file >> process >> success