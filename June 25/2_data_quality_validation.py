from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
import pandas as pd

def validate_data():
    df = pd.read_csv("data/orders.csv")
    required_columns = ["order_id", "customer_id", "amount"]
    null_columns = ["order_id", "customer_id"]
    
    if not all(col in df.columns for col in required_columns):
        raise AirflowFailException("Missing required columns")
    
    if df[null_columns].isnull().values.any():
        raise AirflowFailException("Null values in mandatory fields")

def summarize_data():
    print("Data validation passed - proceeding with summarization")

with DAG(
    dag_id="data_quality_check",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    summarize = PythonOperator(
        task_id="summarize_data",
        python_callable=summarize_data
    )
