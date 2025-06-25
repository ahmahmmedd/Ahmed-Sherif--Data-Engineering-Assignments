from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

def process_response(response):
    data = json.loads(response)
    print(f"Received data: {data}")
    with open("data/api_response.json", "w") as f:
        json.dump(data, f)

with DAG(
    dag_id="api_interaction",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    call_api = SimpleHttpOperator(
        task_id="call_weather_api",
        endpoint="/data/2.5/weather",
        method="GET",
        http_conn_id="openweathermap",
        data={"q": "London,UK", "appid": "your_api_key"},
        response_check=lambda response: response.status_code == 200
    )

    process_data = PythonOperator(
        task_id="process_api_response",
        python_callable=process_response,
        op_args=["{{ ti.xcom_pull(task_ids='call_weather_api') }}"]
    )

    call_api >> process_data