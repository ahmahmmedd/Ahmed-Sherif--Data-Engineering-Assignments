# Process any number of new .csv files in a folder dynamically.

import os, random
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

folderPath = "/opt/airflow/dags/sales_data/summary"
headers = ["EmpID", "Name", "Age"]

@task
def step_1():
    dirList = os.listdir(folderPath)
    filePaths = []

    for i in dirList:
        if i.endswith(".csv"):
            filePaths.append(os.path.join(folderPath, i))

    return filePaths

@task
def step_2(filePaths):
    fileStatsSummary = {"name": [], "headervalid": [], "rowcount": []}
    for i in filePaths:
        df = pd.read_csv(i)
        validHeader = all(i in headers for i in df.columns)
        rowCount = df.shape[1]
        fileStatsSummary["name"].append(i)
        fileStatsSummary["headervalid"].append(validHeader)
        fileStatsSummary["rowcount"].append(rowCount)

    return fileStatsSummary

@task
def step_3(summary):
    fileSummary = pd.DataFrame(summary)
    print("Output file Created")

dag = DAG(
    dag_id="dynamic_dag",
    start_date=datetime(2024, 6, 22),
    catchup=False
)

with dag:
    tasks = step_3(step_2(step_1()))