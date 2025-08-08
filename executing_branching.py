from __future__ import annotations

import airflow
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from random import choice

import pandas as pd

default_args = {
    'owner': 'admin',
}  

def has_driving_license():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_license'):
        return 'eligible_to_drive'
    else:
        return 'not_eligible_to_drive'
    
def eligible_to_drive():
    print("You are eligible to drive.")

def not_eligible_to_drive():
    print("You are not eligible to drive.")

with DAG(
    dag_id='executing_branching',
    description = 'Running branching pipelines',
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule='@once',
    tags = ['study','LinkedIn', 'LrnAirflow', 'Python_Pipeline','branching']
)as dag:

    taskA = PythonOperator(
        task_id='has_driving_license',
        python_callable=has_driving_license,
    )

    taskB = BranchPythonOperator(
        task_id='branch',
        python_callable=branch,
    )

    taskC = PythonOperator(
        task_id='eligible_to_drive',
        python_callable=eligible_to_drive,
    )

    taskD = PythonOperator(
        task_id='not_eligible_to_drive',
        python_callable=not_eligible_to_drive,
    )
    
taskA >> taskB >> [taskC, taskD]